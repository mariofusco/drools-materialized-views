package org.drools.materialized.untyped;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.drools.drl.ast.descr.AndDescr;
import org.drools.drl.ast.descr.BaseDescr;
import org.drools.drl.ast.descr.ExprConstraintDescr;
import org.drools.drl.ast.descr.PackageDescr;
import org.drools.drl.ast.descr.PatternDescr;
import org.drools.drl.ast.descr.QueryDescr;
import org.drools.modelcompiler.ExecutableModelProject;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;

import static org.drools.materialized.SqlUtil.parseQuery;

public class UntypedQueryGenerator {

    public static KieSession query2KieSessionViaDescrs(String query) {
        KieSession ksession = new KieHelper()
                .addContent( queryToPkgDescr( (SqlSelect) parseQuery(query) ) )
                .build(ExecutableModelProject.class)
                .newKieSession();
        return ksession;
    }

    private static PackageDescr queryToPkgDescr(SqlSelect query) {
        PackageDescr packageDescr = new PackageDescr(Fact.class.getPackageName());
        packageDescr.addRule( selectToQueryDescr( query ) );
        return packageDescr;
    }

    private static QueryDescr selectToQueryDescr(SqlSelect select) {
        QueryDescr queryDescr = new QueryDescr("Q0");
        queryDescr.setNamespace(Fact.class.getPackageName());
        queryDescr.setLhs( joinToLhs((SqlJoin) select.getFrom()) );
        return queryDescr;
    }

    private static AndDescr joinToLhs(SqlJoin join) {
        AndDescr lhs = new AndDescr();

        PatternDescr leftPattern = toPatternDescr( (SqlBasicCall) join.getLeft() );
        lhs.addDescr(leftPattern);

        PatternDescr rightPattern = toPatternDescr( (SqlBasicCall) join.getRight() );
        rightPattern.addConstraint(toJoinConstraintDescr(join));
        lhs.addDescr(rightPattern);
        return lhs;
    }

    private static BaseDescr toJoinConstraintDescr(SqlJoin join) {
        SqlBasicCall condition = (SqlBasicCall) join.getCondition();
        String leftValue = transformOperand(condition.getOperands()[0]);
        String rightValue = transformOperand(condition.getOperands()[1]);
        return new ExprConstraintDescr(leftValue + " == " + rightValue);
    }

    private static String transformOperand(SqlNode operand) {
        String op = operand.toString().toLowerCase();
        int dotPosition = op.indexOf('.');
        String field = op.substring(dotPosition+1).trim();
        return op.substring(0, dotPosition+1) + "get(\"" + field + "\")";
    }

    private static PatternDescr toPatternDescr(SqlBasicCall sqlCall) {
        String table = sqlCall.getOperands()[0].toString().toLowerCase();
        String id = sqlCall.getOperands()[1].toString().toLowerCase();
        PatternDescr patternDescr = new PatternDescr(Fact.class.getSimpleName(), id);
        patternDescr.addConstraint(new ExprConstraintDescr("table == \"" + table + "\""));
        return patternDescr;
    }
}
