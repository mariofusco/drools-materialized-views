package org.drools.materialized.prototyped;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlSelect;
import org.drools.model.Index;
import org.drools.model.Model;
import org.drools.model.Prototype;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeVariable;
import org.drools.model.Query;
import org.drools.model.impl.ModelImpl;
import org.drools.modelcompiler.builder.KieBaseBuilder;
import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;

import static org.drools.materialized.SqlUtil.parseQuery;
import static org.drools.model.PatternDSL.query;
import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.variable;

public class PrototypedQueryGenerator {

    private final Map<String, Prototype> prototypes = new HashMap<>();
    private final Map<String, PrototypeVariable> variables = new HashMap<>();

    public KieSession query2KieSessionViaExecModelDSL(String query) {
        Model model = new ModelImpl().addQuery( sqlToExecModelQuery( (SqlSelect) parseQuery(query) ) );
        KieBase kieBase = KieBaseBuilder.createKieBaseFromModel( model );
        return kieBase.newKieSession();
    }

    private Query sqlToExecModelQuery(SqlSelect sql) {
        SqlJoin join = (SqlJoin) sql.getFrom();

        PrototypeDSL.PrototypePatternDef leftPattern = toPattern( (SqlBasicCall) join.getLeft() );
        PrototypeDSL.PrototypePatternDef rightPattern = toPattern( (SqlBasicCall) join.getRight() );

        SqlBasicCall condition = (SqlBasicCall) join.getCondition();

        String leftOperand = condition.getOperands()[0].toString().toLowerCase();
        int leftDotPosition = leftOperand.indexOf('.');
        String leftField = leftOperand.substring(leftDotPosition+1).trim();
        String leftAlias = leftOperand.substring(0, leftDotPosition).trim();
        PrototypeVariable leftVar = variables.get(leftAlias);

        String rightOperand = condition.getOperands()[1].toString().toLowerCase();
        int rightDotPosition = rightOperand.indexOf('.');
        String rightField = rightOperand.substring(rightDotPosition+1).trim();

        PrototypeDSL.PrototypePatternDef firstPattern, secondPattern;
        if (leftVar == leftPattern.getFirstVariable()) {
            firstPattern = leftPattern;
            secondPattern = rightPattern;
        } else {
            firstPattern = rightPattern;
            secondPattern = leftPattern;
        }

        secondPattern.expr(rightField, Index.ConstraintType.EQUAL, leftVar, leftField);

        return query( "Q0" ).build(firstPattern, secondPattern);
    }

    private PrototypeDSL.PrototypePatternDef toPattern(SqlBasicCall sqlCall) {
        String table = sqlCall.getOperands()[0].toString().toLowerCase();
        String id = sqlCall.getOperands()[1].toString().toLowerCase();
        Prototype prototype = prototypes.computeIfAbsent( table, PrototypeDSL::prototype );
        PrototypeVariable protoVar = variable( prototype, id );
        variables.put(id, protoVar);
        return protoPattern( protoVar );
    }

    public Prototype getPrototype(String name) {
        return prototypes.get(name);
    }
}
