package org.drools.materialized.prototyped;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.util.Pair;
import org.drools.core.base.accumulators.CountAccumulateFunction;
import org.drools.model.DSL;
import org.drools.model.Index;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeVariable;
import org.drools.model.Query;
import org.drools.model.Variable;
import org.drools.model.view.ViewItem;

import static org.drools.materialized.SqlUtil.toAlgebra;
import static org.drools.model.PatternDSL.query;

public class AlgebraTransformer extends AbstractSqlTransformer {

    public AlgebraTransformer(PrototypeFactory prototypeFactory) {
        super(prototypeFactory);
    }

    @Override
    public Query apply(String query) {
        return sqlToExecModelQuery( toAlgebra(query) );
    }

    private Query sqlToExecModelQuery(RelRoot relRoot) {
        Context context = new Context(relRoot.fields);
        processRelNode(relRoot.rel, context);
        return query( "Q0" ).build(context.getViewItemsArray());
    }

    private void processRelNode(RelNode relNode, Context context) {
        if (relNode instanceof LogicalSort) {
            processLogicalSort(((LogicalSort) relNode), context);
        } else if (relNode instanceof LogicalAggregate) {
            processLogicalAggregate(((LogicalAggregate) relNode), context);
        } else if (relNode instanceof LogicalProject) {
            processLogicalProject(((LogicalProject) relNode), context);
        } else if (relNode instanceof TableScan) {
            processTableScan(((TableScan) relNode), context);
        } else if (relNode instanceof LogicalJoin) {
            processLogicalJoin(((LogicalJoin) relNode), context);
        } else {
            throw new UnsupportedOperationException("Unknown node: " + relNode);
        }
    }

    private void processLogicalJoin(LogicalJoin joinNode, Context context) {
        processRelNode(joinNode.getLeft(), context);
        ViewItem leftPattern = context.getLastPattern();
        processRelNode(joinNode.getRight(), context);
        PrototypeDSL.PrototypePatternDef rightPattern = (PrototypeDSL.PrototypePatternDef) context.getLastPattern();

        List<RexNode> operands = ((RexCall) joinNode.getCondition()).getOperands();

        PrototypeVariable leftVar = (PrototypeVariable) leftPattern.getFirstVariable();
        String leftField = context.getField( ((RexSlot) operands.get(0)).getIndex() );
        String rightField = context.getField( ((RexSlot) operands.get(1)).getIndex() );

        rightPattern.expr(rightField, Index.ConstraintType.EQUAL, leftVar, leftField);
    }

    private void processTableScan(TableScan tableScan, Context context) {
        context.addPatternForTable( tableScan );
    }

    private PrototypeDSL.PrototypePatternDef toPattern(TableScan tableScan) {
        String table = tableScan.getTable().getQualifiedName().get(0);
        // TODO the algebra doesn't carry information on aliases, so temporarily using the 1st letter as declaration/variable id
        String id = table.substring(0, 1);
        return createPrototypePatternDef(table, id);
    }

    private void processLogicalProject(LogicalProject project, Context context) {
        // TODO: select
        processRelNode(project.getInput(), context);
    }

    private void processLogicalAggregate(LogicalAggregate aggregate, Context context) {
        // group by
        processRelNode(aggregate.getInput(), context);
        PrototypeDSL.PrototypePatternDef pattern = (PrototypeDSL.PrototypePatternDef) context.popLastPattern();

        String groupKey = context.getField( aggregate.getGroupSet().asList().get(0) ).toLowerCase();

        Variable<Object> key = DSL.declarationOf(Object.class, groupKey);
        Variable<Long> accResult = DSL.declarationOf(Long.class, "count");

        context.addPattern( DSL.groupBy( pattern,
                                         pattern.getFirstVariable(), key, p -> p.get(groupKey),
                                         DSL.accFunction(CountAccumulateFunction::new).as(accResult)) );
    }

    private void processLogicalSort(LogicalSort sort, Context context) {
        // TODO: order by (?)
        processRelNode(sort.getInput(), context);
    }

    private class Context {
        private final List<ViewItem> viewItems = new ArrayList<>();

        private final List<Pair<Integer, String>> fields;

        private Context(List<Pair<Integer, String>> fields) {
            this.fields = fields;
        }

        ViewItem[] getViewItemsArray() {
            return viewItems.toArray(ViewItem[]::new);
        }

        String getField(int index) {
            return fields.stream().filter( p -> p.getKey() == index ).findFirst()
                    .map( Pair::getValue )
                    .orElseThrow( () -> new IllegalArgumentException("Unknown field with index: " + index ) );
        }

        PrototypeDSL.PrototypePatternDef addPatternForTable(TableScan tableScan) {
            PrototypeDSL.PrototypePatternDef pattern = toPattern(tableScan);
            viewItems.add(pattern);
            return pattern;
        }

        ViewItem getLastPattern() {
            return viewItems.get(viewItems.size()-1);
        }

        ViewItem popLastPattern() {
            return viewItems.remove(viewItems.size()-1);
        }

        void addPattern(ViewItem pattern) {
            viewItems.add(pattern);
        }
    }
}
