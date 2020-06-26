package org.apache.doris.sql.planner;

import com.google.common.collect.Lists;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PartitionColumnFilter;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.PlannerContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.RowExpressionToExpr;

import java.util.HashMap;
import java.util.Map;

public class PhysicalPlanner {
    public PlanNode createPhysicalPlan(Plan plan, DescriptorTable descTbl, PlannerContext plannerContext, Map<String, SlotId> variableToSlotRef) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        //PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        FragmentProperties fraPro = new FragmentProperties(descTbl, plannerContext, variableToSlotRef);
        PlanNode root = physicalPlanTranslator.visitPlan(plan.getRoot(), fraPro);
        return root;
    }

    private static class PhysicalPlanTranslator
            extends PlanVisitor<PlanNode, FragmentProperties> {

        public PlanNode visitPlan(LogicalPlanNode node, FragmentProperties context) {
            return node.accept(this, context);
        }

        public PlanNode visitOutput(OutputNode node, FragmentProperties context) {
            return visitPlan(node.getSource(), context);
        }

        public PlanNode visitProject(ProjectNode node, FragmentProperties context) {
            return visitPlan(node.getSource(), context);
        }

        public PlanNode visitFilter(FilterNode node, FragmentProperties context) {
            if (node.getSource() instanceof TableScanNode) {
                ScanNode scanNode = (ScanNode) visitPlan(node.getSource(), context);
                RowExpression rowExpression = node.getPredicate();
                scanNode.addConjuncts(Lists.newArrayList(RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef))));
                return scanNode;
            }
            return null;
        }

        public PlanNode visitTableScan(TableScanNode node, FragmentProperties context)
        {
            DorisTableHandle tableHandler = (DorisTableHandle) node.getTable().getConnectorHandle();
            Table referenceTable = tableHandler.getTable();

            context.descTbl.addReferencedTable(referenceTable);

            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);
            
            OlapScanNode scanNode = new OlapScanNode(context.plannerContext.getNextNodeId(), tupleDescriptor, "OlapScanNode");
            try {
                scanNode.updateScanRangeInfoByNewMVSelector(((OlapTable) referenceTable).getBaseIndexId(), false, null);
                scanNode.getScanRangeLocations();
            } catch (Exception ex) {
                ex.printStackTrace();
                return null;
            }

            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDescriptor.setColumn(new Column(variable.getName(), ScalarType.INT));
                slotDescriptor.setIsNullable(true);
                slotDescriptor.setIsMaterialized(true);

                context.variableToSlotRef.put(variable.getName(), slotDescriptor.getId());
            }

            tupleDescriptor.computeMemLayout();
            return scanNode;
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl;
        private final PlannerContext plannerContext;
        private final Map<String, SlotId> variableToSlotRef;
        FragmentProperties (DescriptorTable descTbl, PlannerContext plannerContext, Map<String, SlotId> variableToSlotRef) {
            this.descTbl = descTbl;
            this.plannerContext = plannerContext;
            this.variableToSlotRef = variableToSlotRef;
        }
    }
}
