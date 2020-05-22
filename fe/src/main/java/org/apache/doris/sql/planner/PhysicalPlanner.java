package org.apache.doris.sql.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.clone.TabletScheduler;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;

public class PhysicalPlanner {
    public PlanNode createPhysicalPlan(Plan plan) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        return root;
    }

    private static class PhysicalPlanTranslator
            extends SimplePlanRewriter<FragmentProperties> {

        public PlanNode visitPlan(org.apache.doris.sql.planner.plan.PlanNode node, FragmentProperties context) {
            return null;
        }

        public PlanNode visitOutput(OutputNode node, FragmentProperties context)
        {
            return visitPlan(node, context);
        }

        public PlanNode visitProject(ProjectNode node, FragmentProperties context)
        {
            return visitPlan(node, context);
        }

        public PlanNode visitTableScan(TableScanNode node, FragmentProperties context)
        {
            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            tupleDescriptor.setTable(node.getTable());
            for (VariableReferenceExpression expression : node.getOutputVariables()) {
                SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(0), tupleDescriptor);
                slotDescriptor.setColumn();
                tupleDescriptor.addSlot();
            }
            OlapScanNode olapNode = new OlapScanNode(new PlanNodeId(0), tupleDescriptor, "OlapScanNode");
            return olapNode;
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl = new DescriptorTable();
    }
}
