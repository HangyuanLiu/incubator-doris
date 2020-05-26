package org.apache.doris.sql.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;

public class PhysicalPlanner {
    /*
    public PlanNode createPhysicalPlan(Plan plan) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        return root;
    }

    private static class PhysicalPlanTranslator
            extends SimplePlanRewriter<FragmentProperties> {

        public PlanNode visitPlan(LogicalPlanNode node, FragmentProperties context) {
            return node.accept(this, context);
        }

        public PlanNode visitOutput(OutputNode node, FragmentProperties context)
        {
            return visitPlan(node.getSource(), context);
        }

        public PlanNode visitProject(ProjectNode node, FragmentProperties context)
        {
            return visitPlan(node, context);
        }

        public PlanNode visitTableScan(TableScanNode node, FragmentProperties context)
        {
            DorisTableHandle tableHandler = (DorisTableHandle) node.getTable().getConnectorHandle();
            Table referenceTable = tableHandler.getTable();

            //TTableDescriptor
            context.descTbl.addReferencedTable(referenceTable);

            //TTupleDescriptor
            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            for (VariableReferenceExpression expression : node.getOutputVariables()) {
                SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(0), tupleDescriptor);
                slotDescriptor.setColumn(new Column(expression.getName(), PrimitiveType.INT));
                tupleDescriptor.addSlot(slotDescriptor);
            }
            OlapScanNode olapNode = new OlapScanNode(new PlanNodeId(0), tupleDescriptor, "OlapScanNode");
            return olapNode;
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl = new DescriptorTable();
    }
     */
}
