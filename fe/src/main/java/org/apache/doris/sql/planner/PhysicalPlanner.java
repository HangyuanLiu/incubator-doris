package org.apache.doris.sql.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

public class PhysicalPlanner {
    public PlanNode createPhysicalPlan(Plan plan, DescriptorTable descTbl) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        //PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        FragmentProperties fraPro = new FragmentProperties(descTbl);
        PlanNode root = physicalPlanTranslator.visitPlan(plan.getRoot(), fraPro);
        return root;
    }

    private static class PhysicalPlanTranslator
            extends PlanVisitor<PlanNode, FragmentProperties> {

        public PlanNode visitPlan(LogicalPlanNode node, FragmentProperties context) {
            return node.accept(this, context);
        }

        public PlanNode visitOutput(OutputNode node, FragmentProperties context)
        {
            return visitPlan(node.getSource(), context);
        }

        public PlanNode visitProject(ProjectNode node, FragmentProperties context)
        {
            OlapScanNode olapScanNode = (OlapScanNode) visitPlan(node.getSource(), context);
            TupleId tupleId = olapScanNode.getTupleIds().get(0);
            TupleDescriptor tupleDes = context.descTbl.getTupleDesc(tupleId);

            //List<VariableReferenceExpression> variables = node.getOutputVariables();
            for (VariableReferenceExpression expression : node.getOutputVariables()) {
                SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(0), tupleDes);
                slotDescriptor.setColumn(new Column(expression.getName(), PrimitiveType.INT));
                tupleDes.addSlot(slotDescriptor);
            }
            tupleDes.computeMemLayout();
            return olapScanNode;
        }

        public PlanNode visitTableScan(TableScanNode node, FragmentProperties context)
        {
            DorisTableHandle tableHandler = (DorisTableHandle) node.getTable().getConnectorHandle();
            Table referenceTable = tableHandler.getTable();

            context.descTbl.addReferencedTable(referenceTable);

            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            tupleDescriptor.setTable(referenceTable);

            return new OlapScanNode(new PlanNodeId(0), tupleDescriptor, "OlapScanNode");
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl;
        FragmentProperties (DescriptorTable descTbl) {
            this.descTbl = descTbl;
        }
    }
}
