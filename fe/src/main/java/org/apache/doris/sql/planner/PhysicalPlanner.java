package org.apache.doris.sql.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PartitionColumnFilter;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.PlannerContext;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

public class PhysicalPlanner {
    public PlanNode createPhysicalPlan(Plan plan, DescriptorTable descTbl, PlannerContext plannerContext) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        //PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        FragmentProperties fraPro = new FragmentProperties(descTbl, plannerContext);
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
            /*
            for (VariableReferenceExpression expression : node.getOutputVariables()) {
                System.out.println("output var : " + expression.getName());
                SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDes);
                slotDescriptor.setColumn(new Column(expression.getName(), PrimitiveType.INT));
            }

             */
            SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDes);
            slotDescriptor.setColumn(new Column("user_id", PrimitiveType.INT));
            slotDescriptor.setIsNullable(true);
            slotDescriptor.setIsMaterialized(true);
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
            
            OlapScanNode scanNode = new OlapScanNode(context.plannerContext.getNextNodeId(), tupleDescriptor, "OlapScanNode");
            try {
                scanNode.updateScanRangeInfoByNewMVSelector(((OlapTable) referenceTable).getBaseIndexId(), false, null);
                scanNode.getScanRangeLocations();
            } catch (Exception ex) {
                ex.printStackTrace();
                return null;
            }
            return scanNode;
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl;
        private final PlannerContext plannerContext;
        FragmentProperties (DescriptorTable descTbl, PlannerContext plannerContext) {
            this.descTbl = descTbl;
            this.plannerContext = plannerContext;
        }
    }
}
