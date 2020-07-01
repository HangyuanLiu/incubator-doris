package org.apache.doris.sql.planner;

import com.google.common.collect.Lists;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlannerContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.RowExpressionToExpr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PhysicalPlanner {
    public class PhysicalPlan {
        PlanNode root;
        List<org.apache.doris.planner.ScanNode> scanNodes;
        PhysicalPlan(PlanNode root, List<org.apache.doris.planner.ScanNode> scanNodes) {
            this.root = root;
            this.scanNodes = scanNodes;
        }

        public PlanNode getRoot() {
            return root;
        }

        public List<ScanNode> getScanNodes() {
            return scanNodes;
        }
    }

    public PhysicalPlan createPhysicalPlan(Plan plan, DescriptorTable descTbl, PlannerContext plannerContext, Map<String, SlotId> variableToSlotRef) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        //PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        List<org.apache.doris.planner.ScanNode> scanNodes = new ArrayList<>();
        FragmentProperties fraPro = new FragmentProperties(descTbl, plannerContext, variableToSlotRef, scanNodes);
        PlanNode root = physicalPlanTranslator.visitPlan(plan.getRoot(), fraPro);
        return new PhysicalPlan(root, fraPro.scanNodes);
    }

    private static class PhysicalPlanTranslator
            extends PlanVisitor<PlanNode, FragmentProperties> {
        @Override
        public PlanNode visitPlan(LogicalPlanNode node, FragmentProperties context) {
            return node.accept(this, context);
        }

        @Override
        public PlanNode visitOutput(OutputNode node, FragmentProperties context) {
            return visitPlan(node.getSource(), context);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, FragmentProperties context) {
            return visitPlan(node.getSource(), context);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, FragmentProperties context) {
            if (node.getSource() instanceof TableScanNode) {
                org.apache.doris.planner.ScanNode scanNode = (org.apache.doris.planner.ScanNode) visitPlan(node.getSource(), context);
                RowExpression rowExpression = node.getPredicate();
                scanNode.addConjuncts(Lists.newArrayList(RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef))));
                return scanNode;
            }
            return null;
        }

        @Override
        public PlanNode visitTopN(TopNNode node, FragmentProperties context) {
            PlanNode children = visitPlan(node.getSource(), context);

            OrderingScheme orderingScheme = node.getOrderingScheme();
            List<Expr> sortExpr = orderingScheme.getOrderByVariables().stream().
                    map(rowExpression -> RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef))).collect(Collectors.toList());
            List<Boolean> isAscOrder = orderingScheme.getOrderByVariables().stream().
                    map(rowExpression -> orderingScheme.getOrdering(rowExpression).isAscending()).collect(Collectors.toList());
            List<Boolean> isNullsFirst = orderingScheme.getOrderByVariables().
                    stream().map(rowExpression -> orderingScheme.getOrdering(rowExpression).isNullsFirst()).collect(Collectors.toList());
            SortInfo sortInfo = new SortInfo(sortExpr, isAscOrder, isNullsFirst);

            //tupleSlotExprs
            List<Expr> tupleSlotExprs = Lists.newArrayList();
            for (VariableReferenceExpression variable : node.getSource().getOutputVariables()) {
                tupleSlotExprs.add(new SlotRef(context.descTbl.getSlotDesc(context.variableToSlotRef.get(variable.getName()))));
            }

            //Create Tuple tupleDescriptor
            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDescriptor.setColumn(new Column(variable.getName(), ScalarType.INT));
                slotDescriptor.setIsNullable(true);
                slotDescriptor.setIsMaterialized(true);

                context.variableToSlotRef.put(variable.getName(), slotDescriptor.getId());
            }

            sortInfo.setMaterializedTupleInfo(tupleDescriptor, tupleSlotExprs);

            org.apache.doris.planner.SortNode sortNode =
                    new org.apache.doris.planner.SortNode(context.plannerContext.getNextNodeId(), children, sortInfo, true, true, node.getCount());
            sortNode.resolvedTupleExprs = tupleSlotExprs;

            return sortNode;
        }

        @Override
        public PlanNode visitSort(org.apache.doris.sql.planner.plan.SortNode node, FragmentProperties context) {
            PlanNode children = visitPlan(node.getSource(), context);
            SortInfo sortInfo = null;
            org.apache.doris.planner.SortNode sortNode =
                    new org.apache.doris.planner.SortNode(context.plannerContext.getNextNodeId(), children, sortInfo, true, true, 65535);

            return sortNode;
        }

        @Override
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
            context.scanNodes.add(scanNode);
            return scanNode;
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl;
        private final PlannerContext plannerContext;
        private final Map<String, SlotId> variableToSlotRef;
        private final List<org.apache.doris.planner.ScanNode> scanNodes;
        FragmentProperties (DescriptorTable descTbl, PlannerContext plannerContext, Map<String, SlotId> variableToSlotRef,
                            List<org.apache.doris.planner.ScanNode> scanNodes) {
            this.descTbl = descTbl;
            this.plannerContext = plannerContext;
            this.variableToSlotRef = variableToSlotRef;
            this.scanNodes = scanNodes;
        }
    }
}
