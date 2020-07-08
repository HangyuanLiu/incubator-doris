package org.apache.doris.sql.planner;

import com.google.common.collect.Lists;
import javafx.util.Pair;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlannerContext;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.SortNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.RowExpressionToExpr;
import org.stringtemplate.v4.misc.Aggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PhysicalPlanner {
    public class PhysicalPlan {
        PlanNode root;
        List<org.apache.doris.planner.ScanNode> scanNodes;
        List<Expr> outputExprs;
        PhysicalPlan(PlanNode root, List<org.apache.doris.planner.ScanNode> scanNodes, List<Expr> outputExprs) {
            this.root = root;
            this.scanNodes = scanNodes;
            this.outputExprs = outputExprs;
        }

        public PlanNode getRoot() {
            return root;
        }

        public List<org.apache.doris.planner.ScanNode> getScanNodes() {
            return scanNodes;
        }

        public List<Expr> getOutputExprs() {
            return outputExprs;
        }
    }

    public PhysicalPlan createPhysicalPlan(Plan plan, DescriptorTable descTbl, PlannerContext plannerContext, Map<String, SlotId> variableToSlotRef) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        //PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        List<org.apache.doris.planner.ScanNode> scanNodes = new ArrayList<>();
        List<Expr> outputExprs = new ArrayList<>();
        FragmentProperties fraPro = new FragmentProperties(descTbl, plannerContext, variableToSlotRef, scanNodes, outputExprs);
        PlanNode root = physicalPlanTranslator.visitPlan(plan.getRoot(), fraPro);
        return new PhysicalPlan(root, scanNodes, outputExprs);
    }

    private static class PhysicalPlanTranslator
            extends PlanVisitor<PlanNode, FragmentProperties> {
        @Override
        public PlanNode visitPlan(LogicalPlanNode node, FragmentProperties context) {
            return node.accept(this, context);
        }

        @Override
        public PlanNode visitOutput(OutputNode node, FragmentProperties context) {
            for (int i = 0;i < node.getOutputVariables().size(); ++i) {
                context.outputRowExpression.put(node.getOutputVariables().get(i), null);
            }
            PlanNode root =  visitPlan(node.getSource(), context);

            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                context.outputExprs.add(context.outputRowExpression.get(variable));
            }
            return root;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, FragmentProperties context) {
            PlanNode children = visitPlan(node.getSource(), context);

            ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
            for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                FunctionCallExpr functionCallExpr =
                        (FunctionCallExpr) RowExpressionToExpr.formatRowExpression(aggregation.getCall(),
                                new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                aggExprs.add(functionCallExpr);
            }

            //AggregateInfo aggInfo = AggregateInfo.create(null, aggExprs, null, analyzer);

            //PlanNode newRoot = new org.apache.doris.planner.AggregationNode(context.plannerContext.getNextNodeId(), children, aggInfo);
            //return newRoot;
            return null;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, FragmentProperties context) {
            PlanNode root = visitPlan(node.getSource(), context);
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                if (context.outputRowExpression.containsKey(entry.getKey())) {
                    Expr expr = RowExpressionToExpr.formatRowExpression(entry.getValue(), new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    context.outputRowExpression.put(entry.getKey(), expr);
                }
            }

            return root;
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
                slotDescriptor.setColumn(new Column(variable.getName(), ScalarType.BIGINT));
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
        public PlanNode visitSort(SortNode node, FragmentProperties context) {
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

            org.apache.doris.planner.OlapScanNode scanNode = new org.apache.doris.planner.OlapScanNode(context.plannerContext.getNextNodeId(), tupleDescriptor, "OlapScanNode");
            try {
                scanNode.updateScanRangeInfoByNewMVSelector(((OlapTable) referenceTable).getBaseIndexId(), false, null);
                scanNode.getScanRangeLocations();
            } catch (Exception ex) {
                ex.printStackTrace();
                return null;
            }

            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDescriptor.setColumn(new Column(variable.getName(), ScalarType.BIGINT));
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
        private final List<Expr> outputExprs;
        private final Map<VariableReferenceExpression, Expr> outputRowExpression = Maps.newHashMap();

        FragmentProperties (DescriptorTable descTbl, PlannerContext plannerContext, Map<String, SlotId> variableToSlotRef,
                            List<org.apache.doris.planner.ScanNode> scanNodes, List<Expr> outputExprs) {
            this.descTbl = descTbl;
            this.plannerContext = plannerContext;
            this.variableToSlotRef = variableToSlotRef;
            this.scanNodes = scanNodes;
            this.outputExprs = outputExprs;
        }
    }
}
