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
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlannerContext;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.ExchangeNode;
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

import static org.apache.doris.sql.planner.plan.ExchangeNode.Type.GATHER;

public class PlanFragmentBuilder {
    public class PhysicalPlan {
        List<org.apache.doris.planner.ScanNode> scanNodes;
        List<Expr> outputExprs;
        ArrayList<PlanFragment> fragments;

        PhysicalPlan(List<org.apache.doris.planner.ScanNode> scanNodes, List<Expr> outputExprs, ArrayList<PlanFragment> fragments) {
            this.scanNodes = scanNodes;
            this.outputExprs = outputExprs;
            this.fragments = fragments;
        }

        public List<org.apache.doris.planner.ScanNode> getScanNodes() {
            return scanNodes;
        }

        public List<Expr> getOutputExprs() {
            return outputExprs;
        }

        public ArrayList<PlanFragment> getFragments() {
            return fragments;
        }
    }

    public PhysicalPlan createPhysicalPlan(Plan plan, DescriptorTable descTbl, PlannerContext plannerContext, Map<String, SlotId> variableToSlotRef) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator();
        //PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        List<org.apache.doris.planner.ScanNode> scanNodes = new ArrayList<>();
        List<Expr> outputExprs = new ArrayList<>();
        FragmentProperties fraPro = new FragmentProperties(descTbl, plannerContext, variableToSlotRef, scanNodes, outputExprs);
        PlanFragment root = physicalPlanTranslator.visitPlan(plan.getRoot(), fraPro);
        return new PhysicalPlan(scanNodes, outputExprs, fraPro.fragments);
    }

    private static class PhysicalPlanTranslator
            extends PlanVisitor<PlanFragment, FragmentProperties> {
        @Override
        public PlanFragment visitPlan(LogicalPlanNode node, FragmentProperties context) {
            return node.accept(this, context);
        }

        @Override
        public PlanFragment visitOutput(OutputNode node, FragmentProperties context) {
            for (int i = 0;i < node.getOutputVariables().size(); ++i) {
                context.outputRowExpression.put(node.getOutputVariables().get(i), null);
            }

            PlanFragment root =  visitPlan(node.getSource(), context);

            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                context.outputExprs.add(context.outputRowExpression.get(variable));
            }
            root.setOutputExprs(context.outputExprs);
            return root;
        }

        @Override
        public PlanFragment visitExchange(ExchangeNode node, FragmentProperties context) {

            List<PlanFragment> exchanges = new ArrayList<>();

            for (LogicalPlanNode source : node.getSources()) {
                PlanFragment inputFragment = visitPlan(source, context);

                org.apache.doris.planner.ExchangeNode exchangeNode =
                        new org.apache.doris.planner.ExchangeNode(
                                context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), false);
                exchangeNode.setNumInstances(1);
                PlanFragment exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);
                inputFragment.setDestination(exchangeNode);

                exchanges.add(exchangeFragment);
                context.fragments.add(exchangeFragment);
            }

            //FIXME : support join
            return exchanges.get(0);
        }

        @Override
        public PlanFragment visitProject(ProjectNode node, FragmentProperties context) {
            PlanFragment root = visitPlan(node.getSource(), context);
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                if (context.outputRowExpression.containsKey(entry.getKey())) {
                    Expr expr = RowExpressionToExpr.formatRowExpression(entry.getValue(), new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    context.outputRowExpression.put(entry.getKey(), expr);
                }
            }

            return root;
        }

        @Override
        public PlanFragment visitAggregation(AggregationNode node, FragmentProperties context) {
            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            ArrayList<Expr> groupingExprs = Lists.newArrayList();
            for(VariableReferenceExpression groupKey : node.getGroupingKeys()) {
                SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDescriptor.setColumn(new Column(groupKey.getName(), ScalarType.INT));
                slotDescriptor.setIsNullable(true);
                slotDescriptor.setIsMaterialized(true);

                context.variableToSlotRef.put(groupKey.getName(), slotDescriptor.getId());
                groupingExprs.add(new SlotRef(slotDescriptor));
            }

            ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
            for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                FunctionCallExpr functionCallExpr =
                        (FunctionCallExpr) RowExpressionToExpr.formatRowExpression(aggregation.getCall(),
                                new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                aggExprs.add(functionCallExpr);
            }

            AggregateInfo aggInfo = AggregateInfo.create(groupingExprs, aggExprs, tupleDescriptor);

            if (node.getStep().equals(AggregationNode.Step.FINAL)) {
                PlanFragment inputFragment = visitPlan(node.getSource(), context);
                PlanNode aggregationNode = new org.apache.doris.planner.AggregationNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                inputFragment.setPlanRoot(aggregationNode);
                return inputFragment;
            } else if (node.getStep().equals(AggregationNode.Step.PARTIAL)) {
                PlanFragment inputFragment = visitPlan(node.getSource(), context);
                PlanNode aggregationNode = new org.apache.doris.planner.AggregationNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                inputFragment.setPlanRoot(aggregationNode);
                return inputFragment;
            } else {
                return null;
            }
        }

        @Override
        public PlanFragment visitTableScan(TableScanNode node, FragmentProperties context)
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
                slotDescriptor.setColumn(new Column(variable.getName(), ScalarType.INT));
                slotDescriptor.setIsNullable(true);
                slotDescriptor.setIsMaterialized(true);

                context.variableToSlotRef.put(variable.getName(), slotDescriptor.getId());
            }

            tupleDescriptor.computeMemLayout();
            context.scanNodes.add(scanNode);
            PlanFragment fragment = new PlanFragment(context.plannerContext.getNextFragmentId(), scanNode, DataPartition.RANDOM);
            context.fragments.add(fragment);
            return fragment;
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl;
        private final PlannerContext plannerContext;
        private final Map<String, SlotId> variableToSlotRef;
        private final List<org.apache.doris.planner.ScanNode> scanNodes;
        private final List<Expr> outputExprs;
        private final Map<VariableReferenceExpression, Expr> outputRowExpression = Maps.newHashMap();

        private PlanFragment fragment_ctx;
        private final ArrayList<PlanFragment> fragments = new ArrayList<>();

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
