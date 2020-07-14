package org.apache.doris.sql.planner;

import com.google.common.collect.Lists;
import javafx.util.Pair;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.commons.validator.Var;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlannerContext;
import org.apache.doris.sql.metadata.ColumnHandle;
import org.apache.doris.sql.metadata.DorisColumnHandle;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.planner.plan.SortNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.RowExpressionToExpr;

import java.util.ArrayList;
import java.util.HashMap;
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
        Map<LogicalPlanNode, TupleDescriptor> tupleDescriptorMap = new HashMap<>();
        FragmentProperties fraPro = new FragmentProperties(descTbl, plannerContext, variableToSlotRef, scanNodes, outputExprs, tupleDescriptorMap);
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

            PlanFragment inputFragment = visitPlan(node.getSource(), context);

            if (inputFragment.getPlanRoot() instanceof org.apache.doris.planner.SortNode) {
                org.apache.doris.planner.ExchangeNode exchangeNode =
                        new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), false);
                exchangeNode.setNumInstances(1);
                exchangeNode.setMergeInfo(((org.apache.doris.planner.SortNode) inputFragment.getPlanRoot()).getSortInfo(), 0);
                PlanFragment exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);

                List<Expr> outputExprs = new ArrayList<>();
                for (VariableReferenceExpression variable : node.getOutputVariables()) {
                    Expr expr = RowExpressionToExpr.formatRowExpression(variable, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    outputExprs.add(expr);
                }
                exchangeFragment.setOutputExprs(outputExprs);
                context.outputExprs.addAll(outputExprs);

                inputFragment.setDestination(exchangeNode);

                context.fragments.add(exchangeFragment);

                return exchangeFragment;
            } else {
                org.apache.doris.planner.ExchangeNode exchangeNode =
                        new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), false);
                exchangeNode.setNumInstances(1);
                PlanFragment exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);

                List<Expr> outputExprs = new ArrayList<>();
                for (VariableReferenceExpression variable : node.getOutputVariables()) {
                    Expr expr = RowExpressionToExpr.formatRowExpression(variable, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    outputExprs.add(expr);
                }
                exchangeFragment.setOutputExprs(outputExprs);
                context.outputExprs.addAll(outputExprs);

                inputFragment.setDestination(exchangeNode);

                context.fragments.add(exchangeFragment);

                return exchangeFragment;
            }
            /*
            for (VariableReferenceExpression variable : node.getOutputVariables()) {
                context.outputExprs.add(context.outputRowExpression.get(variable));
            }
            root.setOutputExprs(context.outputExprs);
             */
            //return root;
        }

        @Override
        public PlanFragment visitExchange(ExchangeNode node, FragmentProperties context) {

            List<PlanFragment> exchanges = new ArrayList<>();

            for (LogicalPlanNode source : node.getSources()) {
                PlanFragment inputFragment = visitPlan(source, context);

                ArrayList<Expr> hashPartition = new ArrayList<>();
                for (VariableReferenceExpression var : node.getPartitioningScheme().getHashColumn().get()) {
                    Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    hashPartition.add(expr);
                }
                inputFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));

                org.apache.doris.planner.ExchangeNode exchangeNode =
                        new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), false);
                exchangeNode.setNumInstances(1);

                PlanFragment exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), exchangeNode, DataPartition.hashPartitioned(hashPartition));
                inputFragment.setDestination(exchangeNode);


                for (int i = 0; i < node.getOutputVariables().size(); ++i) {
                    VariableReferenceExpression input = node.getInputs().get(0).get(i);
                    Expr expr = RowExpressionToExpr.formatRowExpression(input, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                    context.variableToSlotRef.put(node.getOutputVariables().get(i).getName(), ((SlotRef) expr).getSlotId());
                }

                exchanges.add(exchangeFragment);
                context.fragments.add(exchangeFragment);
            }

            //FIXME : support join and union
            return exchanges.get(0);

            /*
            PlanFragment fragment;
            if (node.getSources().size() < 2) {
                PlanFragment inputFragment = visitPlan(node.getSources().get(0), context);

                org.apache.doris.planner.ExchangeNode exchangeNode =
                        new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), false);
                exchangeNode.setNumInstances(1);
                inputFragment.setDestination(exchangeNode);

                for (int i = 0; i < node.getOutputVariables().size(); ++i) {
                    VariableReferenceExpression input = node.getInputs().get(0).get(i);
                    Expr expr = RowExpressionToExpr.formatRowExpression(input, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                    context.variableToSlotRef.put(node.getOutputVariables().get(i).getName(), ((SlotRef) expr).getSlotId());
                }

                fragment = new PlanFragment(context.plannerContext.getNextFragmentId(), exchangeNode, inputFragment.getOutputPartition());
                context.fragments.add(fragment);
                return fragment;
            } else {
                for (LogicalPlanNode source : node.getSources()) {
                    PlanFragment inputFragment = visitPlan(source, context);

                    org.apache.doris.planner.ExchangeNode exchangeNode =
                            new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), false);
                    exchangeNode.setNumInstances(1);

                    inputFragment.setDestination(exchangeNode);

                    for (int i = 0; i < node.getOutputVariables().size(); ++i) {
                        VariableReferenceExpression input = node.getInputs().get(0).get(i);
                        Expr expr = RowExpressionToExpr.formatRowExpression(input, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                        context.variableToSlotRef.put(node.getOutputVariables().get(i).getName(), ((SlotRef) expr).getSlotId());
                    }
                }

                fragment = new PlanFragment(context.plannerContext.getNextFragmentId(), exchangeNode, DataPartition.UNPARTITIONED);
                context.fragments.add(fragment);
                return fragment;
            }

             */
        }

        @Override
        public PlanFragment visitJoin(JoinNode node, FragmentProperties context) {
            PlanFragment leftFragment = visitPlan(node.getLeft(), context);
            PlanFragment rightFragment = visitPlan(node.getRight(), context);

            List<Expr> eqJoinConjuncts = new ArrayList<>();
            for (JoinNode.EquiJoinClause equiJoinClause : node.getCriteria()) {
                Expr left = RowExpressionToExpr.formatRowExpression(equiJoinClause.getLeft(),
                        new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                Expr right = RowExpressionToExpr.formatRowExpression(equiJoinClause.getRight(),
                        new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                BinaryPredicate binaryEq = new BinaryPredicate(BinaryPredicate.Operator.EQ, left, right);
                eqJoinConjuncts.add(binaryEq);
            }

            org.apache.doris.planner.HashJoinNode hashJoinNode =
                    new org.apache.doris.planner.HashJoinNode(
                            context.plannerContext.getNextNodeId(),
                            leftFragment.getPlanRoot(), rightFragment.getPlanRoot(),
                            JoinOperator.INNER_JOIN,
                            eqJoinConjuncts,
                            new ArrayList<>());
            hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);

            context.fragments.remove(leftFragment);
            context.fragments.remove(rightFragment);
            leftFragment.setPlanRoot(hashJoinNode);
            context.fragments.add(leftFragment);
            return leftFragment;
        }

        @Override
        public PlanFragment visitSemiJoin(SemiJoinNode node, FragmentProperties context) {
            return null;
        }

        @Override
        public PlanFragment visitTopN(TopNNode node, FragmentProperties context) {
            PlanFragment inputFragment = visitPlan(node.getSource(), context);
            //Get order ascending and is null first
            OrderingScheme orderingScheme = node.getOrderingScheme();
            List<Boolean> isAscOrder = orderingScheme.getOrderByVariables().stream().
                    map(rowExpression -> orderingScheme.getOrdering(rowExpression).isAscending()).collect(Collectors.toList());
            List<Boolean> isNullsFirst = orderingScheme.getOrderByVariables().
                    stream().map(rowExpression -> orderingScheme.getOrdering(rowExpression).isNullsFirst()).collect(Collectors.toList());


            List<Expr> resolvedTupleExprs = new ArrayList<>();
            List<Expr> sortExprs = new ArrayList<>();
            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            for (VariableReferenceExpression orderByVar : orderingScheme.getOrderByVariables()) {
                Expr sortExpr = RowExpressionToExpr.formatRowExpression(orderByVar, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDesc.initFromExpr(sortExpr);
                slotDesc.setIsNullable(true);
                slotDesc.setIsMaterialized(true);

                context.variableToSlotRef.put(orderByVar.getName(), slotDesc.getId());
                resolvedTupleExprs.add(sortExpr);
                sortExprs.add(new SlotRef(slotDesc));
            }
            for (VariableReferenceExpression var : node.getOutputVariables()) {
                if (!orderingScheme.getOrderByVariables().contains(var)) {
                    Expr sortExpr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                    slotDesc.initFromExpr(sortExpr);
                    slotDesc.setIsNullable(true);
                    slotDesc.setIsMaterialized(true);

                    context.variableToSlotRef.put(var.getName(), slotDesc.getId());
                    resolvedTupleExprs.add(sortExpr);
                }
            }

            tupleDescriptor.computeMemLayout();

            SortInfo sortInfo = new SortInfo(sortExprs, isAscOrder, isNullsFirst);
            sortInfo.setMaterializedTupleInfo(tupleDescriptor, resolvedTupleExprs);

            org.apache.doris.planner.SortNode sortNode =
                    new org.apache.doris.planner.SortNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), sortInfo, true, false, 0);
            sortNode.setLimit(node.getCount());
            sortNode.resolvedTupleExprs = resolvedTupleExprs;

            inputFragment.setPlanRoot(sortNode);

            return inputFragment;
        }

        @Override
        public PlanFragment visitSort(SortNode node, FragmentProperties context) {
            return null;
        }

        @Override
        public PlanFragment visitAggregation(AggregationNode node, FragmentProperties context) {
            PlanFragment inputFragment = visitPlan(node.getSource(), context);

            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();

            // grouping expr
            ArrayList<Expr> groupingExprs = Lists.newArrayList();
            List<Expr> partitionExpr = Lists.newArrayList();
            for(VariableReferenceExpression groupKey : node.getGroupingKeys()) {
                Expr groupExpr = RowExpressionToExpr.formatRowExpression(groupKey, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDesc.initFromExpr(groupExpr);
                slotDesc.setIsNullable(true);
                slotDesc.setIsMaterialized(true);

                context.variableToSlotRef.put(groupKey.getName(), slotDesc.getId());
                groupingExprs.add(groupExpr);
                partitionExpr.add(new SlotRef(slotDesc));
            }
            // agg expr
            ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> aggregation : node.getAggregations().entrySet()) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) RowExpressionToExpr.formatRowExpression(aggregation.getValue().getCall(),
                                new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                if (node.getStep().equals(AggregationNode.Step.FINAL)) {
                    functionCallExpr.setMergeAggFn();
                }

                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDesc.initFromExpr(functionCallExpr);
                slotDesc.setIsNullable(true);
                slotDesc.setIsMaterialized(true);

                context.variableToSlotRef.put(aggregation.getKey().getName(), slotDesc.getId());
                aggExprs.add(functionCallExpr);
            }

            tupleDescriptor.computeMemLayout();

            if (node.getStep().equals(AggregationNode.Step.PARTIAL)) {
                AggregateInfo aggInfo = AggregateInfo.create(groupingExprs, aggExprs, tupleDescriptor, AggregateInfo.AggPhase.FIRST);
                org.apache.doris.planner.AggregationNode aggregationNode = new org.apache.doris.planner.AggregationNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                aggregationNode.unsetNeedsFinalize();
                aggregationNode.setIsPreagg(context.plannerContext);
                inputFragment.setPlanRoot(aggregationNode);
                inputFragment.setOutputPartition(DataPartition.hashPartitioned(partitionExpr));
                return inputFragment;
            } else if (node.getStep().equals(AggregationNode.Step.FINAL)) {
                AggregateInfo aggInfo = AggregateInfo.create(groupingExprs, aggExprs, tupleDescriptor, AggregateInfo.AggPhase.SECOND_MERGE);
                PlanNode aggregationNode = new org.apache.doris.planner.AggregationNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
                inputFragment.setPlanRoot(aggregationNode);
                return inputFragment;
            } else {
                return null;
            }
        }

        @Override
        public PlanFragment visitProject(ProjectNode node, FragmentProperties context) {
            PlanFragment inputFragment = visitPlan(node.getSource(), context);
            /*
            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();

            ArrayList<Expr> outputExpr = new ArrayList<>();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                Expr expr = RowExpressionToExpr.formatRowExpression(entry.getValue(), new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDesc.initFromExpr(expr);
                slotDesc.setIsNullable(true);
                slotDesc.setIsMaterialized(true);

                context.variableToSlotRef.put(entry.getKey().getName(), slotDesc.getId());
                outputExpr.add(expr);
            }
            inputFragment.setOutputExprs(outputExpr);
            tupleDescriptor.computeMemLayout();
             */

            return inputFragment;
        }

        @Override
        public PlanFragment visitFilter(FilterNode node, FragmentProperties context) {
            PlanFragment inputFragment = visitPlan(node.getSource(), context);
            if (inputFragment.getPlanRoot() instanceof org.apache.doris.planner.OlapScanNode) {
                RowExpression rowExpression = node.getPredicate();
                inputFragment.getPlanRoot().addConjuncts(Lists.newArrayList(RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef))));
                return inputFragment;
            } else if (inputFragment.getPlanRoot() instanceof  org.apache.doris.planner.AggregationNode) {
                RowExpression rowExpression = node.getPredicate();
                inputFragment.getPlanRoot().addConjuncts(Lists.newArrayList(RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef))));
                return inputFragment;
            }
            return null;
        }

        @Override
        public PlanFragment visitTableScan(TableScanNode node, FragmentProperties context)
        {
            DorisTableHandle tableHandler = (DorisTableHandle) node.getTable().getConnectorHandle();
            Table referenceTable = tableHandler.getTable();

            context.descTbl.addReferencedTable(referenceTable);

            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            context.tupleDescriptorMap.put(node, tupleDescriptor);
            tupleDescriptor.setTable(referenceTable);

            org.apache.doris.planner.OlapScanNode scanNode = new org.apache.doris.planner.OlapScanNode(context.plannerContext.getNextNodeId(), tupleDescriptor, "OlapScanNode");
            try {
                scanNode.updateScanRangeInfoByNewMVSelector(((OlapTable) referenceTable).getBaseIndexId(), false, null);
                scanNode.getScanRangeLocations();
            } catch (Exception ex) {
                ex.printStackTrace();
                return null;
            }

            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {

                String colName = ((DorisColumnHandle) entry.getValue()).getColumnName();

                SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDescriptor.setColumn(new Column(colName, ScalarType.BIGINT));
                slotDescriptor.setIsNullable(true);
                slotDescriptor.setIsMaterialized(true);

                context.variableToSlotRef.put(entry.getKey().getName(), slotDescriptor.getId());
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
        public List<Expr> outputExprs;

        private final Map<LogicalPlanNode, TupleDescriptor> tupleDescriptorMap;

        private final ArrayList<PlanFragment> fragments = new ArrayList<>();

        FragmentProperties (DescriptorTable descTbl, PlannerContext plannerContext, Map<String, SlotId> variableToSlotRef,
                            List<org.apache.doris.planner.ScanNode> scanNodes, List<Expr> outputExprs, Map<LogicalPlanNode, TupleDescriptor> tupleDescriptorMap ) {
            this.descTbl = descTbl;
            this.plannerContext = plannerContext;
            this.variableToSlotRef = variableToSlotRef;
            this.scanNodes = scanNodes;
            this.outputExprs = outputExprs;
            this.tupleDescriptorMap = tupleDescriptorMap;
        }
    }
}
