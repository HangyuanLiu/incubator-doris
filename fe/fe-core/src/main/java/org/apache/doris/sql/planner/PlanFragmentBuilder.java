package org.apache.doris.sql.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import javafx.util.Pair;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.commons.validator.Var;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.AssertNumRowsElement;
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
import org.apache.doris.planner.AssertNumRowsNode;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.MaterializedViewSelector;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlannerContext;
import org.apache.doris.sql.metadata.ColumnHandle;
import org.apache.doris.sql.metadata.DorisColumnHandle;
import org.apache.doris.sql.metadata.DorisTableHandle;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.EnforceSingleRowNode;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.planner.plan.SortNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.RowExpressionToExpr;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeManager;

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
        VariableAllocator variableAllocator = new VariableAllocator();
        TypeManager typeManager;

        PhysicalPlan(List<org.apache.doris.planner.ScanNode> scanNodes, List<Expr> outputExprs, ArrayList<PlanFragment> fragments, TypeManager typeManager) {
            this.scanNodes = scanNodes;
            this.outputExprs = outputExprs;
            this.fragments = fragments;
            this.typeManager = typeManager;
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

    public PhysicalPlan createPhysicalPlan(Plan plan, DescriptorTable descTbl, PlannerContext plannerContext, Map<VariableReferenceExpression, Expr> variableToSlotRef, TypeManager typeManager) {
        PhysicalPlanTranslator physicalPlanTranslator = new PhysicalPlanTranslator(new VariableAllocator(), typeManager);
        //PlanNode root = SimplePlanRewriter.rewriteWith(physicalPlanTranslator, plan.getRoot());
        List<org.apache.doris.planner.ScanNode> scanNodes = new ArrayList<>();
        List<Expr> outputExprs = new ArrayList<>();
        Map<LogicalPlanNode, TupleDescriptor> tupleDescriptorMap = new HashMap<>();
        FragmentProperties fraPro = new FragmentProperties(descTbl, plannerContext, variableToSlotRef, scanNodes, outputExprs, tupleDescriptorMap);
        PlanFragment root = physicalPlanTranslator.visitPlan(plan.getRoot(), fraPro);
        return new PhysicalPlan(scanNodes, outputExprs, fraPro.fragments, typeManager);
    }

    private static class PhysicalPlanTranslator
            extends PlanVisitor<PlanFragment, FragmentProperties> {
        VariableAllocator variableAllocator;
        TypeManager typeManager;

        public PhysicalPlanTranslator(VariableAllocator variableAllocator, TypeManager typeManager) {
            this.variableAllocator = variableAllocator;
            this.typeManager = typeManager;
        }

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
        }

        /*
        @Override
        public PlanFragment visitExchange(ExchangeNode node, FragmentProperties context) {
            if (node.getSources().size() < 2) {
                //FIXME : Add exchange partition hash
                PlanFragment inputFragment = visitPlan(node.getSources().get(0), context);

                org.apache.doris.planner.ExchangeNode exchangeNode =
                        new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), false);
                exchangeNode.setNumInstances(1);

                PlanFragment fragment = new PlanFragment(context.plannerContext.getNextFragmentId(), exchangeNode, inputFragment.getOutputPartition());
                inputFragment.setDestination(exchangeNode);

                for (int i = 0; i < node.getOutputVariables().size(); ++i) {
                    VariableReferenceExpression input = node.getInputs().get(0).get(i);
                    Expr expr = RowExpressionToExpr.formatRowExpression(input, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                    context.variableToSlotRef.put(node.getOutputVariables().get(i), expr);
                }

                context.fragments.add(fragment);
                return fragment;
            }

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

                    context.variableToSlotRef.put(node.getOutputVariables().get(i), expr);
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


        }

         */

        @Override
        public PlanFragment visitJoin(JoinNode node, FragmentProperties context) {
            /*
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
            */
            PlanFragment leftFragment = visitPlan(node.getLeft(), context);
            ArrayList<Expr> hashPartition = new ArrayList<>();
            List<VariableReferenceExpression> leftHashVar =
                    node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).collect(Collectors.toList());
            for (VariableReferenceExpression var : leftHashVar) {
                Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                hashPartition.add(expr);
            }
            leftFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));
            org.apache.doris.planner.ExchangeNode leftExchangeNode =
                    new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), leftFragment.getPlanRoot(), false);
            leftExchangeNode.setNumInstances(1);


            PlanFragment rightFragment = visitPlan(node.getRight(), context);
            hashPartition = new ArrayList<>();
            List<VariableReferenceExpression> rightHashVar =
                    node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).collect(Collectors.toList());
            for (VariableReferenceExpression var : rightHashVar) {
                Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                hashPartition.add(expr);
            }
            rightFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));
            org.apache.doris.planner.ExchangeNode rightExchangeNode =
                    new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), rightFragment.getPlanRoot(), false);
            rightExchangeNode.setNumInstances(1);


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
                            leftExchangeNode, rightExchangeNode,
                            JoinOperator.INNER_JOIN,
                            eqJoinConjuncts,
                            new ArrayList<>());
            hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);

            PlanFragment exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), hashJoinNode, DataPartition.hashPartitioned(hashPartition));
            leftFragment.setDestination(leftExchangeNode);
            rightFragment.setDestination(rightExchangeNode);
            context.fragments.add(exchangeFragment);
            return exchangeFragment;
        }

        @Override
        public PlanFragment visitSemiJoin(SemiJoinNode node, FragmentProperties context) {
            PlanFragment leftFragment = visitPlan(node.getSource(), context);
            ArrayList<Expr> hashPartition = new ArrayList<>();
            List<VariableReferenceExpression> leftHashVar = Lists.newArrayList(node.getSourceJoinVariable());
            for (VariableReferenceExpression var : leftHashVar) {
                Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                hashPartition.add(expr);
            }
            leftFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));
            org.apache.doris.planner.ExchangeNode leftExchangeNode =
                    new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), leftFragment.getPlanRoot(), false);
            leftExchangeNode.setNumInstances(1);


            PlanFragment rightFragment = visitPlan(node.getFilteringSource(), context);
            hashPartition = new ArrayList<>();
            List<VariableReferenceExpression> rightHashVar = Lists.newArrayList(node.getFilteringSourceJoinVariable());
            for (VariableReferenceExpression var : rightHashVar) {
                Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                hashPartition.add(expr);
            }
            rightFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));
            org.apache.doris.planner.ExchangeNode rightExchangeNode =
                    new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), rightFragment.getPlanRoot(), false);
            rightExchangeNode.setNumInstances(1);

            List<Expr> eqJoinConjuncts = new ArrayList<>();
            Expr left = RowExpressionToExpr.formatRowExpression(node.getSourceJoinVariable(),
                    new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
            Expr right = RowExpressionToExpr.formatRowExpression(node.getFilteringSourceJoinVariable(),
                    new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
            BinaryPredicate binaryEq = new BinaryPredicate(BinaryPredicate.Operator.EQ, left, right);
            eqJoinConjuncts.add(binaryEq);

            org.apache.doris.planner.HashJoinNode hashJoinNode =
                    new org.apache.doris.planner.HashJoinNode(
                            context.plannerContext.getNextNodeId(),
                            leftExchangeNode, rightExchangeNode,
                            JoinOperator.LEFT_SEMI_JOIN,
                            eqJoinConjuncts,
                            new ArrayList<>());
            hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);

            PlanFragment exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), hashJoinNode, DataPartition.hashPartitioned(hashPartition));
            leftFragment.setDestination(leftExchangeNode);
            rightFragment.setDestination(rightExchangeNode);
            context.fragments.add(exchangeFragment);
            return exchangeFragment;

            /*
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
                            leftExchangeNode, rightExchangeNode,
                            JoinOperator.INNER_JOIN,
                            eqJoinConjuncts,
                            new ArrayList<>());
            hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);

            PlanFragment exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), hashJoinNode, DataPartition.hashPartitioned(hashPartition));
            leftFragment.setDestination(leftExchangeNode);
            rightFragment.setDestination(rightExchangeNode);
            context.fragments.add(exchangeFragment);
            return exchangeFragment;




            PlanFragment leftFragment = visitPlan(node.getSource(), context);
            PlanFragment rightFragment = visitPlan(node.getFilteringSource(), context);

            List<Expr> eqJoinConjuncts = new ArrayList<>();
            Expr left = RowExpressionToExpr.formatRowExpression(node.getSourceJoinVariable(),
                    new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
            Expr right = RowExpressionToExpr.formatRowExpression(node.getFilteringSourceJoinVariable(),
                    new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
            BinaryPredicate binaryEq = new BinaryPredicate(BinaryPredicate.Operator.EQ, left, right);
            eqJoinConjuncts.add(binaryEq);


            org.apache.doris.planner.HashJoinNode hashJoinNode =
                    new org.apache.doris.planner.HashJoinNode(
                            context.plannerContext.getNextNodeId(),
                            leftFragment.getPlanRoot(), rightFragment.getPlanRoot(),
                            JoinOperator.LEFT_SEMI_JOIN,
                            eqJoinConjuncts,
                            new ArrayList<>());
            hashJoinNode.setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);

            context.fragments.remove(leftFragment);
            context.fragments.remove(rightFragment);
            leftFragment.setPlanRoot(hashJoinNode);
            context.fragments.add(leftFragment);
            return leftFragment;

             */
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

                context.variableToSlotRef.put(orderByVar, new SlotRef(slotDesc));
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

                    context.variableToSlotRef.put(var, new SlotRef(slotDesc));
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

                context.variableToSlotRef.put(orderByVar, new SlotRef(slotDesc));
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

                    context.variableToSlotRef.put(var, new SlotRef(slotDesc));
                    resolvedTupleExprs.add(sortExpr);
                }
            }

            tupleDescriptor.computeMemLayout();

            SortInfo sortInfo = new SortInfo(sortExprs, isAscOrder, isNullsFirst);
            sortInfo.setMaterializedTupleInfo(tupleDescriptor, resolvedTupleExprs);

            org.apache.doris.planner.SortNode sortNode =
                    new org.apache.doris.planner.SortNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), sortInfo, true, false, 0);
            sortNode.setLimit(65535);
            sortNode.resolvedTupleExprs = resolvedTupleExprs;

            inputFragment.setPlanRoot(sortNode);

            return inputFragment;
        }

        @Override
        public PlanFragment visitAggregation(AggregationNode node, FragmentProperties context) {
            PlanFragment inputFragment = visitPlan(node.getSource(), context);

            ArrayList<Expr> finalGrouping = new ArrayList<>();
            Map<VariableReferenceExpression, RowExpression> finalAggregation = new HashMap<>();

            //构建intermediateTupleDesc
            TupleDescriptor intermediateTupleDesc = context.descTbl.createTupleDescriptor();
            List<Expr> partitionExpr = Lists.newArrayList();

            // grouping expr
            ArrayList<Expr> groupingExprs = Lists.newArrayList();
            for(VariableReferenceExpression groupKey : node.getGroupingKeys()) {
                Expr groupExpr = RowExpressionToExpr.formatRowExpression(groupKey, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                //构建分区表达式
                groupingExprs.add(groupExpr);
                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(intermediateTupleDesc);
                slotDesc.setType(groupExpr.getType());
                slotDesc.setIsNullable(true);
                slotDesc.setIsMaterialized(true);

                partitionExpr.add(new SlotRef(slotDesc));
                finalGrouping.add(new SlotRef(slotDesc));
            }
            // agg expr
            ArrayList<FunctionCallExpr> aggExprs = Lists.newArrayList();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> aggregation : node.getAggregations().entrySet()) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) RowExpressionToExpr.formatRowExpression(aggregation.getValue().getCall(),
                        new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                aggExprs.add(functionCallExpr);

                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(intermediateTupleDesc);
                slotDesc.setType(aggregation.getValue().getCall().getFunctionHandle().getInterminateTypes().toDorisType());
                slotDesc.setIsNullable(true);
                slotDesc.setIsMaterialized(true);

                VariableReferenceExpression var = variableAllocator.newVariable(aggregation.toString(), aggregation.getKey().getType());
                context.variableToSlotRef.put(var, new SlotRef(slotDesc));
                finalAggregation.put(aggregation.getKey(),var);
            }
            intermediateTupleDesc.computeMemLayout();

            //构建outputTupleDesc
            TupleDescriptor outputTupleDesc = context.descTbl.createTupleDescriptor();
            // grouping expr
            for(VariableReferenceExpression groupKey : node.getGroupingKeys()) {
                Expr groupExpr = RowExpressionToExpr.formatRowExpression(groupKey, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(outputTupleDesc);
                slotDesc.setType(groupExpr.getType());
                slotDesc.setIsNullable(true);
                slotDesc.setIsMaterialized(true);
                context.variableToSlotRef.put(groupKey, new SlotRef(slotDesc));
            }
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> aggregation : node.getAggregations().entrySet()) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) RowExpressionToExpr.formatRowExpression(aggregation.getValue().getCall(),
                        new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(outputTupleDesc);
                slotDesc.setType(functionCallExpr.getType());
                slotDesc.setIsNullable(true);
                slotDesc.setIsMaterialized(true);
                context.variableToSlotRef.put(aggregation.getKey(), new SlotRef(slotDesc));
            }
            outputTupleDesc.computeMemLayout();

            //构建Partial Aggregation Node
            AggregateInfo aggInfo = AggregateInfo.create(groupingExprs, aggExprs, outputTupleDesc, intermediateTupleDesc, AggregateInfo.AggPhase.FIRST);
            org.apache.doris.planner.AggregationNode aggregationNode = new org.apache.doris.planner.AggregationNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), aggInfo);
            aggregationNode.unsetNeedsFinalize();
            aggregationNode.setIsPreagg(context.plannerContext);
            aggregationNode.setIntermediateTuple();
            inputFragment.setPlanRoot(aggregationNode);
            if (partitionExpr.isEmpty()) {
                inputFragment.setOutputPartition(DataPartition.UNPARTITIONED);
            } else {
                inputFragment.setOutputPartition(DataPartition.hashPartitioned(partitionExpr));
            }

            //构建Exchange Node
            org.apache.doris.planner.ExchangeNode exchangeNode =
                    new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(), false);
            exchangeNode.setNumInstances(1);
            PlanFragment fragment = new PlanFragment(context.plannerContext.getNextFragmentId(), exchangeNode, inputFragment.getOutputPartition());
            inputFragment.setDestination(exchangeNode);
            context.fragments.add(fragment);


            // agg expr
            ArrayList<FunctionCallExpr> aggExprs2 = Lists.newArrayList();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> aggregation : node.getAggregations().entrySet()) {
                FunctionHandle functionHandle = aggregation.getValue().getFunctionHandle();
                CallExpression call = new CallExpression(
                            functionHandle.getFunctionName(),
                            functionHandle,
                            typeManager.getType(functionHandle.getReturnType()),
                            Lists.newArrayList(finalAggregation.get(aggregation.getKey())));

                FunctionCallExpr expr = (FunctionCallExpr) RowExpressionToExpr.formatRowExpression(call, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                expr.setMergeAggFn();
                aggExprs2.add(expr);
            }

            AggregateInfo aggInfo2 = AggregateInfo.create(finalGrouping, aggExprs2, outputTupleDesc, intermediateTupleDesc, AggregateInfo.AggPhase.SECOND_MERGE);
            org.apache.doris.planner.AggregationNode aggregationNode2 = new org.apache.doris.planner.AggregationNode(context.plannerContext.getNextNodeId(), exchangeNode, aggInfo2);
            //aggregationNode.setIntermediateTuple();
            fragment.setPlanRoot(aggregationNode2);
            //inputFragment.setOutputPartition(DataPartition.hashPartitioned(partitionExpr));

            return fragment;
        }

        @Override
        public PlanFragment visitProject(ProjectNode node, FragmentProperties context) {
            PlanFragment inputFragment = visitPlan(node.getSource(), context);
            /*
            ArrayList<TupleId> tupleIds = inputFragment.getPlanRoot().getTupleIds();
            TupleDescriptor tupleDescriptor = context.descTbl.getTupleDesc(tupleIds.get(0));
            ArrayList<Expr> outputExpr = new ArrayList<>();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                Expr expr = RowExpressionToExpr.formatRowExpression(entry.getValue(), new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));

                SlotDescriptor slotDesc =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDesc.initFromExpr(expr);
                slotDesc.setIsNullable(true);
                //slotDesc.setIsMaterialized(true);

                context.variableToSlotRef.put(entry.getKey(), new SlotRef(slotDesc));
                outputExpr.add(expr);
            }
            inputFragment.setOutputExprs(outputExpr);
            tupleDescriptor.computeMemLayout();
             */

            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().getMap().entrySet()) {
                Expr expr = RowExpressionToExpr.formatRowExpression(entry.getValue(), new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                context.variableToSlotRef.put(entry.getKey(), expr);
            }
            return inputFragment;
        }

        @Override
        public PlanFragment visitLimit(LimitNode node, FragmentProperties context) {
            PlanFragment inputFragment = visitPlan(node.getSource(), context);
            inputFragment.getPlanRoot().setLimit(node.getCount());
            return inputFragment;
        }

        @Override
        public PlanFragment visitFilter(FilterNode node, FragmentProperties context) {
            System.out.println("visitFilter");
            PlanFragment inputFragment = visitPlan(node.getSource(), context);
            if (inputFragment.getPlanRoot() instanceof org.apache.doris.planner.OlapScanNode) {
                RowExpression rowExpression = node.getPredicate();
                inputFragment.getPlanRoot().addConjuncts(Lists.newArrayList(RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef))));
                return inputFragment;
            } else if (inputFragment.getPlanRoot() instanceof  org.apache.doris.planner.AggregationNode) {
                RowExpression rowExpression = node.getPredicate();
                inputFragment.getPlanRoot().addConjuncts(Lists.newArrayList(RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef))));
                return inputFragment;
            } else if (inputFragment.getPlanRoot() instanceof org.apache.doris.planner.HashJoinNode) {
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
                scanNode.computePartitionInfo();
                scanNode.updateScanRangeInfoByNewMVSelector(((OlapTable) referenceTable).getBaseIndexId(), true, null);
                scanNode.getScanRangeLocations();
            } catch (Exception ex) {
                ex.printStackTrace();
                return null;
            }

            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                DorisColumnHandle columnHandle = (DorisColumnHandle) entry.getValue();
                String colName = columnHandle.getColumnName();
                Type colType = columnHandle.getColumnType();

                SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDescriptor.setColumn(new Column(colName, colType.getTypeSignature().toDorisType()));
                slotDescriptor.setIsNullable(true);
                slotDescriptor.setIsMaterialized(true);

                context.variableToSlotRef.put(entry.getKey(), new SlotRef(slotDescriptor));
            }

            tupleDescriptor.computeMemLayout();
            context.scanNodes.add(scanNode);
            PlanFragment fragment = new PlanFragment(context.plannerContext.getNextFragmentId(), scanNode, DataPartition.RANDOM);
            context.fragments.add(fragment);
            return fragment;
        }

        @Override
        public PlanFragment visitEnforceSingleRow(EnforceSingleRowNode node, FragmentProperties context) {
            PlanFragment inputFragment = visitPlan(node.getSource(), context);
            AssertNumRowsNode root =
                    new AssertNumRowsNode(context.plannerContext.getNextNodeId(), inputFragment.getPlanRoot(),
                            new AssertNumRowsElement(1," ", AssertNumRowsElement.Assertion.LE));
            inputFragment.setPlanRoot(root);
            return inputFragment;
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl;
        private final PlannerContext plannerContext;
        private final Map<VariableReferenceExpression, Expr> variableToSlotRef;
        private final List<org.apache.doris.planner.ScanNode> scanNodes;
        public List<Expr> outputExprs;

        private final Map<LogicalPlanNode, TupleDescriptor> tupleDescriptorMap;

        private final ArrayList<PlanFragment> fragments = new ArrayList<>();

        FragmentProperties (DescriptorTable descTbl, PlannerContext plannerContext, Map<VariableReferenceExpression, Expr> variableToSlotRef,
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
