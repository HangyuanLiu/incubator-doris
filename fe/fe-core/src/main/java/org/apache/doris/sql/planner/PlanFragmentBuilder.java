package org.apache.doris.sql.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import javafx.util.Pair;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.commons.validator.Var;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.AssertNumRowsElement;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.analysis.TupleIsNullPredicate;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.NotImplementedException;
import org.apache.doris.planner.AssertNumRowsNode;
import org.apache.doris.planner.CrossJoinNode;
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
import org.apache.doris.sql.planner.plan.AssignUniqueId;
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
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.RowExpressionToExpr;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeManager;
import org.apache.doris.sql.type.TypeSignature;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.sql.Row;

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
        FragmentProperties fraPro = new FragmentProperties(descTbl, plannerContext, variableToSlotRef, scanNodes, outputExprs);
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

        public PlanFragment visitJoin2(JoinNode node, FragmentProperties context) {
            //BROADCOST
            System.out.println("type : " + node.getDistributionType());
            if (node.getDistributionType().get().equals(JoinNode.DistributionType.REPLICATED)) {
                //Left exchange
                PlanFragment leftFragment = visitPlan(node.getLeft(), context);
                ArrayList<Expr> hashPartition = new ArrayList<>();
                List<VariableReferenceExpression> leftHashVar =
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).collect(Collectors.toList());
                for (VariableReferenceExpression var : leftHashVar) {
                    Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    hashPartition.add(expr);
                }
                //if (hashPartition.isEmpty()) {
                    leftFragment.setOutputPartition(DataPartition.UNPARTITIONED);
                //} else {
                 //   leftFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));
                //}

                //Right exchange
                PlanFragment rightFragment = visitPlan(node.getRight(), context);
                if (node.getRight() instanceof ProjectNode) {
                    ProjectNode rightTupleProjectNode = (ProjectNode) node.getRight();

                    for (Map.Entry<VariableReferenceExpression, RowExpression> entry : rightTupleProjectNode.getAssignments().getMap().entrySet()) {
                        if (!(entry.getValue() instanceof VariableReferenceExpression)) {
                            Expr expr = context.variableToSlotRef.get(entry.getKey());
                            try {
                                expr = TupleIsNullPredicate.wrapExpr(expr, rightFragment.getPlanRoot().getTupleIds(), null);
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                            context.variableToSlotRef.put(entry.getKey(), expr);
                        }
                    }
                }

                hashPartition = new ArrayList<>();
                List<VariableReferenceExpression> rightHashVar =
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).collect(Collectors.toList());
                for (VariableReferenceExpression var : rightHashVar) {
                    Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    hashPartition.add(expr);
                }
                //if (hashPartition.isEmpty()) {
                    rightFragment.setOutputPartition(DataPartition.UNPARTITIONED);
                //} else {
                 //   rightFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));
               // }
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

                org.apache.doris.planner.PlanNode joinNode;
                if (node.getCriteria().isEmpty()) {
                    joinNode = new CrossJoinNode(context.plannerContext.getNextNodeId(), leftFragment.getPlanRoot(), rightExchangeNode, null);
                } else {
                    JoinOperator joinOperator;
                    if (node.getType().equals(JoinNode.Type.LEFT)) {
                        joinOperator = JoinOperator.LEFT_OUTER_JOIN;
                    } else if (node.getType().equals(JoinNode.Type.INNER)) {
                        joinOperator = JoinOperator.INNER_JOIN;
                    } else {
                        System.out.println("broadcost join type not implement : " + node.getType());
                        return null;
                    }

                    List<Expr> joinFilters = new ArrayList<>();
                    if (node.getFilter().isPresent()) {
                        joinFilters =
                                Lists.newArrayList(RowExpressionToExpr.formatRowExpression(node.getFilter().get(),
                                        new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef)));
                    }
                    joinNode = new org.apache.doris.planner.HashJoinNode(
                            context.plannerContext.getNextNodeId(),
                            leftFragment.getPlanRoot(), rightExchangeNode,
                            joinOperator,
                            eqJoinConjuncts, joinFilters);

                    ((HashJoinNode)joinNode).setDistributionMode(HashJoinNode.DistributionMode.BROADCAST);
                }
                leftFragment.setPlanRoot(joinNode);
                context.fragments.remove(leftFragment);
                context.fragments.add(leftFragment);
                rightFragment.setDestination(rightExchangeNode);
                return leftFragment;
            }


            //////////////////////////SHUFFLE JOIN//////////////////

            //Left exchange
            PlanFragment leftFragment = visitPlan(node.getLeft(), context);
            ArrayList<Expr> hashPartition = new ArrayList<>();
            List<VariableReferenceExpression> leftHashVar =
                    node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).collect(Collectors.toList());
            for (VariableReferenceExpression var : leftHashVar) {
                Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                hashPartition.add(expr);
            }
            if (hashPartition.isEmpty()) {
                leftFragment.setOutputPartition(DataPartition.UNPARTITIONED);
            } else {
                leftFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));
            }
            org.apache.doris.planner.ExchangeNode leftExchangeNode =
                    new org.apache.doris.planner.ExchangeNode(context.plannerContext.getNextNodeId(), leftFragment.getPlanRoot(), false);
            leftExchangeNode.setNumInstances(1);

            //Right exchange
            PlanFragment rightFragment = visitPlan(node.getRight(), context);
            if (node.getRight() instanceof ProjectNode) {
                ProjectNode rightTupleProjectNode = (ProjectNode) node.getRight();

                for (Map.Entry<VariableReferenceExpression, RowExpression> entry : rightTupleProjectNode.getAssignments().getMap().entrySet()) {
                    if (!(entry.getValue() instanceof VariableReferenceExpression)) {
                        Expr expr = context.variableToSlotRef.get(entry.getKey());
                        try {
                            expr = TupleIsNullPredicate.wrapExpr(expr, rightFragment.getPlanRoot().getTupleIds(), null);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        context.variableToSlotRef.put(entry.getKey(), expr);
                    }
                }
            }

            hashPartition = new ArrayList<>();
            List<VariableReferenceExpression> rightHashVar =
                    node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).collect(Collectors.toList());
            for (VariableReferenceExpression var : rightHashVar) {
                Expr expr = RowExpressionToExpr.formatRowExpression(var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                hashPartition.add(expr);
            }
            if (hashPartition.isEmpty()) {
                rightFragment.setOutputPartition(DataPartition.UNPARTITIONED);
            } else {
                rightFragment.setOutputPartition(DataPartition.hashPartitioned(hashPartition));
            }
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

            org.apache.doris.planner.PlanNode joinNode;
            PlanFragment exchangeFragment;
            if (node.getCriteria().isEmpty()) {
               joinNode = new CrossJoinNode(context.plannerContext.getNextNodeId(), leftExchangeNode, rightExchangeNode, null);
               exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), joinNode, DataPartition.UNPARTITIONED);
            } else {
                JoinOperator joinOperator;
                if (node.getType().equals(JoinNode.Type.LEFT)) {
                    joinOperator = JoinOperator.LEFT_OUTER_JOIN;
                } else if (node.getType().equals(JoinNode.Type.INNER)) {
                    joinOperator = JoinOperator.INNER_JOIN;
                } else if (node.getType().equals(JoinNode.Type.RIGHT)) {
                    joinOperator = JoinOperator.RIGHT_OUTER_JOIN;
                } else {
                    System.out.println("shuffle join type not implement : " + node.getType());
                    return null;
                }

                List<Expr> joinFilters = new ArrayList<>();
                if (node.getFilter().isPresent()) {
                    joinFilters =
                            Lists.newArrayList(RowExpressionToExpr.formatRowExpression(node.getFilter().get(),
                                    new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef)));
                }
                joinNode = new org.apache.doris.planner.HashJoinNode(
                                context.plannerContext.getNextNodeId(),
                                leftExchangeNode, rightExchangeNode,
                                joinOperator,
                                eqJoinConjuncts, joinFilters);

                ((HashJoinNode)joinNode).setDistributionMode(HashJoinNode.DistributionMode.PARTITIONED);
                exchangeFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), joinNode, DataPartition.hashPartitioned(hashPartition));
            }

            leftFragment.setDestination(leftExchangeNode);
            rightFragment.setDestination(rightExchangeNode);
            context.fragments.add(exchangeFragment);
            return exchangeFragment;
        }

        public PlanFragment visitJoin(JoinNode node, FragmentProperties context) {
            PlanFragment leftFragment;
            PlanFragment rightFragment;
            JoinOperator joinOperator;

            //确认Join类型，right join会转成left join
            LogicalPlanNode rightNodeToTupleIsNull;
            if (node.getType().equals(JoinNode.Type.RIGHT)) {
                leftFragment = visitPlan(node.getRight(), context);
                rightFragment = visitPlan(node.getLeft(), context);
                joinOperator = JoinOperator.LEFT_OUTER_JOIN;
                rightNodeToTupleIsNull = node.getLeft();
            } else if (node.getType().equals(JoinNode.Type.LEFT)){
                leftFragment = visitPlan(node.getLeft(), context);
                rightFragment = visitPlan(node.getRight(), context);
                joinOperator = JoinOperator.LEFT_OUTER_JOIN;
                rightNodeToTupleIsNull = node.getRight();
            } else {
                leftFragment = visitPlan(node.getLeft(), context);
                rightFragment = visitPlan(node.getRight(), context);
                joinOperator = JoinOperator.INNER_JOIN;
                rightNodeToTupleIsNull = node.getRight();
            }

            //构建等值条件，在TupleIsNull构造之前
            List<Expr> eqJoinConjuncts = new ArrayList<>();
            List<Expr> otherjoinFilters = new ArrayList<>();
            if (node.getCriteria().isEmpty()) {
                joinOperator = JoinOperator.CROSS_JOIN;
            } else {
                for (JoinNode.EquiJoinClause equiJoinClause : node.getCriteria()) {
                    Expr left = RowExpressionToExpr.formatRowExpression(equiJoinClause.getLeft(),
                            new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    Expr right = RowExpressionToExpr.formatRowExpression(equiJoinClause.getRight(),
                            new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
                    BinaryPredicate binaryEq;
                    if (node.getType().equals(JoinNode.Type.RIGHT)) {
                        binaryEq = new BinaryPredicate(BinaryPredicate.Operator.EQ, right, left);
                    } else {
                        binaryEq = new BinaryPredicate(BinaryPredicate.Operator.EQ, left, right);
                    }
                    eqJoinConjuncts.add(binaryEq);
                }

                if (node.getFilter().isPresent()) {
                    otherjoinFilters = Lists.newArrayList(RowExpressionToExpr.formatRowExpression(node.getFilter().get(),
                                    new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef)));
                }
            }

            //为右表添加TupleIsNull
            if (rightNodeToTupleIsNull instanceof ProjectNode) {
                ProjectNode rightTupleProjectNode = (ProjectNode) rightNodeToTupleIsNull;
                for (Map.Entry<VariableReferenceExpression, RowExpression> entry : rightTupleProjectNode.getAssignments().getMap().entrySet()) {
                    if (!(entry.getValue() instanceof VariableReferenceExpression)) {
                        Expr expr = context.variableToSlotRef.get(entry.getKey());
                        try {
                            expr = TupleIsNullPredicate.wrapExpr(expr, rightFragment.getPlanRoot().getTupleIds(), null);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        context.variableToSlotRef.put(entry.getKey(), expr);
                    }
                }
            }

            PlanNode leftPlanNode;
            PlanNode rightPlanNode;
            HashJoinNode.DistributionMode distributionMode;
            if (node.getDistributionType().get().equals(JoinNode.DistributionType.REPLICATED)) {
                distributionMode = HashJoinNode.DistributionMode.BROADCAST;
                leftPlanNode = leftFragment.getPlanRoot();
                rightPlanNode = new org.apache.doris.planner.ExchangeNode(
                        context.plannerContext.getNextNodeId(), rightFragment.getPlanRoot(), false);
                rightPlanNode.setNumInstances(1);
            } else {
                distributionMode = HashJoinNode.DistributionMode.PARTITIONED;
                leftPlanNode = new org.apache.doris.planner.ExchangeNode(
                        context.plannerContext.getNextNodeId(), leftFragment.getPlanRoot(), false);
                leftPlanNode.setNumInstances(1);

                rightPlanNode = new org.apache.doris.planner.ExchangeNode(
                        context.plannerContext.getNextNodeId(), rightFragment.getPlanRoot(), false);
                rightPlanNode.setNumInstances(1);
            }

            org.apache.doris.planner.PlanNode joinNode;
            if (joinOperator.isCrossJoin()) {
                joinNode = new org.apache.doris.planner.CrossJoinNode(context.plannerContext.getNextNodeId(), leftPlanNode, rightPlanNode, null);
            } else {
                joinNode = new org.apache.doris.planner.HashJoinNode(
                        context.plannerContext.getNextNodeId(),
                        leftPlanNode, rightPlanNode,
                        joinOperator,
                        eqJoinConjuncts, otherjoinFilters);

                ((HashJoinNode)joinNode).setDistributionMode(distributionMode);
            }

            if (node.getDistributionType().get().equals(JoinNode.DistributionType.REPLICATED)) {
                leftFragment.setPlanRoot(joinNode);
                context.fragments.remove(leftFragment);
                context.fragments.add(leftFragment);
                rightFragment.setDestination((org.apache.doris.planner.ExchangeNode) rightPlanNode);
                return leftFragment;
            } else {
                List<Expr> leftHash = node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).map(
                        var -> RowExpressionToExpr.formatRowExpression(
                                var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef)))
                        .collect(Collectors.toList());

                List<Expr> rightHash = node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).map(
                        var -> RowExpressionToExpr.formatRowExpression(
                                var, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef)))
                        .collect(Collectors.toList());

                List<Expr> swap;
                if (node.getType().equals(JoinNode.Type.RIGHT)) {
                    swap = leftHash;
                    leftHash = rightHash;
                    rightHash = swap;
                }

                //join fragment
                PlanFragment joinFragment = new PlanFragment(context.plannerContext.getNextFragmentId(), joinNode, DataPartition.hashPartitioned(leftHash));
                //left fragment
                leftFragment.setOutputPartition(leftHash.isEmpty() ? DataPartition.UNPARTITIONED : DataPartition.hashPartitioned(leftHash));
                leftFragment.setDestination((org.apache.doris.planner.ExchangeNode) leftPlanNode);
                //right fragment
                rightFragment.setOutputPartition(rightHash.isEmpty() ? DataPartition.UNPARTITIONED : DataPartition.hashPartitioned(rightHash));
                rightFragment.setDestination((org.apache.doris.planner.ExchangeNode) rightPlanNode);

                context.fragments.add(joinFragment);
                return joinFragment;
            }
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
            context.variableToSlotRef.put(node.getSemiJoinOutput(), binaryEq);

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
            PlanFragment inputFragment = visitPlan(node.getSource(), context);
            RowExpression rowExpression = node.getPredicate();
            Expr exprPredicate = RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef));
            if (exprPredicate instanceof CompoundPredicate && node.getSource() instanceof SemiJoinNode) {
                if (((CompoundPredicate) exprPredicate).getOp().equals(CompoundPredicate.Operator.NOT)) {
                    HashJoinNode root = (HashJoinNode) inputFragment.getPlanRoot();
                    root.setJoinOp(JoinOperator.LEFT_ANTI_JOIN);
                    return inputFragment;
                }
            }

            inputFragment.getPlanRoot().addConjuncts(Lists.newArrayList(RowExpressionToExpr.formatRowExpression(rowExpression, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef))));
            return inputFragment;
        }

        public PlanFragment visitValues(ValuesNode node, FragmentProperties context) {
            TupleDescriptor tupleDescriptor = context.descTbl.createTupleDescriptor();
            org.apache.doris.planner.UnionNode unionNode = new org.apache.doris.planner.UnionNode(context.plannerContext.getNextNodeId(), tupleDescriptor.getId());

            for(VariableReferenceExpression var : node.getOutputVariables()) {
                SlotDescriptor slotDescriptor =  context.descTbl.addSlotDescriptor(tupleDescriptor);
                slotDescriptor.setColumn(new Column(var.getName(), var.getType().getTypeSignature().toDorisType()));
                slotDescriptor.setIsNullable(true);
                slotDescriptor.setIsMaterialized(true);

                context.variableToSlotRef.put(var, new SlotRef(slotDescriptor));
            }

            for (int rowIpos = 0; rowIpos < node.getRows().size(); rowIpos++) {
                List<Expr> exprRow = node.getRows().get(rowIpos).stream().map( entry ->
                        RowExpressionToExpr.formatRowExpression(entry, new RowExpressionToExpr.FormatterContext(context.descTbl, context.variableToSlotRef)))
                        .collect(Collectors.toList());
                unionNode.addConstExprList(exprRow);
            }

            tupleDescriptor.computeMemLayout();
            PlanFragment fragment = new PlanFragment(context.plannerContext.getNextFragmentId(), unionNode, DataPartition.UNPARTITIONED);
            context.fragments.add(fragment);
            return fragment;
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

        @Override
        public PlanFragment visitAssignUniqueId(AssignUniqueId node, FragmentProperties context) {
            return visitPlan(node.getSource(), context);
        }
    }
    private static class FragmentProperties {
        private final DescriptorTable descTbl;
        private final PlannerContext plannerContext;
        private final Map<VariableReferenceExpression, Expr> variableToSlotRef;
        private final List<org.apache.doris.planner.ScanNode> scanNodes;
        public List<Expr> outputExprs;

        private final ArrayList<PlanFragment> fragments = new ArrayList<>();

        FragmentProperties (DescriptorTable descTbl, PlannerContext plannerContext, Map<VariableReferenceExpression, Expr> variableToSlotRef,
                            List<org.apache.doris.planner.ScanNode> scanNodes, List<Expr> outputExprs) {
            this.descTbl = descTbl;
            this.plannerContext = plannerContext;
            this.variableToSlotRef = variableToSlotRef;
            this.scanNodes = scanNodes;
            this.outputExprs = outputExprs;
        }
    }
}
