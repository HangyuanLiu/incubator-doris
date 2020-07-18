package org.apache.doris.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.FunctionMetadata;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.PartitioningScheme;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.type.TypeManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class AddExchanges implements PlanOptimizer {

    private final Metadata metadata;

    public AddExchanges(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan,
                                    Session session,
                                    TypeProvider types,
                                    VariableAllocator variableAllocator,
                                    IdGenerator<PlanNodeId> idAllocator,
                                    WarningCollector warningCollector) {
        return SimplePlanRewriter.rewriteWith(new Rewriter(variableAllocator, idAllocator, metadata.getFunctionManager(), metadata.getTypeManager()), plan, null);
    }

    private static class ExchangeContext
    {
    }

    private static class Rewriter
            extends SimplePlanRewriter<ExchangeContext> {
        private final VariableAllocator variableAllocator;
        private final IdGenerator<PlanNodeId> idAllocator;
        private final FunctionManager functionManager;
        private final TypeManager typeManager;

        private Rewriter(VariableAllocator variableAllocator, IdGenerator<PlanNodeId> idAllocator, FunctionManager functionManager, TypeManager typeManager) {
            this.variableAllocator = requireNonNull(variableAllocator);
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.functionManager = requireNonNull(functionManager);
            this.typeManager = typeManager;
        }

        @Override
        public LogicalPlanNode visitPlan(LogicalPlanNode node, RewriteContext<ExchangeContext> context) {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public LogicalPlanNode visitJoin(JoinNode node, RewriteContext<ExchangeContext> context) {

            List<VariableReferenceExpression> leftHashVar =
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).collect(Collectors.toList());
            List<VariableReferenceExpression> rigthHashVar =
                    node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).collect(Collectors.toList());


            List<VariableReferenceExpression> outputVar = new ArrayList<>();
            outputVar.addAll(node.getLeft().getOutputVariables());
            outputVar.addAll(node.getRight().getOutputVariables());

            ExchangeNode left = new ExchangeNode(
                    idAllocator.getNextId(),
                    ExchangeNode.Type.GATHER,
                    ExchangeNode.Scope.REMOTE_STREAMING,
                    new PartitioningScheme(node.getLeft().getOutputVariables(), Optional.of(leftHashVar)),
                    Lists.newArrayList(node.getLeft()),
                    ImmutableList.of(node.getLeft().getOutputVariables()));

            ExchangeNode right = new ExchangeNode(
                    idAllocator.getNextId(),
                    org.apache.doris.sql.planner.plan.ExchangeNode.Type.GATHER,
                    ExchangeNode.Scope.REMOTE_STREAMING,
                    new PartitioningScheme(node.getRight().getOutputVariables(), Optional.of(rigthHashVar)),
                    Lists.newArrayList(node.getRight()),
                    ImmutableList.of(node.getRight().getOutputVariables()));

            return new JoinNode(
                    node.getId(),
                    node.getType(), left, right, node.getCriteria(), node.getOutputVariables(),
                    node.getFilter(),
                    node.getLeftHashVariable(), node.getRightHashVariable(), Optional.of(JoinNode.DistributionType.PARTITIONED));
        }

        @Override
        public LogicalPlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<ExchangeContext> context) {

            List<VariableReferenceExpression> leftHashVar = Lists.newArrayList(node.getSourceJoinVariable());
            List<VariableReferenceExpression> rigthHashVar = Lists.newArrayList(node.getFilteringSourceJoinVariable());

            ExchangeNode left = new ExchangeNode(
                    idAllocator.getNextId(),
                    ExchangeNode.Type.GATHER,
                    ExchangeNode.Scope.REMOTE_STREAMING,
                    new PartitioningScheme(node.getSource().getOutputVariables(), Optional.of(leftHashVar)),
                    Lists.newArrayList(node.getSource()),
                    ImmutableList.of(node.getSource().getOutputVariables()));

            ExchangeNode right = new ExchangeNode(
                    idAllocator.getNextId(),
                    org.apache.doris.sql.planner.plan.ExchangeNode.Type.GATHER,
                    ExchangeNode.Scope.REMOTE_STREAMING,
                    new PartitioningScheme(node.getFilteringSource().getOutputVariables(), Optional.of(rigthHashVar)),
                    Lists.newArrayList(node.getFilteringSource()),
                    ImmutableList.of(node.getFilteringSource().getOutputVariables()));

            return new SemiJoinNode(node.getId(), left, right,
                        node.getSourceJoinVariable(), node.getFilteringSourceJoinVariable(), node.getSemiJoinOutput(), node.getSourceHashVariable(), node.getFilteringSourceHashVariable(),
                        Optional.of(SemiJoinNode.DistributionType.PARTITIONED));
        }

        /*
        @Override
        public LogicalPlanNode visitAggregation(AggregationNode node, RewriteContext<ExchangeContext> context) {
            LogicalPlanNode source = context.rewrite(node.getSource(), context.get());

            Map<VariableReferenceExpression, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
            Map<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
            List<VariableReferenceExpression> exchangeOutput = new ArrayList<>();
            for (VariableReferenceExpression groupingKey : node.getGroupingKeys()) {
                exchangeOutput.add(variableAllocator.newVariable(groupingKey));
            }

            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation originalAggregation = entry.getValue();
                FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
                //FIXME
                FunctionMetadata functionMetadata = functionManager.getFunctionMetadata(functionHandle);

                String functionName = functionMetadata.getName().getFunctionName();

                VariableReferenceExpression intermediateVariable =
                        variableAllocator.newVariable(functionName, typeManager.getType(functionHandle.getInterminateTypes()));
                VariableReferenceExpression exchangeVariable =
                        variableAllocator.newVariable(functionName, typeManager.getType(functionHandle.getInterminateTypes()));
                exchangeOutput.add(exchangeVariable);

                intermediateAggregation.put(intermediateVariable, new AggregationNode.Aggregation(
                        new CallExpression(
                                functionName,
                                functionHandle,
                                typeManager.getType(functionMetadata.getReturnType()),
                                originalAggregation.getArguments()),
                        originalAggregation.getFilter(),
                        originalAggregation.getOrderBy(),
                        originalAggregation.isDistinct(),
                        originalAggregation.getMask()));

                finalAggregation.put(entry.getKey(), new AggregationNode.Aggregation(
                                new CallExpression(
                                        functionName,
                                        functionHandle,
                                        typeManager.getType(functionHandle.getReturnType()),
                                        Lists.newArrayList(exchangeVariable)),
                                Optional.empty(),
                                Optional.empty(),
                                originalAggregation.isDistinct(),
                                Optional.empty()));
            }
            LogicalPlanNode partial = new AggregationNode(
                    node.getId(),
                    source,
                    intermediateAggregation,
                    node.getGroupingSets(),
                    // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                    // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                    ImmutableList.of(),
                    AggregationNode.Step.PARTIAL,
                    node.getHashVariable(),
                    node.getGroupIdVariable());

            ExchangeNode mergeNode = new ExchangeNode(
                    idAllocator.getNextId(),
                    org.apache.doris.sql.planner.plan.ExchangeNode.Type.GATHER,
                    ExchangeNode.Scope.REMOTE_STREAMING,
                    new PartitioningScheme(exchangeOutput, Optional.empty()),
                    Lists.newArrayList(partial),
                    ImmutableList.of(partial.getOutputVariables()));

            return new AggregationNode(
                    node.getId(),
                    mergeNode,
                    finalAggregation,
                    node.getGroupingSets(),
                    // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                    // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                    ImmutableList.of(),
                    AggregationNode.Step.FINAL,
                    node.getHashVariable(),
                    node.getGroupIdVariable());
        }
         */
    }
}