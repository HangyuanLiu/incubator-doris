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
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.type.BigintType;
import org.apache.doris.sql.type.TypeManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
        public LogicalPlanNode visitAggregation(AggregationNode node, RewriteContext<ExchangeContext> context) {
            Map<VariableReferenceExpression, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
            Map<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
            List<VariableReferenceExpression> exchangeOutput = new ArrayList<>(node.getGroupingKeys());

            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation originalAggregation = entry.getValue();
                FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
                FunctionMetadata functionMetadata = functionManager.getFunctionMetadata(functionHandle);

                String functionName = functionMetadata.getName().getFunctionName();

                VariableReferenceExpression intermediateVariable =
                        variableAllocator.newVariable(functionName, BigintType.BIGINT);
                VariableReferenceExpression exchangeVariable = variableAllocator.newVariable(functionName, BigintType.BIGINT);
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
                                        BigintType.BIGINT,
                                        Lists.newArrayList(exchangeVariable)),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.empty()));
            }
            LogicalPlanNode partial = new AggregationNode(
                    node.getId(),
                    node.getSource(),
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
    }
}