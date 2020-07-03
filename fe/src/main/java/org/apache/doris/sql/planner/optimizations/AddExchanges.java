package org.apache.doris.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AddExchanges
        implements PlanOptimizer {
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
        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class ExchangeContext
    {
    }

    private static class Rewriter
            extends SimplePlanRewriter<ExchangeContext> {

        private final IdGenerator<PlanNodeId> idAllocator;

        private Rewriter(IdGenerator<PlanNodeId> idAllocator) {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public LogicalPlanNode visitPlan(LogicalPlanNode node, RewriteContext<ExchangeContext> context) {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public LogicalPlanNode visitAggregation(AggregationNode node, RewriteContext<ExchangeContext> context) {
            // otherwise, add a partial and final with an exchange in between
            Map<VariableReferenceExpression, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
            Map<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation originalAggregation = entry.getValue();
                String functionName = functionManager.getFunctionMetadata(originalAggregation.getFunctionHandle()).getName().getFunctionName();
                FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
                InternalAggregationFunction function = functionManager.getAggregateFunctionImplementation(functionHandle);
                VariableReferenceExpression intermediateVariable = context.getVariableAllocator().newVariable(functionName, function.getIntermediateType());

                checkState(!originalAggregation.getOrderBy().isPresent(), "Aggregate with ORDER BY does not support partial aggregation");
                intermediateAggregation.put(intermediateVariable, new AggregationNode.Aggregation(
                        new CallExpression(
                                functionName,
                                functionHandle,
                                function.getIntermediateType(),
                                originalAggregation.getArguments()),
                        originalAggregation.getFilter(),
                        originalAggregation.getOrderBy(),
                        originalAggregation.isDistinct(),
                        originalAggregation.getMask()));

                // rewrite final aggregation in terms of intermediate function
                finalAggregation.put(entry.getKey(),
                        new AggregationNode.Aggregation(
                                new CallExpression(
                                        functionName,
                                        functionHandle,
                                        function.getFinalType(),
                                        ImmutableList.<RowExpression>builder()
                                                .add(intermediateVariable)
                                                .addAll(originalAggregation.getArguments()
                                                        .stream()
                                                        .filter(PushPartialAggregationThroughExchange::isLambda)
                                                        .collect(toImmutableList()))
                                                .build()),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.empty()));
            }

            LogicalPlanNode partial = new AggregationNode(
                    context.getIdAllocator().getNextId(),
                    node.getSource(),
                    intermediateAggregation,
                    node.getGroupingSets(),
                    // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                    // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                    ImmutableList.of(),
                    AggregationNode.Step.PARTIAL,
                    node.getHashVariable(),
                    node.getGroupIdVariable());

            return new AggregationNode(
                    node.getId(),
                    partial,
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