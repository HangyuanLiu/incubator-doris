/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.doris.sql.planner.optimizations;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.doris.sql.ExpressionUtils;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.VariablesExtractor;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.EnforceSingleRowNode;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.AggregationNode.singleGroupingSet;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.EQUAL;

public class PlanNodeDecorrelator
{
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;
    private final Lookup lookup;

    public PlanNodeDecorrelator(PlanNodeIdAllocator  idAllocator, VariableAllocator variableAllocator, Lookup lookup)
    {
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    public Optional<DecorrelatedNode> decorrelateFilters(LogicalPlanNode node, List<VariableReferenceExpression> correlation)
    {
        // TODO: when correlations list empty this should return immediately. However this isn't correct
        // right now, because for nested subqueries correlation list is empty while there might exists usages
        // of the outer most correlated symbols

        Optional<DecorrelationResult> decorrelationResultOptional = lookup.resolve(node).accept(new DecorrelatingVisitor(correlation), null);
        return decorrelationResultOptional.flatMap(decorrelationResult -> decorrelatedNode(
                decorrelationResult.correlatedPredicates,
                decorrelationResult.node,
                correlation));
    }

    private class DecorrelatingVisitor
            extends PlanVisitor<Optional<DecorrelationResult>, Void>
    {
        final List<VariableReferenceExpression> correlation;

        DecorrelatingVisitor(List<VariableReferenceExpression> correlation)
        {
            this.correlation = requireNonNull(correlation, "correlation is null");
        }

        @Override
        public Optional<DecorrelationResult> visitPlan(LogicalPlanNode node, Void context)
        {
            return Optional.of(new DecorrelationResult(
                    node,
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    ImmutableMultimap.of(),
                    false));
        }

        @Override
        public Optional<DecorrelationResult> visitFilter(FilterNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = Optional.of(new DecorrelationResult(
                    node.getSource(),
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    ImmutableMultimap.of(),
                    false));

            // try to decorrelate filters down the tree
            if (containsCorrelation(node.getSource(), correlation)) {
                childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            }

            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            Expression predicate = castToExpression(node.getPredicate());
            Map<Boolean, List<Expression>> predicates = ExpressionUtils.extractConjuncts(predicate).stream()
                    .collect(Collectors.partitioningBy(PlanNodeDecorrelator.DecorrelatingVisitor.this::isCorrelated));
            List<Expression> correlatedPredicates = ImmutableList.copyOf(predicates.get(true));
            List<Expression> uncorrelatedPredicates = ImmutableList.copyOf(predicates.get(false));

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            FilterNode newFilterNode = new FilterNode(
                    idAllocator.getNextId(),
                    childDecorrelationResult.node,
                    castToRowExpression(ExpressionUtils.combineConjuncts(uncorrelatedPredicates)));

            Set<VariableReferenceExpression> variablesToPropagate = Sets.difference(VariablesExtractor.extractUnique(correlatedPredicates, variableAllocator.getTypes()), ImmutableSet.copyOf(correlation));
            return Optional.of(new DecorrelationResult(
                    newFilterNode,
                    Sets.union(childDecorrelationResult.variablesToPropagate, variablesToPropagate),
                    ImmutableList.<Expression>builder()
                            .addAll(childDecorrelationResult.correlatedPredicates)
                            .addAll(correlatedPredicates)
                            .build(),
                    ImmutableMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                            .putAll(childDecorrelationResult.correlatedVariablesMapping)
                            .putAll(extractCorrelatedSymbolsMapping(correlatedPredicates))
                            .build(),
                    childDecorrelationResult.atMostSingleRow));
        }

        @Override
        public Optional<DecorrelationResult> visitLimit(LimitNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent() || node.getCount() == 0) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            if (childDecorrelationResult.atMostSingleRow) {
                return childDecorrelationResultOptional;
            }

            if (node.getCount() != 1) {
                return Optional.empty();
            }

            Set<VariableReferenceExpression> constantVariables = childDecorrelationResult.getConstantVariables();
            LogicalPlanNode decorrelatedChildNode = childDecorrelationResult.node;

            if (constantVariables.isEmpty() ||
                    !constantVariables.containsAll(decorrelatedChildNode.getOutputVariables())) {
                return Optional.empty();
            }

            // Rewrite limit to aggregation on constant symbols
            AggregationNode aggregationNode = new AggregationNode(
                    idAllocator.getNextId(),
                    decorrelatedChildNode,
                    ImmutableMap.of(),
                    singleGroupingSet(decorrelatedChildNode.getOutputVariables()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            return Optional.of(new DecorrelationResult(
                    aggregationNode,
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
                    true));
        }

        @Override
        public Optional<DecorrelationResult> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            return childDecorrelationResultOptional.filter(result -> result.atMostSingleRow);
        }

        @Override
        public Optional<DecorrelationResult> visitAggregation(AggregationNode node, Void context)
        {
            if (node.hasEmptyGroupingSet()) {
                return Optional.empty();
            }

            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            Set<VariableReferenceExpression> constantVariables = childDecorrelationResult.getConstantVariables();

            AggregationNode decorrelatedAggregation = childDecorrelationResult.getCorrelatedSymbolMapper()
                    .map(node, childDecorrelationResult.node);

            Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
            List<VariableReferenceExpression> variablesToAdd = childDecorrelationResult.variablesToPropagate.stream()
                    .filter(variable -> !groupingKeys.contains(variable))
                    .collect(Collectors.toList());

            if (!constantVariables.containsAll(variablesToAdd)) {
                return Optional.empty();
            }

            AggregationNode newAggregation = new AggregationNode(
                    decorrelatedAggregation.getId(),
                    decorrelatedAggregation.getSource(),
                    decorrelatedAggregation.getAggregations(),
                    singleGroupingSet(ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(node.getGroupingKeys())
                            .addAll(variablesToAdd)
                            .build()),
                    ImmutableList.of(),
                    decorrelatedAggregation.getStep(),
                    decorrelatedAggregation.getHashVariable(),
                    decorrelatedAggregation.getGroupIdVariable());

            boolean atMostSingleRow = newAggregation.getGroupingSetCount() == 1
                    && constantVariables.containsAll(newAggregation.getGroupingKeys());

            return Optional.of(new DecorrelationResult(
                    newAggregation,
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
                    atMostSingleRow));
        }

        @Override
        public Optional<DecorrelationResult> visitProject(ProjectNode node, Void context)
        {
            Optional<DecorrelationResult> childDecorrelationResultOptional = lookup.resolve(node.getSource()).accept(this, null);
            if (!childDecorrelationResultOptional.isPresent()) {
                return Optional.empty();
            }

            DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
            Set<VariableReferenceExpression> nodeOutputVariables = ImmutableSet.copyOf(node.getOutputVariables());
            List<VariableReferenceExpression> variablesToAdd = childDecorrelationResult.variablesToPropagate.stream()
                    .filter(variable -> !nodeOutputVariables.contains(variable))
                    .collect(Collectors.toList());

            Assignments assignments = Assignments.builder()
                    .putAll(node.getAssignments())
                    .putAll(identitiesAsSymbolReferences(variablesToAdd))
                    .build();

            return Optional.of(new DecorrelationResult(
                    new ProjectNode(idAllocator.getNextId(), childDecorrelationResult.node, assignments),
                    childDecorrelationResult.variablesToPropagate,
                    childDecorrelationResult.correlatedPredicates,
                    childDecorrelationResult.correlatedVariablesMapping,
                    childDecorrelationResult.atMostSingleRow));
        }

        private Multimap<VariableReferenceExpression, VariableReferenceExpression> extractCorrelatedSymbolsMapping(List<Expression> correlatedConjuncts)
        {
            // TODO: handle coercions and non-direct column references
            ImmutableMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> mapping = ImmutableMultimap.builder();
            for (Expression conjunct : correlatedConjuncts) {
                if (!(conjunct instanceof ComparisonExpression)) {
                    continue;
                }

                ComparisonExpression comparison = (ComparisonExpression) conjunct;
                if (!(comparison.getLeft() instanceof SymbolReference
                        && comparison.getRight() instanceof SymbolReference
                        && comparison.getOperator().equals(EQUAL))) {
                    continue;
                }

                VariableReferenceExpression left = variableAllocator.toVariableReference(comparison.getLeft());
                VariableReferenceExpression right = variableAllocator.toVariableReference(comparison.getRight());

                if (correlation.contains(left) && !correlation.contains(right)) {
                    mapping.put(left, right);
                }

                if (correlation.contains(right) && !correlation.contains(left)) {
                    mapping.put(right, left);
                }
            }

            return mapping.build();
        }

        private boolean isCorrelated(Expression expression)
        {
            return correlation.stream().anyMatch(VariablesExtractor.extractUnique(expression, variableAllocator.getTypes())::contains);
        }
    }

    private static class DecorrelationResult
    {
        final LogicalPlanNode node;
        final Set<VariableReferenceExpression> variablesToPropagate;
        final List<Expression> correlatedPredicates;

        // mapping from correlated symbols to their uncorrelated equivalence
        final Multimap<VariableReferenceExpression, VariableReferenceExpression> correlatedVariablesMapping;
        // If a subquery has at most single row for any correlation values?
        final boolean atMostSingleRow;

        DecorrelationResult(
                LogicalPlanNode node,
                Set<VariableReferenceExpression> variablesToPropagate,
                List<Expression> correlatedPredicates,
                Multimap<VariableReferenceExpression, VariableReferenceExpression> correlatedVariablesMapping,
                boolean atMostSingleRow)
        {
            this.node = node;
            this.variablesToPropagate = variablesToPropagate;
            this.correlatedPredicates = correlatedPredicates;
            this.atMostSingleRow = atMostSingleRow;
            this.correlatedVariablesMapping = correlatedVariablesMapping;
            checkState(variablesToPropagate.containsAll(correlatedVariablesMapping.values()), "Expected symbols to propagate to contain all constant symbols");
        }

        SymbolMapper getCorrelatedSymbolMapper()
        {
            SymbolMapper.Builder builder = SymbolMapper.builder();
            correlatedVariablesMapping.forEach(builder::put);
            return builder.build();
        }

        /**
         * @return constant symbols from a perspective of a subquery
         */
        Set<VariableReferenceExpression> getConstantVariables()
        {
            return ImmutableSet.copyOf(correlatedVariablesMapping.values());
        }
    }

    private Optional<DecorrelatedNode> decorrelatedNode(
            List<Expression> correlatedPredicates,
            LogicalPlanNode node,
            List<VariableReferenceExpression> correlation)
    {
        if (containsCorrelation(node, correlation)) {
            // node is still correlated ; /
            return Optional.empty();
        }
        return Optional.of(new DecorrelatedNode(correlatedPredicates, node));
    }

    private boolean containsCorrelation(LogicalPlanNode node, List<VariableReferenceExpression> correlation)
    {
        return Sets.union(VariablesExtractor.extractUnique(node, lookup, variableAllocator.getTypes()), VariablesExtractor.extractOutputVariables(node, lookup)).stream().anyMatch(correlation::contains);
    }

    public static class DecorrelatedNode
    {
        private final List<Expression> correlatedPredicates;
        private final LogicalPlanNode node;

        public DecorrelatedNode(List<Expression> correlatedPredicates, LogicalPlanNode node)
        {
            requireNonNull(correlatedPredicates, "correlatedPredicates is null");
            this.correlatedPredicates = ImmutableList.copyOf(correlatedPredicates);
            this.node = requireNonNull(node, "node is null");
        }

        public Optional<Expression> getCorrelatedPredicates()
        {
            if (correlatedPredicates.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(ExpressionUtils.and(correlatedPredicates));
        }

        public LogicalPlanNode getNode()
        {
            return node;
        }
    }
}
