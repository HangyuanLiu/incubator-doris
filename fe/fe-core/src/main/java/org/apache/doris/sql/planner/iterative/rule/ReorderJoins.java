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

package org.apache.doris.sql.planner.iterative.rule;

import org.apache.doris.sql.expressions.LogicalRowExpressions;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.cost.CostComparator;
import org.apache.doris.sql.planner.cost.CostProvider;
import org.apache.doris.sql.planner.cost.PlanCostEstimate;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.DeterminismEvaluator;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.planner.RowExpressionEqualityInference;
import org.apache.doris.sql.planner.VariablesExtractor;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.JoinNode.DistributionType;
import org.apache.doris.sql.planner.plan.JoinNode.EquiJoinClause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.relational.RowExpressionDeterminismEvaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.doris.sql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static org.apache.doris.sql.expressions.LogicalRowExpressions.and;
import static org.apache.doris.sql.expressions.LogicalRowExpressions.extractConjuncts;
import static org.apache.doris.sql.metadata.Session.JoinDistributionType.AUTOMATIC;
import static org.apache.doris.sql.metadata.Session.getJoinDistributionType;
import static org.apache.doris.sql.planner.RowExpressionEqualityInference.createEqualityInference;
import static org.apache.doris.sql.planner.iterative.rule.DetermineJoinDistributionType.isBelowMaxBroadcastSize;
import static org.apache.doris.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.INFINITE_COST_RESULT;
import static org.apache.doris.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.UNKNOWN_COST_RESULT;
import static org.apache.doris.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode.toMultiJoinNode;
import static org.apache.doris.sql.planner.optimizations.JoinNodeUtils.toRowExpression;
import static org.apache.doris.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static org.apache.doris.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static org.apache.doris.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.INNER;
import static org.apache.doris.sql.planner.plan.Patterns.join;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.powerSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.apache.doris.sql.type.OperatorType.EQUAL;

public class ReorderJoins
        implements Rule<JoinNode>
{

    // We check that join distribution type is absent because we only want
    // to do this transformation once (reordered joins will have distribution type already set).
    private final Pattern<JoinNode> joinNodePattern;

    private final CostComparator costComparator;
    private final Metadata metadata;
    private final FunctionResolution functionResolution;
    private final DeterminismEvaluator determinismEvaluator;

    public ReorderJoins(CostComparator costComparator, Metadata metadata)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionManager());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionManager());

        this.joinNodePattern = join().matching(
                joinNode -> !joinNode.getDistributionType().isPresent()
                        && joinNode.getType() == INNER
                        && determinismEvaluator.isDeterministic(joinNode.getFilter().orElse(TRUE_CONSTANT)));
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return joinNodePattern;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        //return getJoinReorderingStrategy(session) == AUTOMATIC;
        return true;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        //MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), getMaxReorderedJoins(context.getSession()), functionResolution, determinismEvaluator);
        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context.getLookup(), 99, functionResolution, determinismEvaluator);
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                costComparator,
                multiJoinNode.getFilter(),
                context,
                determinismEvaluator,
                functionResolution,
                metadata);
        JoinEnumerationResult result = joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputVariables());
        if (!result.getPlanNode().isPresent()) {
            return Result.empty();
        }
        return Result.ofPlanNode(result.getPlanNode().get());
    }

    @VisibleForTesting
    static class JoinEnumerator
    {
        private final Session session;
        private final CostProvider costProvider;
        // Using Ordering to facilitate rule determinism
        private final Ordering<JoinEnumerationResult> resultComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final RowExpression allFilter;
        private final RowExpressionEqualityInference allFilterInference;
        private final LogicalRowExpressions logicalRowExpressions;
        private final Lookup lookup;
        private final Context context;

        private final Map<Set<LogicalPlanNode>, JoinEnumerationResult> memo = new HashMap<>();

        @VisibleForTesting
        JoinEnumerator(CostComparator costComparator, RowExpression filter, Context context, DeterminismEvaluator determinismEvaluator, FunctionResolution functionResolution, Metadata metadata)
        {
            this.context = requireNonNull(context);
            this.session = requireNonNull(context.getSession(), "session is null");
            this.costProvider = requireNonNull(context.getCostProvider(), "costProvider is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(context.getIdAllocator(), "idAllocator is null");
            this.allFilter = requireNonNull(filter, "filter is null");
            this.lookup = requireNonNull(context.getLookup(), "lookup is null");

            this.metadata = requireNonNull(metadata, "metadata is null");
            this.allFilterInference = createEqualityInference(metadata, filter);
            this.logicalRowExpressions = new LogicalRowExpressions(determinismEvaluator, functionResolution, metadata.getFunctionManager());
        }

        private JoinEnumerationResult chooseJoinOrder(LinkedHashSet<LogicalPlanNode> sources, List<VariableReferenceExpression> outputVariables)
        {
            context.checkTimeoutNotExhausted();

            Set<LogicalPlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            JoinEnumerationResult bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();
                Set<Set<Integer>> partitions = generatePartitions(sources.size());
                for (Set<Integer> partition : partitions) {
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(sources, outputVariables, partition);
                    if (result.equals(UNKNOWN_COST_RESULT)) {
                        memo.put(multiJoinKey, result);
                        return result;
                    }
                    if (!result.equals(INFINITE_COST_RESULT)) {
                        resultBuilder.add(result);
                    }
                }

                List<JoinEnumerationResult> results = resultBuilder.build();
                if (results.isEmpty()) {
                    memo.put(multiJoinKey, INFINITE_COST_RESULT);
                    return INFINITE_COST_RESULT;
                }

                bestResult = resultComparator.min(results);
                memo.put(multiJoinKey, bestResult);
            }

            //bestResult.LogicalPlanNode.ifPresent((LogicalPlanNode) -> log.debug("Least cost join was: %s", LogicalPlanNode));
            return bestResult;
        }

        /**
         * This method generates all the ways of dividing totalNodes into two sets
         * each containing at least one node. It will generate one set for each
         * possible partitioning. The other partition is implied in the absent values.
         * In order not to generate the inverse of any set, we always include the 0th
         * node in our sets.
         *
         * @return A set of sets each of which defines a partitioning of totalNodes
         */
        @VisibleForTesting
        static Set<Set<Integer>> generatePartitions(int totalNodes)
        {
            checkArgument(totalNodes > 1, "totalNodes must be greater than 1");
            Set<Integer> numbers = IntStream.range(0, totalNodes)
                    .boxed()
                    .collect(toImmutableSet());
            return powerSet(numbers).stream()
                    .filter(subSet -> subSet.contains(0))
                    .filter(subSet -> subSet.size() < numbers.size())
                    .collect(toImmutableSet());
        }

        @VisibleForTesting
        JoinEnumerationResult createJoinAccordingToPartitioning(LinkedHashSet<LogicalPlanNode> sources, List<VariableReferenceExpression> outputVariables, Set<Integer> partitioning)
        {
            List<LogicalPlanNode> sourceList = ImmutableList.copyOf(sources);
            LinkedHashSet<LogicalPlanNode> leftSources = partitioning.stream()
                    .map(sourceList::get)
                    .collect(toCollection(LinkedHashSet::new));
            LinkedHashSet<LogicalPlanNode> rightSources = sources.stream()
                    .filter(source -> !leftSources.contains(source))
                    .collect(toCollection(LinkedHashSet::new));
            return createJoin(leftSources, rightSources, outputVariables);
        }

        private JoinEnumerationResult createJoin(LinkedHashSet<LogicalPlanNode> leftSources, LinkedHashSet<LogicalPlanNode> rightSources, List<VariableReferenceExpression> outputVariables)
        {
            Set<VariableReferenceExpression> leftVariables = leftSources.stream()
                    .flatMap(node -> node.getOutputVariables().stream())
                    .collect(toImmutableSet());
            Set<VariableReferenceExpression> rightVariables = rightSources.stream()
                    .flatMap(node -> node.getOutputVariables().stream())
                    .collect(toImmutableSet());

            List<RowExpression> joinPredicates = getJoinPredicates(leftVariables, rightVariables);
            List<EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(JoinEnumerator::isJoinEqualityCondition)
                    .map(predicate -> toEquiJoinClause((CallExpression) predicate, leftVariables, context.getVariableAllocator()))
                    .collect(toImmutableList());
            if (joinConditions.isEmpty()) {
                return INFINITE_COST_RESULT;
            }
            List<RowExpression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<VariableReferenceExpression> requiredJoinVariables = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(outputVariables)
                    .addAll(VariablesExtractor.extractUnique(joinPredicates))
                    .build();

            JoinEnumerationResult leftResult = getJoinSource(
                    leftSources,
                    requiredJoinVariables.stream()
                            .filter(leftVariables::contains)
                            .collect(toImmutableList()));
            if (leftResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (leftResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            LogicalPlanNode left = leftResult.LogicalPlanNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            JoinEnumerationResult rightResult = getJoinSource(
                    rightSources,
                    requiredJoinVariables.stream()
                            .filter(rightVariables::contains)
                            .collect(toImmutableList()));
            if (rightResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (rightResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            LogicalPlanNode right = rightResult.LogicalPlanNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            // sort output variables so that the left input variables are first
            List<VariableReferenceExpression> sortedOutputVariables = Stream.concat(left.getOutputVariables().stream(), right.getOutputVariables().stream())
                    .filter(outputVariables::contains)
                    .collect(toImmutableList());

            return setJoinNodeProperties(new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    joinConditions,
                    sortedOutputVariables,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(and(joinFilters)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()));
        }

        private List<RowExpression> getJoinPredicates(Set<VariableReferenceExpression> leftVariables, Set<VariableReferenceExpression> rightVariables)
        {
            ImmutableList.Builder<RowExpression> joinPredicatesBuilder = ImmutableList.builder();
            // This takes all conjuncts that were part of allFilters that
            // could not be used for equality inference.
            // If they use both the left and right variables, we add them to the list of joinPredicates
            RowExpressionEqualityInference.Builder builder = new RowExpressionEqualityInference.Builder(metadata);
            StreamSupport.stream(builder.nonInferrableConjuncts(allFilter).spliterator(), false)
                    .map(conjunct -> allFilterInference.rewriteExpression(
                            conjunct,
                            variable -> leftVariables.contains(variable) || rightVariables.contains(variable)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right variables
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, leftVariables::contains) == null)
                    .filter(conjunct -> allFilterInference.rewriteExpression(conjunct, rightVariables::contains) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available variables
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            List<RowExpression> joinEqualities = allFilterInference.generateEqualitiesPartitionedBy(
                    variable -> leftVariables.contains(variable) || rightVariables.contains(variable)).getScopeEqualities();
            RowExpressionEqualityInference joinInference = createEqualityInference(metadata, joinEqualities.toArray(new RowExpression[0]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(in(leftVariables)).getScopeStraddlingEqualities());

            return joinPredicatesBuilder.build();
        }

        private JoinEnumerationResult getJoinSource(LinkedHashSet<LogicalPlanNode> nodes, List<VariableReferenceExpression> outputVariables)
        {
            if (nodes.size() == 1) {
                LogicalPlanNode LogicalPlanNode = getOnlyElement(nodes);
                ImmutableList.Builder<RowExpression> predicates = ImmutableList.builder();
                predicates.addAll(allFilterInference.generateEqualitiesPartitionedBy(outputVariables::contains).getScopeEqualities());
                RowExpressionEqualityInference.Builder builder = new RowExpressionEqualityInference.Builder(metadata);
                StreamSupport.stream(builder.nonInferrableConjuncts(allFilter).spliterator(), false)
                        .map(conjunct -> allFilterInference.rewriteExpression(conjunct, outputVariables::contains))
                        .filter(Objects::nonNull)
                        .forEach(predicates::add);
                RowExpression filter = logicalRowExpressions.combineConjuncts(predicates.build());
                if (!TRUE_CONSTANT.equals(filter)) {
                    LogicalPlanNode = new FilterNode(idAllocator.getNextId(), LogicalPlanNode, filter);
                }
                return createJoinEnumerationResult(LogicalPlanNode);
            }
            return chooseJoinOrder(nodes, outputVariables);
        }

        private static boolean isJoinEqualityCondition(RowExpression expression)
        {
            return expression instanceof CallExpression
                    && ((CallExpression) expression).getDisplayName().equals(EQUAL.getFunctionName().getFunctionName())
                    && ((CallExpression) expression).getArguments().size() == 2
                    && ((CallExpression) expression).getArguments().get(0) instanceof VariableReferenceExpression
                    && ((CallExpression) expression).getArguments().get(1) instanceof VariableReferenceExpression;
        }

        private static EquiJoinClause toEquiJoinClause(CallExpression equality, Set<VariableReferenceExpression> leftVariables, VariableAllocator variableAllocator)
        {
            checkArgument(equality.getArguments().size() == 2, "Unexpected number of arguments in binary operator equals");
            VariableReferenceExpression leftVariable = (VariableReferenceExpression) equality.getArguments().get(0);
            VariableReferenceExpression rightVariable = (VariableReferenceExpression) equality.getArguments().get(1);
            EquiJoinClause equiJoinClause = new EquiJoinClause(leftVariable, rightVariable);
            return leftVariables.contains(leftVariable) ? equiJoinClause : equiJoinClause.flip();
        }

        private JoinEnumerationResult setJoinNodeProperties(JoinNode joinNode)
        {
            // TODO avoid stat (but not cost) recalculation for all considered (distribution,flip) pairs, since resulting relation is the same in all case
            if (isAtMostScalar(joinNode.getRight(), lookup)) {
                return createJoinEnumerationResult(joinNode.withDistributionType(REPLICATED));
            }
            if (isAtMostScalar(joinNode.getLeft(), lookup)) {
                return createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(REPLICATED));
            }
            List<JoinEnumerationResult> possibleJoinNodes = getPossibleJoinNodes(joinNode, getJoinDistributionType(session));
            verify(!possibleJoinNodes.isEmpty(), "possibleJoinNodes is empty");
            if (possibleJoinNodes.stream().anyMatch(UNKNOWN_COST_RESULT::equals)) {
                return UNKNOWN_COST_RESULT;
            }
            return resultComparator.min(possibleJoinNodes);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, Session.JoinDistributionType distributionType)
        {
            checkArgument(joinNode.getType() == INNER, "unexpected join node type: %s", joinNode.getType());

            if (joinNode.isCrossJoin()) {
                return getPossibleJoinNodes(joinNode, REPLICATED);
            }

            switch (distributionType) {
                case PARTITIONED:
                    return getPossibleJoinNodes(joinNode, PARTITIONED);
                case BROADCAST:
                    return getPossibleJoinNodes(joinNode, REPLICATED);
                case AUTOMATIC:
                    ImmutableList.Builder<JoinEnumerationResult> result = ImmutableList.builder();
                    result.addAll(getPossibleJoinNodes(joinNode, PARTITIONED));
                    if (isBelowMaxBroadcastSize(joinNode, context)) {
                        result.addAll(getPossibleJoinNodes(joinNode, REPLICATED));
                    }
                    return result.build();
                default:
                    throw new IllegalArgumentException("unexpected join distribution type: " + distributionType);
            }
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType)
        {
            return ImmutableList.of(
                    createJoinEnumerationResult(joinNode.withDistributionType(distributionType)),
                    createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(distributionType)));
        }

        private JoinEnumerationResult createJoinEnumerationResult(LogicalPlanNode LogicalPlanNode)
        {
            return JoinEnumerationResult.createJoinEnumerationResult(Optional.of(LogicalPlanNode), costProvider.getCost(LogicalPlanNode));
        }
    }

    /**
     * This class represents a set of inner joins that can be executed in any order.
     */
    @VisibleForTesting
    static class MultiJoinNode
    {
        // Use a linked hash set to ensure optimizer is deterministic
        private final LinkedHashSet<LogicalPlanNode> sources;
        private final RowExpression filter;
        private final List<VariableReferenceExpression> outputVariables;

        public MultiJoinNode(LinkedHashSet<LogicalPlanNode> sources, RowExpression filter, List<VariableReferenceExpression> outputVariables)
        {
            checkArgument(sources.size() > 1, "sources size is <= 1");

            this.sources = requireNonNull(sources, "sources is null");
            this.filter = requireNonNull(filter, "filter is null");
            this.outputVariables = ImmutableList.copyOf(requireNonNull(outputVariables, "outputVariables is null"));

            List<VariableReferenceExpression> inputVariables = sources.stream().flatMap(source -> source.getOutputVariables().stream()).collect(toImmutableList());
            checkArgument(inputVariables.containsAll(outputVariables), "inputs do not contain all output variables");
        }

        public RowExpression getFilter()
        {
            return filter;
        }

        public LinkedHashSet<LogicalPlanNode> getSources()
        {
            return sources;
        }

        public List<VariableReferenceExpression> getOutputVariables()
        {
            return outputVariables;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sources, ImmutableSet.copyOf(extractConjuncts(filter)), outputVariables);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof MultiJoinNode)) {
                return false;
            }

            MultiJoinNode other = (MultiJoinNode) obj;
            return this.sources.equals(other.sources)
                    && ImmutableSet.copyOf(extractConjuncts(this.filter)).equals(ImmutableSet.copyOf(extractConjuncts(other.filter)))
                    && this.outputVariables.equals(other.outputVariables);
        }

        static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup, int joinLimit, FunctionResolution functionResolution, DeterminismEvaluator determinismEvaluator)
        {
            // the number of sources is the number of joins + 1
            return new JoinNodeFlattener(joinNode, lookup, joinLimit + 1, functionResolution, determinismEvaluator).toMultiJoinNode();
        }

        private static class JoinNodeFlattener
        {
            private final LinkedHashSet<LogicalPlanNode> sources = new LinkedHashSet<>();
            private final List<RowExpression> filters = new ArrayList<>();
            private final List<VariableReferenceExpression> outputVariables;
            private final FunctionResolution functionResolution;
            private final DeterminismEvaluator determinismEvaluator;
            private final Lookup lookup;

            JoinNodeFlattener(JoinNode node, Lookup lookup, int sourceLimit, FunctionResolution functionResolution, DeterminismEvaluator determinismEvaluator)
            {
                requireNonNull(node, "node is null");
                checkState(node.getType() == INNER, "join type must be INNER");
                this.outputVariables = node.getOutputVariables();
                this.lookup = requireNonNull(lookup, "lookup is null");
                this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
                this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
                flattenNode(node, sourceLimit);
            }

            private void flattenNode(LogicalPlanNode node, int limit)
            {
                LogicalPlanNode resolved = lookup.resolve(node);

                // (limit - 2) because you need to account for adding left and right side
                if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {
                    sources.add(node);
                    return;
                }

                JoinNode joinNode = (JoinNode) resolved;
                if (joinNode.getType() != INNER || !determinismEvaluator.isDeterministic(joinNode.getFilter().orElse(TRUE_CONSTANT)) || joinNode.getDistributionType().isPresent()) {
                    sources.add(node);
                    return;
                }

                // we set the left limit to limit - 1 to account for the node on the right
                flattenNode(joinNode.getLeft(), limit - 1);
                flattenNode(joinNode.getRight(), limit);
                joinNode.getCriteria().stream()
                        .map(criteria -> toRowExpression(criteria, functionResolution))
                        .forEach(filters::add);
                joinNode.getFilter().ifPresent(filters::add);
            }

            MultiJoinNode toMultiJoinNode()
            {
                return new MultiJoinNode(sources, and(filters), outputVariables);
            }
        }

        static class Builder
        {
            private List<LogicalPlanNode> sources;
            private RowExpression filter;
            private List<VariableReferenceExpression> outputVariables;

            public Builder setSources(LogicalPlanNode... sources)
            {
                this.sources = ImmutableList.copyOf(sources);
                return this;
            }

            public Builder setFilter(RowExpression filter)
            {
                this.filter = filter;
                return this;
            }

            public Builder setOutputVariables(VariableReferenceExpression... outputVariables)
            {
                this.outputVariables = ImmutableList.copyOf(outputVariables);
                return this;
            }

            public MultiJoinNode build()
            {
                return new MultiJoinNode(new LinkedHashSet<>(sources), filter, outputVariables);
            }
        }
    }

    @VisibleForTesting
    static class JoinEnumerationResult
    {
        public static final JoinEnumerationResult UNKNOWN_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.unknown());
        public static final JoinEnumerationResult INFINITE_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.infinite());

        private final Optional<LogicalPlanNode> LogicalPlanNode;
        private final PlanCostEstimate cost;

        private JoinEnumerationResult(Optional<LogicalPlanNode> LogicalPlanNode, PlanCostEstimate cost)
        {
            this.LogicalPlanNode = requireNonNull(LogicalPlanNode, "LogicalPlanNode is null");
            this.cost = requireNonNull(cost, "cost is null");
            checkArgument((cost.hasUnknownComponents() || cost.equals(PlanCostEstimate.infinite())) && !LogicalPlanNode.isPresent()
                            || (!cost.hasUnknownComponents() || !cost.equals(PlanCostEstimate.infinite())) && LogicalPlanNode.isPresent(),
                    "LogicalPlanNode should be present if and only if cost is known");
        }

        public Optional<LogicalPlanNode> getPlanNode()
        {
            return LogicalPlanNode;
        }

        public PlanCostEstimate getCost()
        {
            return cost;
        }

        static JoinEnumerationResult createJoinEnumerationResult(Optional<LogicalPlanNode> LogicalPlanNode, PlanCostEstimate cost)
        {
            if (cost.hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (cost.equals(PlanCostEstimate.infinite())) {
                return INFINITE_COST_RESULT;
            }
            return new JoinEnumerationResult(LogicalPlanNode, cost);
        }
    }
}
