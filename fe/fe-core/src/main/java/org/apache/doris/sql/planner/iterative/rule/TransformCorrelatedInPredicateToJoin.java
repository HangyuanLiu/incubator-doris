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


import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.VariablesExtractor;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.AssignUniqueId;
import org.apache.doris.sql.planner.plan.AssignmentUtils;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.InPredicate;
import org.apache.doris.sql.tree.IsNotNullPredicate;
import org.apache.doris.sql.tree.IsNullPredicate;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.NotExpression;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.SearchedCaseExpression;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.tree.WhenClause;
import org.apache.doris.sql.util.AstUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.execution.columnar.BOOLEAN;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.doris.sql.ExpressionUtils.and;
import static org.apache.doris.sql.ExpressionUtils.or;
import static org.apache.doris.sql.planner.iterative.matching.Pattern.nonEmpty;
import static org.apache.doris.sql.planner.plan.AggregationNode.singleGroupingSet;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static org.apache.doris.sql.planner.plan.Patterns.Apply.correlation;
import static org.apache.doris.sql.planner.plan.Patterns.applyNode;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.type.BigintType.BIGINT;

/**
 * Replaces correlated ApplyNode with InPredicate expression with SemiJoin
 * <p>
 * Transforms:
 * <pre>
 * - Apply (output: a in B.b)
 *    - input: some plan A producing symbol a
 *    - subquery: some plan B producing symbol b, using symbols from A
 * </pre>
 * Into:
 * <pre>
 * - Project (output: CASE WHEN (countmatches > 0) THEN true WHEN (countnullmatches > 0) THEN null ELSE false END)
 *   - Aggregate (countmatches=count(*) where a, b not null; countnullmatches where a,b null but buildSideKnownNonNull is not null)
 *     grouping by (A'.*)
 *     - LeftJoin on (A and B correlation condition)
 *       - AssignUniqueId (A')
 *         - A
 * </pre>
 * <p>
 *
 * @see TransformCorrelatedScalarAggregationToJoin
 */
public class TransformCorrelatedInPredicateToJoin
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .with(nonEmpty(correlation()));

    private final FunctionResolution functionResolution;

    public TransformCorrelatedInPredicateToJoin(FunctionManager functionManager)
    {
        requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionManager);
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode apply, Captures captures, Context context)
    {
        Assignments subqueryAssignments = apply.getSubqueryAssignments();
        if (subqueryAssignments.size() != 1) {
            return Result.empty();
        }
        Expression assignmentExpression = castToExpression(getOnlyElement(subqueryAssignments.getExpressions()));
        if (!(assignmentExpression instanceof InPredicate)) {
            return Result.empty();
        }

        InPredicate inPredicate = (InPredicate) assignmentExpression;
        VariableReferenceExpression inPredicateOutputVariable = getOnlyElement(subqueryAssignments.getVariables());

        return apply(apply, inPredicate, inPredicateOutputVariable, context.getLookup(), context.getIdAllocator(), context.getVariableAllocator());
    }

    private Result apply(
            ApplyNode apply,
            InPredicate inPredicate,
            VariableReferenceExpression inPredicateOutputVariable,
            Lookup lookup,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator)
    {
        Optional<Decorrelated> decorrelated = new DecorrelatingVisitor(lookup, apply.getCorrelation(), variableAllocator.getTypes())
                .decorrelate(apply.getSubquery());

        if (!decorrelated.isPresent()) {
            return Result.empty();
        }

        LogicalPlanNode projection = buildInPredicateEquivalent(
                apply,
                inPredicate,
                inPredicateOutputVariable,
                decorrelated.get(),
                idAllocator,
                variableAllocator);

        return Result.ofPlanNode(projection);
    }

    private LogicalPlanNode buildInPredicateEquivalent(
            ApplyNode apply,
            InPredicate inPredicate,
            VariableReferenceExpression inPredicateOutputVariable,
            Decorrelated decorrelated,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator)
    {
        Expression correlationCondition = and(decorrelated.getCorrelatedPredicates());
        LogicalPlanNode decorrelatedBuildSource = decorrelated.getDecorrelatedNode();

        AssignUniqueId probeSide = new AssignUniqueId(
                idAllocator.getNextId(),
                apply.getInput(),
                variableAllocator.newVariable("unique", BIGINT));

        VariableReferenceExpression buildSideKnownNonNull = variableAllocator.newVariable("buildSideKnownNonNull", BIGINT);
        ProjectNode buildSide = new ProjectNode(
                idAllocator.getNextId(),
                decorrelatedBuildSource,
                Assignments.builder()
                        .putAll(identitiesAsSymbolReferences(decorrelatedBuildSource.getOutputVariables()))
                        .put(buildSideKnownNonNull, castToRowExpression(bigint(0)))
                        .build());

        checkArgument(inPredicate.getValue() instanceof SymbolReference, "Unexpected expression: %s", inPredicate.getValue());
        SymbolReference probeSideSymbolReference = (SymbolReference) inPredicate.getValue();
        checkArgument(inPredicate.getValueList() instanceof SymbolReference, "Unexpected expression: %s", inPredicate.getValueList());
        SymbolReference buildSideSymbolReference = (SymbolReference) inPredicate.getValueList();

        Expression joinExpression = and(
                or(
                        new IsNullPredicate(probeSideSymbolReference),
                        new ComparisonExpression(ComparisonExpression.Operator.EQUAL, probeSideSymbolReference, buildSideSymbolReference),
                        new IsNullPredicate(buildSideSymbolReference)),
                correlationCondition);

        JoinNode leftOuterJoin = leftOuterJoin(idAllocator, probeSide, buildSide, joinExpression);

        VariableReferenceExpression countMatchesVariable = variableAllocator.newVariable("countMatches", BIGINT);
        VariableReferenceExpression countNullMatchesVariable = variableAllocator.newVariable("countNullMatches", BIGINT);

        Expression matchCondition = and(
                new IsNotNullPredicate(probeSideSymbolReference),
                new IsNotNullPredicate(buildSideSymbolReference));

        Expression nullMatchCondition = and(
                new IsNotNullPredicate(new SymbolReference(buildSideKnownNonNull.getName())),
                new NotExpression(matchCondition));

        AggregationNode aggregation = new AggregationNode(
                idAllocator.getNextId(),
                leftOuterJoin,
                ImmutableMap.<VariableReferenceExpression, AggregationNode.Aggregation>builder()
                        .put(countMatchesVariable, countWithFilter(matchCondition))
                        .put(countNullMatchesVariable, countWithFilter(nullMatchCondition))
                        .build(),
                singleGroupingSet(probeSide.getOutputVariables()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        // TODO since we care only about "some count > 0", we could have specialized node instead of leftOuterJoin that does the job without materializing join results
        SearchedCaseExpression inPredicateEquivalent = new SearchedCaseExpression(
                ImmutableList.of(
                        new WhenClause(isGreaterThan(countMatchesVariable, 0), booleanConstant(true)),
                        new WhenClause(isGreaterThan(countNullMatchesVariable, 0), booleanConstant(null))),
                Optional.of(booleanConstant(false)));
        return new ProjectNode(
                idAllocator.getNextId(),
                aggregation,
                Assignments.builder()
                        .putAll(identitiesAsSymbolReferences(apply.getInput().getOutputVariables()))
                        .put(inPredicateOutputVariable, castToRowExpression(inPredicateEquivalent))
                        .build());
    }

    private static JoinNode leftOuterJoin(PlanNodeIdAllocator idAllocator, AssignUniqueId probeSide, ProjectNode buildSide, Expression joinExpression)
    {
        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.LEFT,
                probeSide,
                buildSide,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(probeSide.getOutputVariables())
                        .addAll(buildSide.getOutputVariables())
                        .build(),
                Optional.of(castToRowExpression(joinExpression)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private AggregationNode.Aggregation countWithFilter(Expression condition)
    {
        return new AggregationNode.Aggregation(
                new CallExpression(
                        "count",
                        functionResolution.countFunction(),
                        BIGINT,
                        ImmutableList.of()),
                Optional.of(castToRowExpression(condition)),
                Optional.empty(),
                false,
                Optional.empty()); /* mask */
    }

    private static Expression isGreaterThan(VariableReferenceExpression variable, long value)
    {
        return new ComparisonExpression(
                ComparisonExpression.Operator.GREATER_THAN,
                new SymbolReference(variable.getName()),
                bigint(value));
    }

    private static Expression bigint(long value)
    {
        return new Cast(new LongLiteral(String.valueOf(value)), BIGINT.toString());
    }

    private static Expression booleanConstant(@Nullable Boolean value)
    {
        if (value == null) {
            return new Cast(new NullLiteral(), BOOLEAN.toString());
        }
        return new BooleanLiteral(value.toString());
    }

    private static class DecorrelatingVisitor
            extends PlanVisitor<Optional<Decorrelated>, LogicalPlanNode>
    {
        private final Lookup lookup;
        private final Set<VariableReferenceExpression> correlation;
        private final TypeProvider types;

        public DecorrelatingVisitor(Lookup lookup, Iterable<VariableReferenceExpression> correlation, TypeProvider types)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.correlation = ImmutableSet.copyOf(requireNonNull(correlation, "correlation is null"));
            this.types = requireNonNull(types, "types is null");
        }

        public Optional<Decorrelated> decorrelate(LogicalPlanNode reference)
        {
            return lookup.resolve(reference).accept(this, reference);
        }

        @Override
        public Optional<Decorrelated> visitProject(ProjectNode node, LogicalPlanNode reference)
        {
            if (isCorrelatedShallowly(node)) {
                // TODO: handle correlated projection
                return Optional.empty();
            }

            Optional<Decorrelated> result = decorrelate(node.getSource());
            return result.map(decorrelated -> {
                Assignments.Builder assignments = Assignments.builder()
                        .putAll(node.getAssignments());

                // Pull up all symbols used by a filter (except correlation)
                decorrelated.getCorrelatedPredicates().stream()
                        .flatMap(AstUtils::preOrder)
                        .filter(SymbolReference.class::isInstance)
                        .map(SymbolReference.class::cast)
                        .map(symbolReference -> new VariableReferenceExpression(symbolReference.getName(), types.get(symbolReference)))
                        .filter(variable -> !correlation.contains(variable))
                        .map(AssignmentUtils::identityAsSymbolReference)
                        .forEach(assignments::put);

                return new Decorrelated(
                        decorrelated.getCorrelatedPredicates(),
                        new ProjectNode(
                                node.getId(), // FIXME should I reuse or not?
                                decorrelated.getDecorrelatedNode(),
                                assignments.build()));
            });
        }

        @Override
        public Optional<Decorrelated> visitFilter(FilterNode node, LogicalPlanNode reference)
        {
            Optional<Decorrelated> result = decorrelate(node.getSource());
            return result.map(decorrelated ->
                    new Decorrelated(
                            ImmutableList.<Expression>builder()
                                    .addAll(decorrelated.getCorrelatedPredicates())
                                    // No need to retain uncorrelated conditions, predicate push down will push them back
                                    .add(castToExpression(node.getPredicate()))
                                    .build(),
                            decorrelated.getDecorrelatedNode()));
        }

        @Override
        public Optional<Decorrelated> visitPlan(LogicalPlanNode node, LogicalPlanNode reference)
        {
            if (isCorrelatedRecursively(node)) {
                return Optional.empty();
            }
            else {
                return Optional.of(new Decorrelated(ImmutableList.of(), reference));
            }
        }

        private boolean isCorrelatedRecursively(LogicalPlanNode node)
        {
            if (isCorrelatedShallowly(node)) {
                return true;
            }
            return node.getSources().stream()
                    .map(lookup::resolve)
                    .anyMatch(this::isCorrelatedRecursively);
        }

        private boolean isCorrelatedShallowly(LogicalPlanNode node)
        {
            return VariablesExtractor.extractUniqueNonRecursive(node, types).stream().anyMatch(correlation::contains);
        }
    }

    private static class Decorrelated
    {
        private final List<Expression> correlatedPredicates;
        private final LogicalPlanNode decorrelatedNode;

        public Decorrelated(List<Expression> correlatedPredicates, LogicalPlanNode decorrelatedNode)
        {
            this.correlatedPredicates = ImmutableList.copyOf(requireNonNull(correlatedPredicates, "correlatedPredicates is null"));
            this.decorrelatedNode = requireNonNull(decorrelatedNode, "decorrelatedNode is null");
        }

        public List<Expression> getCorrelatedPredicates()
        {
            return correlatedPredicates;
        }

        public LogicalPlanNode getDecorrelatedNode()
        {
            return decorrelatedNode;
        }
    }
}
