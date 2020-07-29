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

import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.AggregationNode.Aggregation;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.ExpressionUtils;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.GenericLiteral;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.QuantifiedComparisonExpression;
import org.apache.doris.sql.tree.SearchedCaseExpression;
import org.apache.doris.sql.tree.SimpleCaseExpression;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.doris.sql.type.BooleanType;
import org.apache.doris.sql.type.Type;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.doris.sql.planner.SimplePlanRewriter.rewriteWith;
import static org.apache.doris.sql.planner.plan.AggregationNode.globalAggregation;
import static org.apache.doris.sql.ExpressionUtils.combineConjuncts;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static org.apache.doris.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static org.apache.doris.sql.tree.QuantifiedComparisonExpression.Quantifier.ALL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.type.BigintType.BIGINT;

public class TransformQuantifiedComparisonApplyToLateralJoin
        implements PlanOptimizer
{
    private final FunctionResolution functionResolution;

    public TransformQuantifiedComparisonApplyToLateralJoin(FunctionManager functionManager)
    {
        requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionManager);
    }

    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return rewriteWith(new Rewriter(functionResolution, idAllocator, variableAllocator), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<LogicalPlanNode>
    {
        private final FunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;

        public Rewriter(FunctionResolution functionResolution, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public LogicalPlanNode visitApply(ApplyNode node, RewriteContext<LogicalPlanNode> context)
        {
            if (node.getSubqueryAssignments().size() != 1) {
                return context.defaultRewrite(node);
            }

            Expression expression = castToExpression(getOnlyElement(node.getSubqueryAssignments().getExpressions()));
            if (!(expression instanceof QuantifiedComparisonExpression)) {
                return context.defaultRewrite(node);
            }

            QuantifiedComparisonExpression quantifiedComparison = (QuantifiedComparisonExpression) expression;

            return rewriteQuantifiedApplyNode(node, quantifiedComparison, context);
        }

        private LogicalPlanNode rewriteQuantifiedApplyNode(ApplyNode node, QuantifiedComparisonExpression quantifiedComparison, RewriteContext<LogicalPlanNode> context)
        {
            LogicalPlanNode subqueryPlan = context.rewrite(node.getSubquery());

            VariableReferenceExpression outputColumn = getOnlyElement(subqueryPlan.getOutputVariables());
            Type outputColumnType = outputColumn.getType();
            checkState(outputColumnType.isOrderable(), "Subquery result type must be orderable");

            VariableReferenceExpression minValue = variableAllocator.newVariable("min", outputColumnType);
            VariableReferenceExpression maxValue = variableAllocator.newVariable("max", outputColumnType);
            VariableReferenceExpression countAllValue = variableAllocator.newVariable("count_all", BIGINT);
            VariableReferenceExpression countNonNullValue = variableAllocator.newVariable("count_non_null", BIGINT);

            List<RowExpression> outputColumnReferences = ImmutableList.of(castToRowExpression(toSymbolReference(outputColumn)));

            subqueryPlan = new AggregationNode(
                    idAllocator.getNextId(),
                    subqueryPlan,
                    ImmutableMap.of(
                            minValue, new Aggregation(
                                    new CallExpression(
                                            "min",
                                            functionResolution.minFunction(outputColumnType),
                                            outputColumnType,
                                            outputColumnReferences),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.empty()),
                            maxValue, new Aggregation(
                                    new CallExpression(
                                            "max",
                                            functionResolution.maxFunction(outputColumnType),
                                            outputColumnType,
                                            outputColumnReferences),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.empty()),
                            countAllValue, new Aggregation(
                                    new CallExpression(
                                            "count",
                                            functionResolution.countFunction(),
                                            BIGINT,
                                            emptyList()),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.empty()),
                            countNonNullValue, new Aggregation(
                                    new CallExpression(
                                            "count",
                                            functionResolution.countFunction(outputColumnType),
                                            BIGINT,
                                            outputColumnReferences),
                                    Optional.empty(),
                                    Optional.empty(),
                                    false,
                                    Optional.empty())),
                    globalAggregation(),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            LogicalPlanNode lateralJoinNode = new LateralJoinNode(
                    node.getId(),
                    context.rewrite(node.getInput()),
                    subqueryPlan,
                    node.getCorrelation(),
                    LateralJoinNode.Type.INNER,
                    node.getOriginSubqueryError());

            Expression valueComparedToSubquery = rewriteUsingBounds(
                    quantifiedComparison,
                    minValue,
                    maxValue,
                    countAllValue,
                    countNonNullValue);

            VariableReferenceExpression quantifiedComparisonVariable = getOnlyElement(node.getSubqueryAssignments().getVariables());

            return projectExpressions(lateralJoinNode, Assignments.of(quantifiedComparisonVariable, castToRowExpression(valueComparedToSubquery)));
        }

        public Expression rewriteUsingBounds(
                QuantifiedComparisonExpression quantifiedComparison,
                VariableReferenceExpression minValue,
                VariableReferenceExpression maxValue,
                VariableReferenceExpression countAllValue,
                VariableReferenceExpression countNonNullValue)
        {
            BooleanLiteral emptySetResult = quantifiedComparison.getQuantifier().equals(ALL) ? TRUE_LITERAL : FALSE_LITERAL;
            Function<List<Expression>, Expression> quantifier = quantifiedComparison.getQuantifier().equals(ALL) ?
                    ExpressionUtils::combineConjuncts : ExpressionUtils::combineDisjuncts;
            Expression comparisonWithExtremeValue = getBoundComparisons(quantifiedComparison, minValue, maxValue);

            return new SimpleCaseExpression(
                    toSymbolReference(countAllValue),
                    ImmutableList.of(new WhenClause(
                            new GenericLiteral("bigint", "0"),
                            emptySetResult)),
                    Optional.of(quantifier.apply(ImmutableList.of(
                            comparisonWithExtremeValue,
                            new SearchedCaseExpression(
                                    ImmutableList.of(
                                            new WhenClause(
                                                    new ComparisonExpression(NOT_EQUAL, toSymbolReference(countAllValue), toSymbolReference(countNonNullValue)),
                                                    new Cast(new NullLiteral(), BooleanType.BOOLEAN.toString()))),
                                    Optional.of(emptySetResult))))));
        }

        private Expression getBoundComparisons(QuantifiedComparisonExpression quantifiedComparison, VariableReferenceExpression minValue, VariableReferenceExpression maxValue)
        {
            if (quantifiedComparison.getOperator() == EQUAL && quantifiedComparison.getQuantifier() == ALL) {
                // A = ALL B <=> min B = max B && A = min B
                return combineConjuncts(
                        new ComparisonExpression(EQUAL, toSymbolReference(minValue), toSymbolReference(maxValue)),
                        new ComparisonExpression(EQUAL, quantifiedComparison.getValue(), toSymbolReference(maxValue)));
            }

            if (EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL).contains(quantifiedComparison.getOperator())) {
                // A < ALL B <=> A < min B
                // A > ALL B <=> A > max B
                // A < ANY B <=> A < max B
                // A > ANY B <=> A > min B
                VariableReferenceExpression boundValue = shouldCompareValueWithLowerBound(quantifiedComparison) ? minValue : maxValue;
                return new ComparisonExpression(quantifiedComparison.getOperator(), quantifiedComparison.getValue(), toSymbolReference(boundValue));
            }
            throw new IllegalArgumentException("Unsupported quantified comparison: " + quantifiedComparison);
        }

        private static boolean shouldCompareValueWithLowerBound(QuantifiedComparisonExpression quantifiedComparison)
        {
            switch (quantifiedComparison.getQuantifier()) {
                case ALL:
                    switch (quantifiedComparison.getOperator()) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return true;
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return false;
                    }
                    break;
                case ANY:
                case SOME:
                    switch (quantifiedComparison.getOperator()) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return false;
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return true;
                    }
                    break;
            }
            throw new IllegalArgumentException("Unexpected quantifier: " + quantifiedComparison.getQuantifier());
        }

        private ProjectNode projectExpressions(LogicalPlanNode input, Assignments subqueryAssignments)
        {
            Assignments assignments = Assignments.builder()
                    .putAll(identitiesAsSymbolReferences(input.getOutputVariables()))
                    .putAll(subqueryAssignments)
                    .build();
            return new ProjectNode(
                    idAllocator.getNextId(),
                    input,
                    assignments);
        }

        private SymbolReference toSymbolReference(VariableReferenceExpression variable)
        {
            return new SymbolReference(variable.getName());
        }
    }
}
