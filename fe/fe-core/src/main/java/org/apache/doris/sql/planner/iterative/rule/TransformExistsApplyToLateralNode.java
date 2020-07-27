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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.optimizations.PlanNodeDecorrelator;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.CoalesceExpression;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.ExistsPredicate;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.AggregationNode.globalAggregation;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static org.apache.doris.sql.planner.plan.LateralJoinNode.Type.INNER;
import static org.apache.doris.sql.planner.plan.LateralJoinNode.Type.LEFT;
import static org.apache.doris.sql.planner.plan.LimitNode.Step.FINAL;
import static org.apache.doris.sql.planner.plan.Patterns.applyNode;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.doris.sql.type.BigintType.BIGINT;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;

/**
 * EXISTS is modeled as (if correlated predicates are equality comparisons):
 * <pre>
 *     - Project(exists := COALESCE(subqueryTrue, false))
 *       - LateralJoin(LEFT)
 *         - input
 *         - Project(subqueryTrue := true)
 *           - Limit(count=1)
 *             - subquery
 * </pre>
 * or:
 * <pre>
 *     - LateralJoin(LEFT)
 *       - input
 *       - Project($0 > 0)
 *         - Aggregation(COUNT(*))
 *           - subquery
 * </pre>
 * otherwise
 */
public class TransformExistsApplyToLateralNode
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    private final FunctionResolution functionResolution;

    public TransformExistsApplyToLateralNode(FunctionManager functionManager)
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
    public Result apply(ApplyNode parent, Captures captures, Context context)
    {
        if (parent.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        Expression expression = castToExpression(getOnlyElement(parent.getSubqueryAssignments().getExpressions()));
        if (!(expression instanceof ExistsPredicate)) {
            return Result.empty();
        }

        Optional<LogicalPlanNode> nonDefaultAggregation = rewriteToNonDefaultAggregation(parent, context);
        return nonDefaultAggregation
                .map(Result::ofPlanNode)
                .orElseGet(() -> Result.ofPlanNode(rewriteToDefaultAggregation(parent, context)));
    }

    private Optional<LogicalPlanNode> rewriteToNonDefaultAggregation(ApplyNode applyNode, Context context)
    {
        checkState(applyNode.getSubquery().getOutputVariables().isEmpty(), "Expected subquery output variables to be pruned");

        VariableReferenceExpression exists = getOnlyElement(applyNode.getSubqueryAssignments().getVariables());
        VariableReferenceExpression subqueryTrue = context.getVariableAllocator().newVariable("subqueryTrue", BOOLEAN);

        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(identitiesAsSymbolReferences(applyNode.getInput().getOutputVariables()));
        assignments.put(exists, castToRowExpression(new CoalesceExpression(ImmutableList.of(new SymbolReference(subqueryTrue.getName()), BooleanLiteral.FALSE_LITERAL))));

        LogicalPlanNode subquery = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new LimitNode(
                        context.getIdAllocator().getNextId(),
                        applyNode.getSubquery(),
                        1L,
                        FINAL),
                Assignments.of(subqueryTrue, castToRowExpression(TRUE_LITERAL)));

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getVariableAllocator(), context.getLookup());
        if (!decorrelator.decorrelateFilters(subquery, applyNode.getCorrelation()).isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new ProjectNode(context.getIdAllocator().getNextId(),
                new LateralJoinNode(
                        applyNode.getId(),
                        applyNode.getInput(),
                        subquery,
                        applyNode.getCorrelation(),
                        LEFT,
                        applyNode.getOriginSubqueryError()),
                assignments.build()));
    }

    private LogicalPlanNode rewriteToDefaultAggregation(ApplyNode parent, Context context)
    {
        VariableReferenceExpression count = context.getVariableAllocator().newVariable("count", BIGINT);
        VariableReferenceExpression exists = getOnlyElement(parent.getSubqueryAssignments().getVariables());

        return new LateralJoinNode(
                parent.getId(),
                parent.getInput(),
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                parent.getSubquery(),
                                ImmutableMap.of(count, new AggregationNode.Aggregation(
                                        new CallExpression(
                                                "count",
                                                functionResolution.countFunction(),
                                                BIGINT,
                                                ImmutableList.of()),
                                        Optional.empty(),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())),
                                globalAggregation(),
                                ImmutableList.of(),
                                AggregationNode.Step.SINGLE,
                                Optional.empty(),
                                Optional.empty()),
                        Assignments.of(exists, castToRowExpression(new ComparisonExpression(GREATER_THAN, asSymbolReference(count), new Cast(new LongLiteral("0"), BIGINT.toString()))))),
                parent.getCorrelation(),
                INNER,
                parent.getOriginSubqueryError());
    }
}
