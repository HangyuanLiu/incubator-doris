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

import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.MarkDistinctNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.plan.AssignUniqueId;
import org.apache.doris.sql.planner.plan.EnforceSingleRowNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.tree.SimpleCaseExpression;
import org.apache.doris.sql.tree.StringLiteral;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.type.BooleanType;

import java.util.Optional;

import static org.apache.doris.sql.planner.iterative.matching.Pattern.nonEmpty;
import static org.apache.doris.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.apache.doris.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static org.apache.doris.sql.planner.plan.Patterns.LateralJoin.correlation;
import static org.apache.doris.sql.planner.plan.Patterns.lateralJoin;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.apache.doris.sql.type.BigintType.BIGINT;
import static org.apache.doris.sql.type.StandardTypes.BOOLEAN;

/**
 * Scalar filter scan query is something like:
 * <pre>
 *     SELECT a,b,c FROM rel WHERE a = correlated1 AND b = correlated2
 * </pre>
 * <p>
 * This optimizer can rewrite to mark distinct and filter over a left outer join:
 * <p>
 * From:
 * <pre>
 * - LateralJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (scalar subquery) Project F
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Filter(CASE isDistinct WHEN true THEN true ELSE fail('Scalar sub-query has returned multiple rows'))
 *   - MarkDistinct(isDistinct)
 *     - LateralJoin (with correlation list: [C])
 *       - AssignUniqueId(adds symbol U)
 *         - (input) plan which produces symbols: [A, B, C]
 *       - non scalar subquery
 * </pre>
 * <p>
 * This must be run after {@link TransformCorrelatedScalarAggregationToJoin}
 */
public class TransformCorrelatedScalarSubquery
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(nonEmpty(correlation()));

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        LogicalPlanNode subquery = context.getLookup().resolve(lateralJoinNode.getSubquery());

        if (!searchFrom(subquery, context.getLookup())
                .where(EnforceSingleRowNode.class::isInstance)
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .matches()) {
            return Result.empty();
        }

        LogicalPlanNode rewrittenSubquery = searchFrom(subquery, context.getLookup())
                .where(EnforceSingleRowNode.class::isInstance)
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .removeFirst();

        if (isAtMostScalar(rewrittenSubquery, context.getLookup())) {
            return Result.ofPlanNode(new LateralJoinNode(
                    context.getIdAllocator().getNextId(),
                    lateralJoinNode.getInput(),
                    rewrittenSubquery,
                    lateralJoinNode.getCorrelation(),
                    lateralJoinNode.getType(),
                    lateralJoinNode.getOriginSubqueryError()));
        }

        VariableReferenceExpression unique = context.getVariableAllocator().newVariable("unique", BIGINT);

        LateralJoinNode rewrittenLateralJoinNode = new LateralJoinNode(
                context.getIdAllocator().getNextId(),
                new AssignUniqueId(
                        context.getIdAllocator().getNextId(),
                        lateralJoinNode.getInput(),
                        unique),
                rewrittenSubquery,
                lateralJoinNode.getCorrelation(),
                lateralJoinNode.getType(),
                lateralJoinNode.getOriginSubqueryError());

        VariableReferenceExpression isDistinct = context.getVariableAllocator().newVariable("is_distinct", BooleanType.BOOLEAN);
        MarkDistinctNode markDistinctNode = new MarkDistinctNode(
                context.getIdAllocator().getNextId(),
                rewrittenLateralJoinNode,
                isDistinct,
                rewrittenLateralJoinNode.getInput().getOutputVariables(),
                Optional.empty());

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                markDistinctNode,
                castToRowExpression(new SimpleCaseExpression(
                        new SymbolReference(isDistinct.getName()),
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
                        Optional.of(new Cast(
                                new FunctionCall(
                                        QualifiedName.of("fail"),
                                        ImmutableList.of(
                                                //new LongLiteral(Integer.toString(SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode())),
                                                new LongLiteral(Integer.toString(0)),
                                                new StringLiteral("Scalar sub-query has returned multiple rows"))),
                                BOOLEAN)))));

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                identityAssignmentsAsSymbolReferences(lateralJoinNode.getOutputVariables())));
    }
}