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

import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.optimizations.ScalarAggregationToJoinRewriter;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.EnforceSingleRowNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.ProjectNode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.iterative.matching.Pattern.nonEmpty;
import static org.apache.doris.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.apache.doris.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static org.apache.doris.sql.planner.plan.Patterns.LateralJoin.correlation;
import static org.apache.doris.sql.planner.plan.Patterns.lateralJoin;
import static org.apache.doris.sql.util.MorePredicates.isInstanceOfAny;

/**
 * Scalar aggregation is aggregation with GROUP BY 'a constant' (or empty GROUP BY).
 * It always returns single row.
 * <p>
 * This optimizer rewrites correlated scalar aggregation subquery to left outer join in a way described here:
 * https://github.com/prestodb/presto/wiki/Correlated-subqueries
 * <p>
 * From:
 * <pre>
 * - LateralJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery) Aggregation(GROUP BY (); functions: [sum(F), count(), ...]
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Aggregation(GROUP BY A, B, C, U; functions: [sum(F), count(non_null), ...]
 *   - Join(LEFT_OUTER, D = C)
 *     - AssignUniqueId(adds symbol U)
 *       - (input) plan which produces symbols: [A, B, C]
 *     - Filter(E > 5)
 *       - projection which adds non null symbol used for count() function
 *         - plan which produces symbols: [D, E, F]
 * </pre>
 * <p>
 * Note that only conjunction predicates in FilterNode are supported
 */
public class TransformCorrelatedScalarAggregationToJoin
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(nonEmpty(correlation()));

    @Override
    public Pattern<LateralJoinNode> getPattern()
    {
        return PATTERN;
    }

    private final FunctionManager functionManager;

    public TransformCorrelatedScalarAggregationToJoin(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    @Override
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        LogicalPlanNode subquery = lateralJoinNode.getSubquery();

        if (!isScalar(subquery, context.getLookup())) {
            return Result.empty();
        }

        Optional<AggregationNode> aggregation = findAggregation(subquery, context.getLookup());
        if (!(aggregation.isPresent() && aggregation.get().getGroupingKeys().isEmpty())) {
            return Result.empty();
        }

        ScalarAggregationToJoinRewriter rewriter = new ScalarAggregationToJoinRewriter(functionManager, context.getVariableAllocator(), context.getIdAllocator(), context.getLookup());

        LogicalPlanNode rewrittenNode = rewriter.rewriteScalarAggregation(lateralJoinNode, aggregation.get());

        if (rewrittenNode instanceof LateralJoinNode) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewrittenNode);
    }

    private static Optional<AggregationNode> findAggregation(LogicalPlanNode rootNode, Lookup lookup)
    {
        return searchFrom(rootNode, lookup)
                .where(AggregationNode.class::isInstance)
                .recurseOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class))
                .findFirst();
    }
}
