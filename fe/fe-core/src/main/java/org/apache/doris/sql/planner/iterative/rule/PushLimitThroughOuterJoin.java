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

import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Capture;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;

import static org.apache.doris.sql.planner.iterative.matching.Capture.newCapture;
import static org.apache.doris.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.LEFT;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.RIGHT;
import static org.apache.doris.sql.planner.plan.LimitNode.Step.PARTIAL;
import static org.apache.doris.sql.planner.plan.Patterns.Join.type;
import static org.apache.doris.sql.planner.plan.Patterns.join;
import static org.apache.doris.sql.planner.plan.Patterns.limit;
import static org.apache.doris.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 * - Limit
 *    - Join
 *       - left source
 *       - right source
 * </pre>
 * Into:
 * <pre>
 * - Limit
 *    - Join
 *       - Limit (present if Join is left or outer)
 *          - left source
 *       - Limit (present if Join is right or outer)
 *          - right source
 * </pre>
 */
public class PushLimitThroughOuterJoin
        implements Rule<LimitNode>
{
    private static final Capture<JoinNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN =
            limit()
                    .with(source().matching(
                            join()
                                    .with(type().matching(type -> type == LEFT || type == RIGHT))
                                    .capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        /*
        if (!isPushLimitThroughOuterJoin(context.getSession())) {
            return Result.empty();
        }
        */
        JoinNode joinNode = captures.get(CHILD);
        LogicalPlanNode left = joinNode.getLeft();
        LogicalPlanNode right = joinNode.getRight();

        if (joinNode.getType() == LEFT && !isLimited(left, context.getLookup(), parent.getCount())) {
            left = new LimitNode(context.getIdAllocator().getNextId(), left, parent.getCount(), PARTIAL);
        }

        if (joinNode.getType() == RIGHT && !isLimited(right, context.getLookup(), parent.getCount())) {
            right = new LimitNode(context.getIdAllocator().getNextId(), right, parent.getCount(), PARTIAL);
        }

        if (joinNode.getLeft() != left || joinNode.getRight() != right) {
            return Result.ofPlanNode(
                    parent.replaceChildren(ImmutableList.of(
                            joinNode.replaceChildren(ImmutableList.of(left, right)))));
        }

        return Result.empty();
    }

    private static boolean isLimited(LogicalPlanNode node, Lookup lookup, long limit)
    {
        Range<Long> cardinality = extractCardinality(node, lookup);
        return cardinality.hasUpperBound() && cardinality.upperEndpoint() <= limit;
    }
}
