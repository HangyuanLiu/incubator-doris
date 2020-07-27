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

import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.Set;
import java.util.stream.Stream;

import static org.apache.doris.sql.planner.iterative.rule.Util.restrictOutputs;
import static org.apache.doris.sql.planner.plan.Patterns.semiJoin;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneSemiJoinFilteringSourceColumns
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin();

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        Set<VariableReferenceExpression> requiredFilteringSourceInputs = Streams.concat(
                Stream.of(semiJoinNode.getFilteringSourceJoinVariable()),
                semiJoinNode.getFilteringSourceHashVariable().map(Stream::of).orElse(Stream.empty()))
                .collect(toImmutableSet());

        return restrictOutputs(context.getIdAllocator(), semiJoinNode.getFilteringSource(), requiredFilteringSourceInputs, false)
                .map(newFilteringSource ->
                        semiJoinNode.replaceChildren(ImmutableList.of(semiJoinNode.getSource(), newFilteringSource)))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
