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
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.tree.Expression;

import static org.apache.doris.sql.planner.plan.Patterns.filter;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static org.apache.doris.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class RemoveTrivialFilters
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        Expression predicate = castToExpression(filterNode.getPredicate());

        if (predicate.equals(TRUE_LITERAL)) {
            return Result.ofPlanNode(filterNode.getSource());
        }

        if (predicate.equals(FALSE_LITERAL)) {
            return Result.ofPlanNode(new ValuesNode(context.getIdAllocator().getNextId(), filterNode.getOutputVariables(), ImmutableList.of()));
        }

        return Result.empty();
    }
}
