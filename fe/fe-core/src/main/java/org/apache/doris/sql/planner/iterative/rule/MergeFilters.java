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
import org.apache.doris.sql.planner.iterative.matching.Capture;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.FilterNode;

import static org.apache.doris.sql.ExpressionUtils.combineConjuncts;
import static org.apache.doris.sql.planner.iterative.matching.Capture.newCapture;
import static org.apache.doris.sql.planner.plan.Patterns.filter;
import static org.apache.doris.sql.planner.plan.Patterns.source;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;

public class MergeFilters
        implements Rule<FilterNode>
{
    private static final Capture<FilterNode> CHILD = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(filter().capturedAs(CHILD)));

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode parent, Captures captures, Context context)
    {
        FilterNode child = captures.get(CHILD);

        return Result.ofPlanNode(
                new FilterNode(
                        parent.getId(),
                        child.getSource(),
                        castToRowExpression(combineConjuncts(castToExpression(child.getPredicate()), castToExpression(parent.getPredicate())))));
    }
}
