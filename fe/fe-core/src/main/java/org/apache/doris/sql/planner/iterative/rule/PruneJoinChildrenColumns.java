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

import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.OriginalExpressionUtils;

import java.util.Set;

import static com.google.common.base.Predicates.not;
import static org.apache.doris.sql.planner.VariablesExtractor.extractUnique;
import static org.apache.doris.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.doris.sql.planner.plan.Patterns.join;

/**
 * Non-Cross joins support output variable selection, so make any project-off of child columns explicit in project nodes.
 */
public class PruneJoinChildrenColumns
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(not(JoinNode::isCrossJoin));

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        Set<VariableReferenceExpression> globallyUsableInputs = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(joinNode.getOutputVariables())
                .addAll(
                        joinNode.getFilter()
                                .map(OriginalExpressionUtils::castToExpression)
                                .map(expression -> extractUnique(expression, context.getVariableAllocator().getTypes()))
                                .orElse(ImmutableSet.of()))
                .build();

        Set<VariableReferenceExpression> leftUsableInputs = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(globallyUsableInputs)
                .addAll(
                        joinNode.getCriteria().stream()
                                .map(JoinNode.EquiJoinClause::getLeft)
                                .iterator())
                .addAll(joinNode.getLeftHashVariable().map(ImmutableSet::of).orElse(ImmutableSet.of()))
                .build();

        Set<VariableReferenceExpression> rightUsableInputs = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(globallyUsableInputs)
                .addAll(
                        joinNode.getCriteria().stream()
                                .map(JoinNode.EquiJoinClause::getRight)
                                .iterator())
                .addAll(joinNode.getRightHashVariable().map(ImmutableSet::of).orElse(ImmutableSet.of()))
                .build();

        return restrictChildOutputs(context.getIdAllocator(), joinNode, leftUsableInputs, rightUsableInputs)
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}