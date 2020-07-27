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
import org.apache.doris.sql.planner.optimizations.PlanNodeDecorrelator;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.relational.OriginalExpressionUtils;

import java.util.Optional;

import static org.apache.doris.sql.planner.iterative.matching.Pattern.nonEmpty;
import static org.apache.doris.sql.planner.plan.Patterns.LateralJoin.correlation;
import static org.apache.doris.sql.planner.plan.Patterns.lateralJoin;

/**
 * Tries to decorrelate subquery and rewrite it using normal join.
 * Decorrelated predicates are part of join condition.
 */
public class TransformCorrelatedLateralJoinToJoin
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(nonEmpty(correlation()));

    @Override
    public Pattern<LateralJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        LogicalPlanNode subquery = lateralJoinNode.getSubquery();

        PlanNodeDecorrelator planNodeDecorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getVariableAllocator(), context.getLookup());
        Optional<PlanNodeDecorrelator.DecorrelatedNode> decorrelatedNodeOptional = planNodeDecorrelator.decorrelateFilters(subquery, lateralJoinNode.getCorrelation());

        return decorrelatedNodeOptional.map(decorrelatedNode ->
                Result.ofPlanNode(new JoinNode(
                        context.getIdAllocator().getNextId(),
                        lateralJoinNode.getType().toJoinNodeType(),
                        lateralJoinNode.getInput(),
                        decorrelatedNode.getNode(),
                        ImmutableList.of(),
                        lateralJoinNode.getOutputVariables(),
                        decorrelatedNode.getCorrelatedPredicates().map(OriginalExpressionUtils::castToRowExpression),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))).orElseGet(Result::empty);
    }
}
