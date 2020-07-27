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

import com.google.common.collect.Streams;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.doris.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.doris.sql.planner.plan.Patterns.topN;

public class PruneTopNColumns
        extends ProjectOffPushDownRule<TopNNode>
{
    public PruneTopNColumns()
    {
        super(topN());
    }

    @Override
    protected Optional<LogicalPlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, TopNNode topNNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        Set<VariableReferenceExpression> prunedTopNInputs = Streams.concat(
                referencedOutputs.stream(),
                topNNode.getOrderingScheme().getOrderByVariables().stream())
                .collect(toImmutableSet());

        return restrictChildOutputs(idAllocator, topNNode, prunedTopNInputs);
    }
}
