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

import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.Optional;
import java.util.Set;

import static org.apache.doris.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.doris.sql.planner.plan.Patterns.limit;

public class PruneLimitColumns
        extends ProjectOffPushDownRule<LimitNode>
{
    public PruneLimitColumns()
    {
        super(limit());
    }

    @Override
    protected Optional<LogicalPlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, LimitNode limitNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        return restrictChildOutputs(idAllocator, limitNode, referencedOutputs);
    }
}
