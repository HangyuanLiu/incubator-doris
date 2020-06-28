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
package org.apache.doris.sql.planner.iterative;

import com.google.common.collect.ImmutableList;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

public class GroupReference
        extends LogicalPlanNode
{
    private final int groupId;
    private final List<VariableReferenceExpression> outputs;

    public GroupReference(PlanNodeId id, int groupId, List<VariableReferenceExpression> outputs)
    {
        super(id);
        this.groupId = groupId;
        this.outputs = ImmutableList.copyOf(outputs);
    }

    public int getGroupId()
    {
        return groupId;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupReference(this, context);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputs;
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        throw new UnsupportedOperationException();
    }
}
