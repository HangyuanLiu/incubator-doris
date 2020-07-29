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
package org.apache.doris.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AssignUniqueId
        extends LogicalPlanNode
{
    private final LogicalPlanNode source;
    private final VariableReferenceExpression idVariable;

    public AssignUniqueId(PlanNodeId id, LogicalPlanNode source, VariableReferenceExpression idVariable)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.idVariable = requireNonNull(idVariable, "idVariable is null");
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(source.getOutputVariables())
                .add(idVariable)
                .build();
    }

    public LogicalPlanNode getSource()
    {
        return source;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    public VariableReferenceExpression getIdVariable()
    {
        return idVariable;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAssignUniqueId(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected newChildren to contain 1 node");
        return new AssignUniqueId(getId(), Iterables.getOnlyElement(newChildren), idVariable);
    }
}
