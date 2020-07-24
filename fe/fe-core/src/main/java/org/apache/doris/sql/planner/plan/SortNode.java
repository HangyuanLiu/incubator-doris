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

import static java.util.Objects.requireNonNull;

public class SortNode
        extends LogicalPlanNode
{
    private final LogicalPlanNode source;
    private final OrderingScheme orderingScheme;
    private final boolean isPartial;

    public SortNode(PlanNodeId id,
                    LogicalPlanNode source,
                    OrderingScheme orderingScheme,
                    boolean isPartial)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.orderingScheme = orderingScheme;
        this.isPartial = isPartial;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    public LogicalPlanNode getSource()
    {
        return source;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    public OrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    public boolean isPartial()
    {
        return isPartial;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSort(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        return new SortNode(getId(), Iterables.getOnlyElement(newChildren), orderingScheme, isPartial);
    }
}
