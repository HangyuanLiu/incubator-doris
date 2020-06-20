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


import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

public final class FilterNode
        extends LogicalPlanNode
{
    private final LogicalPlanNode source;
    private final RowExpression predicate;

    public FilterNode(PlanNodeId id,
                      LogicalPlanNode source,
                      RowExpression predicate)
    {
        super(id);

        this.source = source;
        this.predicate = predicate;
    }

    /**
     * Get the predicate (a RowExpression of boolean type) of the FilterNode.
     * It serves as the criteria to determine whether the incoming rows should be filtered out or not.
     */
    public RowExpression getPredicate()
    {
        return predicate;
    }

    /**
     * FilterNode only expects a single upstream PlanNode.
     */
    public LogicalPlanNode getSource()
    {
        return source;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return unmodifiableList(singletonList(source));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitFilter(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        // FilterNode only expects a single upstream PlanNode
        if (newChildren == null || newChildren.size() != 1) {
            throw new IllegalArgumentException("Expect exactly one child to replace");
        }
        return new FilterNode(getId(), newChildren.get(0), predicate);
    }
}
