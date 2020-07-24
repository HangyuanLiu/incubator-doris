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
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class LimitNode
        extends LogicalPlanNode
{
    /**
     * Stages of `LimitNode`:
     *
     * PARTIAL:   `LimitNode` is in the distributed plan and generates partial results on local workers.
     * FINAL:     `LimitNode` is in the distributed plan and finalizes the partial results from `PARTIAL` nodes.
     */
    public enum Step
    {
        PARTIAL,
        FINAL
    }

    private final LogicalPlanNode source;
    private final long count;
    private final Step step;

    public LimitNode(
            PlanNodeId id,
            LogicalPlanNode source,
            long count,
            Step step)
    {
        super(id);

        this.source = requireNonNull(source, "source is null");
        this.count = count;
        this.step = requireNonNull(step, "step is null");
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return Collections.singletonList(source);
    }

    /**
     * LimitNode only expects a single upstream PlanNode.
     */
    public LogicalPlanNode getSource()
    {
        return source;
    }

    /**
     * Get the limit `N` number of results to return.
     */
    public long getCount()
    {
        return count;
    }

    public Step getStep()
    {
        return step;
    }

    public boolean isPartial()
    {
        return step == Step.PARTIAL;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren) {
        return new LimitNode(getId(), newChildren.get(0), count, getStep());
    }
}
