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

import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public final class TopNNode
        extends LogicalPlanNode
{
    /**
     * Stages of `TopNNode`:
     *
     * SINGLE:    `TopNNode` is in the logical plan.
     * PARTIAL:   `TopNNode` is in the distributed plan, and generates partial results of `TopN` on local workers.
     * FINAL:     `TopNNode` is in the distributed plan, and finalizes the partial results from `PARTIAL` nodes.
     */
    public enum Step
    {
        SINGLE,
        PARTIAL,
        FINAL
    }

    private final LogicalPlanNode source;
    private final long count;
    private final OrderingScheme orderingScheme;
    private final Step step;

    public TopNNode(
            PlanNodeId id,
            LogicalPlanNode source,
            long count,
            OrderingScheme orderingScheme,
            Step step)
    {
        super(id);

        requireNonNull(source, "source is null");
        checkArgument(count >= 0, "count must be positive");
        //checkCondition(count <= Integer.MAX_VALUE, NOT_SUPPORTED, "ORDER BY LIMIT > %s is not supported", Integer.MAX_VALUE);
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.count = count;
        this.orderingScheme = orderingScheme;
        this.step = requireNonNull(step, "step is null");
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return singletonList(source);
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

    public long getCount()
    {
        return count;
    }

    public OrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    public Step getStep()
    {
        return step;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTopN(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        //checkCondition(newChildren != null && newChildren.size() == 1, GENERIC_INTERNAL_ERROR, "Expect exactly 1 child PlanNode");
        return new TopNNode(getId(), newChildren.get(0), count, orderingScheme, step);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
    /*
    private static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(errorCode, format(formatString, args));
        }
    }
     */
}
