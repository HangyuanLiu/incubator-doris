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
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;

public class ApplyNode
        extends LogicalPlanNode
{
    private final LogicalPlanNode input;
    private final LogicalPlanNode subquery;

    /**
     * Correlation variables, returned from input (outer plan) used in subquery (inner plan)
     */
    private final List<VariableReferenceExpression> correlation;

    /**
     * Expressions that use subquery symbols.
     * <p>
     * Subquery expressions are different than other expressions
     * in a sense that they might use an entire subquery result
     * as an input (e.g: "x IN (subquery)", "x < ALL (subquery)").
     * Such expressions are invalid in linear operator context
     * (e.g: ProjectNode) in logical plan, but are correct in
     * ApplyNode context.
     * <p>
     * Example 1:
     * - expression: input_symbol_X IN (subquery_symbol_Y)
     * - meaning: if set consisting of all values for subquery_symbol_Y contains value represented by input_symbol_X
     * <p>
     * Example 2:
     * - expression: input_symbol_X < ALL (subquery_symbol_Y)
     * - meaning: if input_symbol_X is smaller than all subquery values represented by subquery_symbol_Y
     * <p>
     */
    private final Assignments subqueryAssignments;

    /**
     * This information is only used for sanity check.
     */
    private final String originSubqueryError;

    public ApplyNode(
            PlanNodeId id, LogicalPlanNode input, LogicalPlanNode subquery,
            Assignments subqueryAssignments, List<VariableReferenceExpression> correlation, String originSubqueryError)
    {
        super(id);
        checkArgument(input.getOutputVariables().containsAll(correlation), "Input does not contain symbols from correlation");
        verifySubquerySupported(subqueryAssignments);

        this.input = requireNonNull(input, "input is null");
        this.subquery = requireNonNull(subquery, "subquery is null");
        this.subqueryAssignments = requireNonNull(subqueryAssignments, "assignments is null");
        this.correlation = ImmutableList.copyOf(requireNonNull(correlation, "correlation is null"));
        this.originSubqueryError = requireNonNull(originSubqueryError, "originSubqueryError is null");
    }

    public LogicalPlanNode getInput()
    {
        return input;
    }

    public LogicalPlanNode getSubquery()
    {
        return subquery;
    }

    public Assignments getSubqueryAssignments()
    {
        return subqueryAssignments;
    }

    public List<VariableReferenceExpression> getCorrelation()
    {
        return correlation;
    }

    public String getOriginSubqueryError()
    {
        return originSubqueryError;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return ImmutableList.of(input, subquery);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(input.getOutputVariables())
                .addAll(subqueryAssignments.getOutputs())
                .build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitApply(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new ApplyNode(getId(), newChildren.get(0), newChildren.get(1), subqueryAssignments, correlation, originSubqueryError);
    }
}
