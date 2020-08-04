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

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class ExplainAnalyzeNode
        extends LogicalPlanNode
{
    private final LogicalPlanNode source;
    private final VariableReferenceExpression outputVariable;
    private final boolean verbose;

    public ExplainAnalyzeNode(PlanNodeId id, LogicalPlanNode source, VariableReferenceExpression outputVariable, boolean verbose)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.outputVariable = requireNonNull(outputVariable, "outputVariable is null");
        this.verbose = verbose;
    }

    public VariableReferenceExpression getOutputVariable()
    {
        return outputVariable;
    }

    public LogicalPlanNode getSource()
    {
        return source;
    }

    public boolean isVerbose()
    {
        return verbose;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.of(outputVariable);
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplainAnalyze(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        return new ExplainAnalyzeNode(getId(), Iterables.getOnlyElement(newChildren), outputVariable, isVerbose());
    }
}
