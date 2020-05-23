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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import javax.annotation.concurrent.Immutable;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Immutable
public class OutputNode
        extends LogicalPlanNode
{
    private final LogicalPlanNode source;
    private final List<String> columnNames;
    private final List<VariableReferenceExpression> outputVariables; // column name = variable.name

    @JsonCreator
    public OutputNode(@JsonProperty("id") PlanNodeId id,
                      @JsonProperty("source") LogicalPlanNode source,
                      @JsonProperty("columnNames") List<String> columnNames,
                      @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(columnNames, "columnNames is null");
        Preconditions.checkArgument(columnNames.size() == outputVariables.size(), "columnNames and assignments sizes don't match");

        this.source = source;
        this.columnNames = columnNames;
        this.outputVariables = ImmutableList.copyOf(outputVariables);
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public LogicalPlanNode getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitOutput(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        return new OutputNode(getId(), Iterables.getOnlyElement(newChildren), columnNames, outputVariables);
    }
}
