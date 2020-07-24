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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class ValuesNode
        extends LogicalPlanNode
{
    private final List<VariableReferenceExpression> outputVariables;
    private final List<List<RowExpression>> rows;

    public ValuesNode(PlanNodeId id, List<VariableReferenceExpression> outputVariables, List<List<RowExpression>> rows)
    {
        super(id);
        this.outputVariables = immutableListCopyOf(outputVariables);
        this.rows = immutableListCopyOf(requireNonNull(rows, "lists is null").stream().map(ValuesNode::immutableListCopyOf).collect(Collectors.toList()));

        for (List<RowExpression> row : rows) {
            if (!(row.size() == outputVariables.size() || row.size() == 0)) {
                throw new IllegalArgumentException(format("Expected row to have %s values, but row has %s values", outputVariables.size(), row.size()));
            }
        }
    }

    public List<List<RowExpression>> getRows()
    {
        return rows;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return unmodifiableList(emptyList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitValues(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        if (!newChildren.isEmpty()) {
            throw new IllegalArgumentException("newChildren is not empty");
        }
        return this;
    }

    private static <T> List<T> immutableListCopyOf(List<T> list)
    {
        return unmodifiableList(new ArrayList<>(requireNonNull(list, "list is null")));
    }
}
