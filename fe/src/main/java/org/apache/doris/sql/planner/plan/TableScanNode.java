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
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.metadata.ColumnHandle;
import org.apache.doris.sql.metadata.TableHandle;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import javax.annotation.concurrent.Immutable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

@Immutable
public final class TableScanNode
        extends LogicalPlanNode
{
    private final TableHandle table;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;
    private final List<VariableReferenceExpression> outputVariables;

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<VariableReferenceExpression> outputVariables,
            Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.outputVariables = unmodifiableList(requireNonNull(outputVariables, "outputVariables is null"));
        this.assignments = unmodifiableMap(new HashMap<>(requireNonNull(assignments, "assignments is null")));
        checkArgument(assignments.keySet().containsAll(outputVariables), "assignments does not cover all of outputs");
    }

    /**
     * Get the table handle provided by connector
     */
    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    /**
     * Get the mapping from symbols to columns
     */
    @JsonProperty
    public Map<VariableReferenceExpression, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        // table scan should be the leaf node
        return emptyList();
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder(this.getClass().getSimpleName());
        stringBuilder.append(" {");
        stringBuilder.append("table='").append(table).append('\'');
        stringBuilder.append(", outputVariables='").append(outputVariables).append('\'');
        stringBuilder.append(", assignments='").append(assignments).append('\'');
        stringBuilder.append('}');
        return stringBuilder.toString();
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }

    private static void checkArgument(boolean test, String errorMessage)
    {
        if (!test) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private static void checkState(boolean test, String errorMessage)
    {
        if (!test) {
            throw new IllegalStateException(errorMessage);
        }
    }
}
