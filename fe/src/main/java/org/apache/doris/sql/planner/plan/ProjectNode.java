package org.apache.doris.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public final class ProjectNode
        extends PlanNode
{
    private final PlanNode source;
    private final Assignments assignments;

    // TODO: pass in the "assignments" and the "outputs" separately (i.e., get rid if the symbol := symbol idiom)
    @JsonCreator
    public ProjectNode(@JsonProperty("id") PlanNodeId id,
                       @JsonProperty("source") PlanNode source,
                       @JsonProperty("assignments") Assignments assignments)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(assignments, "assignments is null");

        this.source = source;
        this.assignments = assignments;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return assignments.getOutputs();
    }

    @JsonProperty
    public Assignments getAssignments()
    {
        return assignments;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return singletonList(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitProject(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        requireNonNull(newChildren, "newChildren list is null");
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("newChildren list has multiple items");
        }
        return new ProjectNode(getId(), newChildren.get(0), assignments);
    }
}
