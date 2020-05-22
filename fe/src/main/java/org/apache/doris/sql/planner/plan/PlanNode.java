package org.apache.doris.sql.planner.plan;

import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class PlanNode
{
    private final PlanNodeId id;

    protected PlanNode(PlanNodeId id)
    {
        requireNonNull(id, "id is null");
        this.id = id;
    }

    public PlanNodeId getId()
    {
        return id;
    }

    /**
     * Get the upstream PlanNodes (i.e., children) of the current PlanNode.
     */
    public abstract List<PlanNode> getSources();

    /**
     * The output from the upstream PlanNodes.
     * It should serve as the input for the current PlanNode.
     */
    public abstract List<VariableReferenceExpression> getOutputVariables();

    /**
     * Alter the upstream PlanNodes of the current PlanNode.
     */
    public abstract PlanNode replaceChildren(List<PlanNode> newChildren);

    /**
     * A visitor pattern interface to operate on IR.
     */
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
