package org.apache.doris.sql.planner.plan;

public abstract class PlanVisitor<R, C>
{
    /**
     * The default behavior to perform when visiting a PlanNode
     */
    public abstract R visitPlan(LogicalPlanNode node, C context);

    public R visitOutput(OutputNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitProject(ProjectNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableScan(TableScanNode node, C context)
    {
        return visitPlan(node, context);
    }
}
