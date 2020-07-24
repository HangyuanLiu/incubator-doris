package org.apache.doris.sql.planner.plan;

import org.apache.doris.sql.planner.iterative.GroupReference;

public abstract class PlanVisitor<R, C>
{
    /**
     * The default behavior to perform when visiting a PlanNode
     */
    public abstract R visitPlan(LogicalPlanNode node, C context);

    public R visitAggregation(AggregationNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitOutput(OutputNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitProject(ProjectNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitFilter(FilterNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTableScan(TableScanNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitValues(ValuesNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitLimit(LimitNode node, C context) {
        return visitPlan(node, context);
    }

    public R visitGroupReference(GroupReference node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitJoin(JoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSemiJoin(SemiJoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitSort(SortNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTopN(TopNNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitGroupId(GroupIdNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitExchange(ExchangeNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitEnforceSingleRow(EnforceSingleRowNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitApply(ApplyNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitLateralJoin(LateralJoinNode node, C context)
    {
        return visitPlan(node, context);
    }
}
