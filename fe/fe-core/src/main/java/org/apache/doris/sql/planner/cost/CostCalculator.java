package org.apache.doris.sql.planner.cost;

import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;

public interface CostCalculator {
    /**
     * Calculates cumulative cost of a node.
     *
     * @param node  The node to compute cost for.
     * @param stats The stats provider for node's stats and child nodes' stats, to be used if stats are needed to compute cost for the {@code node}
     */
    PlanCostEstimate calculateCost(
            LogicalPlanNode node,
            StatsProvider stats,
            CostProvider sourcesCosts,
            Session session);
}