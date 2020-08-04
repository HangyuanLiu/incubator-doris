package org.apache.doris.sql.planner;

import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.planner.cost.StatsAndCosts;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;

import static java.util.Objects.requireNonNull;

public class Plan
{
    private final LogicalPlanNode root;
    private final TypeProvider types;
    private final StatsAndCosts statsAndCosts;

    public Plan(LogicalPlanNode root, TypeProvider types, StatsAndCosts statsAndCosts)
    {
        this.root = requireNonNull(root, "root is null");
        this.types = requireNonNull(types, "types is null");
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
    }

    public LogicalPlanNode getRoot()
    {
        return root;
    }

    public TypeProvider getTypes()
    {
        return types;
    }

    public StatsAndCosts getStatsAndCosts()
    {
        return statsAndCosts;
    }
}
