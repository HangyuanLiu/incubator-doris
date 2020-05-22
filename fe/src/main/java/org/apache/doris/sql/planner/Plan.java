package org.apache.doris.sql.planner;

import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public class Plan
{
    private final PlanNode root;
    private final TypeProvider types;
    //private final StatsAndCosts statsAndCosts;

    public Plan(PlanNode root, TypeProvider types)
    {
        this.root = requireNonNull(root, "root is null");
        this.types = requireNonNull(types, "types is null");
        //this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public TypeProvider getTypes()
    {
        return types;
    }
}
