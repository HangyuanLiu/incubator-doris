package org.apache.doris.sql.planner;

public class LogicalPlanner {
    public enum Stage {
        CREATED, OPTIMIZED, OPTIMIZED_AND_VALIDATED
    }

    public LogicalPlanner() {

    }
}
