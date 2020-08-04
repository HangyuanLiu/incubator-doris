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
package org.apache.doris.sql.analyzer;

import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.metadata.AccessControl;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.LogicalPlanner;
import org.apache.doris.sql.planner.Plan;
import org.apache.doris.sql.planner.PlanOptimizers;
import org.apache.doris.sql.planner.cost.CostCalculator;
import org.apache.doris.sql.planner.cost.StatsCalculator;
import org.apache.doris.sql.planner.optimizations.PlanOptimizer;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.planPrinter.PlanPrinter;
import org.apache.doris.sql.tree.ExplainType.Type;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryExplainer
{
    private final List<PlanOptimizer> planOptimizers;
    private final Metadata metadata;
    private AccessControl accessControl;
    private final SqlParser sqlParser;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    @Inject
    public QueryExplainer(
            PlanOptimizers planOptimizers,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator)
    {
        this(
                planOptimizers.get(),
                metadata,
                accessControl,
                sqlParser,
                statsCalculator,
                costCalculator);
    }

    public QueryExplainer(
            List<PlanOptimizer> planOptimizers,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator)
    {
        this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        //this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    public Analysis analyze(Session session, Statement statement, List<Expression> parameters, WarningCollector warningCollector)
    {
        Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.of(this), parameters, warningCollector);
        return analyzer.analyze(statement);
    }

    public String getPlan(Session session, Statement statement, Type planType, List<Expression> parameters, boolean verbose, WarningCollector warningCollector) throws Exception
    {

        switch (planType) {
            case LOGICAL:
                Plan plan = getLogicalPlan(session, statement, parameters, warningCollector);
                return PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata.getFunctionManager(), plan.getStatsAndCosts(), session, 0, verbose);
                /*
            case DISTRIBUTED:
                SubPlan subPlan = getDistributedPlan(session, statement, parameters, warningCollector);
                return PlanPrinter.textDistributedPlan(subPlan, metadata.getFunctionManager(), session, verbose);
            case IO:
                return IOPlanPrinter.textIOPlan(getLogicalPlan(session, statement, parameters, warningCollector).getRoot(), metadata, session);

                 */
        }
        throw new IllegalArgumentException("Unhandled plan type: " + planType);
    }

    public Plan getLogicalPlan(Session session, Statement statement, List<Expression> parameters, WarningCollector warningCollector) throws Exception
    {
        return getLogicalPlan(session, statement, parameters, warningCollector, new PlanNodeIdAllocator(PlanNodeId.createGenerator()));
    }

    public Plan getLogicalPlan(Session session, Statement statement, List<Expression> parameters, WarningCollector warningCollector, PlanNodeIdAllocator idAllocator) throws Exception
    {
        // analyze statement
        Analysis analysis = analyze(session, statement, parameters, warningCollector);
        // plan statement
        LogicalPlanner logicalPlanner = new LogicalPlanner(true, session, planOptimizers, idAllocator, metadata, statsCalculator, costCalculator, warningCollector);
        return logicalPlanner.plan(analysis);
    }
}
