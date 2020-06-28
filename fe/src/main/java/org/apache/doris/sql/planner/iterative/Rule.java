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
package org.apache.doris.sql.planner.iterative;

import com.sun.org.glassfish.external.probe.provider.StatsProvider;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.cost.CostProvider;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface Rule<T>
{
    /**
     * Returns a pattern to which plan nodes this rule applies.
     */
    Pattern<T> getPattern();

    default boolean isEnabled(Session session)
    {
        return true;
    }

    Result apply(T node, Captures captures, Context context);

    interface Context
    {
        Lookup getLookup();

        IdGenerator<PlanNodeId> getIdAllocator();

        VariableAllocator getVariableAllocator();

        Session getSession();

        StatsProvider getStatsProvider();

        CostProvider getCostProvider();

        void checkTimeoutNotExhausted();

        WarningCollector getWarningCollector();
    }

    final class Result
    {
        public static Result empty()
        {
            return new Result(Optional.empty());
        }

        public static Result ofPlanNode(LogicalPlanNode transformedPlan)
        {
            return new Result(Optional.of(transformedPlan));
        }

        private final Optional<LogicalPlanNode> transformedPlan;

        private Result(Optional<LogicalPlanNode> transformedPlan)
        {
            this.transformedPlan = requireNonNull(transformedPlan, "transformedPlan is null");
        }

        public Optional<LogicalPlanNode> getTransformedPlan()
        {
            return transformedPlan;
        }

        public boolean isEmpty()
        {
            return !transformedPlan.isPresent();
        }
    }
}
