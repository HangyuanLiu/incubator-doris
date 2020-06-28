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
package org.apache.doris.sql.planner.cost;

import com.sun.org.glassfish.external.probe.provider.StatsProvider;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;

public interface StatsCalculator
{
    /**
     * Calculate stats for the {@code node}.
     *
     * @param node The node to compute stats for.
     * @param sourceStats The stats provider for any child nodes' stats, if needed to compute stats for the {@code node}
     * @param lookup Lookup to be used when resolving source nodes, allowing stats calculation to work within {@link IterativeOptimizer}
     * @param types
     */
    PlanNodeStatsEstimate calculateStats(
            LogicalPlanNode node,
            StatsProvider sourceStats,
            Lookup lookup,
            Session session,
            TypeProvider types);
}
