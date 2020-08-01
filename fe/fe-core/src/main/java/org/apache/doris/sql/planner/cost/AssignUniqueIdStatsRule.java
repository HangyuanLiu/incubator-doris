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

import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.AssignUniqueId;
import org.apache.doris.sql.planner.plan.Patterns;

import java.util.Optional;

import static org.apache.doris.sql.type.BigintType.BIGINT;

public class AssignUniqueIdStatsRule
        implements ComposableStatsCalculator.Rule<AssignUniqueId>
{
    private static final Pattern<AssignUniqueId> PATTERN = Patterns.assignUniqueId();

    @Override
    public Pattern<AssignUniqueId> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(AssignUniqueId assignUniqueId, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(assignUniqueId.getSource());
        return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats)
                .addVariableStatistics(assignUniqueId.getIdVariable(), VariableStatsEstimate.builder()
                        .setDistinctValuesCount(sourceStats.getOutputRowCount())
                        .setNullsFraction(0.0)
                        .setAverageRowSize(BIGINT.getFixedSize())
                        .build())
                .build());
    }
}
