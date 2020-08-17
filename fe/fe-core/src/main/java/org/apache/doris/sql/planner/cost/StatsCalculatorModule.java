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


import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.metadata.Metadata;

public class StatsCalculatorModule
{
    public static StatsCalculator createNewStatsCalculator(
            Metadata metadata,
            ScalarStatsCalculator scalarStatsCalculator,
            StatsNormalizer normalizer,
            FilterStatsCalculator filterStatsCalculator)
    {
        ImmutableList.Builder<ComposableStatsCalculator.Rule<?>> rules = ImmutableList.builder();
        rules.add(new OutputStatsRule());
        rules.add(new TableScanStatsRule(metadata, normalizer));
        //rules.add(new SimpleFilterProjectSemiJoinStatsRule(normalizer, filterStatsCalculator, metadata.getFunctionManager())); // this must be before FilterStatsRule
        rules.add(new FilterStatsRule(normalizer, filterStatsCalculator));
        rules.add(new ValuesStatsRule(metadata));
        rules.add(new LimitStatsRule(normalizer));
        rules.add(new EnforceSingleRowStatsRule(normalizer));
        rules.add(new ProjectStatsRule(scalarStatsCalculator, normalizer));
        rules.add(new JoinStatsRule(filterStatsCalculator, normalizer));
        rules.add(new AggregationStatsRule(normalizer));
        rules.add(new AssignUniqueIdStatsRule());
        rules.add(new SemiJoinStatsRule());
        rules.add(new SortStatsRule());

        return new ComposableStatsCalculator(rules.build());
    }
}
