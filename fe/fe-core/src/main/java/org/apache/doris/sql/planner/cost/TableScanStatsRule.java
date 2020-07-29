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
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.ColumnHandle;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.statistics.ColumnStatistics;
import org.apache.doris.sql.planner.statistics.TableStatistics;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.Patterns.tableScan;

public class TableScanStatsRule
        extends SimpleStatsRule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan();

    private final Metadata metadata;

    public TableScanStatsRule(Metadata metadata, StatsNormalizer normalizer)
    {
        super(normalizer); // Use stats normalization since connector can return inconsistent stats values
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(TableScanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        // TODO Construct predicate like AddExchanges's LayoutConstraintEvaluator
        //Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint());

        TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), ImmutableList.copyOf(node.getAssignments().values()));
        Map<VariableReferenceExpression, VariableStatsEstimate> outputVariableStats = new HashMap<>();

        for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
            Optional<ColumnStatistics> columnStatistics = Optional.ofNullable(tableStatistics.getColumnStatistics().get(entry.getValue()));
            outputVariableStats.put(entry.getKey(), columnStatistics.map(statistics -> StatsUtil.toVariableStatsEstimate(tableStatistics, statistics)).orElse(VariableStatsEstimate.unknown()));
        }

        return Optional.of(PlanNodeStatsEstimate.builder()
                .setOutputRowCount(tableStatistics.getRowCount().getValue())
                .addVariableStatistics(outputVariableStats)
                .build());
    }
}
