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
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.Map;
import java.util.Optional;

import static org.apache.doris.sql.planner.plan.Patterns.project;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public class ProjectStatsRule
        extends SimpleStatsRule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();

    private final ScalarStatsCalculator scalarStatsCalculator;

    public ProjectStatsRule(ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        super(normalizer);
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(ProjectNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        PlanNodeStatsEstimate.Builder calculatedStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(sourceStats.getOutputRowCount());

        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
            RowExpression expression = entry.getValue();
            if (isExpression(expression)) {
                calculatedStats.addVariableStatistics(entry.getKey(), scalarStatsCalculator.calculate(castToExpression(expression), sourceStats, session, types));
            }
            else {
                calculatedStats.addVariableStatistics(entry.getKey(), scalarStatsCalculator.calculate(expression, sourceStats, session));
            }
        }
        return Optional.of(calculatedStats.build());
    }
}
