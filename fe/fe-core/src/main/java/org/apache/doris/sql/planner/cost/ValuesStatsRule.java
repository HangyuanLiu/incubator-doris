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

import org.apache.doris.common.NotImplementedException;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.iterative.Lookup;
import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.UnknownType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.apache.doris.sql.planner.RowExpressionInterpreter.evaluateConstantRowExpression;
import static org.apache.doris.sql.planner.cost.StatsUtil.toStatsRepresentation;
import static org.apache.doris.sql.planner.plan.Patterns.values;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;

public class ValuesStatsRule
        implements ComposableStatsCalculator.Rule<ValuesNode>
{
    private static final Pattern<ValuesNode> PATTERN = values();

    private final Metadata metadata;

    public ValuesStatsRule(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<ValuesNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(ValuesNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();
        statsBuilder.setOutputRowCount(node.getRows().size());

        for (int variableId = 0; variableId < node.getOutputVariables().size(); ++variableId) {
            VariableReferenceExpression variable = node.getOutputVariables().get(variableId);
            List<Object> symbolValues = getVariableValues(node, variableId, session, variable.getType());
            statsBuilder.addVariableStatistics(variable, buildVariableStatistics(symbolValues, session, variable.getType()));
        }

        return Optional.of(statsBuilder.build());
    }

    private List<Object> getVariableValues(ValuesNode valuesNode, int symbolId, Session session, Type type)
    {
        if (UnknownType.UNKNOWN.equals(type)) {
            // special casing for UNKNOWN as evaluateConstantExpression does not handle that
            return IntStream.range(0, valuesNode.getRows().size())
                    .mapToObj(rowId -> null)
                    .collect(toList());
        }
        return valuesNode.getRows().stream()
                .map(row -> row.get(symbolId))
                .map(rowExpression -> {
                    if (isExpression(rowExpression)) {
                        System.out.println("not implement");
                        //return evaluateConstantExpression(castToExpression(rowExpression), type, metadata, session, ImmutableList.of());
                    }
                    return evaluateConstantRowExpression(rowExpression, metadata, session.toConnectorSession());
                })
                .collect(toList());
    }

    private VariableStatsEstimate buildVariableStatistics(List<Object> values, Session session, Type type)
    {
        List<Object> nonNullValues = values.stream()
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (nonNullValues.isEmpty()) {
            return VariableStatsEstimate.zero();
        }

        double[] valuesAsDoubles = nonNullValues.stream()
                .map(value -> toStatsRepresentation(metadata, session, type, value))
                .filter(OptionalDouble::isPresent)
                .mapToDouble(OptionalDouble::getAsDouble)
                .toArray();

        double lowValue = DoubleStream.of(valuesAsDoubles).min().orElse(Double.NEGATIVE_INFINITY);
        double highValue = DoubleStream.of(valuesAsDoubles).max().orElse(Double.POSITIVE_INFINITY);
        double valuesCount = values.size();
        double nonNullValuesCount = nonNullValues.size();
        long distinctValuesCount = nonNullValues.stream().distinct().count();

        return VariableStatsEstimate.builder()
                .setNullsFraction((valuesCount - nonNullValuesCount) / valuesCount)
                .setLowValue(lowValue)
                .setHighValue(highValue)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }
}
