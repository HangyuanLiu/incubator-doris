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
package org.apache.doris.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.VariablesExtractor;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.FunctionResolution;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;
import static org.apache.doris.sql.type.BigintType.BIGINT;

public class AggregationNodeUtils
{
    private AggregationNodeUtils() {}

    public static AggregationNode.Aggregation count(FunctionManager functionManager)
    {
        return new AggregationNode.Aggregation(
                new CallExpression("count",
                        new FunctionResolution(functionManager).countFunction(),
                        BIGINT,
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    public static Set<VariableReferenceExpression> extractAggregationUniqueVariables(AggregationNode.Aggregation aggregation, TypeProvider types)
    {
        // types will be no longer needed once everything is RowExpression.
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        aggregation.getArguments().forEach(argument -> builder.addAll(extractAll(argument, types)));
        aggregation.getFilter().ifPresent(filter -> builder.addAll(extractAll(filter, types)));
        aggregation.getOrderBy().ifPresent(orderingScheme -> builder.addAll(orderingScheme.getOrderByVariables()));
        return builder.build();
    }

    private static List<VariableReferenceExpression> extractAll(RowExpression expression, TypeProvider types)
    {
        if (isExpression(expression)) {
            return VariablesExtractor.extractAll(castToExpression(expression), types);
        }
        return VariablesExtractor.extractAll(expression)
                .stream()
                .collect(toImmutableList());
    }
}
