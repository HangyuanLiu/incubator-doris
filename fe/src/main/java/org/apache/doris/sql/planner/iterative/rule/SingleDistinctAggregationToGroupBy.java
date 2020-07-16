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
package org.apache.doris.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.OriginalExpressionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static org.apache.doris.sql.planner.plan.AggregationNode.Step.SINGLE;
import static org.apache.doris.sql.planner.plan.AggregationNode.singleGroupingSet;
import static org.apache.doris.sql.planner.plan.Patterns.aggregation;

/**
 * Implements distinct aggregations with similar inputs by transforming plans of the following shape:
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT s0, s1, ...),
 *        F2(DISTINCT s0, s1, ...),
 *     - X
 * </pre>
 * into
 * <pre>
 * - Aggregation
 *          GROUP BY (k)
 *          F1(x)
 *          F2(x)
 *      - Aggregation
 *             GROUP BY (k, s0, s1, ...)
 *          - X
 * </pre>
 * <p>
 * Assumes s0, s1, ... are symbol references (i.e., complex expressions have been pre-projected)
 */
public class SingleDistinctAggregationToGroupBy
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(SingleDistinctAggregationToGroupBy::hasSingleDistinctInput)
            .matching(SingleDistinctAggregationToGroupBy::allDistinctAggregates)
            .matching(SingleDistinctAggregationToGroupBy::noFilters)
            .matching(SingleDistinctAggregationToGroupBy::noMasks);

    private static boolean hasSingleDistinctInput(AggregationNode aggregation)
    {
        return extractArgumentSets(aggregation)
                .count() == 1;
    }

    private static boolean allDistinctAggregates(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .allMatch(AggregationNode.Aggregation::isDistinct);
    }

    private static boolean noFilters(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .noneMatch(instance -> instance.getFilter().isPresent());
    }

    private static boolean noMasks(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .noneMatch(e -> e.getMask().isPresent());
    }

    private static Stream<Set<RowExpression>> extractArgumentSets(AggregationNode aggregation)
    {
        return aggregation.getAggregations()
                .values().stream()
                .filter(AggregationNode.Aggregation::isDistinct)
                .map(AggregationNode.Aggregation::getArguments)
                .<Set<RowExpression>>map(HashSet::new)
                .distinct();
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        List<Set<RowExpression>> argumentSets = extractArgumentSets(aggregation)
                .collect(Collectors.toList());

        Set<VariableReferenceExpression> variables = Iterables.getOnlyElement(argumentSets).stream()
                .map(OriginalExpressionUtils::castToExpression)
                .map(context.getVariableAllocator()::toVariableReference)
                .collect(Collectors.toSet());

        return Result.ofPlanNode(
                new AggregationNode(
                        aggregation.getId(),
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                aggregation.getSource(),
                                ImmutableMap.of(),
                                singleGroupingSet(ImmutableList.<VariableReferenceExpression>builder()
                                        .addAll(aggregation.getGroupingKeys())
                                        .addAll(variables)
                                        .build()),
                                ImmutableList.of(),
                                SINGLE,
                                Optional.empty(),
                                Optional.empty()),
                        // remove DISTINCT flag from function calls
                        aggregation.getAggregations()
                                .entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> removeDistinct(e.getValue()))),
                        aggregation.getGroupingSets(),
                        emptyList(),
                        aggregation.getStep(),
                        aggregation.getHashVariable(),
                        aggregation.getGroupIdVariable()));
    }

    private static AggregationNode.Aggregation removeDistinct(AggregationNode.Aggregation aggregation)
    {
        checkArgument(aggregation.isDistinct(), "Expected aggregation to have DISTINCT input");

        return new AggregationNode.Aggregation(
                aggregation.getCall(),
                aggregation.getFilter(),
                aggregation.getOrderBy(),
                false,
                aggregation.getMask());
    }
}
