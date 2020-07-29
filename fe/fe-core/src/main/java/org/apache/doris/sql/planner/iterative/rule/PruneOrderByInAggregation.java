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


import com.google.common.collect.ImmutableMap;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.Patterns.aggregation;

public class PruneOrderByInAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();
    private final FunctionManager functionManager;

    public PruneOrderByInAggregation(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context) {
        /*
        if (!node.hasOrderings()) {
            return Result.empty();
        }

        boolean anyRewritten = false;
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            if (!aggregation.getOrderBy().isPresent()) {
                aggregations.put(entry);
            }
            // getAggregateFunctionImplementation can be expensive, so check it last.
            else if (functionManager.getAggregateFunctionImplementation(aggregation.getFunctionHandle()).isOrderSensitive()) {
                aggregations.put(entry);
            }
            else {
                anyRewritten = true;

                aggregations.put(entry.getKey(), new AggregationNode.Aggregation(
                        aggregation.getCall(),
                        aggregation.getFilter(),
                        Optional.empty(),
                        aggregation.isDistinct(),
                        aggregation.getMask()));
            }
        }

        if (!anyRewritten) {
            return Result.empty();
        }
        return Result.ofPlanNode(new AggregationNode(
                node.getId(),
                node.getSource(),
                aggregations.build(),
                node.getGroupingSets(),
                node.getPreGroupedVariables(),
                node.getStep(),
                node.getHashVariable(),
                node.getGroupIdVariable()));
    }
         */
        return Result.empty();
    }
}
