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

import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static org.apache.doris.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static org.apache.doris.sql.planner.plan.Patterns.aggregation;
import static org.apache.doris.sql.relational.Expressions.constant;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.type.BigintType.BIGINT;

/**
 * A count over a subquery can be reduced to a VALUES(1) provided
 * the subquery is a scalar
 */
public class PruneCountAggregationOverScalar
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();
    private final FunctionResolution functionResolution;

    public PruneCountAggregationOverScalar(FunctionManager functionManager)
    {
        requireNonNull(functionManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionManager);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        if (!parent.hasDefaultOutput() || parent.getOutputVariables().size() != 1) {
            return Result.empty();
        }
        Map<VariableReferenceExpression, AggregationNode.Aggregation> assignments = parent.getAggregations();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : assignments.entrySet()) {
            AggregationNode.Aggregation aggregation = entry.getValue();
            requireNonNull(aggregation, "aggregation is null");
            if (!functionResolution.isCountFunction(aggregation.getFunctionHandle()) || !aggregation.getArguments().isEmpty()) {
                return Result.empty();
            }
        }
        if (!assignments.isEmpty() && isScalar(parent.getSource(), context.getLookup())) {
            return Result.ofPlanNode(new ValuesNode(
                    parent.getId(),
                    parent.getOutputVariables(),
                    ImmutableList.of(ImmutableList.of(constant(1L, BIGINT)))));
        }
        return Result.empty();
    }
}
