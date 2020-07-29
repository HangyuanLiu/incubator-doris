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


import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.iterative.matching.Capture;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.ProjectNode;

import org.apache.doris.sql.planner.iterative.Rule;

import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Literal;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static org.apache.doris.sql.planner.PlannerUtils.toVariableReference;
import static org.apache.doris.sql.planner.iterative.matching.Capture.newCapture;
import static org.apache.doris.sql.planner.plan.Patterns.aggregation;
import static org.apache.doris.sql.planner.plan.Patterns.project;
import static org.apache.doris.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.type.BigintType.BIGINT;

public class SimplifyCountOverConstant
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(CHILD)));

    private final FunctionResolution functionResolution;

    public SimplifyCountOverConstant(FunctionManager functionManager)
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
        ProjectNode child = captures.get(CHILD);

        boolean changed = false;
        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(parent.getAggregations());

        for (Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : parent.getAggregations().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (isCountOverConstant(aggregation, child.getAssignments(), context.getVariableAllocator().getTypes())) {
                changed = true;
                aggregations.put(variable, new AggregationNode.Aggregation(
                        new CallExpression(
                                "count",
                                functionResolution.countFunction(),
                                BIGINT,
                                ImmutableList.of()),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        aggregation.getMask()));
            }
        }

        if (!changed) {
            return Result.empty();
        }

        return Result.ofPlanNode(new AggregationNode(
                parent.getId(),
                child,
                aggregations,
                parent.getGroupingSets(),
                ImmutableList.of(),
                parent.getStep(),
                parent.getHashVariable(),
                parent.getGroupIdVariable()));
    }

    private boolean isCountOverConstant(AggregationNode.Aggregation aggregation, Assignments inputs, TypeProvider types)
    {
        if (!functionResolution.isCountFunction(aggregation.getFunctionHandle()) || aggregation.getArguments().size() != 1) {
            return false;
        }

        RowExpression argument = aggregation.getArguments().get(0);
        Expression assigned = null;
        if (castToExpression(argument) instanceof SymbolReference) {
            assigned = castToExpression(inputs.get(toVariableReference(castToExpression(argument), types)));
        }

        return assigned instanceof Literal && !(assigned instanceof NullLiteral);
    }
}
