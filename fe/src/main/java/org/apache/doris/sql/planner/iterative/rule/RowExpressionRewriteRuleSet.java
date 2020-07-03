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
import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.builder;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.Patterns.aggregation;
import static org.apache.doris.sql.planner.plan.Patterns.filter;
import static org.apache.doris.sql.planner.plan.Patterns.project;

public class RowExpressionRewriteRuleSet
{
    public interface PlanRowExpressionRewriter
    {
        RowExpression rewrite(RowExpression expression, Rule.Context context);
    }

    protected final PlanRowExpressionRewriter rewriter;

    public RowExpressionRewriteRuleSet(PlanRowExpressionRewriter rewriter)
    {
        this.rewriter = requireNonNull(rewriter, "rewriter is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                filterRowExpressionRewriteRule(),
                projectRowExpressionRewriteRule(),
                aggregationRowExpressionRewriteRule());
    }

    public Rule<FilterNode> filterRowExpressionRewriteRule()
    {
        return new FilterRowExpressionRewrite();
    }

    public Rule<ProjectNode> projectRowExpressionRewriteRule()
    {
        return new ProjectRowExpressionRewrite();
    }

    public Rule<AggregationNode> aggregationRowExpressionRewriteRule()
    {
        return new AggregationRowExpressionRewrite();
    }

    private final class ProjectRowExpressionRewrite
            implements Rule<ProjectNode>
    {
        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            Assignments.Builder builder = Assignments.builder();
            boolean anyRewritten = false;
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().getMap().entrySet()) {
                RowExpression rewritten = rewriter.rewrite(entry.getValue(), context);
                if (!rewritten.equals(entry.getValue())) {
                    anyRewritten = true;
                }
                builder.put(entry.getKey(), rewritten);
            }
            Assignments assignments = builder.build();
            if (anyRewritten) {
                return Result.ofPlanNode(new ProjectNode(projectNode.getId(), projectNode.getSource(), assignments));
            }
            return Result.empty();
        }
    }

    private final class FilterRowExpressionRewrite
            implements Rule<FilterNode>
    {
        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            checkState(filterNode.getSource() != null);
            RowExpression rewritten = rewriter.rewrite(filterNode.getPredicate(), context);

            if (filterNode.getPredicate().equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterNode.getSource(), rewritten));
        }
    }

    private final class AggregationRowExpressionRewrite
            implements Rule<AggregationNode>
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return aggregation();
        }

        @Override
        public Result apply(AggregationNode node, Captures captures, Context context)
        {
            checkState(node.getSource() != null);

            boolean changed = false;
            ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> rewrittenAggregation = builder();
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                AggregationNode.Aggregation rewritten = rewriteAggregation(entry.getValue(), context);
                rewrittenAggregation.put(entry.getKey(), rewritten);
                if (!rewritten.equals(entry.getValue())) {
                    changed = true;
                }
            }

            if (changed) {
                AggregationNode aggregationNode = new AggregationNode(
                        node.getId(),
                        node.getSource(),
                        rewrittenAggregation.build(),
                        node.getGroupingSets(),
                        node.getPreGroupedVariables(),
                        node.getStep(),
                        node.getHashVariable(),
                        node.getGroupIdVariable());
                return Result.ofPlanNode(aggregationNode);
            }
            return Result.empty();
        }
    }


    private AggregationNode.Aggregation rewriteAggregation(AggregationNode.Aggregation aggregation, Rule.Context context)
    {
        RowExpression rewrittenCall = rewriter.rewrite(aggregation.getCall(), context);
        checkArgument(rewrittenCall instanceof CallExpression, "Aggregation CallExpression must be rewritten to CallExpression");
        return new AggregationNode.Aggregation(
                (CallExpression) rewrittenCall,
                aggregation.getFilter().map(filter -> rewriter.rewrite(filter, context)),
                aggregation.getOrderBy(),
                aggregation.isDistinct(),
                aggregation.getMask());
    }
}
