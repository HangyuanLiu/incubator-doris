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

import org.apache.doris.sql.planner.iterative.matching.Captures;
import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.AggregationNode.Aggregation;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.AssignmentUtils;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.doris.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static org.apache.doris.sql.planner.plan.Patterns.aggregation;
import static org.apache.doris.sql.planner.plan.Patterns.applyNode;
import static org.apache.doris.sql.planner.plan.Patterns.filter;
import static org.apache.doris.sql.planner.plan.Patterns.join;
import static org.apache.doris.sql.planner.plan.Patterns.project;
import static org.apache.doris.sql.planner.plan.Patterns.values;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ExpressionRewriteRuleSet
{
    public interface ExpressionRewriter
    {
        Expression rewrite(Expression expression, Rule.Context context);
    }

    private final ExpressionRewriter rewriter;

    public ExpressionRewriteRuleSet(ExpressionRewriter rewriter)
    {
        this.rewriter = requireNonNull(rewriter, "rewriter is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                aggregationExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite(),
                applyExpressionRewrite());
    }

    public Rule<?> projectExpressionRewrite()
    {
        return new ProjectExpressionRewrite(rewriter);
    }

    public Rule<?> aggregationExpressionRewrite()
    {
        return new AggregationExpressionRewrite(rewriter);
    }

    public Rule<?> filterExpressionRewrite()
    {
        return new FilterExpressionRewrite(rewriter);
    }

    public Rule<?> joinExpressionRewrite()
    {
        return new JoinExpressionRewrite(rewriter);
    }

    public Rule<?> valuesExpressionRewrite()
    {
        return new ValuesExpressionRewrite(rewriter);
    }

    public Rule<?> applyExpressionRewrite()
    {
        return new ApplyExpressionRewrite(rewriter);
    }

    private static final class ProjectExpressionRewrite
            implements Rule<ProjectNode>
    {
        private final ExpressionRewriter rewriter;

        ProjectExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            Assignments assignments = AssignmentUtils.rewrite(projectNode.getAssignments(), x -> rewriter.rewrite(x, context));
            if (projectNode.getAssignments().equals(assignments)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new ProjectNode(projectNode.getId(), projectNode.getSource(), assignments));
        }
    }

    private static final class AggregationExpressionRewrite
            implements Rule<AggregationNode>
    {
        private final ExpressionRewriter rewriter;

        AggregationExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return aggregation();
        }

        @Override
        public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
        {
            boolean anyRewritten = false;
            ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
                Aggregation aggregation = entry.getValue();
                Aggregation rewritten = new Aggregation(
                        new CallExpression(aggregation.getCall().getDisplayName(),
                                aggregation.getCall().getFunctionHandle(),
                                aggregation.getCall().getType(),
                                aggregation.getCall().getArguments()
                                        .stream()
                                        .map(argument -> castToRowExpression(rewriter.rewrite(castToExpression(argument), context)))
                                        .collect(toImmutableList())),
                        aggregation.getFilter()
                                .map(filter -> castToRowExpression(rewriter.rewrite(castToExpression(filter), context))),
                        aggregation.getOrderBy(),
                        aggregation.isDistinct(),
                        aggregation.getMask());

                aggregations.put(entry.getKey(), rewritten);
                if (!aggregation.equals(rewritten)) {
                    anyRewritten = true;
                }
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new AggregationNode(
                        aggregationNode.getId(),
                        aggregationNode.getSource(),
                        aggregations.build(),
                        aggregationNode.getGroupingSets(),
                        aggregationNode.getPreGroupedVariables(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashVariable(),
                        aggregationNode.getGroupIdVariable()));
            }
            return Result.empty();
        }
    }

    private static final class FilterExpressionRewrite
            implements Rule<FilterNode>
    {
        private final ExpressionRewriter rewriter;

        FilterExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            RowExpression rewritten;
            if (isExpression(filterNode.getPredicate())) {
                rewritten = castToRowExpression(rewriter.rewrite(castToExpression(filterNode.getPredicate()), context));
            }
            else {
                rewritten = filterNode.getPredicate();
            }
            if (filterNode.getPredicate().equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterNode.getSource(), rewritten));
        }
    }

    private static final class JoinExpressionRewrite
            implements Rule<JoinNode>
    {
        private final ExpressionRewriter rewriter;

        JoinExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return join();
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            Optional<Expression> filter = joinNode.getFilter().map(x -> rewriter.rewrite(castToExpression(x), context));
            if (!joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).equals(filter)) {
                return Result.ofPlanNode(new JoinNode(
                        joinNode.getId(),
                        joinNode.getType(),
                        joinNode.getLeft(),
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        joinNode.getOutputVariables(),
                        filter.map(OriginalExpressionUtils::castToRowExpression),
                        joinNode.getLeftHashVariable(),
                        joinNode.getRightHashVariable(),
                        joinNode.getDistributionType()));
            }
            return Result.empty();
        }
    }

    private static final class ValuesExpressionRewrite
            implements Rule<ValuesNode>
    {
        private final ExpressionRewriter rewriter;

        ValuesExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<ValuesNode> getPattern()
        {
            return values();
        }

        @Override
        public Result apply(ValuesNode valuesNode, Captures captures, Context context)
        {
            boolean anyRewritten = false;
            ImmutableList.Builder<List<RowExpression>> rows = ImmutableList.builder();
            for (List<RowExpression> row : valuesNode.getRows()) {
                ImmutableList.Builder<RowExpression> newRow = ImmutableList.builder();
                for (RowExpression rowExpression : row) {
                    if (isExpression(rowExpression)) {
                        Expression expression = castToExpression(rowExpression);
                        Expression rewritten = rewriter.rewrite(expression, context);
                        newRow.add(castToRowExpression(rewritten));
                        if (!expression.equals(rewritten)) {
                            anyRewritten = true;
                        }
                    }
                    else {
                        // expression rewrite is to desugar AST; row expression should not change
                        newRow.add(rowExpression);
                    }
                }
                rows.add(newRow.build());
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), valuesNode.getOutputVariables(), rows.build()));
            }
            return Result.empty();
        }
    }

    private static final class ApplyExpressionRewrite
            implements Rule<ApplyNode>
    {
        private final ExpressionRewriter rewriter;

        ApplyExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<ApplyNode> getPattern()
        {
            return applyNode();
        }

        @Override
        public Result apply(ApplyNode applyNode, Captures captures, Context context)
        {
            Assignments subqueryAssignments = AssignmentUtils.rewrite(applyNode.getSubqueryAssignments(), x -> rewriter.rewrite(x, context));
            if (applyNode.getSubqueryAssignments().equals(subqueryAssignments)) {
                return Result.empty();
            }
            verifySubquerySupported(subqueryAssignments);
            return Result.ofPlanNode(new ApplyNode(
                    applyNode.getId(),
                    applyNode.getInput(),
                    applyNode.getSubquery(),
                    subqueryAssignments,
                    applyNode.getCorrelation(),
                    applyNode.getOriginSubqueryError()));
        }
    }
}
