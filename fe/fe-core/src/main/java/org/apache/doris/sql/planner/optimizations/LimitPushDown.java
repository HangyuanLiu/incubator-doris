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

import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.MarkDistinctNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.planner.plan.SortNode;
import com.google.common.collect.ImmutableList;

import static org.apache.doris.sql.planner.plan.LimitNode.Step.FINAL;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LimitPushDown
        implements PlanOptimizer
{
    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class LimitContext
    {
        private final long count;
        private final LimitNode.Step step;

        public LimitContext(long count, LimitNode.Step step)
        {
            this.count = count;
            this.step = step;
        }

        public long getCount()
        {
            return count;
        }

        public LimitNode.Step getStep()
        {
            return step;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("count", count)
                    .add("step", step)
                    .toString();
        }
    }

    private static class Rewriter
            extends SimplePlanRewriter<LimitContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public LogicalPlanNode visitPlan(LogicalPlanNode node, RewriteContext<LimitContext> context)
        {
            LogicalPlanNode rewrittenNode = context.defaultRewrite(node);

            LimitContext limit = context.get();
            if (limit != null) {
                // Drop in a LimitNode b/c we cannot push our limit down any further
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, limit.getCount(), limit.getStep());
            }
            return rewrittenNode;
        }

        @Override
        public LogicalPlanNode visitLimit(LimitNode node, RewriteContext<LimitContext> context)
        {
            long count = node.getCount();
            if (context.get() != null) {
                count = Math.min(count, context.get().getCount());
            }

            // return empty ValuesNode in case of limit 0
            if (count == 0) {
                return new ValuesNode(idAllocator.getNextId(),
                        node.getOutputVariables(),
                        ImmutableList.of());
            }

            // default visitPlan logic will insert the limit node
            return context.rewrite(node.getSource(), new LimitContext(count, FINAL));
        }

        @Override
        @Deprecated
        public LogicalPlanNode visitAggregation(AggregationNode node, RewriteContext<LimitContext> context)
        {
            LimitContext limit = context.get();
            /*
            if (limit != null &&
                    node.getAggregations().isEmpty() &&
                    node.getOutputVariables().size() == node.getGroupingKeys().size() &&
                    node.getOutputVariables().containsAll(node.getGroupingKeys())) {
                LogicalPlanNode rewrittenSource = context.rewrite(node.getSource());
                return new DistinctLimitNode(idAllocator.getNextId(), rewrittenSource, limit.getCount(), false, rewrittenSource.getOutputVariables(), Optional.empty());
            }

             */
            LogicalPlanNode rewrittenNode = context.defaultRewrite(node);
            if (limit != null) {
                // Drop in a LimitNode b/c limits cannot be pushed through aggregations
                rewrittenNode = new LimitNode(idAllocator.getNextId(), rewrittenNode, limit.getCount(), limit.getStep());
            }
            return rewrittenNode;
        }

        @Override
        public LogicalPlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<LimitContext> context)
        {
            // the fallback logic (in visitPlan) for node types we don't know about introduces a limit node,
            // so we need this here to push the limit through this trivial node type
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public LogicalPlanNode visitProject(ProjectNode node, RewriteContext<LimitContext> context)
        {
            // the fallback logic (in visitPlan) for node types we don't know about introduces a limit node,
            // so we need this here to push the limit through this trivial node type
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public LogicalPlanNode visitTopN(TopNNode node, RewriteContext<LimitContext> context)
        {
            LimitContext limit = context.get();

            LogicalPlanNode rewrittenSource = context.rewrite(node.getSource());
            if (rewrittenSource == node.getSource() && limit == null) {
                return node;
            }

            long count = node.getCount();
            if (limit != null) {
                count = Math.min(count, limit.getCount());
            }
            return new TopNNode(node.getId(), rewrittenSource, count, node.getOrderingScheme(), node.getStep());
        }

        @Override
        @Deprecated
        public LogicalPlanNode visitSort(SortNode node, RewriteContext<LimitContext> context)
        {
            LimitContext limit = context.get();

            LogicalPlanNode rewrittenSource = context.rewrite(node.getSource());
            if (limit != null) {
                return new TopNNode(node.getId(), rewrittenSource, limit.getCount(), node.getOrderingScheme(), TopNNode.Step.SINGLE);
            }
            else if (rewrittenSource != node.getSource()) {
                return new SortNode(node.getId(), rewrittenSource, node.getOrderingScheme(), node.isPartial());
            }
            return node;
        }

        @Override
        public LogicalPlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<LimitContext> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource(), context.get());
            if (source != node.getSource()) {
                return new SemiJoinNode(
                        node.getId(),
                        source,
                        node.getFilteringSource(),
                        node.getSourceJoinVariable(),
                        node.getFilteringSourceJoinVariable(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashVariable(),
                        node.getFilteringSourceHashVariable(),
                        node.getDistributionType());
            }
            return node;
        }
    }
}
