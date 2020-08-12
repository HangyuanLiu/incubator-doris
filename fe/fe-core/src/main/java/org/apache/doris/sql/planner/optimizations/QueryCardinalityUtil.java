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

import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.planner.iterative.GroupReference;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.plan.EnforceSingleRowNode;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.planner.plan.PlanVisitor;
import com.google.common.collect.Range;

import static org.apache.doris.sql.planner.iterative.Lookup.noLookup;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public final class QueryCardinalityUtil
{
    private QueryCardinalityUtil()
    {
    }

    public static boolean isScalar(LogicalPlanNode node)
    {
        return isScalar(node, noLookup());
    }

    public static boolean isScalar(LogicalPlanNode node, Lookup lookup)
    {
        return Range.singleton(1L).encloses(extractCardinality(node, lookup));
    }

    public static boolean isAtMostScalar(LogicalPlanNode node)
    {
        return isAtMostScalar(node, noLookup());
    }

    public static boolean isAtMostScalar(LogicalPlanNode node, Lookup lookup)
    {
        return isAtMost(node, lookup, 1L);
    }

    private static boolean isAtMost(LogicalPlanNode node, Lookup lookup, long maxCardinality)
    {
        return Range.closed(0L, maxCardinality).encloses(extractCardinality(node, lookup));
    }

    public static Range<Long> extractCardinality(LogicalPlanNode node)
    {
        return extractCardinality(node, noLookup());
    }

    public static Range<Long> extractCardinality(LogicalPlanNode node, Lookup lookup)
    {
        return node.accept(new CardinalityExtractorPlanVisitor(lookup), null);
    }

    private static final class CardinalityExtractorPlanVisitor
            extends PlanVisitor<Range<Long>, Void>
    {
        private final Lookup lookup;

        public CardinalityExtractorPlanVisitor(Lookup lookup)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        public Range<Long> visitPlan(LogicalPlanNode node, Void context)
        {
            return Range.atLeast(0L);
        }

        @Override
        public Range<Long> visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        @Override
        public Range<Long> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return Range.singleton(1L);
        }

        @Override
        public Range<Long> visitAggregation(AggregationNode node, Void context)
        {
            if (node.hasEmptyGroupingSet()) {
                return Range.singleton(1L);
            }
            return Range.atLeast(0L);
        }

        @Override
        public Range<Long> visitExchange(ExchangeNode node, Void context)
        {
            if (node.getSources().size() == 1) {
                return getOnlyElement(node.getSources()).accept(this, null);
            }
            return Range.atLeast(0L);
        }

        @Override
        public Range<Long> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, null);
        }

        @Override
        public Range<Long> visitFilter(FilterNode node, Void context)
        {
            Range<Long> sourceCardinalityRange = node.getSource().accept(this, null);
            if (sourceCardinalityRange.hasUpperBound()) {
                return Range.closed(0L, sourceCardinalityRange.upperEndpoint());
            }
            return Range.atLeast(0L);
        }

        @Override
        public Range<Long> visitValues(ValuesNode node, Void context)
        {
            return Range.singleton((long) node.getRows().size());
        }

        @Override
        public Range<Long> visitLimit(LimitNode node, Void context)
        {
            Range<Long> sourceCardinalityRange = node.getSource().accept(this, null);
            long upper = node.getCount();
            if (sourceCardinalityRange.hasUpperBound()) {
                upper = min(sourceCardinalityRange.upperEndpoint(), node.getCount());
            }
            long lower = min(upper, sourceCardinalityRange.lowerEndpoint());
            return Range.closed(lower, upper);
        }
    }
}