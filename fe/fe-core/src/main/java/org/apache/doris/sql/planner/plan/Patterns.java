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
package org.apache.doris.sql.planner.plan;


import org.apache.doris.sql.planner.iterative.matching.Pattern;
import org.apache.doris.sql.planner.iterative.matching.Property;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Optional;

import static org.apache.doris.sql.planner.iterative.matching.Pattern.typeOf;
import static org.apache.doris.sql.planner.iterative.matching.Property.optionalProperty;
import static org.apache.doris.sql.planner.iterative.matching.Property.property;

public class Patterns
{
    private Patterns() {}

    public static Pattern<AggregationNode> aggregation()
    {
        return typeOf(AggregationNode.class);
    }

    public static Pattern<ApplyNode> applyNode()
    {
        return typeOf(ApplyNode.class);
    }

    public static Pattern<ProjectNode> project()
    {
        return typeOf(ProjectNode.class);
    }

    public static Pattern<SemiJoinNode> semiJoin()
    {
        return typeOf(SemiJoinNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<JoinNode> join()
    {
        return typeOf(JoinNode.class);
    }

    public static Pattern<LateralJoinNode> lateralJoin()
    {
        return typeOf(LateralJoinNode.class);
    }

    public static Pattern<LimitNode> limit()
    {
        return typeOf(LimitNode.class);
    }

    public static Pattern<SortNode> sort()
    {
        return typeOf(SortNode.class);
    }

    public static Pattern<TopNNode> topN()
    {
        return typeOf(TopNNode.class);
    }

    public static Pattern<ValuesNode> values()
    {
        return typeOf(ValuesNode.class);
    }

    public static Pattern<TableScanNode> tableScan()
    {
        return typeOf(TableScanNode.class);
    }

    public static Property<LogicalPlanNode, LogicalPlanNode> source()
    {
        return optionalProperty("source", node -> node.getSources().size() == 1 ?
                Optional.of(node.getSources().get(0)) :
                Optional.empty());
    }

    public static class Apply
    {
        public static Property<ApplyNode, List<VariableReferenceExpression>> correlation()
        {
            return property("correlation", ApplyNode::getCorrelation);
        }
    }

    public static class Join
    {
        public static Property<JoinNode, JoinNode.Type> type()
        {
            return property("type", JoinNode::getType);
        }
    }

    public static class LateralJoin
    {
        public static Property<LateralJoinNode, List<VariableReferenceExpression>> correlation()
        {
            return property("correlation", LateralJoinNode::getCorrelation);
        }

        public static Property<LateralJoinNode, LogicalPlanNode> subquery()
        {
            return property("subquery", LateralJoinNode::getSubquery);
        }
    }

    public static class Limit
    {
        public static Property<LimitNode, Long> count()
        {
            return property("count", LimitNode::getCount);
        }
    }
}
