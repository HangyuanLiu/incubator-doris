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

import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.AggregationNode.Step.SINGLE;

public final class AggregationNode
        extends LogicalPlanNode
{
    private final LogicalPlanNode source;
    private final Map<VariableReferenceExpression, Aggregation> aggregations;
    private final GroupingSetDescriptor groupingSets;
    private final List<VariableReferenceExpression> preGroupedVariables;
    private final Step step;
    private final Optional<VariableReferenceExpression> hashVariable;
    private final Optional<VariableReferenceExpression> groupIdVariable;
    private final List<VariableReferenceExpression> outputs;

    public AggregationNode(
            PlanNodeId id,
            LogicalPlanNode source,
            Map<VariableReferenceExpression, Aggregation> aggregations,
            GroupingSetDescriptor groupingSets,
            List<VariableReferenceExpression> preGroupedVariables,
            Step step,
            Optional<VariableReferenceExpression> hashVariable,
            Optional<VariableReferenceExpression> groupIdVariable)
    {
        super(id);

        this.source = source;
        this.aggregations = unmodifiableMap(new LinkedHashMap<>(requireNonNull(aggregations, "aggregations is null")));

        requireNonNull(groupingSets, "groupingSets is null");
        groupIdVariable.ifPresent(variable -> checkArgument(groupingSets.getGroupingKeys().contains(variable), "Grouping columns does not contain groupId column"));
        this.groupingSets = groupingSets;

        this.groupIdVariable = requireNonNull(groupIdVariable);

        boolean noOrderBy = aggregations.values().stream()
                .map(Aggregation::getOrderBy)
                .noneMatch(Optional::isPresent);
        checkArgument(noOrderBy || step == SINGLE, "ORDER BY does not support distributed aggregation");

        this.step = step;
        this.hashVariable = hashVariable;

        requireNonNull(preGroupedVariables, "preGroupedVariables is null");
        checkArgument(preGroupedVariables.isEmpty() || groupingSets.getGroupingKeys().containsAll(preGroupedVariables), "Pre-grouped variables must be a subset of the grouping keys");
        this.preGroupedVariables = unmodifiableList(new ArrayList<>(preGroupedVariables));

        ArrayList<VariableReferenceExpression> outputs = new ArrayList<>(groupingSets.getGroupingKeys());
        hashVariable.ifPresent(outputs::add);
        outputs.addAll(new ArrayList<>(aggregations.keySet()));

        this.outputs = unmodifiableList(outputs);
    }

    public List<VariableReferenceExpression> getGroupingKeys()
    {
        return groupingSets.getGroupingKeys();
    }

    public GroupingSetDescriptor getGroupingSets()
    {
        return groupingSets;
    }

    /**
     * @return whether this node should produce default output in case of no input pages.
     * For example for query:
     * <p>
     * SELECT count(*) FROM nation WHERE nationkey < 0
     * <p>
     * A default output of "0" is expected to be produced by FINAL aggregation operator.
     */
    public boolean hasDefaultOutput()
    {
        return hasEmptyGroupingSet() && (step.isOutputPartial() || step.equals(SINGLE));
    }

    public boolean hasEmptyGroupingSet()
    {
        return !groupingSets.getGlobalGroupingSets().isEmpty();
    }

    public boolean hasNonEmptyGroupingSet()
    {
        return groupingSets.getGroupingSetCount() > groupingSets.getGlobalGroupingSets().size();
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return unmodifiableList(Collections.singletonList(source));
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputs;
    }

    public Map<VariableReferenceExpression, Aggregation> getAggregations()
    {
        return aggregations;
    }

    public List<VariableReferenceExpression> getPreGroupedVariables()
    {
        return preGroupedVariables;
    }

    public int getGroupingSetCount()
    {
        return groupingSets.getGroupingSetCount();
    }

    public Set<Integer> getGlobalGroupingSets()
    {
        return groupingSets.getGlobalGroupingSets();
    }

    public LogicalPlanNode getSource()
    {
        return source;
    }

    public Step getStep()
    {
        return step;
    }

    public Optional<VariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    public Optional<VariableReferenceExpression> getGroupIdVariable()
    {
        return groupIdVariable;
    }

    public boolean hasOrderings()
    {
        return aggregations.values().stream()
                .map(Aggregation::getOrderBy)
                .anyMatch(Optional::isPresent);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAggregation(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "Unexpected number of elements in list newChildren");
        return new AggregationNode(getId(), newChildren.get(0), aggregations, groupingSets, preGroupedVariables, step, hashVariable, groupIdVariable);
    }

    public boolean isStreamable()
    {
        return !preGroupedVariables.isEmpty() && groupingSets.getGroupingSetCount() == 1 && groupingSets.getGlobalGroupingSets().isEmpty();
    }

    public static GroupingSetDescriptor globalAggregation()
    {
        return singleGroupingSet(unmodifiableList(emptyList()));
    }

    public static GroupingSetDescriptor singleGroupingSet(List<VariableReferenceExpression> groupingKeys)
    {
        Set<Integer> globalGroupingSets;
        if (groupingKeys.isEmpty()) {
            globalGroupingSets = unmodifiableSet(Collections.singleton(0));
        }
        else {
            globalGroupingSets = unmodifiableSet(emptySet());
        }

        return new GroupingSetDescriptor(groupingKeys, 1, globalGroupingSets);
    }

    public static GroupingSetDescriptor groupingSets(List<VariableReferenceExpression> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets)
    {
        return new GroupingSetDescriptor(groupingKeys, groupingSetCount, globalGroupingSets);
    }

    public static class GroupingSetDescriptor
    {
        private final List<VariableReferenceExpression> groupingKeys;
        private final int groupingSetCount;
        private final Set<Integer> globalGroupingSets;

        public GroupingSetDescriptor(
                List<VariableReferenceExpression> groupingKeys,
                int groupingSetCount,
                Set<Integer> globalGroupingSets)
        {
            requireNonNull(globalGroupingSets, "globalGroupingSets is null");
            checkArgument(globalGroupingSets.size() <= groupingSetCount, "list of empty global grouping sets must be no larger than grouping set count");
            requireNonNull(groupingKeys, "groupingKeys is null");
            if (groupingKeys.isEmpty()) {
                checkArgument(!globalGroupingSets.isEmpty(), "no grouping keys implies at least one global grouping set, but none provided");
            }

            this.groupingKeys = unmodifiableList(new ArrayList<>(groupingKeys));
            this.groupingSetCount = groupingSetCount;
            this.globalGroupingSets = unmodifiableSet(new LinkedHashSet<>(globalGroupingSets));
        }

        public List<VariableReferenceExpression> getGroupingKeys()
        {
            return groupingKeys;
        }

        public int getGroupingSetCount()
        {
            return groupingSetCount;
        }

        public Set<Integer> getGlobalGroupingSets()
        {
            return globalGroupingSets;
        }
    }

    public enum Step
    {
        PARTIAL(true, true),
        FINAL(false, false),
        INTERMEDIATE(false, true),
        SINGLE(true, false);

        private final boolean inputRaw;
        private final boolean outputPartial;

        Step(boolean inputRaw, boolean outputPartial)
        {
            this.inputRaw = inputRaw;
            this.outputPartial = outputPartial;
        }

        public boolean isInputRaw()
        {
            return inputRaw;
        }

        public boolean isOutputPartial()
        {
            return outputPartial;
        }

        public static Step partialOutput(Step step)
        {
            if (step.isInputRaw()) {
                return Step.PARTIAL;
            }
            else {
                return Step.INTERMEDIATE;
            }
        }

        public static Step partialInput(Step step)
        {
            if (step.isOutputPartial()) {
                return Step.INTERMEDIATE;
            }
            else {
                return Step.FINAL;
            }
        }
    }

    public static class Aggregation
    {
        private final CallExpression call;
        private final Optional<RowExpression> filter;
        private final Optional<OrderingScheme> orderingScheme;
        private final boolean isDistinct;
        private final Optional<VariableReferenceExpression> mask;

        public Aggregation(
                CallExpression call,
                Optional<RowExpression> filter,
                Optional<OrderingScheme> orderingScheme,
                boolean isDistinct,
                Optional<VariableReferenceExpression> mask)
        {
            this.call = requireNonNull(call, "call is null");
            this.filter = requireNonNull(filter, "filter is null");
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
            this.isDistinct = isDistinct;
            this.mask = requireNonNull(mask, "mask is null");
        }

        public CallExpression getCall()
        {
            return call;
        }

        public FunctionHandle getFunctionHandle()
        {
            return call.getFunctionHandle();
        }

        public List<RowExpression> getArguments()
        {
            return call.getArguments();
        }

        public Optional<OrderingScheme> getOrderBy()
        {
            return orderingScheme;
        }

        public Optional<RowExpression> getFilter()
        {
            return filter;
        }

        public boolean isDistinct()
        {
            return isDistinct;
        }

        public Optional<VariableReferenceExpression> getMask()
        {
            return mask;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Aggregation)) {
                return false;
            }
            Aggregation that = (Aggregation) o;
            return isDistinct == that.isDistinct &&
                    Objects.equals(call, that.call) &&
                    Objects.equals(filter, that.filter) &&
                    Objects.equals(orderingScheme, that.orderingScheme) &&
                    Objects.equals(mask, that.mask);
        }

        @Override
        public String toString()
        {
            return "Aggregation{" +
                    "call=" + call +
                    ", filter=" + filter +
                    ", orderingScheme=" + orderingScheme +
                    ", isDistinct=" + isDistinct +
                    ", mask=" + mask +
                    '}';
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(call, filter, orderingScheme, isDistinct, mask);
        }
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}