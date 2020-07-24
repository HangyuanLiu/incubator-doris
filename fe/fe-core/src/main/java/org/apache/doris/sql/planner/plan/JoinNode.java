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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static org.apache.doris.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.*;

public class JoinNode
        extends LogicalPlanNode
{
    private final Type type;
    private final LogicalPlanNode left;
    private final LogicalPlanNode right;
    private final List<EquiJoinClause> criteria;
    private final List<VariableReferenceExpression> outputVariables;
    private final Optional<RowExpression> filter;
    private final Optional<VariableReferenceExpression> leftHashVariable;
    private final Optional<VariableReferenceExpression> rightHashVariable;
    private final Optional<DistributionType> distributionType;
    
    public JoinNode(PlanNodeId id, Type type, LogicalPlanNode left,
                    LogicalPlanNode right,
                    List<EquiJoinClause> criteria,
                    List<VariableReferenceExpression> outputVariables,
                    Optional<RowExpression> filter,
                    Optional<VariableReferenceExpression> leftHashVariable,
                    Optional<VariableReferenceExpression> rightHashVariable,
                    Optional<DistributionType> distributionType)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(outputVariables, "outputVariables is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashVariable, "leftHashVariable is null");
        requireNonNull(rightHashVariable, "rightHashVariable is null");
        requireNonNull(distributionType, "distributionType is null");

        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.outputVariables = ImmutableList.copyOf(outputVariables);
        this.filter = filter;
        this.leftHashVariable = leftHashVariable;
        this.rightHashVariable = rightHashVariable;
        this.distributionType = distributionType;

        Set<VariableReferenceExpression> inputVariables = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(left.getOutputVariables())
                .addAll(right.getOutputVariables())
                .build();
        checkArgument(new HashSet<>(inputVariables).containsAll(outputVariables), "Left and right join inputs do not contain all output variables");
        checkArgument(!isCrossJoin() || inputVariables.size() == outputVariables.size(), "Cross join does not support output variables pruning or reordering");

        checkArgument(!(criteria.isEmpty() && leftHashVariable.isPresent()), "Left hash variable is only valid in an equijoin");
        checkArgument(!(criteria.isEmpty() && rightHashVariable.isPresent()), "Right hash variable is only valid in an equijoin");

        if (distributionType.isPresent()) {
            // The implementation of full outer join only works if the data is hash partitioned.
            checkArgument(
                    !(distributionType.get() == REPLICATED && type.mustPartition()),
                    "%s join do not work with %s distribution type",
                    type,
                    distributionType.get());
            // It does not make sense to PARTITION when there is nothing to partition on
            checkArgument(
                    !(distributionType.get() == PARTITIONED && type.mustReplicate(criteria)),
                    "Equi criteria are empty, so %s join should not have %s distribution type",
                    type,
                    distributionType.get());
        }
    }

    public JoinNode flipChildren()
    {
        return new JoinNode(
                getId(),
                flipType(type),
                right,
                left,
                flipJoinCriteria(criteria),
                flipOutputVariables(getOutputVariables(), left, right),
                filter,
                rightHashVariable,
                leftHashVariable,
                distributionType);
    }

    private static Type flipType(Type type)
    {
        switch (type) {
            case INNER:
                return INNER;
            case FULL:
                return FULL;
            case LEFT:
                return RIGHT;
            case RIGHT:
                return LEFT;
            default:
                throw new IllegalStateException("No inverse defined for join type: " + type);
        }
    }

    private static List<EquiJoinClause> flipJoinCriteria(List<EquiJoinClause> joinCriteria)
    {
        return joinCriteria.stream()
                .map(EquiJoinClause::flip)
                .collect(Collectors.toList());
    }

    private static List<VariableReferenceExpression> flipOutputVariables(List<VariableReferenceExpression> outputVariables, LogicalPlanNode left, LogicalPlanNode right)
    {
        List<VariableReferenceExpression> leftVariables = outputVariables.stream()
                .filter(variable -> left.getOutputVariables().contains(variable))
                .collect(Collectors.toList());
        List<VariableReferenceExpression> rightVariables = outputVariables.stream()
                .filter(variable -> right.getOutputVariables().contains(variable))
                .collect(Collectors.toList());
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(rightVariables)
                .addAll(leftVariables)
                .build();
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        FULL("FullJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public boolean mustPartition()
        {
            // With REPLICATED, the unmatched rows from right-side would be duplicated.
            return this == RIGHT || this == FULL;
        }

        public boolean mustReplicate(List<JoinNode.EquiJoinClause> criteria)
        {
            // There is nothing to partition on
            return criteria.isEmpty() && (this == INNER || this == LEFT);
        }
    }


    public Type getType()
    {
        return type;
    }


    public LogicalPlanNode getLeft()
    {
        return left;
    }

    public LogicalPlanNode getRight()
    {
        return right;
    }

    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    public Optional<RowExpression> getFilter()
    {
        return filter;
    }

    /*
    public Optional<SortExpressionContext> getSortExpressionContext(FunctionManager functionManager)
    {
        return filter
                .flatMap(filter -> extractSortExpression(ImmutableSet.copyOf(right.getOutputVariables()), filter, functionManager));
    }
    */

    public Optional<VariableReferenceExpression> getLeftHashVariable()
    {
        return leftHashVariable;
    }


    public Optional<VariableReferenceExpression> getRightHashVariable()
    {
        return rightHashVariable;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return ImmutableList.of(left, right);
    }

    @Override

    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }


    public Optional<DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new JoinNode(getId(), type, newChildren.get(0), newChildren.get(1), criteria, outputVariables, filter, leftHashVariable, rightHashVariable, distributionType);
    }

    public JoinNode withDistributionType(DistributionType distributionType)
    {
        return new JoinNode(getId(), type, left, right, criteria, outputVariables, filter, leftHashVariable, rightHashVariable, Optional.of(distributionType));
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && !filter.isPresent() && type == INNER;
    }

    public static class EquiJoinClause
    {
        private final VariableReferenceExpression left;
        private final VariableReferenceExpression right;

        public EquiJoinClause(VariableReferenceExpression left, VariableReferenceExpression right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

    
        public VariableReferenceExpression getLeft()
        {
            return left;
        }

    
        public VariableReferenceExpression getRight()
        {
            return right;
        }

        public EquiJoinClause flip()
        {
            return new EquiJoinClause(right, left);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            EquiJoinClause other = (EquiJoinClause) obj;

            return Objects.equals(this.left, other.left) &&
                    Objects.equals(this.right, other.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(left, right);
        }

        @Override
        public String toString()
        {
            return format("%s = %s", left, right);
        }
    }
}
