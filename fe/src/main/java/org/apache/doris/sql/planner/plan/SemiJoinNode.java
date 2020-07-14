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
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SemiJoinNode
        extends LogicalPlanNode
{
    private final LogicalPlanNode source;
    private final LogicalPlanNode filteringSource;
    private final VariableReferenceExpression sourceJoinVariable;
    private final VariableReferenceExpression filteringSourceJoinVariable;
    private final VariableReferenceExpression semiJoinOutput;
    private final Optional<VariableReferenceExpression> sourceHashVariable;
    private final Optional<VariableReferenceExpression> filteringSourceHashVariable;
    private final Optional<DistributionType> distributionType;

    public SemiJoinNode(PlanNodeId id,
                        LogicalPlanNode source,
                        LogicalPlanNode filteringSource,
                        VariableReferenceExpression sourceJoinVariable,
                       VariableReferenceExpression filteringSourceJoinVariable,
                       VariableReferenceExpression semiJoinOutput,
                       Optional<VariableReferenceExpression> sourceHashVariable,
                       Optional<VariableReferenceExpression> filteringSourceHashVariable,
                        Optional<DistributionType> distributionType)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.filteringSource = requireNonNull(filteringSource, "filteringSource is null");
        this.sourceJoinVariable = requireNonNull(sourceJoinVariable, "sourceJoinVariable is null");
        this.filteringSourceJoinVariable = requireNonNull(filteringSourceJoinVariable, "filteringSourceJoinVariable is null");
        this.semiJoinOutput = requireNonNull(semiJoinOutput, "semiJoinOutput is null");
        this.sourceHashVariable = requireNonNull(sourceHashVariable, "sourceHashVariable is null");
        this.filteringSourceHashVariable = requireNonNull(filteringSourceHashVariable, "filteringSourceHashVariable is null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");

        checkArgument(source.getOutputVariables().contains(sourceJoinVariable), "Source does not contain join symbol");
        checkArgument(filteringSource.getOutputVariables().contains(filteringSourceJoinVariable), "Filtering source does not contain filtering join symbol");
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    public LogicalPlanNode getSource()
    {
        return source;
    }

    public LogicalPlanNode getFilteringSource()
    {
        return filteringSource;
    }

    public VariableReferenceExpression getSourceJoinVariable()
    {
        return sourceJoinVariable;
    }

    public VariableReferenceExpression getFilteringSourceJoinVariable()
    {
        return filteringSourceJoinVariable;
    }

    public VariableReferenceExpression getSemiJoinOutput()
    {
        return semiJoinOutput;
    }

    public Optional<VariableReferenceExpression> getSourceHashVariable()
    {
        return sourceHashVariable;
    }

    public Optional<VariableReferenceExpression> getFilteringSourceHashVariable()
    {
        return filteringSourceHashVariable;
    }

    public Optional<DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @Override
    public List<LogicalPlanNode> getSources()
    {
        return ImmutableList.of(source, filteringSource);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(source.getOutputVariables())
                .add(semiJoinOutput)
                .build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitSemiJoin(this, context);
    }

    @Override
    public LogicalPlanNode replaceChildren(List<LogicalPlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new SemiJoinNode(
                getId(),
                newChildren.get(0),
                newChildren.get(1),
                sourceJoinVariable,
                filteringSourceJoinVariable,
                semiJoinOutput,
                sourceHashVariable,
                filteringSourceHashVariable,
                distributionType);
    }

    public SemiJoinNode withDistributionType(DistributionType distributionType)
    {
        return new SemiJoinNode(
                getId(),
                source,
                filteringSource,
                sourceJoinVariable,
                filteringSourceJoinVariable,
                semiJoinOutput,
                sourceHashVariable,
                filteringSourceHashVariable,
                Optional.of(distributionType));
    }
}
