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

import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.AssignUniqueId;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.iterative.Lookup;
import org.apache.doris.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import org.apache.doris.sql.planner.plan.EnforceSingleRowNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.doris.sql.type.BooleanType;
import org.apache.doris.sql.type.Type;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.doris.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.apache.doris.sql.planner.plan.AggregationNode.singleGroupingSet;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.tree.BooleanLiteral.TRUE_LITERAL;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.type.BigintType.BIGINT;

// TODO: move this class to TransformCorrelatedScalarAggregationToJoin when old optimizer is gone
public class ScalarAggregationToJoinRewriter
{
    private final FunctionResolution functionResolution;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Lookup lookup;
    private final PlanNodeDecorrelator planNodeDecorrelator;

    public ScalarAggregationToJoinRewriter(FunctionManager functionManager, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        requireNonNull(functionManager, "metadata is null");
        this.functionResolution = new FunctionResolution(functionManager);
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.planNodeDecorrelator = new PlanNodeDecorrelator(idAllocator, variableAllocator, lookup);
    }

    public LogicalPlanNode rewriteScalarAggregation(LateralJoinNode lateralJoinNode, AggregationNode aggregation)
    {
        List<VariableReferenceExpression> correlation = lateralJoinNode.getCorrelation();
        Optional<DecorrelatedNode> source = planNodeDecorrelator.decorrelateFilters(lookup.resolve(aggregation.getSource()), correlation);
        if (!source.isPresent()) {
            return lateralJoinNode;
        }

        VariableReferenceExpression nonNull = variableAllocator.newVariable("non_null", BooleanType.BOOLEAN);
        Assignments scalarAggregationSourceAssignments = Assignments.builder()
                .putAll(identitiesAsSymbolReferences(source.get().getNode().getOutputVariables()))
                .put(nonNull, castToRowExpression(TRUE_LITERAL))
                .build();
        ProjectNode scalarAggregationSourceWithNonNullableVariable = new ProjectNode(
                idAllocator.getNextId(),
                source.get().getNode(),
                scalarAggregationSourceAssignments);

        return rewriteScalarAggregation(
                lateralJoinNode,
                aggregation,
                scalarAggregationSourceWithNonNullableVariable,
                source.get().getCorrelatedPredicates(),
                nonNull);
    }

    private LogicalPlanNode rewriteScalarAggregation(
            LateralJoinNode lateralJoinNode,
            AggregationNode scalarAggregation,
            LogicalPlanNode scalarAggregationSource,
            Optional<Expression> joinExpression,
            VariableReferenceExpression nonNull)
    {
        AssignUniqueId inputWithUniqueColumns = new AssignUniqueId(
                idAllocator.getNextId(),
                lateralJoinNode.getInput(),
                variableAllocator.newVariable("unique", BIGINT));

        JoinNode leftOuterJoin = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.LEFT,
                inputWithUniqueColumns,
                scalarAggregationSource,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(inputWithUniqueColumns.getOutputVariables())
                        .addAll(scalarAggregationSource.getOutputVariables())
                        .build(),
                joinExpression.map(OriginalExpressionUtils::castToRowExpression),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Optional<AggregationNode> aggregationNode = createAggregationNode(
                scalarAggregation,
                leftOuterJoin,
                nonNull);

        if (!aggregationNode.isPresent()) {
            return lateralJoinNode;
        }

        Optional<ProjectNode> subqueryProjection = searchFrom(lateralJoinNode.getSubquery(), lookup)
                .where(ProjectNode.class::isInstance)
                .recurseOnlyWhen(EnforceSingleRowNode.class::isInstance)
                .findFirst();

        List<VariableReferenceExpression> aggregationOutputVariables = getTruncatedAggregationVariables(lateralJoinNode, aggregationNode.get());

        if (subqueryProjection.isPresent()) {
            Assignments assignments = Assignments.builder()
                    .putAll(identitiesAsSymbolReferences(aggregationOutputVariables))
                    .putAll(subqueryProjection.get().getAssignments())
                    .build();

            return new ProjectNode(
                    idAllocator.getNextId(),
                    aggregationNode.get(),
                    assignments);
        }
        else {
            return new ProjectNode(
                    idAllocator.getNextId(),
                    aggregationNode.get(),
                    identityAssignmentsAsSymbolReferences(aggregationOutputVariables));
        }
    }

    private List<VariableReferenceExpression> getTruncatedAggregationVariables(LateralJoinNode lateralJoinNode, AggregationNode aggregationNode)
    {
        Set<VariableReferenceExpression> applyVariables = new HashSet<>(lateralJoinNode.getOutputVariables());
        return aggregationNode.getOutputVariables().stream()
                .filter(applyVariables::contains)
                .collect(Collectors.toList());
    }

    private Optional<AggregationNode> createAggregationNode(
            AggregationNode scalarAggregation,
            JoinNode leftOuterJoin,
            VariableReferenceExpression nonNull)
    {
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : scalarAggregation.getAggregations().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            if (functionResolution.isCountFunction(entry.getValue().getFunctionHandle())) {
                Type scalarAggregationSourceType = nonNull.getType();
                aggregations.put(variable, new AggregationNode.Aggregation(
                        new CallExpression(
                                "count",
                                functionResolution.countFunction(scalarAggregationSourceType),
                                BIGINT,
                                ImmutableList.of(castToRowExpression(asSymbolReference(nonNull)))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        entry.getValue().getMask()));
            }
            else {
                aggregations.put(variable, entry.getValue());
            }
        }

        return Optional.of(new AggregationNode(
                idAllocator.getNextId(),
                leftOuterJoin,
                aggregations.build(),
                singleGroupingSet(leftOuterJoin.getLeft().getOutputVariables()),
                ImmutableList.of(),
                scalarAggregation.getStep(),
                scalarAggregation.getHashVariable(),
                Optional.empty()));
    }
}
