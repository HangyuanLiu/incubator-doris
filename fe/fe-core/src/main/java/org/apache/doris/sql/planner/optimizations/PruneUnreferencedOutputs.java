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
import org.apache.doris.sql.metadata.ColumnHandle;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.AggregationNode.Aggregation;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.MarkDistinctNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.planner.PartitioningScheme;
import org.apache.doris.sql.planner.VariablesExtractor;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.AssignUniqueId;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.planner.plan.GroupIdNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.planner.plan.SortNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.doris.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static org.apache.doris.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static org.apache.doris.sql.planner.optimizations.QueryCardinalityUtil.isScalar;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;

/**
 * Removes all computation that does is not referenced transitively from the root of the plan
 * <p>
 * E.g.,
 * <p>
 * {@code Output[$0] -> Project[$0 := $1 + $2, $3 = $4 / $5] -> ...}
 * <p>
 * gets rewritten as
 * <p>
 * {@code Output[$0] -> Project[$0 := $1 + $2] -> ...}
 */
public class PruneUnreferencedOutputs
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

        return SimplePlanRewriter.rewriteWith(new Rewriter(variableAllocator), plan, ImmutableSet.of());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Set<VariableReferenceExpression>>
    {
        private final VariableAllocator variableAllocator;

        public Rewriter(VariableAllocator variableAllocator)
        {
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public LogicalPlanNode visitJoin(JoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedFilterInputs = new HashSet<>();
            if (node.getFilter().isPresent()) {
                if (isExpression(node.getFilter().get())) {
                    expectedFilterInputs = ImmutableSet.<VariableReferenceExpression>builder()
                            .addAll(VariablesExtractor.extractUnique(castToExpression(node.getFilter().get()), variableAllocator.getTypes()))
                            .addAll(context.get())
                            .build();
                }
                else {
                    expectedFilterInputs = ImmutableSet.<VariableReferenceExpression>builder()
                            .addAll(VariablesExtractor.extractUnique(node.getFilter().get()))
                            .addAll(context.get())
                            .build();
                }
            }

            ImmutableSet.Builder<VariableReferenceExpression> leftInputsBuilder = ImmutableSet.builder();
            leftInputsBuilder.addAll(context.get()).addAll(Iterables.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft));
            if (node.getLeftHashVariable().isPresent()) {
                leftInputsBuilder.add(node.getLeftHashVariable().get());
            }
            leftInputsBuilder.addAll(expectedFilterInputs);
            Set<VariableReferenceExpression> leftInputs = leftInputsBuilder.build();

            ImmutableSet.Builder<VariableReferenceExpression> rightInputsBuilder = ImmutableSet.builder();
            rightInputsBuilder.addAll(context.get()).addAll(Iterables.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight));
            if (node.getRightHashVariable().isPresent()) {
                rightInputsBuilder.add(node.getRightHashVariable().get());
            }
            rightInputsBuilder.addAll(expectedFilterInputs);
            Set<VariableReferenceExpression> rightInputs = rightInputsBuilder.build();

            LogicalPlanNode left = context.rewrite(node.getLeft(), leftInputs);
            LogicalPlanNode right = context.rewrite(node.getRight(), rightInputs);

            List<VariableReferenceExpression> outputVariables;
            if (node.isCrossJoin()) {
                // do not prune nested joins output since it is not supported
                // TODO: remove this "if" branch when output symbols selection is supported by nested loop join
                outputVariables = ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build();
            }
            else {
                outputVariables = node.getOutputVariables().stream()
                        .filter(variable -> context.get().contains(variable))
                        .distinct()
                        .collect(Collectors.toList());
            }

            return new JoinNode(node.getId(), node.getType(), left, right, node.getCriteria(), outputVariables, node.getFilter(), node.getLeftHashVariable(), node.getRightHashVariable(), node.getDistributionType());
        }

        @Override
        public LogicalPlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> sourceInputsBuilder = ImmutableSet.builder();
            sourceInputsBuilder.addAll(context.get()).add(node.getSourceJoinVariable());
            if (node.getSourceHashVariable().isPresent()) {
                sourceInputsBuilder.add(node.getSourceHashVariable().get());
            }
            Set<VariableReferenceExpression> sourceInputs = sourceInputsBuilder.build();

            ImmutableSet.Builder<VariableReferenceExpression> filteringSourceInputBuilder = ImmutableSet.builder();
            filteringSourceInputBuilder.add(node.getFilteringSourceJoinVariable());
            if (node.getFilteringSourceHashVariable().isPresent()) {
                filteringSourceInputBuilder.add(node.getFilteringSourceHashVariable().get());
            }
            Set<VariableReferenceExpression> filteringSourceInputs = filteringSourceInputBuilder.build();

            LogicalPlanNode source = context.rewrite(node.getSource(), sourceInputs);
            LogicalPlanNode filteringSource = context.rewrite(node.getFilteringSource(), filteringSourceInputs);

            return new SemiJoinNode(node.getId(),
                    source,
                    filteringSource,
                    node.getSourceJoinVariable(),
                    node.getFilteringSourceJoinVariable(),
                    node.getSemiJoinOutput(),
                    node.getSourceHashVariable(),
                    node.getFilteringSourceHashVariable(),
                    node.getDistributionType());
        }

        @Override
        public LogicalPlanNode visitAggregation(AggregationNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(node.getGroupingKeys());
            if (node.getHashVariable().isPresent()) {
                expectedInputs.add(node.getHashVariable().get());
            }

            ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();

                if (context.get().contains(variable)) {
                    Aggregation aggregation = entry.getValue();
                    expectedInputs.addAll(extractAggregationUniqueVariables(aggregation, variableAllocator.getTypes()));
                    aggregation.getMask().ifPresent(expectedInputs::add);
                    aggregations.put(variable, aggregation);
                }
            }

            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new AggregationNode(node.getId(),
                    source,
                    aggregations.build(),
                    node.getGroupingSets(),
                    ImmutableList.of(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable());
        }

        @Override
        public LogicalPlanNode visitTableScan(TableScanNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            List<VariableReferenceExpression> newOutputs = node.getOutputVariables().stream()
                    .filter(context.get()::contains)
                    .collect(Collectors.toList());

            Map<VariableReferenceExpression, ColumnHandle> newAssignments = newOutputs.stream()
                    .collect(Collectors.toMap(identity(), node.getAssignments()::get));

            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    newOutputs,
                    newAssignments);
                    //node.getCurrentConstraint(),
                    //node.getEnforcedConstraint());
        }

        @Override
        public LogicalPlanNode visitFilter(FilterNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs;
            if (isExpression(node.getPredicate())) {
                expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(VariablesExtractor.extractUnique(castToExpression(node.getPredicate()), variableAllocator.getTypes()))
                        .addAll(context.get())
                        .build();
            }
            else {
                expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(VariablesExtractor.extractUnique(node.getPredicate()))
                        .addAll(context.get())
                        .build();
            }

            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new FilterNode(node.getId(), source, node.getPredicate());
        }

        @Override
        public LogicalPlanNode visitGroupId(GroupIdNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.builder();

            List<VariableReferenceExpression> newAggregationArguments = node.getAggregationArguments().stream()
                    .filter(context.get()::contains)
                    .collect(Collectors.toList());
            expectedInputs.addAll(newAggregationArguments);

            ImmutableList.Builder<List<VariableReferenceExpression>> newGroupingSets = ImmutableList.builder();
            Map<VariableReferenceExpression, VariableReferenceExpression> newGroupingMapping = new HashMap<>();

            for (List<VariableReferenceExpression> groupingSet : node.getGroupingSets()) {
                ImmutableList.Builder<VariableReferenceExpression> newGroupingSet = ImmutableList.builder();

                for (VariableReferenceExpression output : groupingSet) {
                    if (context.get().contains(output)) {
                        newGroupingSet.add(output);
                        newGroupingMapping.putIfAbsent(output, node.getGroupingColumns().get(output));
                        expectedInputs.add(node.getGroupingColumns().get(output));
                    }
                }
                newGroupingSets.add(newGroupingSet.build());
            }

            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new GroupIdNode(node.getId(), source, newGroupingSets.build(), newGroupingMapping, newAggregationArguments, node.getGroupIdVariable());
        }

        @Override
        public LogicalPlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (!context.get().contains(node.getMarkerVariable())) {
                return context.rewrite(node.getSource(), context.get());
            }

            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(node.getDistinctVariables())
                    .addAll(context.get().stream()
                            .filter(variable -> !variable.equals(node.getMarkerVariable()))
                            .collect(Collectors.toList()));

            if (node.getHashVariable().isPresent()) {
                expectedInputs.add(node.getHashVariable().get());
            }
            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new MarkDistinctNode(node.getId(), source, node.getMarkerVariable(), node.getDistinctVariables(), node.getHashVariable());
        }

        @Override
        public LogicalPlanNode visitProject(ProjectNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.builder();

            Assignments.Builder builder = Assignments.builder();
            node.getAssignments().forEach((variable, expression) -> {
                if (context.get().contains(variable)) {
                    if (isExpression(expression)) {
                        expectedInputs.addAll(VariablesExtractor.extractUnique(castToExpression(expression), variableAllocator.getTypes()));
                    }
                    else {
                        expectedInputs.addAll(VariablesExtractor.extractUnique(expression));
                    }
                    builder.put(variable, expression);
                }
            });

            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new ProjectNode(node.getId(), source, builder.build());
        }

        @Override
        public LogicalPlanNode visitOutput(OutputNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.copyOf(node.getOutputVariables());
            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs);
            return new OutputNode(node.getId(), source, node.getColumnNames(), node.getOutputVariables());
        }

        @Override
        public LogicalPlanNode visitLimit(LimitNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get());
            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs.build());
            return new LimitNode(node.getId(), source, node.getCount(), node.getStep());
        }

        @Override
        public LogicalPlanNode visitTopN(TopNNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableSet.Builder<VariableReferenceExpression> expectedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(node.getOrderingScheme().getOrderByVariables());

            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs.build());

            return new TopNNode(node.getId(), source, node.getCount(), node.getOrderingScheme(), node.getStep());
        }


        @Override
        public LogicalPlanNode visitSort(SortNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Set<VariableReferenceExpression> expectedInputs = ImmutableSet.copyOf(concat(context.get(), node.getOrderingScheme().getOrderByVariables()));

            LogicalPlanNode source = context.rewrite(node.getSource(), expectedInputs);

            return new SortNode(node.getId(), source, node.getOrderingScheme(), node.isPartial());
        }

        @Override
        public LogicalPlanNode visitValues(ValuesNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            ImmutableList.Builder<VariableReferenceExpression> rewrittenOutputVariablesBuilder = ImmutableList.builder();
            ImmutableList.Builder<ImmutableList.Builder<RowExpression>> rowBuildersBuilder = ImmutableList.builder();
            // Initialize builder for each row
            for (int i = 0; i < node.getRows().size(); i++) {
                rowBuildersBuilder.add(ImmutableList.builder());
            }
            ImmutableList<ImmutableList.Builder<RowExpression>> rowBuilders = rowBuildersBuilder.build();
            for (int i = 0; i < node.getOutputVariables().size(); i++) {
                VariableReferenceExpression outputVariable = node.getOutputVariables().get(i);
                // If output symbol is used
                if (context.get().contains(outputVariable)) {
                    rewrittenOutputVariablesBuilder.add(outputVariable);
                    // Add the value of the output symbol for each row
                    for (int j = 0; j < node.getRows().size(); j++) {
                        rowBuilders.get(j).add(node.getRows().get(j).get(i));
                    }
                }
            }
            List<List<RowExpression>> rewrittenRows = rowBuilders.stream()
                    .map(ImmutableList.Builder::build)
                    .collect(Collectors.toList());
            return new ValuesNode(node.getId(), rewrittenOutputVariablesBuilder.build(), rewrittenRows);
        }

        @Override
        public LogicalPlanNode visitApply(ApplyNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            // remove unused apply nodes
            if (intersection(node.getSubqueryAssignments().getVariables(), context.get()).isEmpty()) {
                return context.rewrite(node.getInput(), context.get());
            }

            // extract symbols required subquery plan
            ImmutableSet.Builder<VariableReferenceExpression> subqueryAssignmentsVariablesBuilder = ImmutableSet.builder();
            Assignments.Builder subqueryAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getSubqueryAssignments().getMap().entrySet()) {
                VariableReferenceExpression output = entry.getKey();
                RowExpression expression = entry.getValue();
                if (context.get().contains(output)) {
                    if (isExpression(expression)) {
                        subqueryAssignmentsVariablesBuilder.addAll(VariablesExtractor.extractUnique(castToExpression(expression), variableAllocator.getTypes()));
                    }
                    else {
                        subqueryAssignmentsVariablesBuilder.addAll(VariablesExtractor.extractUnique(expression));
                    }
                    subqueryAssignments.put(output, expression);
                }
            }

            Set<VariableReferenceExpression> subqueryAssignmentsVariables = subqueryAssignmentsVariablesBuilder.build();
            LogicalPlanNode subquery = context.rewrite(node.getSubquery(), subqueryAssignmentsVariables);

            // prune not used correlation symbols
            Set<VariableReferenceExpression> subquerySymbols = VariablesExtractor.extractUnique(subquery, variableAllocator.getTypes());
            List<VariableReferenceExpression> newCorrelation = node.getCorrelation().stream()
                    .filter(subquerySymbols::contains)
                    .collect(Collectors.toList());

            Set<VariableReferenceExpression> inputContext = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(newCorrelation)
                    .addAll(subqueryAssignmentsVariables) // need to include those: e.g: "expr" from "expr IN (SELECT 1)"
                    .build();
            LogicalPlanNode input = context.rewrite(node.getInput(), inputContext);
            Assignments assignments = subqueryAssignments.build();
            verifySubquerySupported(assignments);
            return new ApplyNode(node.getId(), input, subquery, assignments, newCorrelation, node.getOriginSubqueryError());
        }

        @Override
        public LogicalPlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (!context.get().contains(node.getIdVariable())) {
                return context.rewrite(node.getSource(), context.get());
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public LogicalPlanNode visitLateralJoin(LateralJoinNode node, SimplePlanRewriter.RewriteContext<Set<VariableReferenceExpression>> context)
        {
            LogicalPlanNode subquery = context.rewrite(node.getSubquery(), context.get());

            // remove unused lateral nodes
            if (intersection(ImmutableSet.copyOf(subquery.getOutputVariables()), context.get()).isEmpty() && isScalar(subquery)) {
                return context.rewrite(node.getInput(), context.get());
            }

            // prune not used correlation symbols
            Set<VariableReferenceExpression> subqueryVariables = VariablesExtractor.extractUnique(subquery, variableAllocator.getTypes());
            List<VariableReferenceExpression> newCorrelation = node.getCorrelation().stream()
                    .filter(subqueryVariables::contains)
                    .collect(Collectors.toList());

            Set<VariableReferenceExpression> inputContext = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(context.get())
                    .addAll(newCorrelation)
                    .build();
            LogicalPlanNode input = context.rewrite(node.getInput(), inputContext);

            // remove unused lateral nodes
            if (intersection(ImmutableSet.copyOf(input.getOutputVariables()), inputContext).isEmpty() && isScalar(input)) {
                return subquery;
            }

            return new LateralJoinNode(node.getId(), input, subquery, newCorrelation, node.getType(), node.getOriginSubqueryError());
        }
    }
}
