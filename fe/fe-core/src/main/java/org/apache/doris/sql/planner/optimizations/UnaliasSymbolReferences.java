package org.apache.doris.sql.planner.optimizations;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.doris.sql.ExpressionDeterminismEvaluator;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.RowExpressionVariableInliner;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.Symbol;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.AssignUniqueId;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.EnforceSingleRowNode;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.GroupIdNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.MarkDistinctNode;
import org.apache.doris.sql.planner.plan.Ordering;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.planner.plan.SortNode;
import org.apache.doris.sql.planner.plan.SortOrder;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.Expressions;
import org.apache.doris.sql.relational.RowExpressionDeterminismEvaluator;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.ExpressionRewriter;
import org.apache.doris.sql.tree.ExpressionTreeRewriter;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static org.apache.doris.sql.planner.plan.JoinNode.Type.INNER;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;

/**
 * Re-maps symbol references that are just aliases of each other (e.g., due to projections like {@code $0 := $1})
 * <p/>
 * E.g.,
 * <p/>
 * {@code Output[$0, $1] -> Project[$0 := $2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 * <p/>
 * gets rewritten as
 * <p/>
 * {@code Output[$2, $1] -> Project[$2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 */
public class UnaliasSymbolReferences
        implements PlanOptimizer
{
    private final FunctionManager functionManager;

    public UnaliasSymbolReferences(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(types, functionManager), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Map<String, String> mapping = new HashMap<>();
        private final TypeProvider types;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;

        private Rewriter(TypeProvider types, FunctionManager functionManager)
        {
            this.types = types;
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionManager);
        }

        @Override
        public LogicalPlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());
            //TODO: use mapper in other methods
            SymbolMapper mapper = new SymbolMapper(mapping, types);
            return mapper.map(node, source);
        }

        @Override
        public LogicalPlanNode visitGroupId(GroupIdNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());

            Map<VariableReferenceExpression, VariableReferenceExpression> newGroupingMappings = new HashMap<>();
            ImmutableList.Builder<List<VariableReferenceExpression>> newGroupingSets = ImmutableList.builder();

            for (List<VariableReferenceExpression> groupingSet : node.getGroupingSets()) {
                ImmutableList.Builder<VariableReferenceExpression> newGroupingSet = ImmutableList.builder();
                for (VariableReferenceExpression output : groupingSet) {
                    newGroupingMappings.putIfAbsent(canonicalize(output), canonicalize(node.getGroupingColumns().get(output)));
                    newGroupingSet.add(canonicalize(output));
                }
                newGroupingSets.add(newGroupingSet.build());
            }

            return new GroupIdNode(node.getId(), source, newGroupingSets.build(), newGroupingMappings, canonicalizeAndDistinct(node.getAggregationArguments()), canonicalize(node.getGroupIdVariable()));
        }

        @Override
        public LogicalPlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());
            return new MarkDistinctNode(node.getId(), source, canonicalize(node.getMarkerVariable()), canonicalizeAndDistinct(node.getDistinctVariables()), canonicalize(node.getHashVariable()));
        }

        private List<RowExpression> canonicalizeCallExpression(CallExpression callExpression)
        {
            // TODO: arguments will be pure RowExpression once we introduce subquery expression for RowExpression.
            return callExpression.getArguments()
                    .stream()
                    .map(this::canonicalize)
                    .collect(toImmutableList());
        }

        @Override
        public LogicalPlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return node;
        }

        private void mapExchangeNodeSymbols(ExchangeNode node)
        {
            if (node.getInputs().size() == 1) {
                mapExchangeNodeOutputToInputSymbols(node);
                return;
            }

            // Mapping from list [node.getInput(0).get(symbolIndex), node.getInput(1).get(symbolIndex), ...] to node.getOutputVariables(symbolIndex).
            // All symbols are canonical.
            Map<List<VariableReferenceExpression>, VariableReferenceExpression> inputsToOutputs = new HashMap<>();
            // Map each same list of input symbols [I1, I2, ..., In] to the same output symbol O
            for (int variableIndex = 0; variableIndex < node.getOutputVariables().size(); variableIndex++) {
                VariableReferenceExpression canonicalOutput = canonicalize(node.getOutputVariables().get(variableIndex));
                List<VariableReferenceExpression> canonicalInputs = canonicalizeExchangeNodeInputs(node, variableIndex);
                VariableReferenceExpression output = inputsToOutputs.get(canonicalInputs);

                if (output == null || canonicalOutput.equals(output)) {
                    inputsToOutputs.put(canonicalInputs, canonicalOutput);
                }
                else {
                    map(canonicalOutput, output);
                }
            }
        }

        private void mapExchangeNodeOutputToInputSymbols(ExchangeNode node)
        {
            checkState(node.getInputs().size() == 1);

            for (int variableIndex = 0; variableIndex < node.getOutputVariables().size(); variableIndex++) {
                VariableReferenceExpression canonicalOutput = canonicalize(node.getOutputVariables().get(variableIndex));
                VariableReferenceExpression canonicalInput = canonicalize(node.getInputs().get(0).get(variableIndex));

                if (!canonicalOutput.equals(canonicalInput)) {
                    map(canonicalOutput, canonicalInput);
                }
            }
        }

        private List<VariableReferenceExpression> canonicalizeExchangeNodeInputs(ExchangeNode node, int symbolIndex)
        {
            return node.getInputs().stream()
                    .map(input -> canonicalize(input.get(symbolIndex)))
                    .collect(toImmutableList());
        }

        @Override
        public LogicalPlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            return context.defaultRewrite(node);
        }

        @Override
        public LogicalPlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            List<List<RowExpression>> canonicalizedRows = node.getRows().stream()
                    .map(rowExpressions -> rowExpressions.stream()
                            .map(this::canonicalize)
                            .collect(toImmutableList()))
                    .collect(toImmutableList());
            List<VariableReferenceExpression> canonicalizedOutputVariables = canonicalizeAndDistinct(node.getOutputVariables());
            checkState(node.getOutputVariables().size() == canonicalizedOutputVariables.size(), "Values output symbols were pruned");
            return new ValuesNode(
                    node.getId(),
                    canonicalizedOutputVariables,
                    canonicalizedRows);
        }

        @Override
        public LogicalPlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());

            return new FilterNode(node.getId(), source, canonicalize(node.getPredicate()));
        }

        @Override
        public LogicalPlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());
            return new ProjectNode(node.getId(), source, canonicalize(node.getAssignments()));
        }

        @Override
        public LogicalPlanNode visitOutput(OutputNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());

            List<VariableReferenceExpression> canonical = Lists.transform(node.getOutputVariables(), this::canonicalize);
            return new OutputNode(node.getId(), source, node.getColumnNames(), canonical);
        }

        @Override
        public LogicalPlanNode visitEnforceSingleRow(EnforceSingleRowNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());

            return new EnforceSingleRowNode(node.getId(), source);
        }

        @Override
        public LogicalPlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());

            return new AssignUniqueId(node.getId(), source, node.getIdVariable());
        }

        @Override
        public LogicalPlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getInput());
            LogicalPlanNode subquery = context.rewrite(node.getSubquery());
            List<VariableReferenceExpression> canonicalCorrelation = Lists.transform(node.getCorrelation(), this::canonicalize);

            Assignments assignments = canonicalize(node.getSubqueryAssignments());
            verifySubquerySupported(assignments);
            return new ApplyNode(node.getId(), source, subquery, assignments, canonicalCorrelation, node.getOriginSubqueryError());
        }

        @Override
        public LogicalPlanNode visitLateralJoin(LateralJoinNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getInput());
            LogicalPlanNode subquery = context.rewrite(node.getSubquery());
            List<VariableReferenceExpression> canonicalCorrelation = canonicalizeAndDistinct(node.getCorrelation());

            return new LateralJoinNode(node.getId(), source, subquery, canonicalCorrelation, node.getType(), node.getOriginSubqueryError());
        }

        @Override
        public LogicalPlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());

            SymbolMapper mapper = new SymbolMapper(mapping, types);
            return mapper.map(node, source, node.getId());
        }

        @Override
        public LogicalPlanNode visitSort(SortNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());

            return new SortNode(node.getId(), source, canonicalizeAndDistinct(node.getOrderingScheme()), node.isPartial());
        }

        @Override
        public LogicalPlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode left = context.rewrite(node.getLeft());
            LogicalPlanNode right = context.rewrite(node.getRight());

            List<JoinNode.EquiJoinClause> canonicalCriteria = canonicalizeJoinCriteria(node.getCriteria());
            Optional<RowExpression> canonicalFilter = node.getFilter().map(this::canonicalize);
            Optional<VariableReferenceExpression> canonicalLeftHashVariable = canonicalize(node.getLeftHashVariable());
            Optional<VariableReferenceExpression> canonicalRightHashVariable = canonicalize(node.getRightHashVariable());

            if (node.getType().equals(INNER)) {
                canonicalCriteria.stream()
                        .filter(clause -> clause.getLeft().getType().equals(clause.getRight().getType()))
                        .filter(clause -> node.getOutputVariables().contains(clause.getLeft()))
                        .forEach(clause -> map(clause.getRight(), clause.getLeft()));
            }

            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    left,
                    right,
                    canonicalCriteria,
                    canonicalizeAndDistinct(node.getOutputVariables()),
                    canonicalFilter,
                    canonicalLeftHashVariable,
                    canonicalRightHashVariable,
                    node.getDistributionType());
        }

        @Override
        public LogicalPlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            LogicalPlanNode source = context.rewrite(node.getSource());
            LogicalPlanNode filteringSource = context.rewrite(node.getFilteringSource());

            return new SemiJoinNode(
                    node.getId(),
                    source,
                    filteringSource,
                    canonicalize(node.getSourceJoinVariable()),
                    canonicalize(node.getFilteringSourceJoinVariable()),
                    canonicalize(node.getSemiJoinOutput()),
                    canonicalize(node.getSourceHashVariable()),
                    canonicalize(node.getFilteringSourceHashVariable()),
                    node.getDistributionType());
        }

        @Override
        public LogicalPlanNode visitPlan(LogicalPlanNode node, RewriteContext<Void> context)
        {
            throw new UnsupportedOperationException("Unsupported plan node " + node.getClass().getSimpleName());
        }

        private void map(VariableReferenceExpression variable, VariableReferenceExpression canonical)
        {
            Preconditions.checkArgument(!variable.equals(canonical), "Can't map variable to itself: %s", variable);
            mapping.put(variable.getName(), canonical.getName());
        }

        private Assignments canonicalize(Assignments oldAssignments)
        {
            Map<RowExpression, VariableReferenceExpression> computedExpressions = new HashMap<>();
            Assignments.Builder assignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : oldAssignments.getMap().entrySet()) {
                RowExpression expression = canonicalize(entry.getValue());
                if (expression instanceof VariableReferenceExpression) {
                    // Always map a trivial variable projection
                    VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                    if (!variable.getName().equals(entry.getKey().getName())) {
                        map(entry.getKey(), variable);
                    }
                }
                else if (isExpression(expression) && castToExpression(expression) instanceof SymbolReference) {
                    // Always map a trivial symbol projection
                    VariableReferenceExpression variable = new VariableReferenceExpression(Symbol.from(castToExpression(expression)).getName(), types.get(castToExpression(expression)));
                    if (!variable.getName().equals(entry.getKey().getName())) {
                        map(entry.getKey(), variable);
                    }
                }
                else if (!isNull(expression) && isDeterministic(expression)) {
                    // Try to map same deterministic expressions within a projection into the same symbol
                    // Omit NullLiterals since those have ambiguous types
                    VariableReferenceExpression computedVariable = computedExpressions.get(expression);
                    if (computedVariable == null) {
                        // If we haven't seen the expression before in this projection, record it
                        computedExpressions.put(expression, entry.getKey());
                    }
                    else {
                        // If we have seen the expression before and if it is deterministic
                        // then we can rewrite references to the current symbol in terms of the parallel computedVariable in the projection
                        map(entry.getKey(), computedVariable);
                    }
                }

                VariableReferenceExpression canonical = canonicalize(entry.getKey());
                assignments.put(canonical, expression);
            }
            return assignments.build();
        }

        private boolean isDeterministic(RowExpression expression)
        {
            if (isExpression(expression)) {
                return ExpressionDeterminismEvaluator.isDeterministic(castToExpression(expression));
            }
            return determinismEvaluator.isDeterministic(expression);
        }

        private static boolean isNull(RowExpression expression)
        {
            if (isExpression(expression)) {
                return castToExpression(expression) instanceof NullLiteral;
            }
            return Expressions.isNull(expression);
        }

        private Symbol canonicalize(Symbol symbol)
        {
            String canonical = symbol.getName();
            while (mapping.containsKey(canonical)) {
                canonical = mapping.get(canonical);
            }
            return new Symbol(canonical);
        }

        private VariableReferenceExpression canonicalize(VariableReferenceExpression variable)
        {
            String canonical = variable.getName();
            while (mapping.containsKey(canonical)) {
                canonical = mapping.get(canonical);
            }
            return new VariableReferenceExpression(canonical, types.get(new SymbolReference(canonical)));
        }

        private Optional<VariableReferenceExpression> canonicalize(Optional<VariableReferenceExpression> variable)
        {
            if (variable.isPresent()) {
                return Optional.of(canonicalize(variable.get()));
            }
            return Optional.empty();
        }

        private RowExpression canonicalize(RowExpression value)
        {
            if (isExpression(value)) {
                // TODO remove once all UnaliasSymbolReference are above translateExpressions
                return castToRowExpression(canonicalize(castToExpression(value)));
            }
            return RowExpressionVariableInliner.inlineVariables(this::canonicalize, value);
        }

        private Expression canonicalize(Expression value)
        {
            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
                {
                    Symbol canonical = canonicalize(Symbol.from(node));
                    return canonical.toSymbolReference();
                }
            }, value);
        }

        private List<VariableReferenceExpression> canonicalizeAndDistinct(List<VariableReferenceExpression> outputs)
        {
            Set<VariableReferenceExpression> added = new HashSet<>();
            ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
            for (VariableReferenceExpression variable : outputs) {
                VariableReferenceExpression canonical = canonicalize(variable);
                if (added.add(canonical)) {
                    builder.add(canonical);
                }
            }
            return builder.build();
        }

        private OrderingScheme canonicalizeAndDistinct(OrderingScheme orderingScheme)
        {
            Set<VariableReferenceExpression> added = new HashSet<>();
            ImmutableList.Builder<VariableReferenceExpression> variables = ImmutableList.builder();
            ImmutableMap.Builder<VariableReferenceExpression, SortOrder> orderings = ImmutableMap.builder();
            for (VariableReferenceExpression variable : orderingScheme.getOrderByVariables()) {
                VariableReferenceExpression canonical = canonicalize(variable);
                if (added.add(canonical)) {
                    variables.add(canonical);
                    orderings.put(canonical, orderingScheme.getOrdering(variable));
                }
            }

            ImmutableMap<VariableReferenceExpression, SortOrder> orderingsMap = orderings.build();
            return new OrderingScheme(variables.build().stream().map(variable -> new Ordering(variable, orderingsMap.get(variable))).collect(toImmutableList()));
        }

        private Set<VariableReferenceExpression> canonicalize(Set<VariableReferenceExpression> variables)
        {
            return variables.stream()
                    .map(this::canonicalize).collect(Collectors.toSet());
        }

        private List<JoinNode.EquiJoinClause> canonicalizeJoinCriteria(List<JoinNode.EquiJoinClause> criteria)
        {
            ImmutableList.Builder<JoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause clause : criteria) {
                builder.add(new JoinNode.EquiJoinClause(canonicalize(clause.getLeft()), canonicalize(clause.getRight())));
            }

            return builder.build();
        }

        private Map<VariableReferenceExpression, List<VariableReferenceExpression>> canonicalizeSetOperationVariableMap(Map<VariableReferenceExpression, List<VariableReferenceExpression>> setOperationVariableMap)
        {
            LinkedHashMap<VariableReferenceExpression, List<VariableReferenceExpression>> result = new LinkedHashMap<>();
            Set<VariableReferenceExpression> addVariables = new HashSet<>();
            for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : setOperationVariableMap.entrySet()) {
                VariableReferenceExpression canonicalOutputVariable = canonicalize(entry.getKey());
                if (addVariables.add(canonicalOutputVariable)) {
                    result.put(canonicalOutputVariable, ImmutableList.copyOf(Iterables.transform(entry.getValue(), this::canonicalize)));
                }
            }
            return result;
        }

        private List<VariableReferenceExpression> canonicalizeSetOperationOutputVariables(List<VariableReferenceExpression> setOperationOutputVariables)
        {
            ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
            for (VariableReferenceExpression variable : setOperationOutputVariables) {
                builder.add(canonicalize(variable));
            }
            return builder.build();
        }
    }
}

