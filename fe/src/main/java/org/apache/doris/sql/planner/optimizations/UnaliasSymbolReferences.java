package org.apache.doris.sql.planner.optimizations;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.RowExpressionVariableInliner;
import org.apache.doris.sql.planner.SimplePlanRewriter;
import org.apache.doris.sql.planner.Symbol;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.ExchangeNode;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.JoinNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.Ordering;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.OutputNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.SemiJoinNode;
import org.apache.doris.sql.planner.plan.SortNode;
import org.apache.doris.sql.planner.plan.SortOrder;
import org.apache.doris.sql.planner.plan.TableScanNode;
import org.apache.doris.sql.planner.plan.TopNNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.Expressions;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.ExpressionRewriter;
import org.apache.doris.sql.tree.ExpressionTreeRewriter;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
public class UnaliasSymbolReferences implements PlanOptimizer {
    public UnaliasSymbolReferences()
    {
    }

    @Override
    public LogicalPlanNode optimize(LogicalPlanNode plan,
                                    Session session,
                                    TypeProvider types,
                                    VariableAllocator variableAllocator,
                                    IdGenerator<PlanNodeId> idAllocator,
                                    WarningCollector warningCollector) {
        return SimplePlanRewriter.rewriteWith(new Rewriter(types), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Map<String, String> mapping = new HashMap<>();
        private final TypeProvider types;
        private Rewriter(TypeProvider types) {
            this.types = types;
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
        public LogicalPlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return node;
        }

        @Override
        public LogicalPlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context) {
            return context.defaultRewrite(node);
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
        public LogicalPlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            return context.defaultRewrite(node);
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
            /*
            if (isExpression(expression)) {
                return ExpressionDeterminismEvaluator.isDeterministic(castToExpression(expression));
            }
            return determinismEvaluator.isDeterministic(expression);

             */
            return true;
        }

        private static boolean isNull(RowExpression expression)
        {
            if (isExpression(expression)) {
                return castToExpression(expression) instanceof NullLiteral;
            }
            return Expressions.isNull(expression);
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
            return RowExpressionVariableInliner.inlineVariables(this::canonicalize, value);
        }

        private List<JoinNode.EquiJoinClause> canonicalizeJoinCriteria(List<JoinNode.EquiJoinClause> criteria)
        {
            ImmutableList.Builder<JoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause clause : criteria) {
                builder.add(new JoinNode.EquiJoinClause(canonicalize(clause.getLeft()), canonicalize(clause.getRight())));
            }

            return builder.build();
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
    }
}
