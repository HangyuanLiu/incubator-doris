package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.AssignmentUtils;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.tree.DefaultExpressionTraversalVisitor;
import org.apache.doris.sql.tree.DereferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Identifier;
import org.apache.doris.sql.tree.InPredicate;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.SubqueryExpression;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.SemanticExceptions.notSupportedException;
import static org.apache.doris.sql.analyzer.SemanticExceptions.subQueryNotSupportedError;
import static org.apache.doris.sql.planner.ExpressionNodeInliner.replaceExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;
import static org.apache.doris.sql.util.AstUtils.nodeContains;

class SubqueryPlanner {
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final IdGenerator<PlanNodeId> idAllocator;
    private final Metadata metadata;
    private final Session session;

    SubqueryPlanner(
            Analysis analysis,
            VariableAllocator variableAllocator,
            IdGenerator<PlanNodeId> idAllocator,
            Metadata metadata,
            Session session) {
        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node, true);
        }
        return builder;
    }

    public PlanBuilder handleUncorrelatedSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node, false);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node)
    {
        return handleSubqueries(builder, expression, node, true);
    }

    private PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node, boolean correlationAllowed)
    {
        builder = appendInPredicateApplyNodes(builder, collectInPredicateSubqueries(expression, node), correlationAllowed, node);
        return builder;
    }

    public Set<InPredicate> collectInPredicateSubqueries(Expression expression, Node node)
    {
        return analysis.getInPredicateSubqueries(node)
                .stream()
                .filter(inPredicate -> nodeContains(expression, inPredicate.getValueList()))
                .collect(toImmutableSet());
    }
    private PlanBuilder appendInPredicateApplyNodes(PlanBuilder subPlan, Set<InPredicate> inPredicates, boolean correlationAllowed, Node node)
    {
        for (InPredicate inPredicate : inPredicates) {
            subPlan = appendInPredicateApplyNode(subPlan, inPredicate, correlationAllowed, node);
        }
        return subPlan;
    }

    private PlanBuilder appendInPredicateApplyNode(PlanBuilder subPlan, InPredicate inPredicate, boolean correlationAllowed, Node node)
    {
        if (subPlan.canTranslate(inPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        subPlan = handleSubqueries(subPlan, inPredicate.getValue(), node);

        subPlan = subPlan.appendProjections(ImmutableList.of(inPredicate.getValue()), variableAllocator, idAllocator);

        checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        SubqueryExpression valueListSubquery = (SubqueryExpression) inPredicate.getValueList();
        SubqueryExpression uncoercedValueListSubquery = uncoercedSubquery(valueListSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedValueListSubquery);

        subqueryPlan = subqueryPlan.appendProjections(ImmutableList.of(valueListSubquery), variableAllocator, idAllocator);
        SymbolReference valueList = new SymbolReference(subqueryPlan.translate(valueListSubquery).getName());

        VariableReferenceExpression rewrittenValue = subPlan.translate(inPredicate.getValue());
        InPredicate inPredicateSubqueryExpression = new InPredicate(new SymbolReference(rewrittenValue.getName()), valueList);
        VariableReferenceExpression inPredicateSubqueryVariable = variableAllocator.newVariable(inPredicateSubqueryExpression, BOOLEAN);

        subPlan.getTranslations().put(inPredicate, inPredicateSubqueryVariable);

        return appendApplyNode(subPlan, inPredicate, subqueryPlan.getRoot(), Assignments.of(inPredicateSubqueryVariable, castToRowExpression(inPredicateSubqueryExpression)), correlationAllowed);
    }

    /**
     * Implicit coercions are added when mapping an expression to symbol in {@link TranslationMap}. Coercions
     * for expression are obtained from {@link Analysis} by identity comparison. Create a copy of subquery
     * in order to get a subquery expression that does not have any coercion assigned to it {@link Analysis}.
     */
    private SubqueryExpression uncoercedSubquery(SubqueryExpression subquery)
    {
        return new SubqueryExpression(subquery.getQuery());
    }

    private PlanBuilder appendApplyNode(PlanBuilder subPlan, Node subquery, LogicalPlanNode subqueryNode, Assignments subqueryAssignments, boolean correlationAllowed)
    {
        Map<Expression, Expression> correlation = extractCorrelation(subPlan, subqueryNode);
        if (!correlationAllowed && !correlation.isEmpty()) {
            throw notSupportedException(subquery, "Correlated subquery in given context");
        }
        subPlan = subPlan.appendProjections(correlation.keySet(), variableAllocator, idAllocator);
        subqueryNode = replaceExpressionsWithSymbols(subqueryNode, correlation);

        TranslationMap translations = subPlan.copyTranslations();
        LogicalPlanNode root = subPlan.getRoot();
        //verifySubquerySupported(subqueryAssignments);
        return new PlanBuilder(translations,
                new ApplyNode(idAllocator.getNextId(),
                        root,
                        subqueryNode,
                        subqueryAssignments,
                        ImmutableList.copyOf(VariablesExtractor.extractUnique(correlation.values(), variableAllocator.getTypes())),
                        subQueryNotSupportedError(subquery, "Given correlated subquery")),
                analysis.getParameters());
    }

    private Map<Expression, Expression> extractCorrelation(PlanBuilder subPlan, LogicalPlanNode subquery)
    {
        Set<Expression> missingReferences = extractOuterColumnReferences(subquery);
        ImmutableMap.Builder<Expression, Expression> correlation = ImmutableMap.builder();
        for (Expression missingReference : missingReferences) {
            // missing reference expression can be solved within current subPlan,
            // or within outer plans in case of multiple nesting levels of subqueries.
            tryResolveMissingExpression(subPlan, missingReference)
                    .ifPresent(symbolReference -> correlation.put(missingReference, symbolReference));
        }
        return correlation.build();
    }

    /**
     * Checks if give reference expression can resolved within given plan.
     */
    private static Optional<Expression> tryResolveMissingExpression(PlanBuilder subPlan, Expression expression)
    {
        Expression rewritten = subPlan.rewrite(expression);
        if (rewritten != expression) {
            return Optional.of(rewritten);
        }
        return Optional.empty();
    }

    private PlanBuilder createPlanBuilder(Node node)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, metadata)
                .process(node, null);
        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        if (node instanceof Expression && relationPlan.getFieldMappings().size() == 1) {
            translations.put((Expression) node, getOnlyElement(relationPlan.getFieldMappings()));
        }

        return new PlanBuilder(translations, relationPlan.getRoot(), analysis.getParameters());
    }

    /**
     * @return a set of reference expressions which cannot be resolved within this plan. For plan representing:
     * SELECT a, b FROM (VALUES 1) T(a). It will return a set containing single expression reference to 'b'.
     */
    private Set<Expression> extractOuterColumnReferences(LogicalPlanNode planNode)
    {
        // at this point all the column references are already rewritten to SymbolReference
        // when reference expression is not rewritten that means it cannot be satisfied within given PlaNode
        // see that TranslationMap only resolves (local) fields in current scope
        return ExpressionExtractor.extractExpressions(planNode).stream()
                .filter(OriginalExpressionUtils::isExpression)
                .map(OriginalExpressionUtils::castToExpression)
                .flatMap(expression -> extractColumnReferences(expression, analysis.getColumnReferences()).stream())
                .collect(toImmutableSet());
    }

    private static Set<Expression> extractColumnReferences(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<Expression> expressionColumnReferences = ImmutableSet.builder();
        new ColumnReferencesExtractor(columnReferences).process(expression, expressionColumnReferences);
        return expressionColumnReferences.build();
    }

    private LogicalPlanNode replaceExpressionsWithSymbols(LogicalPlanNode planNode, Map<Expression, Expression> mapping)
    {
        if (mapping.isEmpty()) {
            return planNode;
        }

        return SimplePlanRewriter.rewriteWith(new ExpressionReplacer(idAllocator, mapping), planNode, null);
    }

    private static class ColumnReferencesExtractor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<Expression>>
    {
        private final Set<NodeRef<Expression>> columnReferences;

        private ColumnReferencesExtractor(Set<NodeRef<Expression>> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<Expression> builder)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                builder.add(node);
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<Expression> builder)
        {
            builder.add(node);
            return null;
        }
    }

    private static class ExpressionReplacer
            extends SimplePlanRewriter<Void>
    {
        private final IdGenerator<PlanNodeId> idAllocator;
        private final Map<Expression, Expression> mapping;

        public ExpressionReplacer(IdGenerator<PlanNodeId> idAllocator, Map<Expression, Expression> mapping)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.mapping = requireNonNull(mapping, "mapping is null");
        }

        @Override
        public LogicalPlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            ProjectNode rewrittenNode = (ProjectNode) context.defaultRewrite(node);

            Assignments assignments = AssignmentUtils.rewrite(rewrittenNode.getAssignments(), expression -> replaceExpression(expression, mapping));

            return new ProjectNode(idAllocator.getNextId(), rewrittenNode.getSource(), assignments);
        }

        @Override
        public LogicalPlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            FilterNode rewrittenNode = (FilterNode) context.defaultRewrite(node);
            return new FilterNode(idAllocator.getNextId(), rewrittenNode.getSource(), castToRowExpression(replaceExpression(castToExpression(rewrittenNode.getPredicate()), mapping)));
        }
    }
}