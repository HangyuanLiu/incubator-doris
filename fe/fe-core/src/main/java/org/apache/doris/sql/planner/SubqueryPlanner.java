package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.ApplyNode;
import org.apache.doris.sql.planner.plan.AssignmentUtils;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.EnforceSingleRowNode;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.LateralJoinNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.ValuesNode;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.DefaultExpressionTraversalVisitor;
import org.apache.doris.sql.tree.DereferenceExpression;
import org.apache.doris.sql.tree.ExistsPredicate;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Identifier;
import org.apache.doris.sql.tree.InPredicate;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.NotExpression;
import org.apache.doris.sql.tree.QuantifiedComparisonExpression;
import org.apache.doris.sql.tree.Query;
import org.apache.doris.sql.tree.SubqueryExpression;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.util.MorePredicates;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.SemanticExceptions.notSupportedException;
import static org.apache.doris.sql.analyzer.SemanticExceptions.subQueryNotSupportedError;
import static org.apache.doris.sql.planner.ExpressionNodeInliner.replaceExpression;
import static org.apache.doris.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static org.apache.doris.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;
import static org.apache.doris.sql.util.AstUtils.nodeContains;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;

    SubqueryPlanner(
            Analysis analysis,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

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
        builder = appendScalarSubqueryApplyNodes(builder, collectScalarSubqueries(expression, node), correlationAllowed);
        builder = appendExistsSubqueryApplyNodes(builder, collectExistsSubqueries(expression, node), correlationAllowed);
        builder = appendQuantifiedComparisonApplyNodes(builder, collectQuantifiedComparisonSubqueries(expression, node), correlationAllowed, node);
        return builder;
    }

    public Set<InPredicate> collectInPredicateSubqueries(Expression expression, Node node)
    {
        return analysis.getInPredicateSubqueries(node)
                .stream()
                .filter(inPredicate -> nodeContains(expression, inPredicate.getValueList()))
                .collect(Collectors.toSet());
    }

    public Set<SubqueryExpression> collectScalarSubqueries(Expression expression, Node node)
    {
        return analysis.getScalarSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(Collectors.toSet());
    }

    public Set<ExistsPredicate> collectExistsSubqueries(Expression expression, Node node)
    {
        return analysis.getExistsSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(Collectors.toSet());
    }

    public Set<QuantifiedComparisonExpression> collectQuantifiedComparisonSubqueries(Expression expression, Node node)
    {
        return analysis.getQuantifiedComparisonSubqueries(node)
                .stream()
                .filter(quantifiedComparison -> nodeContains(expression, quantifiedComparison.getSubquery()))
                .collect(Collectors.toSet());
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

    private PlanBuilder appendScalarSubqueryApplyNodes(PlanBuilder builder, Set<SubqueryExpression> scalarSubqueries, boolean correlationAllowed)
    {
        for (SubqueryExpression scalarSubquery : scalarSubqueries) {
            builder = appendScalarSubqueryApplyNode(builder, scalarSubquery, correlationAllowed);
        }
        return builder;
    }

    private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan, SubqueryExpression scalarSubquery, boolean correlationAllowed)
    {
        if (subPlan.canTranslate(scalarSubquery)) {
            // given subquery is already appended
            return subPlan;
        }

        List<Expression> coercions = coercionsFor(scalarSubquery);

        SubqueryExpression uncoercedScalarSubquery = uncoercedSubquery(scalarSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedScalarSubquery);
        subqueryPlan = subqueryPlan.withNewRoot(new EnforceSingleRowNode(idAllocator.getNextId(), subqueryPlan.getRoot()));
        subqueryPlan = subqueryPlan.appendProjections(coercions, variableAllocator, idAllocator);

        VariableReferenceExpression uncoercedScalarSubqueryVariable = subqueryPlan.translate(uncoercedScalarSubquery);
        subPlan.getTranslations().put(uncoercedScalarSubquery, uncoercedScalarSubqueryVariable);

        for (Expression coercion : coercions) {
            VariableReferenceExpression coercionVariable = subqueryPlan.translate(coercion);
            subPlan.getTranslations().put(coercion, coercionVariable);
        }

        return appendLateralJoin(subPlan, subqueryPlan, scalarSubquery.getQuery(), correlationAllowed, LateralJoinNode.Type.LEFT);
    }

    public PlanBuilder appendLateralJoin(PlanBuilder subPlan, PlanBuilder subqueryPlan, Query query, boolean correlationAllowed, LateralJoinNode.Type type)
    {
        LogicalPlanNode subqueryNode = subqueryPlan.getRoot();
        Map<Expression, Expression> correlation = extractCorrelation(subPlan, subqueryNode);
        if (!correlationAllowed && !correlation.isEmpty()) {
            throw notSupportedException(query, "Correlated subquery in given context");
        }
        subqueryNode = replaceExpressionsWithSymbols(subqueryNode, correlation);

        return new PlanBuilder(
                subPlan.copyTranslations(),
                new LateralJoinNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subqueryNode,
                        ImmutableList.copyOf(VariablesExtractor.extractUnique(correlation.values(), variableAllocator.getTypes())),
                        type,
                        subQueryNotSupportedError(query, "Given correlated subquery")),
                analysis.getParameters());
    }

    private PlanBuilder appendExistsSubqueryApplyNodes(PlanBuilder builder, Set<ExistsPredicate> existsPredicates, boolean correlationAllowed)
    {
        for (ExistsPredicate existsPredicate : existsPredicates) {
            builder = appendExistSubqueryApplyNode(builder, existsPredicate, correlationAllowed);
        }
        return builder;
    }

    /**
     * Exists is modeled as:
     * <pre>
     *     - Project($0 > 0)
     *       - Aggregation(COUNT(*))
     *         - Limit(1)
     *           -- subquery
     * </pre>
     */
    private PlanBuilder appendExistSubqueryApplyNode(PlanBuilder subPlan, ExistsPredicate existsPredicate, boolean correlationAllowed)
    {
        if (subPlan.canTranslate(existsPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        PlanBuilder subqueryPlan = createPlanBuilder(existsPredicate.getSubquery());

        LogicalPlanNode subqueryPlanRoot = subqueryPlan.getRoot();
        if (isAggregationWithEmptyGroupBy(subqueryPlanRoot)) {
            subPlan.getTranslations().put(existsPredicate, BooleanLiteral.TRUE_LITERAL);
            return subPlan;
        }

        // add an explicit projection that removes all columns
        LogicalPlanNode subqueryNode = new ProjectNode(idAllocator.getNextId(), subqueryPlan.getRoot(), Assignments.of());

        VariableReferenceExpression exists = variableAllocator.newVariable("exists", BOOLEAN);
        subPlan.getTranslations().put(existsPredicate, exists);
        ExistsPredicate rewrittenExistsPredicate = new ExistsPredicate(BooleanLiteral.TRUE_LITERAL);
        return appendApplyNode(
                subPlan,
                existsPredicate.getSubquery(),
                subqueryNode,
                Assignments.of(exists, castToRowExpression(rewrittenExistsPredicate)),
                correlationAllowed);
    }

    private PlanBuilder appendQuantifiedComparisonApplyNodes(PlanBuilder subPlan, Set<QuantifiedComparisonExpression> quantifiedComparisons, boolean correlationAllowed, Node node)
    {
        for (QuantifiedComparisonExpression quantifiedComparison : quantifiedComparisons) {
            subPlan = appendQuantifiedComparisonApplyNode(subPlan, quantifiedComparison, correlationAllowed, node);
        }
        return subPlan;
    }

    private PlanBuilder appendQuantifiedComparisonApplyNode(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed, Node node)
    {
        if (subPlan.canTranslate(quantifiedComparison)) {
            // given subquery is already appended
            return subPlan;
        }
        switch (quantifiedComparison.getOperator()) {
            case EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        return planQuantifiedApplyNode(subPlan, quantifiedComparison, correlationAllowed);
                    case ANY:
                    case SOME:
                        // A = ANY B <=> A IN B
                        InPredicate inPredicate = new InPredicate(quantifiedComparison.getValue(), quantifiedComparison.getSubquery());
                        subPlan = appendInPredicateApplyNode(subPlan, inPredicate, correlationAllowed, node);
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.translate(inPredicate));
                        return subPlan;
                }
                break;

            case NOT_EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        // A <> ALL B <=> !(A IN B) <=> !(A = ANY B)
                        QuantifiedComparisonExpression rewrittenAny = new QuantifiedComparisonExpression(
                                EQUAL,
                                QuantifiedComparisonExpression.Quantifier.ANY,
                                quantifiedComparison.getValue(),
                                quantifiedComparison.getSubquery());
                        Expression notAny = new NotExpression(rewrittenAny);
                        // "A <> ALL B" is equivalent to "NOT (A = ANY B)" so add a rewrite for the initial quantifiedComparison to notAny
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.getTranslations().rewrite(notAny));
                        // now plan "A = ANY B" part by calling ourselves for rewrittenAny
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAny, correlationAllowed, node);
                    case ANY:
                    case SOME:
                        // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A = ALL B)
                        QuantifiedComparisonExpression rewrittenAll = new QuantifiedComparisonExpression(
                                EQUAL,
                                QuantifiedComparisonExpression.Quantifier.ALL,
                                quantifiedComparison.getValue(),
                                quantifiedComparison.getSubquery());
                        Expression notAll = new NotExpression(rewrittenAll);
                        // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial quantifiedComparison to notAll
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.getTranslations().rewrite(notAll));
                        // now plan "A = ALL B" part by calling ourselves for rewrittenAll
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAll, correlationAllowed, node);
                }
                break;

            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return planQuantifiedApplyNode(subPlan, quantifiedComparison, correlationAllowed);
        }
        // all cases are checked, so this exception should never be thrown
        throw new IllegalArgumentException(
                format("Unexpected quantified comparison: '%s %s'", quantifiedComparison.getOperator().getValue(), quantifiedComparison.getQuantifier()));
    }

    private PlanBuilder planQuantifiedApplyNode(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed)
    {
        subPlan = subPlan.appendProjections(ImmutableList.of(quantifiedComparison.getValue()), variableAllocator, idAllocator);

        checkState(quantifiedComparison.getSubquery() instanceof SubqueryExpression);
        SubqueryExpression quantifiedSubquery = (SubqueryExpression) quantifiedComparison.getSubquery();

        SubqueryExpression uncoercedQuantifiedSubquery = uncoercedSubquery(quantifiedSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedQuantifiedSubquery);
        subqueryPlan = subqueryPlan.appendProjections(ImmutableList.of(quantifiedSubquery), variableAllocator, idAllocator);

        QuantifiedComparisonExpression coercedQuantifiedComparison = new QuantifiedComparisonExpression(
                quantifiedComparison.getOperator(),
                quantifiedComparison.getQuantifier(),
                new SymbolReference(subPlan.translate(quantifiedComparison.getValue()).getName()),
                new SymbolReference(subqueryPlan.translate(quantifiedSubquery).getName()));

        VariableReferenceExpression coercedQuantifiedComparisonVariable = variableAllocator.newVariable(coercedQuantifiedComparison, BOOLEAN);
        subPlan.getTranslations().put(quantifiedComparison, coercedQuantifiedComparisonVariable);

        return appendApplyNode(
                subPlan,
                quantifiedComparison.getSubquery(),
                subqueryPlan.getRoot(),
                Assignments.of(coercedQuantifiedComparisonVariable, castToRowExpression(coercedQuantifiedComparison)),
                correlationAllowed);
    }

    private static boolean isAggregationWithEmptyGroupBy(LogicalPlanNode LogicalPlanNode)
    {
        return searchFrom(LogicalPlanNode)
                .recurseOnlyWhen(MorePredicates.isInstanceOfAny(ProjectNode.class))
                .where(AggregationNode.class::isInstance)
                .findFirst()
                .map(AggregationNode.class::cast)
                .map(aggregation -> aggregation.getGroupingKeys().isEmpty())
                .orElse(false);
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

    private List<Expression> coercionsFor(Expression expression)
    {
        return analysis.getCoercions().keySet().stream()
                .map(NodeRef::getNode)
                .filter(coercionExpression -> coercionExpression.equals(expression))
                .collect(Collectors.toList());
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
        verifySubquerySupported(subqueryAssignments);
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
        RelationPlan relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, metadata, session)
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
    private Set<Expression> extractOuterColumnReferences(LogicalPlanNode LogicalPlanNode)
    {
        // at this point all the column references are already rewritten to SymbolReference
        // when reference expression is not rewritten that means it cannot be satisfied within given PlaNode
        // see that TranslationMap only resolves (local) fields in current scope
        return ExpressionExtractor.extractExpressions(LogicalPlanNode).stream()
                .filter(OriginalExpressionUtils::isExpression)
                .map(OriginalExpressionUtils::castToExpression)
                .flatMap(expression -> extractColumnReferences(expression, analysis.getColumnReferences()).stream())
                .collect(Collectors.toSet());
    }

    private static Set<Expression> extractColumnReferences(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<Expression> expressionColumnReferences = ImmutableSet.builder();
        new ColumnReferencesExtractor(columnReferences).process(expression, expressionColumnReferences);
        return expressionColumnReferences.build();
    }

    private LogicalPlanNode replaceExpressionsWithSymbols(LogicalPlanNode LogicalPlanNode, Map<Expression, Expression> mapping)
    {
        if (mapping.isEmpty()) {
            return LogicalPlanNode;
        }

        return SimplePlanRewriter.rewriteWith(new ExpressionReplacer(idAllocator, mapping), LogicalPlanNode, null);
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
        private final PlanNodeIdAllocator idAllocator;
        private final Map<Expression, Expression> mapping;

        public ExpressionReplacer(PlanNodeIdAllocator idAllocator, Map<Expression, Expression> mapping)
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

        @Override
        public LogicalPlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            ValuesNode rewrittenNode = (ValuesNode) context.defaultRewrite(node);
            List<List<RowExpression>> rewrittenRows = rewrittenNode.getRows().stream()
                    .map(row -> row.stream()
                            .map(column -> castToRowExpression(replaceExpression(castToExpression(column), mapping)))
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());
            return new ValuesNode(
                    idAllocator.getNextId(),
                    rewrittenNode.getOutputVariables(),
                    rewrittenRows);
        }
    }
}