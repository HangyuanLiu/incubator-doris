package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.ExpressionUtils;
import org.apache.doris.sql.analyzer.RelationType;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.optimizations.JoinNodeUtils;
import org.apache.doris.sql.planner.plan.*;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.Field;
import org.apache.doris.sql.analyzer.Scope;
import org.apache.doris.sql.metadata.ColumnHandle;
import org.apache.doris.sql.metadata.TableHandle;
import org.apache.doris.sql.tree.*;
import org.apache.doris.sql.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.doris.sql.analyzer.ExpressionTreeUtils.isEqualComparisonExpression;
import static org.apache.doris.sql.analyzer.SemanticExceptions.notSupportedException;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.tree.Join.Type.INNER;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void> {
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;

    RelationPlanner(Analysis analysis, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator,
                    Metadata metadata,
                    Session session) {
        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, variableAllocator, idAllocator, metadata, session);

    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Scope scope = analysis.getScope(node);
        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : scope.getRelationType().getAllFields()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(field.getName().get(), field.getType());
            outputVariablesBuilder.add(variable);
            columns.put(variable, analysis.getColumn(field));
        }

        List<VariableReferenceExpression> outputVariables = outputVariablesBuilder.build();
        LogicalPlanNode root = new TableScanNode(idAllocator.getNextId(), handle, outputVariables, columns.build());
        return new RelationPlan(root, scope, outputVariables);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        LogicalPlanNode root = subPlan.getRoot();
        List<VariableReferenceExpression> mappings = subPlan.getFieldMappings();

        if (node.getColumnNames() != null) {
            ImmutableList.Builder<VariableReferenceExpression> newMappings = ImmutableList.builder();
            Assignments.Builder assignments = Assignments.builder();

            // project only the visible columns from the underlying relation
            for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
                Field field = subPlan.getDescriptor().getFieldByIndex(i);
                if (!field.isHidden()) {
                    VariableReferenceExpression aliasedColumn = variableAllocator.newVariable(field);
                    assignments.put(aliasedColumn, castToRowExpression(asSymbolReference(subPlan.getFieldMappings().get(i))));
                    newMappings.add(aliasedColumn);
                }
            }

            root = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build());
            mappings = newMappings.build();
        }

        return new RelationPlan(root, analysis.getScope(node), mappings);
    }


    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);

        RelationPlan rightPlan = process(node.getRight(), context);

        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        // NOTE: variables must be in the same order as the outputDescriptor
        List<VariableReferenceExpression> outputs = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();

        ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
        List<Expression> complexJoinExpressions = new ArrayList<>();
        List<Expression> postInnerJoinConditions = new ArrayList<>();

        if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
            Expression criteria = analysis.getJoinCriteria(node);

            RelationType left = analysis.getOutputDescriptor(node.getLeft());
            RelationType right = analysis.getOutputDescriptor(node.getRight());

            List<Expression> leftComparisonExpressions = new ArrayList<>();
            List<Expression> rightComparisonExpressions = new ArrayList<>();
            List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
                conjunct = ExpressionUtils.normalize(conjunct);

                if (!isEqualComparisonExpression(conjunct) && node.getType() != INNER) {
                    complexJoinExpressions.add(conjunct);
                    continue;
                }

                Set<QualifiedName> dependencies = VariablesExtractor.extractNames(conjunct, analysis.getColumnReferences());

                if (dependencies.stream().allMatch(left::canResolve) || dependencies.stream().allMatch(right::canResolve)) {
                    // If the conjunct can be evaluated entirely with the inputs on either side of the join, add
                    // it to the list complex expressions and let the optimizers figure out how to push it down later.
                    complexJoinExpressions.add(conjunct);
                }
                else if (conjunct instanceof ComparisonExpression) {
                    Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
                    Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
                    ComparisonExpression.Operator comparisonOperator = ((ComparisonExpression) conjunct).getOperator();
                    Set<QualifiedName> firstDependencies = VariablesExtractor.extractNames(firstExpression, analysis.getColumnReferences());
                    Set<QualifiedName> secondDependencies = VariablesExtractor.extractNames(secondExpression, analysis.getColumnReferences());

                    if (firstDependencies.stream().allMatch(left::canResolve) && secondDependencies.stream().allMatch(right::canResolve)) {
                        leftComparisonExpressions.add(firstExpression);
                        rightComparisonExpressions.add(secondExpression);
                        joinConditionComparisonOperators.add(comparisonOperator);
                    }
                    else if (firstDependencies.stream().allMatch(right::canResolve) && secondDependencies.stream().allMatch(left::canResolve)) {
                        leftComparisonExpressions.add(secondExpression);
                        rightComparisonExpressions.add(firstExpression);
                        joinConditionComparisonOperators.add(comparisonOperator.flip());
                    }
                    else {
                        // the case when we mix variables from both left and right join side on either side of condition.
                        complexJoinExpressions.add(conjunct);
                    }
                }
                else {
                    complexJoinExpressions.add(conjunct);
                }
            }

            leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, leftComparisonExpressions, node);
            rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder, rightComparisonExpressions, node);

            // Add projections for join criteria
            leftPlanBuilder = leftPlanBuilder.appendProjections(leftComparisonExpressions, variableAllocator, idAllocator);
            rightPlanBuilder = rightPlanBuilder.appendProjections(rightComparisonExpressions, variableAllocator, idAllocator);

            for (int i = 0; i < leftComparisonExpressions.size(); i++) {
                if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
                    VariableReferenceExpression leftVariable = leftPlanBuilder.translateToVariable(leftComparisonExpressions.get(i));
                    VariableReferenceExpression righVariable = rightPlanBuilder.translateToVariable(rightComparisonExpressions.get(i));

                    equiClauses.add(new JoinNode.EquiJoinClause(leftVariable, righVariable));
                }
                else {
                    Expression leftExpression = leftPlanBuilder.rewrite(leftComparisonExpressions.get(i));
                    Expression rightExpression = rightPlanBuilder.rewrite(rightComparisonExpressions.get(i));
                    postInnerJoinConditions.add(new ComparisonExpression(joinConditionComparisonOperators.get(i), leftExpression, rightExpression));
                }
            }
        }

        LogicalPlanNode root = new JoinNode(idAllocator.getNextId(),
                JoinNodeUtils.typeConvert(node.getType()),
                leftPlanBuilder.getRoot(),
                rightPlanBuilder.getRoot(),
                equiClauses.build(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftPlanBuilder.getRoot().getOutputVariables())
                        .addAll(rightPlanBuilder.getRoot().getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        if (node.getType() != INNER) {
            for (Expression complexExpression : complexJoinExpressions) {
                Set<InPredicate> inPredicates = subqueryPlanner.collectInPredicateSubqueries(complexExpression, node);
                if (!inPredicates.isEmpty()) {
                    InPredicate inPredicate = Iterables.getLast(inPredicates);
                    throw notSupportedException(inPredicate, "IN with subquery predicate in join condition");
                }
            }

            // subqueries can be applied only to one side of join - left side is selected in arbitrary way
            leftPlanBuilder = subqueryPlanner.handleUncorrelatedSubqueries(leftPlanBuilder, complexJoinExpressions, node);
        }

        RelationPlan intermediateRootRelationPlan = new RelationPlan(root, analysis.getScope(node), outputs);
        TranslationMap translationMap = new TranslationMap(intermediateRootRelationPlan, analysis);
        translationMap.setFieldMappings(outputs);
        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());

        if (node.getType() != INNER && !complexJoinExpressions.isEmpty()) {
            Expression joinedFilterCondition = ExpressionUtils.and(complexJoinExpressions);
            Expression rewrittenFilterCondition = translationMap.rewrite(joinedFilterCondition);
            root = new JoinNode(idAllocator.getNextId(),
                    JoinNodeUtils.typeConvert(node.getType()),
                    leftPlanBuilder.getRoot(),
                    rightPlanBuilder.getRoot(),
                    equiClauses.build(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(leftPlanBuilder.getRoot().getOutputVariables())
                            .addAll(rightPlanBuilder.getRoot().getOutputVariables())
                            .build(),
                    Optional.of(castToRowExpression(rewrittenFilterCondition)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        if (node.getType() == INNER) {
            // rewrite all the other conditions using output variables from left + right plan node.
            PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root, analysis.getParameters());
            rootPlanBuilder = subqueryPlanner.handleSubqueries(rootPlanBuilder, complexJoinExpressions, node);

            for (Expression expression : complexJoinExpressions) {
                postInnerJoinConditions.add(rootPlanBuilder.rewrite(expression));
            }
            root = rootPlanBuilder.getRoot();

            Expression postInnerJoinCriteria;
            if (!postInnerJoinConditions.isEmpty()) {
                postInnerJoinCriteria = ExpressionUtils.and(postInnerJoinConditions);
                root = new FilterNode(idAllocator.getNextId(), root, castToRowExpression(postInnerJoinCriteria));
            }
        }

        return new RelationPlan(root, analysis.getScope(node), outputs);
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator, metadata, session)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator, metadata, session)
                .plan(node);
    }

    private PlanBuilder initializePlanBuilder(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->variable mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot(), analysis.getParameters());
    }
}
