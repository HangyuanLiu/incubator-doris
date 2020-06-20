package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.analyzer.Scope;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.Query;
import org.apache.doris.sql.tree.QuerySpecification;
import org.apache.doris.sql.tree.SymbolReference;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;

public class QueryPlanner {
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final IdGenerator<PlanNodeId> idAllocator;

    QueryPlanner(Analysis analysis, VariableAllocator variableAllocator, IdGenerator<PlanNodeId> idAllocator) {
        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
    }

    public RelationPlan plan(Query query)
    {
        PlanBuilder builder = planQueryBody(query);

        //List<Expression> orderBy = analysis.getOrderByExpressions(query);
        //builder = handleSubqueries(builder, query, orderBy);
        //List<Expression> outputs = analysis.getOutputExpressions(query);
        //builder = handleSubqueries(builder, query, outputs);
        //builder = project(builder, Iterables.concat(outputs));

        //builder = sort(builder, query);
        //builder = limit(builder, query);
        builder = project(builder, analysis.getOutputExpressions(query));

        return new RelationPlan(builder.getRoot(), analysis.getScope(query), computeOutputs(builder, analysis.getOutputExpressions(query)));
    }

    public RelationPlan plan(QuerySpecification node)
    {
        PlanBuilder builder = planFrom(node);
        //RelationPlan fromRelationPlan = builder.getRelationPlan();

        builder = filter(builder, analysis.getWhere(node), node);
        //builder = aggregate(builder, node);
        //builder = filter(builder, analysis.getHaving(node), node);

        //builder = window(builder, node);

        List<Expression> outputs = analysis.getOutputExpressions(node);
        builder = project(builder, outputs);

        return new RelationPlan(builder.getRoot(), analysis.getScope(node), computeOutputs(builder, outputs));
    }

    private static List<VariableReferenceExpression> computeOutputs(PlanBuilder builder, List<Expression> outputExpressions)
    {
        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        for (Expression expression : outputExpressions) {
            outputs.add(builder.translate(expression));
        }
        return outputs.build();
    }

    private PlanBuilder planQueryBody(Query query)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator)
                .process(query.getQueryBody(), null);

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        RelationPlan relationPlan;

        //if (node.getFrom().isPresent()) {
            relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator)
                    .process(node.getFrom().get(), null);
        //}
        //else {
            //relationPlan = planImplicitTable();
       // }

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope)
    {
        return planBuilderFor(new RelationPlan(builder.getRoot(), scope, builder.getRoot().getOutputVariables()));
    }

    private PlanBuilder planBuilderFor(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->variable mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot(), analysis.getParameters());
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node)
    {
        if (predicate == null) {
            return subPlan;
        }

        // rewrite expressions which contain already handled subqueries
        /*
        Expression rewrittenBeforeSubqueries = subPlan.rewrite(predicate);
        subPlan = subqueryPlanner.handleSubqueries(subPlan, rewrittenBeforeSubqueries, node);
        Expression rewrittenAfterSubqueries = subPlan.rewrite(predicate);
         */

        return subPlan.withNewRoot(new FilterNode(idAllocator.getNextId(), subPlan.getRoot(), castToRowExpression(predicate)));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions, RelationPlan parentRelationPlan)
    {
        return project(subPlan, Iterables.concat(expressions, toSymbolReferences(parentRelationPlan.getFieldMappings())));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions)
    {
        TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);

        Assignments.Builder projections = Assignments.builder();
        for (Expression expression : expressions) {
            if (expression instanceof SymbolReference) {
                VariableReferenceExpression variable = variableAllocator.toVariableReference(expression);
                projections.put(variable, castToRowExpression(expression));
                outputTranslations.put(expression, variable);
                continue;
            }

            VariableReferenceExpression variable = variableAllocator.newVariable(expression, analysis.getTypeWithCoercions(expression));
            projections.put(variable, castToRowExpression(subPlan.rewrite(expression)));
            outputTranslations.put(expression, variable);
        }

        return new PlanBuilder(outputTranslations, new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                projections.build()),
                analysis.getParameters());
    }

    private static List<Expression> toSymbolReferences(List<VariableReferenceExpression> variables)
    {
        return variables.stream()
                .map(variable -> new SymbolReference(variable.getName()))
                .collect(toImmutableList());
    }
}
