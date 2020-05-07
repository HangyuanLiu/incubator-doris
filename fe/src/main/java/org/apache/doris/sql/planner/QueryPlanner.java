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
package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.analyzer.FieldId;
import org.apache.doris.sql.analyzer.Scope;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.plan.*;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.*;

import java.util.*;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;

class QueryPlanner
{
    private final Analysis analysis;
    private final PlanVariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;
    //private final SubqueryPlanner subqueryPlanner;

    QueryPlanner(
            Analysis analysis,
            PlanVariableAllocator variableAllocator,
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
        //this.subqueryPlanner = new SubqueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session);
    }

    public RelationPlan plan(Query query)
    {
        PlanBuilder builder = planQueryBody(query);

        List<Expression> orderBy = analysis.getOrderByExpressions(query);
        //builder = handleSubqueries(builder, query, orderBy);
        List<Expression> outputs = analysis.getOutputExpressions(query);
        //builder = handleSubqueries(builder, query, outputs);
        builder = project(builder, Iterables.concat(orderBy, outputs));

        //builder = sort(builder, query);
        builder = limit(builder, query);
        builder = project(builder, analysis.getOutputExpressions(query));

        return new RelationPlan(builder.getRoot(), analysis.getScope(query), computeOutputs(builder, analysis.getOutputExpressions(query)));
    }

    public RelationPlan plan(QuerySpecification node)
    {
        PlanBuilder builder = planFrom(node);
        RelationPlan fromRelationPlan = builder.getRelationPlan();

        builder = filter(builder, analysis.getWhere(node), node);
        //builder = aggregate(builder, node);
        builder = filter(builder, analysis.getHaving(node), node);

        //builder = window(builder, node);

        List<Expression> outputs = analysis.getOutputExpressions(node);
        //builder = handleSubqueries(builder, node, outputs);

        if (node.getOrderBy().isPresent()) {
            if (!analysis.isAggregation(node)) {
                // ORDER BY requires both output and source fields to be visible if there are no aggregations
                builder = project(builder, outputs, fromRelationPlan);
                outputs = toSymbolReferences(computeOutputs(builder, outputs));
                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()));
            }
            else {
                // ORDER BY requires output fields, groups and translated aggregations to be visible for queries with aggregation
                List<Expression> orderByAggregates = analysis.getOrderByAggregates(node.getOrderBy().get());
                builder = project(builder, Iterables.concat(outputs, orderByAggregates));
                outputs = toSymbolReferences(computeOutputs(builder, outputs));
                List<Expression> complexOrderByAggregatesToRemap = orderByAggregates.stream()
                        .filter(expression -> !analysis.isColumnReference(expression))
                        .collect(toImmutableList());
                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()), complexOrderByAggregatesToRemap);
            }

            //builder = window(builder, node.getOrderBy().get());
        }

        List<Expression> orderBy = analysis.getOrderByExpressions(node);
        //builder = handleSubqueries(builder, node, orderBy);
        builder = project(builder, Iterables.concat(orderBy, outputs));

        //builder = distinct(builder, node);
        //builder = sort(builder, node);
        builder = limit(builder, node);
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
        RelationPlan relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, metadata, session)
                .process(query.getQueryBody(), null);

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        RelationPlan relationPlan;

        if (node.getFrom().isPresent()) {
            relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, metadata, session)
                    .process(node.getFrom().get(), null);
        }
        else {
            relationPlan = planImplicitTable();
        }

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope, Iterable<? extends Expression> expressionsToRemap)
    {
        PlanBuilder newBuilder = planBuilderFor(builder, scope);
        // We can't deduplicate expressions here because even if two expressions are equal,
        // the TranslationMap maps sql names to symbols, and any lambda expressions will be
        // resolved differently since the lambdaDeclarationToVariableMap is identity based.
        stream(expressionsToRemap)
                .forEach(expression -> newBuilder.getTranslations().put(expression, builder.translate(expression)));
        return newBuilder;
    }

    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope)
    {
        return planBuilderFor(new RelationPlan(builder.getRoot(), scope, builder.getRoot().getOutputVariables()));
    }

    private PlanBuilder planBuilderFor(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);

        // Make field->variable mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot(), analysis.getParameters());
    }

    private RelationPlan planImplicitTable()
    {
        Scope scope = Scope.create();
        return new RelationPlan(
                new ValuesNode(idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of(ImmutableList.of())),
                scope,
                ImmutableList.of());
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node)
    {
        if (predicate == null) {
            return subPlan;
        }

        // rewrite expressions which contain already handled subqueries
        Expression rewrittenBeforeSubqueries = subPlan.rewrite(predicate);
        subPlan = subqueryPlanner.handleSubqueries(subPlan, rewrittenBeforeSubqueries, node);
        Expression rewrittenAfterSubqueries = subPlan.rewrite(predicate);

        return subPlan.withNewRoot(new FilterNode(idAllocator.getNextId(), subPlan.getRoot(), castToRowExpression(rewrittenAfterSubqueries)));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions, RelationPlan parentRelationPlan)
    {
        return project(subPlan, Iterables.concat(expressions, toSymbolReferences(parentRelationPlan.getFieldMappings())));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions)
    {
        TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);

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

    private Map<VariableReferenceExpression, RowExpression> coerce(Iterable<? extends Expression> expressions, PlanBuilder subPlan, TranslationMap translations)
    {
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> projections = ImmutableMap.builder();

        for (Expression expression : expressions) {
            Type type = analysis.getType(expression);
            Type coercion = analysis.getCoercion(expression);
            VariableReferenceExpression variable = variableAllocator.newVariable(expression, firstNonNull(coercion, type));
            Expression rewritten = subPlan.rewrite(expression);
            if (coercion != null) {
                rewritten = new Cast(
                        rewritten,
                        coercion.getTypeSignature().toString(),
                        false,
                        metadata.getTypeManager().isTypeOnlyCoercion(type, coercion));
            }
            projections.put(variable, castToRowExpression(rewritten));
            translations.put(expression, variable);
        }

        return projections.build();
    }

    private PlanBuilder explicitCoercionFields(PlanBuilder subPlan, Iterable<Expression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);
        Assignments.Builder projections = Assignments.builder();

        projections.putAll(coerce(uncoerced, subPlan, translations));

        for (Expression expression : alreadyCoerced) {
            if (expression instanceof SymbolReference) {
                // If this is an identity projection, no need to rewrite it
                // This is needed because certain synthetic identity expressions such as "group id" introduced when planning GROUPING
                // don't have a corresponding analysis, so the code below doesn't work for them
                projections.put(variableAllocator.toVariableReference(expression), castToRowExpression(expression));
                continue;
            }

            VariableReferenceExpression variable = variableAllocator.newVariable(expression, analysis.getType(expression));
            Expression rewritten = subPlan.rewrite(expression);
            projections.put(variable, castToRowExpression(rewritten));
            translations.put(expression, variable);
        }

        return new PlanBuilder(translations, new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                projections.build()),
                analysis.getParameters());
    }

    private PlanBuilder explicitCoercionVariables(PlanBuilder subPlan, List<VariableReferenceExpression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
    {
        TranslationMap translations = subPlan.copyTranslations();

        Assignments assignments = Assignments.builder()
                .putAll(coerce(uncoerced, subPlan, translations))
                .putAll(identitiesAsSymbolReferences(alreadyCoerced))
                .build();

        return new PlanBuilder(translations, new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                assignments),
                analysis.getParameters());
    }

    private PlanBuilder limit(PlanBuilder subPlan, Query node)
    {
        return limit(subPlan, node.getLimit());
    }

    private PlanBuilder limit(PlanBuilder subPlan, QuerySpecification node)
    {
        return limit(subPlan, node.getLimit());
    }

    private PlanBuilder limit(PlanBuilder subPlan, Optional<String> limit)
    {
        if (!limit.isPresent()) {
            return subPlan;
        }

        if (!limit.get().equalsIgnoreCase("all")) {
            long limitValue = Long.parseLong(limit.get());
            subPlan = subPlan.withNewRoot(new LimitNode(idAllocator.getNextId(), subPlan.getRoot(), limitValue, FINAL));
        }

        return subPlan;
    }

    private static List<Expression> toSymbolReferences(List<VariableReferenceExpression> variables)
    {
        return variables.stream()
                .map(variable -> new SymbolReference(variable.getName()))
                .collect(toImmutableList());
    }
}
