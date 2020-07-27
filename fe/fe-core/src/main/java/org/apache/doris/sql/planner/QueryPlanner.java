package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.sql.analyzer.FieldId;
import org.apache.doris.sql.analyzer.Scope;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.plan.AggregationNode;
import org.apache.doris.sql.planner.plan.AssignmentUtils;
import org.apache.doris.sql.planner.plan.Assignments;
import org.apache.doris.sql.planner.plan.FilterNode;
import org.apache.doris.sql.planner.plan.GroupIdNode;
import org.apache.doris.sql.planner.plan.LimitNode;
import org.apache.doris.sql.planner.plan.LogicalPlanNode;
import org.apache.doris.sql.planner.plan.OrderingScheme;
import org.apache.doris.sql.planner.plan.PlanNodeIdAllocator;
import org.apache.doris.sql.planner.plan.ProjectNode;
import org.apache.doris.sql.planner.plan.SortNode;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.analyzer.Analysis;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.FieldReference;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.GroupingOperation;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.OrderBy;
import org.apache.doris.sql.tree.Query;
import org.apache.doris.sql.tree.QuerySpecification;
import org.apache.doris.sql.tree.SortItem;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static org.apache.doris.sql.planner.PlannerUtils.toOrderingScheme;
import static org.apache.doris.sql.planner.plan.AggregationNode.groupingSets;
import static org.apache.doris.sql.planner.plan.AggregationNode.singleGroupingSet;
import static org.apache.doris.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static org.apache.doris.sql.type.BigintType.BIGINT;

public class QueryPlanner {
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;

    QueryPlanner(Analysis analysis,
                 VariableAllocator variableAllocator,
                 PlanNodeIdAllocator idAllocator,
                 Metadata metadata,
                 Session session) {
        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, variableAllocator, idAllocator, metadata, session);

    }

    public RelationPlan plan(Query query)
    {
        PlanBuilder builder = planQueryBody(query);

        List<Expression> orderBy = analysis.getOrderByExpressions(query);
        builder = handleSubqueries(builder, query, orderBy);
        List<Expression> outputs = analysis.getOutputExpressions(query);
        builder = handleSubqueries(builder, query, outputs);
        builder = project(builder, Iterables.concat(orderBy, outputs));

        builder = sort(builder, query);
        builder = limit(builder, query);
        builder = project(builder, analysis.getOutputExpressions(query));

        return new RelationPlan(builder.getRoot(), analysis.getScope(query), computeOutputs(builder, analysis.getOutputExpressions(query)));
    }

    public RelationPlan plan(QuerySpecification node)
    {
        PlanBuilder builder = planFrom(node);
        RelationPlan fromRelationPlan = builder.getRelationPlan();

        builder = filter(builder, analysis.getWhere(node), node);
        builder = aggregate(builder, node);
        builder = filter(builder, analysis.getHaving(node), node);

        //builder = window(builder, node);

        List<Expression> outputs = analysis.getOutputExpressions(node);
        builder = handleSubqueries(builder, node, outputs);

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
                        .collect(Collectors.toList());
                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()), complexOrderByAggregatesToRemap);
            }

            //builder = window(builder, node.getOrderBy().get());
        }

        List<Expression> orderBy = analysis.getOrderByExpressions(node);
        builder = handleSubqueries(builder, node, orderBy);
        builder = project(builder, Iterables.concat(orderBy, outputs));

        builder = distinct(builder, node);
        builder = sort(builder, node);
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

        //if (node.getFrom().isPresent()) {
            relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, metadata, session)
                    .process(node.getFrom().get(), null);
        //}
        //else {
            //relationPlan = planImplicitTable();
       // }

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
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
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

    private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node) {
        if (!analysis.isAggregation(node)) {
            return subPlan;
        }

        // 1. Pre-project all scalar inputs (arguments and non-trivial group by expressions)
        Set<Expression> groupByExpressions = ImmutableSet.copyOf(analysis.getGroupByExpressions(node));

        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getArguments)
                .flatMap(List::stream)
                //.filter(exp -> !(exp instanceof LambdaExpression)) // lambda expression is generated at execution time
                .forEach(arguments::add);

        analysis.getAggregates(node).stream()
                .map(FunctionCall::getOrderBy)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(OrderBy::getSortItems)
                .flatMap(List::stream)
                .map(SortItem::getSortKey)
                .forEach(arguments::add);


        // filter expressions need to be projected first
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getFilter)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(arguments::add);

        Iterable<Expression> inputs = Iterables.concat(groupByExpressions, arguments.build());
        subPlan = handleSubqueries(subPlan, node, inputs);

        if (!Iterables.isEmpty(inputs)) { // avoid an empty projection if the only aggregation is COUNT (which has no arguments)
            subPlan = project(subPlan, inputs);
        }

        // 2. Aggregate

        // 2.a. Rewrite aggregate arguments
        TranslationMap argumentTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);

        ImmutableList.Builder<VariableReferenceExpression> aggregationArgumentsBuilder = ImmutableList.builder();
        for (Expression argument : arguments.build()) {
            VariableReferenceExpression variable = subPlan.translate(argument);
            argumentTranslations.put(argument, variable);
            aggregationArgumentsBuilder.add(variable);
        }
        List<VariableReferenceExpression> aggregationArguments = aggregationArgumentsBuilder.build();

        // 2.b. Rewrite grouping columns
        TranslationMap groupingTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        Map<VariableReferenceExpression, VariableReferenceExpression> groupingSetMappings = new LinkedHashMap<>();

        for (Expression expression : groupByExpressions) {
            VariableReferenceExpression input = subPlan.translate(expression);
            VariableReferenceExpression output = variableAllocator.newVariable(expression, analysis.getTypeWithCoercions(expression), "gid");
            groupingTranslations.put(expression, output);
            groupingSetMappings.put(output, input);
        }

        // This tracks the grouping sets before complex expressions are considered (see comments below)
        // It's also used to compute the descriptors needed to implement grouping()
        List<Set<FieldId>> columnOnlyGroupingSets = ImmutableList.of(ImmutableSet.of());
        List<List<VariableReferenceExpression>> groupingSets = ImmutableList.of(ImmutableList.of());

        if (node.getGroupBy().isPresent()) {
            // For the purpose of "distinct", we need to canonicalize column references that may have varying
            // syntactic forms (e.g., "t.a" vs "a"). Thus we need to enumerate grouping sets based on the underlying
            // fieldId associated with each column reference expression.

            // The catch is that simple group-by expressions can be arbitrary expressions (this is a departure from the SQL specification).
            // But, they don't affect the number of grouping sets or the behavior of "distinct" . We can compute all the candidate
            // grouping sets in terms of fieldId, dedup as appropriate and then cross-join them with the complex expressions.
            Analysis.GroupingSetAnalysis groupingSetAnalysis = analysis.getGroupingSets(node);
            columnOnlyGroupingSets = enumerateGroupingSets(groupingSetAnalysis);

            if (node.getGroupBy().get().isDistinct()) {
                columnOnlyGroupingSets = columnOnlyGroupingSets.stream()
                        .distinct()
                        .collect(Collectors.toList());
            }

            // add in the complex expressions an turn materialize the grouping sets in terms of plan columns
            ImmutableList.Builder<List<VariableReferenceExpression>> groupingSetBuilder = ImmutableList.builder();
            for (Set<FieldId> groupingSet : columnOnlyGroupingSets) {
                ImmutableList.Builder<VariableReferenceExpression> columns = ImmutableList.builder();
                groupingSetAnalysis.getComplexExpressions().stream()
                        .map(groupingTranslations::get)
                        .forEach(columns::add);

                groupingSet.stream()
                        .map(field -> groupingTranslations.get(new FieldReference(field.getFieldIndex())))
                        .forEach(columns::add);

                groupingSetBuilder.add(columns.build());
            }

            groupingSets = groupingSetBuilder.build();
        }

        // 2.c. Generate GroupIdNode (multiple grouping sets) or ProjectNode (single grouping set)
        Optional<VariableReferenceExpression> groupIdVariable = Optional.empty();
        if (groupingSets.size() > 1) {
            groupIdVariable = Optional.of(variableAllocator.newVariable("groupId", BIGINT));
            GroupIdNode groupId = new GroupIdNode(idAllocator.getNextId(), subPlan.getRoot(), groupingSets, groupingSetMappings, aggregationArguments, groupIdVariable.get());
            subPlan = new PlanBuilder(groupingTranslations, groupId, analysis.getParameters());
        }
        else {
            Assignments.Builder assignments = Assignments.builder();
            aggregationArguments.stream().map(AssignmentUtils::identityAsSymbolReference).forEach(assignments::put);
            groupingSetMappings.forEach((key, value) -> assignments.put(key, castToRowExpression(asSymbolReference(value))));

            ProjectNode project = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build());
            subPlan = new PlanBuilder(groupingTranslations, project, analysis.getParameters());
        }

        TranslationMap aggregationTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        aggregationTranslations.copyMappingsFrom(groupingTranslations);

        // 2.d. Rewrite aggregates
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregationsBuilder = ImmutableMap.builder();
        boolean needPostProjectionCoercion = false;
        for (FunctionCall aggregate : analysis.getAggregates(node)) {
            Expression rewritten = argumentTranslations.rewrite(aggregate);
            VariableReferenceExpression newVariable = variableAllocator.newVariable(rewritten, analysis.getType(aggregate));

            // TODO: this is a hack, because we apply coercions to the output of expressions, rather than the arguments to expressions.
            // Therefore we can end up with this implicit cast, and have to move it into a post-projection
            if (rewritten instanceof Cast) {
                rewritten = ((Cast) rewritten).getExpression();
                needPostProjectionCoercion = true;
            }
            aggregationTranslations.put(aggregate, newVariable);
            FunctionCall rewrittenFunction = (FunctionCall) rewritten;

            aggregationsBuilder.put(newVariable,
                    new AggregationNode.Aggregation(
                            new CallExpression(
                                    aggregate.getName().getSuffix(),
                                    analysis.getFunctionHandle(aggregate),
                                    analysis.getType(aggregate),
                                    rewrittenFunction.getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(Collectors.toList())),
                            rewrittenFunction.getFilter().map(OriginalExpressionUtils::castToRowExpression),
                            rewrittenFunction.getOrderBy().map(orderBy -> toOrderingScheme(orderBy, variableAllocator.getTypes())),
                            rewrittenFunction.isDistinct(),
                            Optional.empty()));
        }
        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = aggregationsBuilder.build();

        ImmutableSet.Builder<Integer> globalGroupingSets = ImmutableSet.builder();
        for (int i = 0; i < groupingSets.size(); i++) {
            if (groupingSets.get(i).isEmpty()) {
                globalGroupingSets.add(i);
            }
        }

        ImmutableList.Builder<VariableReferenceExpression> groupingKeys = ImmutableList.builder();
        groupingSets.stream()
                .flatMap(List::stream)
                .distinct()
                .forEach(groupingKeys::add);
        groupIdVariable.ifPresent(groupingKeys::add);

        AggregationNode aggregationNode = new AggregationNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                aggregations,
                groupingSets(
                        groupingKeys.build(),
                        groupingSets.size(),
                        globalGroupingSets.build()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                groupIdVariable);

        subPlan = new PlanBuilder(aggregationTranslations, aggregationNode, analysis.getParameters());

        // 3. Post-projection
        // Add back the implicit casts that we removed in 2.a
        // TODO: this is a hack, we should change type coercions to coerce the inputs to functions/operators instead of coercing the output
        if (needPostProjectionCoercion) {
            ImmutableList.Builder<Expression> alreadyCoerced = ImmutableList.builder();
            alreadyCoerced.addAll(groupByExpressions);
            groupIdVariable.map(variable -> new SymbolReference(variable.getName())).ifPresent(alreadyCoerced::add);

            subPlan = explicitCoercionFields(subPlan, alreadyCoerced.build(), analysis.getAggregates(node));
        }

        // 4. Project and re-write all grouping functions
        return handleGroupingOperations(subPlan, node, groupIdVariable, columnOnlyGroupingSets);
    }

    private List<Set<FieldId>> enumerateGroupingSets(Analysis.GroupingSetAnalysis groupingSetAnalysis)
    {
        List<List<Set<FieldId>>> partialSets = new ArrayList<>();

        for (Set<FieldId> cube : groupingSetAnalysis.getCubes()) {
            partialSets.add(ImmutableList.copyOf(Sets.powerSet(cube)));
        }

        for (List<FieldId> rollup : groupingSetAnalysis.getRollups()) {
            List<Set<FieldId>> sets = IntStream.rangeClosed(0, rollup.size())
                    .mapToObj(i -> ImmutableSet.copyOf(rollup.subList(0, i)))
                    .collect(Collectors.toList());

            partialSets.add(sets);
        }

        partialSets.addAll(groupingSetAnalysis.getOrdinarySets());

        if (partialSets.isEmpty()) {
            return ImmutableList.of(ImmutableSet.of());
        }

        // compute the cross product of the partial sets
        List<Set<FieldId>> allSets = new ArrayList<>();
        partialSets.get(0)
                .stream()
                .map(ImmutableSet::copyOf)
                .forEach(allSets::add);

        for (int i = 1; i < partialSets.size(); i++) {
            List<Set<FieldId>> groupingSets = partialSets.get(i);
            List<Set<FieldId>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(allSets);
            allSets.clear();
            for (Set<FieldId> existingSet : oldGroupingSetsCrossProduct) {
                for (Set<FieldId> groupingSet : groupingSets) {
                    Set<FieldId> concatenatedSet = ImmutableSet.<FieldId>builder()
                            .addAll(existingSet)
                            .addAll(groupingSet)
                            .build();
                    allSets.add(concatenatedSet);
                }
            }
        }

        return allSets;
    }

    private PlanBuilder handleGroupingOperations(PlanBuilder subPlan, QuerySpecification node, Optional<VariableReferenceExpression> groupIdVariable, List<Set<FieldId>> groupingSets)
    {
        if (analysis.getGroupingOperations(node).isEmpty()) {
            return subPlan;
        }

        TranslationMap newTranslations = subPlan.copyTranslations();

        Assignments.Builder projections = Assignments.builder();
        projections.putAll(identitiesAsSymbolReferences(subPlan.getRoot().getOutputVariables()));

        List<Set<Integer>> descriptor = groupingSets.stream()
                .map(set -> set.stream()
                        .map(FieldId::getFieldIndex)
                        .collect(Collectors.toSet()))
                .collect(Collectors.toList());

        for (GroupingOperation groupingOperation : analysis.getGroupingOperations(node)) {
            Expression rewritten = GroupingOperationRewriter.rewriteGroupingOperation(groupingOperation, descriptor, analysis.getColumnReferenceFields(), groupIdVariable);
            Type coercion = analysis.getCoercion(groupingOperation);
            VariableReferenceExpression variable = variableAllocator.newVariable(rewritten, analysis.getTypeWithCoercions(groupingOperation));
            if (coercion != null) {
                rewritten = new Cast(
                        rewritten,
                        coercion.getTypeSignature().toString(),
                        false,
                        metadata.getTypeManager().isTypeOnlyCoercion(analysis.getType(groupingOperation), coercion));
            }
            projections.put(variable, castToRowExpression(rewritten));
            newTranslations.put(groupingOperation, variable);
        }

        return new PlanBuilder(newTranslations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()), analysis.getParameters());
    }


    private PlanBuilder handleSubqueries(PlanBuilder subPlan, Node node, Iterable<Expression> inputs)
    {
        for (Expression input : inputs) {
            subPlan = subqueryPlanner.handleSubqueries(subPlan, subPlan.rewrite(input), node);
        }
        return subPlan;
    }

    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node)
    {
        if (node.getSelect().isDistinct()) {
            return subPlan.withNewRoot(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            ImmutableMap.of(),
                            singleGroupingSet(subPlan.getRoot().getOutputVariables()),
                            ImmutableList.of(),
                            AggregationNode.Step.SINGLE,
                            Optional.empty(),
                            Optional.empty()));
        }

        return subPlan;
    }

    private PlanBuilder sort(PlanBuilder subPlan, Query node)
    {
        return sort(subPlan, node.getOrderBy(), analysis.getOrderByExpressions(node));
    }

    private PlanBuilder sort(PlanBuilder subPlan, QuerySpecification node)
    {
        return sort(subPlan, node.getOrderBy(), analysis.getOrderByExpressions(node));
    }

    private PlanBuilder sort(PlanBuilder subPlan, Optional<OrderBy> orderBy, List<Expression> orderByExpressions)
    {
        if (!orderBy.isPresent()) {
            return subPlan;
        }

        LogicalPlanNode planNode;
        OrderingScheme orderingScheme = toOrderingScheme(
                orderByExpressions.stream().map(subPlan::translate).collect(Collectors.toList()),
                orderBy.get().getSortItems().stream().map(PlannerUtils::toSortOrder).collect(Collectors.toList()));
        planNode = new SortNode(idAllocator.getNextId(), subPlan.getRoot(), orderingScheme, false);

        return subPlan.withNewRoot(planNode);
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
            subPlan = subPlan.withNewRoot(new LimitNode(idAllocator.getNextId(), subPlan.getRoot(), limitValue, LimitNode.Step.FINAL));
        }

        return subPlan;
    }

    private static List<Expression> toSymbolReferences(List<VariableReferenceExpression> variables)
    {
        return variables.stream()
                .map(variable -> new SymbolReference(variable.getName()))
                .collect(Collectors.toList());
    }
}
