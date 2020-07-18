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
package org.apache.doris.sql.analyzer;


import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.metadata.FunctionMetadata;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.tree.ArithmeticBinaryExpression;
import org.apache.doris.sql.tree.ArithmeticUnaryExpression;
import org.apache.doris.sql.tree.AstVisitor;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.DereferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.FieldReference;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.GroupingOperation;
import org.apache.doris.sql.tree.Identifier;
import org.apache.doris.sql.tree.Literal;
import org.apache.doris.sql.tree.LogicalBinaryExpression;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.SortItem;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static org.apache.doris.sql.analyzer.ExpressionTreeUtils.extractWindowFunctions;
import static org.apache.doris.sql.analyzer.ScopeReferenceExtractor.getReferencesToScope;
import static org.apache.doris.sql.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static org.apache.doris.sql.analyzer.ScopeReferenceExtractor.isFieldFromScope;

/**
 * Checks whether an expression is constant with respect to the group
 */
class AggregationAnalyzer
{
    // fields and expressions in the group by clause
    private final Set<FieldId> groupingFields;
    private final List<Expression> expressions;
    private final Map<NodeRef<Expression>, FieldId> columnReferences;

    private final Metadata metadata;
    private final Analysis analysis;

    private final Scope sourceScope;
    private final Optional<Scope> orderByScope;
    private final WarningCollector warningCollector;
    private final FunctionResolution functionResolution;

    public static void verifySourceAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Expression expression,
            Metadata metadata,
            Analysis analysis,
            WarningCollector warningCollector)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.empty(), metadata, analysis, warningCollector);
        analyzer.analyze(expression);
    }

    public static void verifyOrderByAggregations(
            List<Expression> groupByExpressions,
            Scope sourceScope,
            Scope orderByScope,
            Expression expression,
            Metadata metadata,
            Analysis analysis,
            WarningCollector warningCollector)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.of(orderByScope), metadata, analysis, warningCollector);
        analyzer.analyze(expression);
    }

    private AggregationAnalyzer(List<Expression> groupByExpressions, Scope sourceScope, Optional<Scope> orderByScope, Metadata metadata, Analysis analysis, WarningCollector warningCollector)
    {
        this.sourceScope = sourceScope;
        this.warningCollector = warningCollector;
        this.orderByScope = orderByScope;
        this.metadata = metadata;
        this.analysis = analysis;
        this.functionResolution = new FunctionResolution(metadata.getFunctionManager());
        this.expressions = groupByExpressions;

        this.columnReferences = analysis.getColumnReferenceFields();

        this.groupingFields = groupByExpressions.stream()
                .map(NodeRef::of)
                .filter(columnReferences::containsKey)
                .map(columnReferences::get)
                .collect(toImmutableSet());

        this.groupingFields.forEach(fieldId -> {
            checkState(isFieldFromScope(fieldId, sourceScope),
                    "Grouping field %s should originate from %s", fieldId, sourceScope.getRelationType());
        });
    }

    private void analyze(Expression expression)
    {
        Visitor visitor = new Visitor();
        if (!visitor.process(expression, null)) {
            throw new SemanticException(SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY, expression, "'%s' must be an aggregate expression or appear in GROUP BY clause", expression);
        }
    }

    /**
     * visitor returns true if all expressions are constant with respect to the group.
     */
    private class Visitor
            extends AstVisitor<Boolean, Void>
    {
        @Override
        protected Boolean visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("aggregation analysis not yet implemented for: " + node.getClass().getName());
        }

        @Override
        protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitLiteral(Literal node, Void context)
        {
            return true;
        }

        @Override
        protected Boolean visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitFunctionCall(FunctionCall node, Void context)
        {
            if (metadata.getFunctionManager().getFunctionMetadata(analysis.getFunctionHandle(node)).getFunctionKind() == FunctionHandle.FunctionKind.AGGREGATE) {
                /*
                if (functionResolution.isCountFunction(analysis.getFunctionHandle(node)) && node.isDistinct()) {
                    warningCollector.add(new PrestoWarning(
                            PERFORMANCE_WARNING,
                            "COUNT(DISTINCT xxx) can be a very expensive operation when the cardinality is high for xxx. In most scenarios, using approx_distinct instead would be enough"));
                }
                 */
                if (!node.getWindow().isPresent()) {
                    List<FunctionCall> aggregateFunctions = extractAggregateFunctions(analysis.getFunctionHandles(), node.getArguments(), metadata.getFunctionManager());
                    List<FunctionCall> windowFunctions = extractWindowFunctions(node.getArguments());

                    if (!aggregateFunctions.isEmpty()) {
                        throw new SemanticException(SemanticErrorCode.NESTED_AGGREGATION,
                                node,
                                "Cannot nest aggregations inside aggregation '%s': %s",
                                node.getName(),
                                aggregateFunctions);
                    }

                    if (!windowFunctions.isEmpty()) {
                        throw new SemanticException(SemanticErrorCode.NESTED_WINDOW,
                                node,
                                "Cannot nest window functions inside aggregation '%s': %s",
                                node.getName(),
                                windowFunctions);
                    }

                    if (node.getOrderBy().isPresent()) {
                        List<Expression> sortKeys = node.getOrderBy().get().getSortItems().stream()
                                .map(SortItem::getSortKey)
                                .collect(toImmutableList());
                        if (node.isDistinct()) {
                            List<FieldId> fieldIds = node.getArguments().stream()
                                    .map(NodeRef::of)
                                    .map(columnReferences::get)
                                    .filter(Objects::nonNull)
                                    .collect(toImmutableList());
                            for (Expression sortKey : sortKeys) {
                                if (!node.getArguments().contains(sortKey) && !fieldIds.contains(columnReferences.get(NodeRef.of(sortKey)))) {
                                    throw new SemanticException(
                                            SemanticErrorCode.ORDER_BY_MUST_BE_IN_AGGREGATE,
                                            sortKey,
                                            "For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");
                                }
                            }
                        }
                        // ensure that no output fields are referenced from ORDER BY clause
                        if (orderByScope.isPresent()) {
                            for (Expression sortKey : sortKeys) {
                                verifyNoOrderByReferencesToOutputColumns(
                                        sortKey,
                                        SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                                        "ORDER BY clause in aggregation function must not reference query output columns");
                            }
                        }
                    }

                    // ensure that no output fields are referenced from ORDER BY clause
                    if (orderByScope.isPresent()) {
                        node.getArguments().stream()
                                .forEach(argument -> verifyNoOrderByReferencesToOutputColumns(
                                        argument,
                                        SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                                        "Invalid reference to output projection attribute from ORDER BY aggregation"));
                    }

                    return true;
                }
            }
            else {
                if (node.getFilter().isPresent()) {
                    throw new SemanticException(SemanticErrorCode.MUST_BE_AGGREGATION_FUNCTION,
                            node,
                            "Filter is only valid for aggregation functions",
                            node);
                }
                if (node.getOrderBy().isPresent()) {
                    throw new SemanticException(SemanticErrorCode.MUST_BE_AGGREGATION_FUNCTION, node, "ORDER BY is only valid for aggregation functions");
                }
            }

            if (node.getWindow().isPresent() && !process(node.getWindow().get(), context)) {
                return false;
            }

            return node.getArguments().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitIdentifier(Identifier node, Void context)
        {
            return isGroupingKey(node);
        }

        @Override
        protected Boolean visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            if (columnReferences.containsKey(NodeRef.<Expression>of(node))) {
                return isGroupingKey(node);
            }

            // Allow SELECT col1.f1 FROM table1 GROUP BY col1
            return process(node.getBase(), context);
        }

        private boolean isGroupingKey(Expression node)
        {
            FieldId fieldId = columnReferences.get(NodeRef.of(node));
            requireNonNull(fieldId, () -> "No FieldId for " + node);

            if (orderByScope.isPresent() && isFieldFromScope(fieldId, orderByScope.get())) {
                return true;
            }

            return groupingFields.contains(fieldId);
        }

        @Override
        protected Boolean visitFieldReference(FieldReference node, Void context)
        {
            if (orderByScope.isPresent()) {
                return true;
            }

            FieldId fieldId = requireNonNull(columnReferences.get(NodeRef.<Expression>of(node)), "No FieldId for FieldReference");
            boolean inGroup = groupingFields.contains(fieldId);
            if (!inGroup) {
                Field field = sourceScope.getRelationType().getFieldByIndex(node.getFieldIndex());

                String column;
                if (!field.getName().isPresent()) {
                    column = Integer.toString(node.getFieldIndex() + 1);
                }
                else if (field.getRelationAlias().isPresent()) {
                    column = String.format("'%s.%s'", field.getRelationAlias().get(), field.getName().get());
                }
                else {
                    column = "'" + field.getName().get() + "'";
                }

                throw new SemanticException(SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY, node, "Column %s not in GROUP BY clause", column);
            }
            return inGroup;
        }

        @Override
        protected Boolean visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            return process(node.getValue(), context);
        }


        @Override
        protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        public Boolean visitGroupingOperation(GroupingOperation node, Void context)
        {
            // ensure that no output fields are referenced from ORDER BY clause
            if (orderByScope.isPresent()) {
                node.getGroupingColumns().forEach(groupingColumn -> verifyNoOrderByReferencesToOutputColumns(
                        groupingColumn,
                        SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING,
                        "Invalid reference to output of SELECT clause from grouping() expression in ORDER BY"));
            }

            Optional<Expression> argumentNotInGroupBy = node.getGroupingColumns().stream()
                    .filter(argument -> !columnReferences.containsKey(NodeRef.of(argument)) || !isGroupingKey(argument))
                    .findAny();
            if (argumentNotInGroupBy.isPresent()) {
                throw new SemanticException(
                        SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS,
                        node,
                        "The arguments to GROUPING() must be expressions referenced by the GROUP BY at the associated query level. Mismatch due to %s.",
                        argumentNotInGroupBy.get());
            }
            return true;
        }

        @Override
        public Boolean process(Node node, Void context)
        {
            if (expressions.stream().anyMatch(node::equals)
                    && (!orderByScope.isPresent() || !hasOrderByReferencesToOutputColumns(node))) {
                    //&& !hasFreeReferencesToLambdaArgument(node, analysis)) {
                return true;
            }

            return super.process(node, context);
        }
    }

    private boolean hasOrderByReferencesToOutputColumns(Node node)
    {
        return hasReferencesToScope(node, analysis, orderByScope.get());
    }

    private void verifyNoOrderByReferencesToOutputColumns(Node node, SemanticErrorCode errorCode, String errorString)
    {
        getReferencesToScope(node, analysis, orderByScope.get())
                .findFirst()
                .ifPresent(expression -> {
                    throw new SemanticException(errorCode, expression, errorString);
                });
    }
}