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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.*;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.type.BooleanType;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.tree.*;

import javax.annotation.Nullable;
import java.util.*;

public class ExpressionAnalyzer
{
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT = 63;
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER = 31;

    private final TypeProvider symbolTypes;
    private final boolean isDescribe;

    private final Map<NodeRef<Expression>, Type> expressionCoercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();
    private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons = new LinkedHashSet<>();
    private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();

    private final List<Expression> parameters;
    private final WarningCollector warningCollector;

    private ExpressionAnalyzer(
            TypeProvider symbolTypes,
            List<Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        this.symbolTypes = Objects.requireNonNull(symbolTypes, "symbolTypes is null");
        this.parameters = Objects.requireNonNull(parameters, "parameters is null");
        this.isDescribe = isDescribe;
        //this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.warningCollector = null;
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes()
    {
        return Collections.unmodifiableMap(expressionTypes);
    }

    public Type setExpressionType(Expression expression, Type type)
    {
        Objects.requireNonNull(expression, "expression cannot be null");
        Objects.requireNonNull(type, "type cannot be null");

        expressionTypes.put(NodeRef.of(expression), type);

        return type;
    }

    private Type getExpressionType(Expression expression)
    {
        Objects.requireNonNull(expression, "expression cannot be null");

        Type type = expressionTypes.get(NodeRef.of(expression));
        Preconditions.checkState(type != null, "Expression not yet analyzed: %s", expression);
        return type;
    }

    public Map<NodeRef<Expression>, Type> getExpressionCoercions()
    {
        return Collections.unmodifiableMap(expressionCoercions);
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions()
    {
        return Collections.unmodifiableSet(typeOnlyCoercions);
    }

    public Map<NodeRef<Expression>, FieldId> getColumnReferences()
    {
        return Collections.unmodifiableMap(columnReferences);
    }

    public Type analyze(Expression expression, Scope scope)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(new Context(scope)));
    }

    private Type analyze(Expression expression, Scope baseScope, Context context)
    {
        Visitor visitor = new Visitor(baseScope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(context));
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons()
    {
        return Collections.unmodifiableSet(quantifiedComparisons);
    }

    public Multimap<QualifiedObjectName, String> getTableColumnReferences()
    {
        return tableColumnReferences;
    }

    private class Visitor
            extends StackableAstVisitor<Type, Context> {
        // Used to resolve FieldReferences (e.g. during local execution planning)
        private final Scope baseScope;
        private final WarningCollector warningCollector;

        public Visitor(Scope baseScope, WarningCollector warningCollector) {
            this.baseScope = Objects.requireNonNull(baseScope, "baseScope is null");
            this.warningCollector = warningCollector;
        }

        @Override
        public Type process(Node node, @Nullable StackableAstVisitorContext<Context> context) {
            if (node instanceof Expression) {
                // don't double process a node
                Type type = expressionTypes.get(NodeRef.of(((Expression) node)));
                if (type != null) {
                    return type;
                }
            }
            return super.process(node, context);
        }

        @Override
        protected Type visitSymbolReference(SymbolReference node, StackableAstVisitorContext<Context> context) {
            //Type type = symbolTypes.get(node);
            //return setExpressionType(node, type);
            return null;
        }

        @Override
        protected Type visitIdentifier(Identifier node, StackableAstVisitorContext<Context> context) {
            ResolvedField resolvedField = context.getContext().getScope().resolveField(node, QualifiedName.of(node.getValue()));
            return handleResolvedField(node, resolvedField, context);
        }

        private Type handleResolvedField(Expression node, ResolvedField resolvedField, StackableAstVisitorContext<Context> context) {
            return handleResolvedField(node, FieldId.from(resolvedField), resolvedField.getField(), context);
        }

        private Type handleResolvedField(Expression node, FieldId fieldId, Field field, StackableAstVisitorContext<Context> context) {
            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                tableColumnReferences.put(field.getOriginTable().get(), field.getOriginColumnName().get());
            }

            FieldId previous = columnReferences.put(NodeRef.of(node), fieldId);
            Preconditions.checkState(previous == null, "%s already known to refer to %s", node, previous);
            return setExpressionType(node, field.getType());
        }

        @Override
        protected Type visitDereferenceExpression(DereferenceExpression node, StackableAstVisitorContext<Context> context) {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

            // If this Dereference looks like column reference, try match it to column first.
            if (qualifiedName != null) {
                Scope scope = context.getContext().getScope();
                Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
                if (resolvedField.isPresent()) {
                    return handleResolvedField(node, resolvedField.get(), context);
                }
                if (!scope.isColumnReference(qualifiedName)) {
                    throw SemanticExceptions.missingAttributeException(node, qualifiedName);
                }
            }
            return null;
        }

        @Override
        protected Type visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, StackableAstVisitorContext<Context> context) {
            Expression value = node.getValue();
            process(value, context);

            Expression subquery = node.getSubquery();
            process(subquery, context);
            /*
            Type comparisonType = coerceToSingleType(context, node, "Value expression and result of subquery must be of the same type for quantified comparison: %s vs %s", value, subquery);

            switch (node.getOperator()) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    if (!comparisonType.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Type [%s] must be orderable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                case EQUAL:
                case NOT_EQUAL:
                    if (!comparisonType.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Type [%s] must be comparable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                default:
                    throw new IllegalStateException(format("Unexpected comparison type: %s", node.getOperator()));
            }
            */
            return setExpressionType(node, BooleanType.BOOLEAN);
        }

        @Override
        public Type visitFieldReference(FieldReference node, StackableAstVisitorContext<Context> context) {
            Field field = baseScope.getRelationType().getFieldByIndex(node.getFieldIndex());
            return handleResolvedField(node, new FieldId(baseScope.getRelationId(), node.getFieldIndex()), field, context);
        }
    }

    private static class Context
    {
        private final Scope scope;

        private Context(
                Scope scope)
        {
            this.scope = Objects.requireNonNull(scope, "scope is null");
        }

        Scope getScope()
        {
            return scope;
        }
    }

    public static Map<NodeRef<Expression>, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            TypeProvider types,
            Expression expression,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        return getExpressionTypes(session, metadata, sqlParser, types, expression, parameters, warningCollector, false);
    }

    public static Map<NodeRef<Expression>, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            TypeProvider types,
            Expression expression,
            List<Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return getExpressionTypes(session, metadata, sqlParser, types, ImmutableList.of(expression), parameters, warningCollector, isDescribe);
    }

    public static Map<NodeRef<Expression>, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            TypeProvider types,
            Iterable<Expression> expressions,
            List<Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        //return analyzeExpressions(session, metadata, sqlParser, types, expressions, parameters, warningCollector, isDescribe).getExpressionTypes();
        return null;
    }

    public static ExpressionAnalysis analyzeExpression(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            Scope scope,
            Analysis analysis,
            Expression expression,
            WarningCollector warningCollector)
    {
        ExpressionAnalyzer analyzer = create(analysis, session, metadata, sqlParser, accessControl, TypeProvider.empty(), warningCollector);
        analyzer.analyze(expression, scope);

        Map<NodeRef<Expression>, Type> expressionTypes = analyzer.getExpressionTypes();
        Map<NodeRef<Expression>, Type> expressionCoercions = analyzer.getExpressionCoercions();
        Set<NodeRef<Expression>> typeOnlyCoercions = analyzer.getTypeOnlyCoercions();
        //Map<NodeRef<FunctionCall>, FunctionHandle> resolvedFunctions = analyzer.getResolvedFunctions();

        analysis.addTypes(expressionTypes);
        analysis.addCoercions(expressionCoercions, typeOnlyCoercions);
        //analysis.addFunctionHandles(resolvedFunctions);
        analysis.addColumnReferences(analyzer.getColumnReferences());
        //analysis.addLambdaArgumentReferences(analyzer.getLambdaArgumentReferences());
        //analysis.addTableColumnReferences(accessControl, session.getIdentity(), analyzer.getTableColumnReferences());

        return new ExpressionAnalysis(
                expressionTypes,
                expressionCoercions,
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions()
        );
    }

    private static ExpressionAnalyzer create(
            Analysis analysis,
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        return new ExpressionAnalyzer(
                types,
                analysis.getParameters(),
                warningCollector,
                analysis.isDescribe());
    }
}
