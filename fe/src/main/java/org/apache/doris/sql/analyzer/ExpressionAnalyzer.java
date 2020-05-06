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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.doris.sql.metadata.QualifiedObjectName;
import org.apache.doris.sql.metadata.Type;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.planner.TypeProvider;
import org.apache.doris.sql.tree.*;

import javax.annotation.Nullable;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static org.apache.doris.sql.analyzer.SemanticExceptions.missingAttributeException;

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
        this.symbolTypes = requireNonNull(symbolTypes, "symbolTypes is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.isDescribe = isDescribe;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes()
    {
        return unmodifiableMap(expressionTypes);
    }

    public Type setExpressionType(Expression expression, Type type)
    {
        requireNonNull(expression, "expression cannot be null");
        requireNonNull(type, "type cannot be null");

        expressionTypes.put(NodeRef.of(expression), type);

        return type;
    }

    private Type getExpressionType(Expression expression)
    {
        requireNonNull(expression, "expression cannot be null");

        Type type = expressionTypes.get(NodeRef.of(expression));
        checkState(type != null, "Expression not yet analyzed: %s", expression);
        return type;
    }

    public Map<NodeRef<Expression>, Type> getExpressionCoercions()
    {
        return unmodifiableMap(expressionCoercions);
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions()
    {
        return unmodifiableSet(typeOnlyCoercions);
    }

    public Map<NodeRef<Expression>, FieldId> getColumnReferences()
    {
        return unmodifiableMap(columnReferences);
    }

    public Type analyze(Expression expression, Scope scope)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(Context.notInLambda(scope)));
    }

    private Type analyze(Expression expression, Scope baseScope, Context context)
    {
        Visitor visitor = new Visitor(baseScope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(context));
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons()
    {
        return unmodifiableSet(quantifiedComparisons);
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
            this.baseScope = requireNonNull(baseScope, "baseScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
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
            if (context.getContext().isInLambda()) {
                Optional<ResolvedField> resolvedField = context.getContext().getScope().tryResolveField(node, QualifiedName.of(node.getName()));
                if (resolvedField.isPresent() && context.getContext().getFieldToLambdaArgumentDeclaration().containsKey(FieldId.from(resolvedField.get()))) {
                    return setExpressionType(node, resolvedField.get().getType());
                }
            }
            Type type = symbolTypes.get(node);
            return setExpressionType(node, type);
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
            checkState(previous == null, "%s already known to refer to %s", node, previous);
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
                    throw missingAttributeException(node, qualifiedName);
                }
            }
            return null;
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, StackableAstVisitorContext<Context> context) {
            coerceType(context, node.getLeft(), BOOLEAN, "Left side of logical expression");
            coerceType(context, node.getRight(), BOOLEAN, "Right side of logical expression");

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, StackableAstVisitorContext<Context> context) {
            OperatorType operatorType = OperatorType.valueOf(node.getOperator().name());
            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitArithmeticUnary(ArithmeticUnaryExpression node, StackableAstVisitorContext<Context> context) {
            switch (node.getSign()) {
                case PLUS:
                    Type type = process(node.getValue(), context);

                    if (!type.equals(DOUBLE) && !type.equals(REAL) && !type.equals(BIGINT) && !type.equals(INTEGER) && !type.equals(SMALLINT) && !type.equals(TINYINT)) {
                        // TODO: figure out a type-agnostic way of dealing with this. Maybe add a special unary operator
                        // that types can chose to implement, or piggyback on the existence of the negation operator
                        throw new SemanticException(TYPE_MISMATCH, node, "Unary '+' operator cannot by applied to %s type", type);
                    }
                    return setExpressionType(node, type);
                case MINUS:
                    return getOperator(context, node, OperatorType.NEGATION, node.getValue());
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected Type visitArithmeticBinary(ArithmeticBinaryExpression node, StackableAstVisitorContext<Context> context) {
            return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, StackableAstVisitorContext<Context> context) {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return setExpressionType(node, INTEGER);
            }

            return setExpressionType(node, BIGINT);
        }


        @Override
        protected Type visitSubqueryExpression(SubqueryExpression node, StackableAstVisitorContext<Context> context) {
            if (context.getContext().isInLambda()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Lambda expression cannot contain subqueries");
            }
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node);
            Scope subqueryScope = Scope.builder()
                    .withParent(context.getContext().getScope())
                    .build();
            Scope queryScope = analyzer.analyze(node.getQuery(), subqueryScope);

            // Subquery should only produce one column
            if (queryScope.getRelationType().getVisibleFieldCount() != 1) {
                throw new SemanticException(MULTIPLE_FIELDS_FROM_SUBQUERY,
                        node,
                        "Multiple columns returned by subquery are not yet supported. Found %s",
                        queryScope.getRelationType().getVisibleFieldCount());
            }

            Node previousNode = context.getPreviousNode().orElse(null);
            if (previousNode instanceof InPredicate && ((InPredicate) previousNode).getValue() != node) {
                subqueryInPredicates.add(NodeRef.of((InPredicate) previousNode));
            } else if (previousNode instanceof QuantifiedComparisonExpression) {
                quantifiedComparisons.add(NodeRef.of((QuantifiedComparisonExpression) previousNode));
            } else {
                scalarSubqueries.add(NodeRef.of(node));
            }

            Type type = getOnlyElement(queryScope.getRelationType().getVisibleFields()).getType();
            return setExpressionType(node, type);
        }


        @Override
        protected Type visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, StackableAstVisitorContext<Context> context) {
            Expression value = node.getValue();
            process(value, context);

            Expression subquery = node.getSubquery();
            process(subquery, context);

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

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        public Type visitFieldReference(FieldReference node, StackableAstVisitorContext<Context> context) {
            Field field = baseScope.getRelationType().getFieldByIndex(node.getFieldIndex());
            return handleResolvedField(node, new FieldId(baseScope.getRelationId(), node.getFieldIndex()), field, context);
        }

        private Type getOperator(StackableAstVisitorContext<Context> context, Expression node, OperatorType operatorType, Expression... arguments)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : arguments) {
                argumentTypes.add(process(expression, context));
            }

            FunctionMetadata operatorMetadata;
            try {
                operatorMetadata = functionManager.getFunctionMetadata(functionManager.resolveOperator(operatorType, fromTypes(argumentTypes.build())));
            }
            catch (OperatorNotFoundException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "%s", e.getMessage());
            }
            catch (PrestoException e) {
                if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode().getCode()) {
                    throw new SemanticException(SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node, e.getMessage());
                }
                throw e;
            }

            for (int i = 0; i < arguments.length; i++) {
                Expression expression = arguments[i];
                Type type = typeManager.getType(operatorMetadata.getArgumentTypes().get(i));
                coerceType(context, expression, type, format("Operator %s argument %d", operatorMetadata, i));
            }

            Type type = typeManager.getType(operatorMetadata.getReturnType());
            return setExpressionType(node, type);
        }

        private void coerceType(Expression expression, Type actualType, Type expectedType, String message)
        {
            if (!actualType.equals(expectedType)) {
                if (!typeManager.canCoerce(actualType, expectedType)) {
                    throw new SemanticException(TYPE_MISMATCH, expression, message + " must evaluate to a %s (actual: %s)", expectedType, actualType);
                }
                addOrReplaceExpressionCoercion(expression, actualType, expectedType);
            }
        }

        private void coerceType(StackableAstVisitorContext<Context> context, Expression expression, Type expectedType, String message)
        {
            Type actualType = process(expression, context);
            coerceType(expression, actualType, expectedType, message);
        }

        private Type coerceToSingleType(StackableAstVisitorContext<Context> context, Node node, String message, Expression first, Expression second)
        {
            Type firstType = UNKNOWN;
            if (first != null) {
                firstType = process(first, context);
            }
            Type secondType = UNKNOWN;
            if (second != null) {
                secondType = process(second, context);
            }

            // coerce types if possible
            Optional<Type> superTypeOptional = typeManager.getCommonSuperType(firstType, secondType);
            if (superTypeOptional.isPresent()
                    && typeManager.canCoerce(firstType, superTypeOptional.get())
                    && typeManager.canCoerce(secondType, superTypeOptional.get())) {
                Type superType = superTypeOptional.get();
                if (!firstType.equals(superType)) {
                    addOrReplaceExpressionCoercion(first, firstType, superType);
                }
                if (!secondType.equals(superType)) {
                    addOrReplaceExpressionCoercion(second, secondType, superType);
                }
                return superType;
            }

            throw new SemanticException(TYPE_MISMATCH, node, message, firstType, secondType);
        }

        private Type coerceToSingleType(StackableAstVisitorContext<Context> context, String message, List<Expression> expressions)
        {
            // determine super type
            Type superType = UNKNOWN;
            for (Expression expression : expressions) {
                Optional<Type> newSuperType = typeManager.getCommonSuperType(superType, process(expression, context));
                if (!newSuperType.isPresent()) {
                    throw new SemanticException(TYPE_MISMATCH, expression, message, superType);
                }
                superType = newSuperType.get();
            }

            // verify all expressions can be coerced to the superType
            for (Expression expression : expressions) {
                Type type = process(expression, context);
                if (!type.equals(superType)) {
                    if (!typeManager.canCoerce(type, superType)) {
                        throw new SemanticException(TYPE_MISMATCH, expression, message, superType);
                    }
                    addOrReplaceExpressionCoercion(expression, type, superType);
                }
            }

            return superType;
        }
    }

    private static class Context
    {
        private final Scope scope;

        // functionInputTypes and nameToLambdaDeclarationMap can be null or non-null independently. All 4 combinations are possible.

        // The list of types when expecting a lambda (i.e. processing lambda parameters of a function); null otherwise.
        // Empty list represents expecting a lambda with no arguments.
        private final List<Type> functionInputTypes;
        // The mapping from names to corresponding lambda argument declarations when inside a lambda; null otherwise.
        // Empty map means that the all lambda expressions surrounding the current node has no arguments.
        private final Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration;

        private Context(
                Scope scope,
                List<Type> functionInputTypes,
                Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration)
        {
            this.scope = requireNonNull(scope, "scope is null");
            this.functionInputTypes = functionInputTypes;
            this.fieldToLambdaArgumentDeclaration = fieldToLambdaArgumentDeclaration;
        }

        public static Context notInLambda(Scope scope)
        {
            return new Context(scope, null, null);
        }

        public static Context inLambda(Scope scope, Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration)
        {
            return new Context(scope, null, requireNonNull(fieldToLambdaArgumentDeclaration, "fieldToLambdaArgumentDeclaration is null"));
        }

        public Context expectingLambda(List<Type> functionInputTypes)
        {
            return new Context(scope, requireNonNull(functionInputTypes, "functionInputTypes is null"), this.fieldToLambdaArgumentDeclaration);
        }

        public Context notExpectingLambda()
        {
            return new Context(scope, null, this.fieldToLambdaArgumentDeclaration);
        }

        Scope getScope()
        {
            return scope;
        }

        public boolean isInLambda()
        {
            return fieldToLambdaArgumentDeclaration != null;
        }

        public boolean isExpectingLambda()
        {
            return functionInputTypes != null;
        }

        public Map<FieldId, LambdaArgumentDeclaration> getFieldToLambdaArgumentDeclaration()
        {
            checkState(isInLambda());
            return fieldToLambdaArgumentDeclaration;
        }

        public List<Type> getFunctionInputTypes()
        {
            checkState(isExpectingLambda());
            return functionInputTypes;
        }
    }

    public static FunctionHandle resolveFunction(Optional<TransactionId> transactionId, FunctionCall node, List<TypeSignatureProvider> argumentTypes, FunctionManager functionManager)
    {
        try {
            return functionManager.resolveFunction(transactionId, node.getName(), argumentTypes);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw new SemanticException(SemanticErrorCode.FUNCTION_NOT_FOUND, node, e.getMessage());
            }
            if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode().getCode()) {
                throw new SemanticException(SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node, e.getMessage());
            }
            throw e;
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
        return analyzeExpressions(session, metadata, sqlParser, types, expressions, parameters, warningCollector, isDescribe).getExpressionTypes();
    }

    public static ExpressionAnalysis analyzeExpressions(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            TypeProvider types,
            Iterable<Expression> expressions,
            List<Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        // expressions at this point can not have sub queries so deny all access checks
        // in the future, we will need a full access controller here to verify access to functions
        Analysis analysis = new Analysis(null, parameters, isDescribe);
        ExpressionAnalyzer analyzer = create(analysis, session, metadata, sqlParser, new DenyAllAccessControl(), types, warningCollector);
        for (Expression expression : expressions) {
            analyzer.analyze(expression, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType()).build());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getLambdaArgumentReferences(),
                analyzer.getWindowFunctions());
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
        Map<NodeRef<FunctionCall>, FunctionHandle> resolvedFunctions = analyzer.getResolvedFunctions();

        analysis.addTypes(expressionTypes);
        analysis.addCoercions(expressionCoercions, typeOnlyCoercions);
        analysis.addFunctionHandles(resolvedFunctions);
        analysis.addColumnReferences(analyzer.getColumnReferences());
        analysis.addLambdaArgumentReferences(analyzer.getLambdaArgumentReferences());
        analysis.addTableColumnReferences(accessControl, session.getIdentity(), analyzer.getTableColumnReferences());

        return new ExpressionAnalysis(
                expressionTypes,
                expressionCoercions,
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getLambdaArgumentReferences(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeSqlFunctionExpression(
            Metadata metadata,
            SqlFunctionProperties sqlFunctionProperties,
            Expression expression,
            Map<String, Type> argumentTypes)
    {
        ExpressionAnalyzer analyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionManager(),
                metadata.getTypeManager(),
                Optional.empty(),
                sqlFunctionProperties,
                TypeProvider.copyOf(argumentTypes),
                emptyList(),
                node -> new SemanticException(NOT_SUPPORTED, node, "SQL function does not support subquery"),
                WarningCollector.NOOP,
                false);

        analyzer.analyze(
                expression,
                Scope.builder()
                        .withRelationType(
                                RelationId.anonymous(),
                                new RelationType(argumentTypes.entrySet().stream()
                                        .map(entry -> Field.newUnqualified(entry.getKey(), entry.getValue()))
                                        .collect(toImmutableList()))).build());
        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getLambdaArgumentReferences(),
                analyzer.getWindowFunctions());
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
                metadata.getFunctionManager(),
                metadata.getTypeManager(),
                node -> new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector),
                session.getTransactionId(),
                session.getSqlFunctionProperties(),
                types,
                analysis.getParameters(),
                warningCollector,
                analysis.isDescribe());
    }

    public static ExpressionAnalyzer createConstantAnalyzer(Metadata metadata, Session session, List<Expression> parameters, WarningCollector warningCollector)
    {
        return createWithoutSubqueries(
                metadata.getFunctionManager(),
                metadata.getTypeManager(),
                session,
                parameters,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                warningCollector,
                false);
    }

    public static ExpressionAnalyzer createConstantAnalyzer(Metadata metadata, Session session, List<Expression> parameters, WarningCollector warningCollector, boolean isDescribe)
    {
        return createWithoutSubqueries(
                metadata.getFunctionManager(),
                metadata.getTypeManager(),
                session,
                parameters,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            FunctionManager functionManager,
            TypeManager typeManager,
            Session session,
            List<Expression> parameters,
            SemanticErrorCode errorCode,
            String message,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return createWithoutSubqueries(
                functionManager,
                typeManager,
                session,
                TypeProvider.empty(),
                parameters,
                node -> new SemanticException(errorCode, node, message),
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            FunctionManager functionManager,
            TypeManager typeManager,
            Session session,
            TypeProvider symbolTypes,
            List<Expression> parameters,
            Function<? super Node, ? extends RuntimeException> statementAnalyzerRejection,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return createWithoutSubqueries(
                functionManager,
                typeManager,
                session.getTransactionId(),
                session.getSqlFunctionProperties(),
                symbolTypes,
                parameters,
                statementAnalyzerRejection,
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            FunctionManager functionManager,
            TypeManager typeManager,
            Optional<TransactionId> transactionId,
            SqlFunctionProperties sqlFunctionProperties,
            TypeProvider symbolTypes,
            List<Expression> parameters,
            Function<? super Node, ? extends RuntimeException> statementAnalyzerRejection,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return new ExpressionAnalyzer(
                functionManager,
                typeManager,
                node -> {
                    throw statementAnalyzerRejection.apply(node);
                },
                transactionId,
                sqlFunctionProperties,
                symbolTypes,
                parameters,
                warningCollector,
                isDescribe);
    }
}
