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
import org.apache.doris.sql.metadata.*;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.TypeProvider;
import org.apache.doris.sql.tree.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

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


    private final Map<NodeRef<Expression>, Type> expressionCoercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();
    private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons = new LinkedHashSet<>();
    private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();

    private final WarningCollector warningCollector;

    private ExpressionAnalyzer(
            WarningCollector warningCollector)
    {
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
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(Scope.create()));
    }

    private Type analyze(Expression expression, Scope baseScope, Scope context)
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
            extends StackableAstVisitor<Type, Scope> {
        // Used to resolve FieldReferences (e.g. during local execution planning)
        private final Scope baseScope;
        private final WarningCollector warningCollector;

        public Visitor(Scope baseScope, WarningCollector warningCollector) {
            this.baseScope = requireNonNull(baseScope, "baseScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        public Type process(Node node, @Nullable StackableAstVisitorContext<Scope> context) {
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
        protected Type visitIdentifier(Identifier node, StackableAstVisitorContext<Scope> context) {
            ResolvedField resolvedField = context.getContext().resolveField(node, QualifiedName.of(node.getValue()));
            return handleResolvedField(node, resolvedField, context);
        }

        private Type handleResolvedField(Expression node, ResolvedField resolvedField, StackableAstVisitorContext<Scope> context) {
            return handleResolvedField(node, FieldId.from(resolvedField), resolvedField.getField(), context);
        }

        private Type handleResolvedField(Expression node, FieldId fieldId, Field field, StackableAstVisitorContext<Scope> context) {
            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                tableColumnReferences.put(field.getOriginTable().get(), field.getOriginColumnName().get());
            }

            FieldId previous = columnReferences.put(NodeRef.of(node), fieldId);
            checkState(previous == null, "%s already known to refer to %s", node, previous);
            return setExpressionType(node, field.getType());
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
        ExpressionAnalyzer analyzer = create(warningCollector);
        for (Expression expression : expressions) {
            analyzer.analyze(expression, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType()).build());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions());
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
        ExpressionAnalyzer analyzer = create(warningCollector);
        analyzer.analyze(expression, scope);

        Map<NodeRef<Expression>, Type> expressionTypes = analyzer.getExpressionTypes();
        Map<NodeRef<Expression>, Type> expressionCoercions = analyzer.getExpressionCoercions();
        Set<NodeRef<Expression>> typeOnlyCoercions = analyzer.getTypeOnlyCoercions();

        analysis.addTypes(expressionTypes);
        analysis.addCoercions(expressionCoercions, typeOnlyCoercions);
        analysis.addColumnReferences(analyzer.getColumnReferences());
        analysis.addTableColumnReferences(accessControl, session.getIdentity(), analyzer.getTableColumnReferences());

        return new ExpressionAnalysis(
                expressionTypes,
                expressionCoercions,
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions());
    }

    private static ExpressionAnalyzer create(
            WarningCollector warningCollector)
    {
        return new ExpressionAnalyzer(
                warningCollector
        );
    }
}
