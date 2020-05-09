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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.apache.doris.sql.metadata.*;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.tree.AliasedRelation;
import org.apache.doris.sql.tree.AllColumns;
import org.apache.doris.sql.tree.AstUtils;
import org.apache.doris.sql.tree.DefaultTraversalVisitor;
import org.apache.doris.sql.tree.DereferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.FieldReference;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.GroupingElement;
import org.apache.doris.sql.tree.Identifier;
import org.apache.doris.sql.tree.Join;
import org.apache.doris.sql.tree.JoinCriteria;
import org.apache.doris.sql.tree.JoinOn;
import org.apache.doris.sql.tree.JoinUsing;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.NaturalJoin;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.OrderBy;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.tree.Query;
import org.apache.doris.sql.tree.QuerySpecification;
import org.apache.doris.sql.tree.SampledRelation;
import org.apache.doris.sql.tree.Select;
import org.apache.doris.sql.tree.SelectItem;
import org.apache.doris.sql.tree.SingleColumn;
import org.apache.doris.sql.tree.SortItem;
import org.apache.doris.sql.tree.Statement;
import org.apache.doris.sql.tree.Table;
import org.apache.doris.sql.tree.With;
import org.apache.doris.sql.tree.WithQuery;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.SemanticErrorCode.*;

public class StatementAnalyzer
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final Session session;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final WarningCollector warningCollector;

    public StatementAnalyzer(
            Analysis analysis,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Session session,
            WarningCollector warningCollector)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.session = requireNonNull(session, "session is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public Scope analyze(Node node, Scope outerQueryScope)
    {
        return analyze(node, Optional.of(outerQueryScope));
    }

    public Scope analyze(Node node, Optional<Scope> outerQueryScope)
    {
        return new Visitor(outerQueryScope, warningCollector).process(node, Optional.empty());
    }

    /**
     * Visitor context represents local query scope (if exists). The invariant is
     * that the local query scopes hierarchy should always have outer query scope
     * (if provided) as ancestor.
     */
    private class Visitor
            extends DefaultTraversalVisitor<Scope, Optional<Scope>>
    {
        private final Optional<Scope> outerQueryScope;
        private final WarningCollector warningCollector;

        private Visitor(Optional<Scope> outerQueryScope, WarningCollector warningCollector)
        {
            this.outerQueryScope = requireNonNull(outerQueryScope, "outerQueryScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        public Scope process(Node node, Optional<Scope> scope)
        {
            Scope returnScope = super.process(node, scope);
            checkState(returnScope.getOuterQueryParent().equals(outerQueryScope), "result scope should have outer query scope equal with parameter outer query scope");
            if (scope.isPresent()) {
                checkState(hasScopeAsLocalParent(returnScope, scope.get()), "return scope should have context scope as one of ancestors");
            }
            return returnScope;
        }

        private Scope process(Node node, Scope scope)
        {
            return process(node, Optional.of(scope));
        }

        @Override
        protected Scope visitQuery(Query node, Optional<Scope> scope)
        {
            Scope withScope = analyzeWith(node, scope);
            Scope queryBodyScope = process(node.getQueryBody(), withScope);
            if (node.getOrderBy().isPresent()) {
                //analyzeOrderBy(node, queryBodyScope);
            }
            else {
                analysis.setOrderByExpressions(node, emptyList());
            }

            // Input fields == Output fields
            analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

            Scope queryScope = Scope.builder()
                    .withParent(withScope)
                    .withRelationType(RelationId.of(node), queryBodyScope.getRelationType())
                    .build();

            analysis.setScope(node, queryScope);
            return queryScope;
        }

        @Override
        protected Scope visitTable(Table table, Optional<Scope> scope)
        {
            if (!table.getName().getPrefix().isPresent()) {
                // is this a reference to a WITH query?
                String name = table.getName().getSuffix();

                Optional<WithQuery> withQuery = createScope(scope).getNamedQuery(name);
                if (withQuery.isPresent()) {
                    Query query = withQuery.get().getQuery();
                    analysis.registerNamedQuery(table, query);

                    // re-alias the fields with the name assigned to the query in the WITH declaration
                    RelationType queryDescriptor = analysis.getOutputDescriptor(query);

                    List<Field> fields;
                    Optional<List<Identifier>> columnNames = withQuery.get().getColumnNames();
                    if (columnNames.isPresent()) {
                        // if columns are explicitly aliased -> WITH cte(alias1, alias2 ...)
                        ImmutableList.Builder<Field> fieldBuilder = ImmutableList.builder();

                        int field = 0;
                        for (Identifier columnName : columnNames.get()) {
                            Field inputField = queryDescriptor.getFieldByIndex(field);
                            fieldBuilder.add(Field.newQualified(
                                    QualifiedName.of(name),
                                    Optional.of(columnName.getValue()),
                                    inputField.getType(),
                                    false,
                                    inputField.getOriginTable(),
                                    inputField.getOriginColumnName(),
                                    inputField.isAliased()));

                            field++;
                        }

                        fields = fieldBuilder.build();
                    }
                    else {
                        fields = queryDescriptor.getAllFields().stream()
                                .map(field -> Field.newQualified(
                                        QualifiedName.of(name),
                                        field.getName(),
                                        field.getType(),
                                        field.isHidden(),
                                        field.getOriginTable(),
                                        field.getOriginColumnName(),
                                        field.isAliased()))
                                .collect(toImmutableList());
                    }

                    return createAndAssignScope(table, scope, fields);
                }
            }

            QualifiedObjectName name = createQualifiedObjectName(session, table, table.getName());
            if (name.getObjectName().isEmpty()) {
                throw new SemanticException(MISSING_TABLE, table, "Table name is empty");
            }
            if (name.getSchemaName().isEmpty()) {
                throw new SemanticException(MISSING_SCHEMA, table, "Schema name is empty");
            }
            analysis.addEmptyColumnReferencesForTable(accessControl, session.getIdentity(), name);

            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);
            if (!tableHandle.isPresent()) {
                if (!metadata.getCatalogHandle(session, name.getCatalogName()).isPresent()) {
                    throw new SemanticException(MISSING_CATALOG, table, "Catalog %s does not exist", name.getCatalogName());
                }
                if (!metadata.schemaExists(session, new CatalogSchemaName(name.getCatalogName(), name.getSchemaName()))) {
                    throw new SemanticException(MISSING_SCHEMA, table, "Schema %s does not exist", name.getSchemaName());
                }
                throw new SemanticException(MISSING_TABLE, table, "Table %s does not exist", name);
            }
            TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

            // TODO: discover columns lazily based on where they are needed (to support connectors that can't enumerate all tables)
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                Field field = Field.newQualified(
                        table.getName(),
                        Optional.of(column.getName()),
                        column.getType(),
                        column.isHidden(),
                        Optional.of(name),
                        Optional.of(column.getName()),
                        false);
                fields.add(field);
                ColumnHandle columnHandle = columnHandles.get(column.getName());
                checkArgument(columnHandle != null, "Unknown field %s", field);
                analysis.setColumn(field, columnHandle);
            }

            analysis.registerTable(table, tableHandle.get());

            return createAndAssignScope(table, scope, fields.build());
        }

        @Override
        protected Scope visitAliasedRelation(AliasedRelation relation, Optional<Scope> scope)
        {
            Scope relationScope = process(relation.getRelation(), scope);

            // todo this check should be inside of TupleDescriptor.withAlias, but the exception needs the node object
            RelationType relationType = relationScope.getRelationType();
            if (relation.getColumnNames() != null) {
                int totalColumns = relationType.getVisibleFieldCount();
                if (totalColumns != relation.getColumnNames().size()) {
                    throw new SemanticException(MISMATCHED_COLUMN_ALIASES, relation, "Column alias list has %s entries but '%s' has %s columns available", relation.getColumnNames().size(), relation.getAlias(), totalColumns);
                }
            }

            List<String> aliases = null;
            if (relation.getColumnNames() != null) {
                aliases = relation.getColumnNames().stream()
                        .map(Identifier::getValue)
                        .collect(Collectors.toList());
            }

            RelationType descriptor = relationType.withAlias(relation.getAlias().getValue(), aliases);

            return createAndAssignScope(relation, scope, descriptor);
        }

        @Override
        protected Scope visitSampledRelation(SampledRelation relation, Optional<Scope> scope)
        {
            return null;
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            // TODO: extract candidate names from SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions
            // to pass down to analyzeFrom

            Scope sourceScope = analyzeFrom(node, scope);


            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            //analyzeHaving(node, sourceScope);

            Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

            List<Expression> orderByExpressions = emptyList();
            Optional<Scope> orderByScope = Optional.empty();
            if (node.getOrderBy().isPresent()) {
                orderByScope = Optional.of(computeAndAssignOrderByScope(node.getOrderBy().get(), sourceScope, outputScope));
            }
            else {
                analysis.setOrderByExpressions(node, emptyList());
            }

            List<Expression> sourceExpressions = new ArrayList<>(outputExpressions);
            node.getHaving().ifPresent(sourceExpressions::add);

            return outputScope;
        }

        private Multimap<QualifiedName, Expression> extractNamedOutputExpressions(Select node)
        {
            // Compute aliased output terms so we can resolve order by expressions against them first
            ImmutableMultimap.Builder<QualifiedName, Expression> assignments = ImmutableMultimap.builder();
            for (SelectItem item : node.getSelectItems()) {
                if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;
                    Optional<Identifier> alias = column.getAlias();
                    if (alias.isPresent()) {
                        assignments.put(QualifiedName.of(alias.get().getValue()), column.getExpression()); // TODO: need to know if alias was quoted
                    }
                    else if (column.getExpression() instanceof Identifier) {
                        assignments.put(QualifiedName.of(((Identifier) column.getExpression()).getValue()), column.getExpression());
                    }
                }
            }

            return assignments.build();
        }

        private Scope computeAndAssignOutputScope(QuerySpecification node, Optional<Scope> scope, Scope sourceScope)
        {
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    // expand * and T.*
                    Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                    for (Field field : sourceScope.getRelationType().resolveFieldsWithPrefix(starPrefix)) {
                        outputFields.add(Field.newUnqualified(field.getName(), field.getType(), field.getOriginTable(), field.getOriginColumnName(), false));
                    }
                }
                else if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;

                    Expression expression = column.getExpression();
                    Optional<Identifier> field = column.getAlias();

                    Optional<QualifiedObjectName> originTable = Optional.empty();
                    Optional<String> originColumn = Optional.empty();
                    QualifiedName name = null;

                    if (expression instanceof Identifier) {
                        name = QualifiedName.of(((Identifier) expression).getValue());
                    }
                    else if (expression instanceof DereferenceExpression) {
                        name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
                    }

                    if (name != null) {
                        List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
                        if (!matchingFields.isEmpty()) {
                            originTable = matchingFields.get(0).getOriginTable();
                            originColumn = matchingFields.get(0).getOriginColumnName();
                        }
                    }

                    if (!field.isPresent()) {
                        if (name != null) {
                            field = Optional.of(new Identifier(getLast(name.getOriginalParts())));
                        }
                    }

                    outputFields.add(Field.newUnqualified(field.map(Identifier::getValue), analysis.getType(expression), originTable, originColumn, column.getAlias().isPresent())); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            return createAndAssignScope(node, scope, outputFields.build());
        }

        private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope)
        {
            // ORDER BY should "see" both output and FROM fields during initial analysis and non-aggregation query planning
            Scope orderByScope = Scope.builder()
                    .withParent(sourceScope)
                    .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
                    .build();
            analysis.setScope(node, orderByScope);
            return orderByScope;
        }

        private List<Expression> analyzeSelect(QuerySpecification node, Scope scope)
        {
            ImmutableList.Builder<Expression> outputExpressionBuilder = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    // expand * and T.*
                    Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                    RelationType relationType = scope.getRelationType();
                    List<Field> fields = relationType.resolveFieldsWithPrefix(starPrefix);
                    if (fields.isEmpty()) {
                        if (starPrefix.isPresent()) {
                            throw new SemanticException(MISSING_TABLE, item, "Table '%s' not found", starPrefix.get());
                        }
                        if (!node.getFrom().isPresent()) {
                            throw new SemanticException(WILDCARD_WITHOUT_FROM, item, "SELECT * not allowed in queries without FROM clause");
                        }
                        throw new SemanticException(COLUMN_NAME_NOT_SPECIFIED, item, "SELECT * not allowed from relation that has no columns");
                    }

                    for (Field field : fields) {
                        int fieldIndex = relationType.indexOf(field);
                        FieldReference expression = new FieldReference(fieldIndex);
                        outputExpressionBuilder.add(expression);
                        ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);

                        Type type = expressionAnalysis.getType(expression);
                        if (node.getSelect().isDistinct() && !type.isComparable()) {
                            throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s)", type);
                        }
                    }
                }
                else if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;
                    ExpressionAnalysis expressionAnalysis = analyzeExpression(column.getExpression(), scope);
                    outputExpressionBuilder.add(column.getExpression());

                    Type type = expressionAnalysis.getType(column.getExpression());
                    if (node.getSelect().isDistinct() && !type.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node.getSelect(), "DISTINCT can only be applied to comparable types (actual: %s): %s", type, column.getExpression());
                    }
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            ImmutableList<Expression> result = outputExpressionBuilder.build();
            analysis.setOutputExpressions(node, result);

            return result;
        }

        private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }

            return createScope(scope);
        }

        private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
        {
            return ExpressionAnalyzer.analyzeExpression(
                    session,
                    metadata,
                    accessControl,
                    sqlParser,
                    scope,
                    analysis,
                    expression,
                    WarningCollector.NOOP);
        }

        private List<Expression> descriptorToFields(Scope scope)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (int fieldIndex = 0; fieldIndex < scope.getRelationType().getAllFieldCount(); fieldIndex++) {
                FieldReference expression = new FieldReference(fieldIndex);
                builder.add(expression);
                analyzeExpression(expression, scope);
            }
            return builder.build();
        }

        private Scope analyzeWith(Query node, Optional<Scope> scope)
        {
            // analyze WITH clause
            if (!node.getWith().isPresent()) {
                return createScope(scope);
            }
            With with = node.getWith().get();
            if (with.isRecursive()) {
                throw new SemanticException(NOT_SUPPORTED, with, "Recursive WITH queries are not supported");
            }

            Scope.Builder withScopeBuilder = scopeBuilder(scope);
            for (WithQuery withQuery : with.getQueries()) {
                Query query = withQuery.getQuery();
                process(query, withScopeBuilder.build());

                String name = withQuery.getName().getValue().toLowerCase(ENGLISH);
                if (withScopeBuilder.containsNamedQuery(name)) {
                    throw new SemanticException(DUPLICATE_RELATION, withQuery, "WITH query name '%s' specified more than once", name);
                }

                // check if all or none of the columns are explicitly alias
                if (withQuery.getColumnNames().isPresent()) {
                    List<Identifier> columnNames = withQuery.getColumnNames().get();
                    RelationType queryDescriptor = analysis.getOutputDescriptor(query);
                    if (columnNames.size() != queryDescriptor.getVisibleFieldCount()) {
                        throw new SemanticException(MISMATCHED_COLUMN_ALIASES, withQuery, "WITH column alias list has %s entries but WITH query(%s) has %s columns", columnNames.size(), name, queryDescriptor.getVisibleFieldCount());
                    }
                }

                withScopeBuilder.withNamedQuery(name, withQuery);
            }

            Scope withScope = withScopeBuilder.build();
            analysis.setScope(with, withScope);
            return withScope;
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope)
        {
            return createAndAssignScope(node, parentScope, emptyList());
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, RelationType relationType)
        {
            Scope scope = scopeBuilder(parentScope)
                    .withRelationType(RelationId.of(node), relationType)
                    .build();

            analysis.setScope(node, scope);
            return scope;
        }

        private Scope createScope(Optional<Scope> parentScope)
        {
            return scopeBuilder(parentScope).build();
        }

        private Scope.Builder scopeBuilder(Optional<Scope> parentScope)
        {
            Scope.Builder scopeBuilder = Scope.builder();

            if (parentScope.isPresent()) {
                // parent scope represents local query scope hierarchy. Local query scope
                // hierarchy should have outer query scope as ancestor already.
                scopeBuilder.withParent(parentScope.get());
            }
            else if (outerQueryScope.isPresent()) {
                scopeBuilder.withOuterQueryParent(outerQueryScope.get());
            }

            return scopeBuilder;
        }
    }

    private static boolean hasScopeAsLocalParent(Scope root, Scope parent)
    {
        Scope scope = root;
        while (scope.getLocalParent().isPresent()) {
            scope = scope.getLocalParent().get();
            if (scope.equals(parent)) {
                return true;
            }
        }

        return false;
    }
}
