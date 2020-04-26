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
import com.google.common.collect.Iterables;

import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Statement;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Analyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;
    private final List<Expression> parameters;
    private final WarningCollector warningCollector;

    public Analyzer(Session session,
                    Metadata metadata,
                    SqlParser sqlParser,
                    AccessControl accessControl,
                    Optional<QueryExplainer> queryExplainer,
                    List<Expression> parameters,
                    WarningCollector warningCollector)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.queryExplainer = requireNonNull(queryExplainer, "query explainer is null");
        this.parameters = parameters;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public Analysis analyze(Statement statement)
    {
        return analyze(statement, false);
    }

    public Analysis analyze(Statement statement, boolean isDescribe)
    {
        //Statement rewrittenStatement = StatementRewrite.rewrite(session, metadata, sqlParser, queryExplainer, statement, parameters, accessControl, warningCollector);
        Analysis analysis = new Analysis(statement, parameters, isDescribe);
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector);
        analyzer.analyze(rewrittenStatement, Optional.empty());

        // check column access permissions for each table
        analysis.getTableColumnReferences().forEach((accessControlInfo, tableColumnReferences) ->
                tableColumnReferences.forEach((tableName, columns) ->
                        accessControlInfo.getAccessControl().checkCanSelectFromColumns(
                                session.getRequiredTransactionId(),
                                accessControlInfo.getIdentity(),
                                tableName,
                                columns)));
        return analysis;
    }

    static void verifyNoAggregateWindowOrGroupingFunctions(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, FunctionManager functionManager, Expression predicate, String clause)
    {
        List<FunctionCall> aggregates = extractAggregateFunctions(functionHandles, ImmutableList.of(predicate), functionManager);

        List<FunctionCall> windowExpressions = extractWindowFunctions(ImmutableList.of(predicate));

        List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

        List<Expression> found = ImmutableList.copyOf(Iterables.concat(
                aggregates,
                windowExpressions,
                groupingOperations));

        if (!found.isEmpty()) {
            throw new SemanticException(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, predicate, "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found);
        }
    }
}
