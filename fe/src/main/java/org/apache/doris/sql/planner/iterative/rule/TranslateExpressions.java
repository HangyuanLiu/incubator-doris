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
package org.apache.doris.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import org.apache.doris.sql.TypeProvider;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relational.OriginalExpressionUtils;
import org.apache.doris.sql.relational.SqlToRowExpressionTranslator;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.type.Type;

import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static org.apache.doris.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.castToExpression;
import static org.apache.doris.sql.relational.OriginalExpressionUtils.isExpression;

public class TranslateExpressions
        extends RowExpressionRewriteRuleSet
{
    public TranslateExpressions(Metadata metadata, SqlParser sqlParser)
    {
        super(createRewriter(metadata, sqlParser));
    }

    private static PlanRowExpressionRewriter createRewriter(Metadata metadata, SqlParser sqlParser)
    {
        return new PlanRowExpressionRewriter()
        {
            @Override
            public RowExpression rewrite(RowExpression expression, Rule.Context context)
            {
                // special treatment of the CallExpression in Aggregation
                if (expression instanceof CallExpression && ((CallExpression) expression).getArguments().stream().anyMatch(OriginalExpressionUtils::isExpression)) {
                    return removeOriginalExpressionArguments((CallExpression) expression, context.getSession(), context.getVariableAllocator());
                }
                return removeOriginalExpression(expression, context);
            }

            private RowExpression removeOriginalExpressionArguments(CallExpression callExpression, Session session, VariableAllocator variableAllocator)
            {
                Map<NodeRef<Expression>, Type> types = analyzeCallExpressionTypes(callExpression, session, variableAllocator.getTypes());
                return new CallExpression(
                        callExpression.getDisplayName(),
                        callExpression.getFunctionHandle(),
                        callExpression.getType(),
                        callExpression.getArguments().stream()
                                .map(expression -> removeOriginalExpression(expression, session, types))
                                .collect(toImmutableList()));
            }

            private Map<NodeRef<Expression>, Type> analyzeCallExpressionTypes(CallExpression callExpression, Session session, TypeProvider typeProvider)
            {
                ImmutableMap.Builder<NodeRef<Expression>, Type> builder = ImmutableMap.<NodeRef<Expression>, Type>builder();
                for (RowExpression argument : callExpression.getArguments()) {
                    if (!isExpression(argument)) {
                        continue;
                    }
                    builder.putAll(analyze(castToExpression(argument), session, typeProvider));
                }
                return builder.build();
            }

            private Map<NodeRef<Expression>, Type> analyze(Expression expression, Session session, TypeProvider typeProvider)
            {
                return getExpressionTypes(
                        session,
                        metadata,
                        sqlParser,
                        typeProvider,
                        expression,
                        emptyList(),
                        null);
            }

            private RowExpression toRowExpression(Expression expression, Session session, Map<NodeRef<Expression>, Type> types)
            {
                return SqlToRowExpressionTranslator.translate(expression, types, ImmutableMap.of(), metadata.getFunctionManager(), metadata.getTypeManager());
            }

            private RowExpression removeOriginalExpression(RowExpression expression, Rule.Context context)
            {
                if (isExpression(expression)) {
                    return toRowExpression(
                            castToExpression(expression),
                            context.getSession(),
                            analyze(castToExpression(expression), context.getSession(), context.getVariableAllocator().getTypes()));
                }
                return expression;
            }

            private RowExpression removeOriginalExpression(RowExpression rowExpression, Session session, Map<NodeRef<Expression>, Type> types)
            {
                if (isExpression(rowExpression)) {
                    Expression expression = castToExpression(rowExpression);
                    return toRowExpression(expression, session, types);
                }
                return rowExpression;
            }
        };
    }
}
