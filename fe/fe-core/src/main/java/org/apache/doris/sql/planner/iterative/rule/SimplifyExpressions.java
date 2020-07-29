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



import com.google.common.collect.ImmutableSet;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.metadata.WarningCollector;
import org.apache.doris.sql.parser.SqlParser;
import org.apache.doris.sql.planner.ExpressionInterpreter;
import org.apache.doris.sql.planner.LiteralEncoder;
import org.apache.doris.sql.planner.VariableAllocator;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.Literal;
import org.apache.doris.sql.tree.NodeRef;
import org.apache.doris.sql.tree.SymbolReference;
import org.apache.doris.sql.type.Type;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static org.apache.doris.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static org.apache.doris.sql.planner.iterative.rule.PushDownNegationsExpressionRewriter.pushDownNegations;

public class SimplifyExpressions
        extends ExpressionRewriteRuleSet
{
    static Expression rewrite(Expression expression, Session session, VariableAllocator variableAllocator, Metadata metadata, LiteralEncoder literalEncoder, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");
        if (expression instanceof SymbolReference) {
            return expression;
        }
        expression = pushDownNegations(expression);
        expression = extractCommonPredicates(expression);
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, variableAllocator.getTypes(), expression, emptyList(), WarningCollector.NOOP);
        //FIXME
        //ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
        //return literalEncoder.toExpression(interpreter.optimize(NoOpVariableResolver.INSTANCE), expressionTypes.get(NodeRef.of(expression)));
        return expression;
    }

    public SimplifyExpressions(Metadata metadata, SqlParser sqlParser)
    {
        super(createRewrite(metadata, sqlParser));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite()); // ApplyNode and AggregationNode are not supported, because ExpressionInterpreter doesn't support them
    }

    private static ExpressionRewriter createRewrite(Metadata metadata, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");
        //LiteralEncoder literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        //FIXME
        LiteralEncoder literalEncoder = null;

        return (expression, context) -> rewrite(expression, context.getSession(), context.getVariableAllocator(), metadata, literalEncoder, sqlParser);
    }
}
