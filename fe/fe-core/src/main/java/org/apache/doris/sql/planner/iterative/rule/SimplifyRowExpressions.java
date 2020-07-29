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

import org.apache.doris.sql.expressions.LogicalRowExpressions;
import org.apache.doris.sql.expressions.RowExpressionRewriter;
import org.apache.doris.sql.expressions.RowExpressionTreeRewriter;
import org.apache.doris.sql.metadata.ConnectorSession;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.planner.iterative.Rule;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relational.FunctionResolution;
import org.apache.doris.sql.relational.RowExpressionDeterminismEvaluator;
import org.apache.doris.sql.relational.RowExpressionOptimizer;
import org.apache.doris.sql.type.BooleanType;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.AND;
import static org.apache.doris.sql.relation.SpecialFormExpression.Form.OR;

public class SimplifyRowExpressions
        extends RowExpressionRewriteRuleSet
{
    public SimplifyRowExpressions(Metadata metadata)
    {
        super(new Rewriter(metadata));
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final RowExpressionOptimizer optimizer;
        private final LogicalExpressionRewriter logicalExpressionRewriter;

        public Rewriter(Metadata metadata)
        {
            requireNonNull(metadata, "metadata is null");
            this.optimizer = new RowExpressionOptimizer(metadata);
            this.logicalExpressionRewriter = new LogicalExpressionRewriter(metadata.getFunctionManager());
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return rewrite(expression, context.getSession().toConnectorSession());
        }

        private RowExpression rewrite(RowExpression expression, ConnectorSession session)
        {
            RowExpression optimizedRowExpression = optimizer.optimize(expression, SERIALIZABLE, session);
            if (optimizedRowExpression instanceof ConstantExpression || !BooleanType.BOOLEAN.equals(optimizedRowExpression.getType())) {
                return optimizedRowExpression;
            }
            return RowExpressionTreeRewriter.rewriteWith(logicalExpressionRewriter, optimizedRowExpression, true);
        }
    }

    public static RowExpression rewrite(RowExpression expression, Metadata metadata, ConnectorSession session)
    {
        return new Rewriter(metadata).rewrite(expression, session);
    }

    private static class LogicalExpressionRewriter
            extends RowExpressionRewriter<Boolean>
    {
        private final FunctionResolution functionResolution;
        private final LogicalRowExpressions logicalRowExpressions;

        public LogicalExpressionRewriter(FunctionManager functionManager)
        {
            requireNonNull(functionManager, "functionManager is null");
            this.functionResolution = new FunctionResolution(functionManager);
            this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(functionManager), new FunctionResolution(functionManager), functionManager);
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Boolean isRoot, RowExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (functionResolution.isNotFunction(node.getFunctionHandle())) {
                checkState(BooleanType.BOOLEAN.equals(node.getType()), "NOT must be boolean function");
                return rewriteBooleanExpression(node, isRoot);
            }
            if (isRoot) {
                return treeRewriter.rewrite(node, false);
            }
            return null;
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Boolean isRoot, RowExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (isConjunctiveDisjunctive(node.getForm())) {
                checkState(BooleanType.BOOLEAN.equals(node.getType()), "AND/OR must be boolean function");
                return rewriteBooleanExpression(node, isRoot);
            }
            if (isRoot) {
                return treeRewriter.rewrite(node, false);
            }
            return null;
        }

        private boolean isConjunctiveDisjunctive(SpecialFormExpression.Form form)
        {
            return form == AND || form == OR;
        }

        private RowExpression rewriteBooleanExpression(RowExpression expression, boolean isRoot)
        {
            if (isRoot) {
                return logicalRowExpressions.convertToConjunctiveNormalForm(expression);
            }
            return logicalRowExpressions.minimalNormalForm(expression);
        }
    }
}
