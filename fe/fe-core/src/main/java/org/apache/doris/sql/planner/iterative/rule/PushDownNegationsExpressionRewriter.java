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


import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.ExpressionRewriter;
import org.apache.doris.sql.tree.ExpressionTreeRewriter;
import org.apache.doris.sql.tree.LogicalBinaryExpression;
import org.apache.doris.sql.tree.NotExpression;

import java.util.List;
import java.util.stream.Collectors;


import static org.apache.doris.sql.ExpressionUtils.combinePredicates;
import static org.apache.doris.sql.ExpressionUtils.extractPredicates;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;

public class PushDownNegationsExpressionRewriter
{
    public static Expression pushDownNegations(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private PushDownNegationsExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteNotExpression(NotExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getValue() instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression child = (LogicalBinaryExpression) node.getValue();
                List<Expression> predicates = extractPredicates(child);
                List<Expression> negatedPredicates = predicates.stream().map(predicate -> treeRewriter.rewrite((Expression) new NotExpression(predicate), context)).collect(Collectors.toList());
                return combinePredicates(child.getOperator().flip(), negatedPredicates);
            }
            else if (node.getValue() instanceof ComparisonExpression && ((ComparisonExpression) node.getValue()).getOperator() != IS_DISTINCT_FROM) {
                ComparisonExpression child = (ComparisonExpression) node.getValue();
                return new ComparisonExpression(child.getOperator().negate(), treeRewriter.rewrite(child.getLeft(), context), treeRewriter.rewrite(child.getRight(), context));
            }
            else if (node.getValue() instanceof NotExpression) {
                NotExpression child = (NotExpression) node.getValue();
                return treeRewriter.rewrite(child.getValue(), context);
            }

            return new NotExpression(treeRewriter.rewrite(node.getValue(), context));
        }
    }
}
