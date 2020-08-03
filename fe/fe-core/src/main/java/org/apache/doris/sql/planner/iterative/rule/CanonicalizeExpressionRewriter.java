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

import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.tree.ArithmeticBinaryExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.ExpressionRewriter;
import org.apache.doris.sql.tree.ExpressionTreeRewriter;
import org.apache.doris.sql.tree.Extract;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.IntervalLiteral;
import org.apache.doris.sql.tree.IsNotNullPredicate;
import org.apache.doris.sql.tree.IsNullPredicate;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.NotExpression;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.tree.SearchedCaseExpression;
import org.apache.doris.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.type.IntervalType;
import org.apache.doris.sql.type.TypeSignature;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CanonicalizeExpressionRewriter
{
    private CanonicalizeExpressionRewriter() {}

    public static Expression canonicalizeExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(null), expression);
    }

    public static Expression canonicalizeExpression(Expression expression, FunctionManager functionManager)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(functionManager), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void> {
        FunctionManager functionManager;

        Visitor(FunctionManager functionManager) {
            this.functionManager = functionManager;
        }

        @Override
        public Expression rewriteIsNotNullPredicate(IsNotNullPredicate node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Expression value = treeRewriter.rewrite(node.getValue(), context);
            return new NotExpression(new IsNullPredicate(value));
        }

        @Override
        public Expression rewriteExtract(Extract node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            Expression value = treeRewriter.rewrite(node.getExpression(), context);

            switch (node.getField()) {
                case YEAR:
                    return new FunctionCall(QualifiedName.of("year"), ImmutableList.of(value));
                case QUARTER:
                    return new FunctionCall(QualifiedName.of("quarter"), ImmutableList.of(value));
                case MONTH:
                    return new FunctionCall(QualifiedName.of("month"), ImmutableList.of(value));
                case WEEK:
                    return new FunctionCall(QualifiedName.of("week"), ImmutableList.of(value));
                case DAY:
                case DAY_OF_MONTH:
                    return new FunctionCall(QualifiedName.of("day"), ImmutableList.of(value));
                case DAY_OF_WEEK:
                case DOW:
                    return new FunctionCall(QualifiedName.of("day_of_week"), ImmutableList.of(value));
                case DAY_OF_YEAR:
                case DOY:
                    return new FunctionCall(QualifiedName.of("day_of_year"), ImmutableList.of(value));
                case YEAR_OF_WEEK:
                case YOW:
                    return new FunctionCall(QualifiedName.of("year_of_week"), ImmutableList.of(value));
                case HOUR:
                    return new FunctionCall(QualifiedName.of("hour"), ImmutableList.of(value));
                case MINUTE:
                    return new FunctionCall(QualifiedName.of("minute"), ImmutableList.of(value));
                case SECOND:
                    return new FunctionCall(QualifiedName.of("second"), ImmutableList.of(value));
                case TIMEZONE_MINUTE:
                    return new FunctionCall(QualifiedName.of("timezone_minute"), ImmutableList.of(value));
                case TIMEZONE_HOUR:
                    return new FunctionCall(QualifiedName.of("timezone_hour"), ImmutableList.of(value));
            }

            throw new UnsupportedOperationException("not yet implemented: " + node.getField());
        }

        @Override
        public Expression rewriteArithmeticBinary(ArithmeticBinaryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
            if (node.getOperator().equals(ArithmeticBinaryExpression.Operator.ADD) || node.getOperator().equals(ArithmeticBinaryExpression.Operator.SUBTRACT)) {
                Expression expr;
                IntervalLiteral interval;
                if (node.getLeft() instanceof IntervalLiteral) {
                    expr = node.getRight();
                    interval = (IntervalLiteral) node.getLeft();
                } else if (node.getRight() instanceof IntervalLiteral) {
                    expr = node.getLeft();
                    interval = (IntervalLiteral) node.getRight();
                } else {
                    return node;
                }

                String funcOpName = "";
                String timeUnit = interval.getStartField().toString();
                if (node.getOperator().equals(ArithmeticBinaryExpression.Operator.ADD)) {
                    funcOpName = String.format("%sS_%s", timeUnit, "ADD");
                } else {
                    funcOpName = String.format("%sS_%s", timeUnit, "SUB");
                }
                LongLiteral interal = new LongLiteral(interval.getValue());

                Expression value = treeRewriter.rewrite(expr, context);
                return new FunctionCall(QualifiedName.of(funcOpName), ImmutableList.of(value, interal));
            }
            return node;
        }
    }
}
