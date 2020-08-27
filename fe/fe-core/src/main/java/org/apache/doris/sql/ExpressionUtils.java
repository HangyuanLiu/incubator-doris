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
package org.apache.doris.sql;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.doris.sql.planner.VariablesExtractor;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.tree.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Predicates.not;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.doris.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static org.apache.doris.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.apache.doris.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;

public final class ExpressionUtils
{
    private ExpressionUtils() {}

    public static List<Expression> extractConjuncts(Expression expression)
    {
        return extractPredicates(LogicalBinaryExpression.Operator.AND, expression);
    }

    public static List<Expression> extractDisjuncts(Expression expression)
    {
        return extractPredicates(LogicalBinaryExpression.Operator.OR, expression);
    }

    public static List<Expression> extractPredicates(LogicalBinaryExpression expression)
    {
        return extractPredicates(expression.getOperator(), expression);
    }

    public static List<Expression> extractPredicates(LogicalBinaryExpression.Operator operator, Expression expression)
    {
        if (expression instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) expression).getOperator() == operator) {
            LogicalBinaryExpression logicalBinaryExpression = (LogicalBinaryExpression) expression;
            return ImmutableList.<Expression>builder()
                    .addAll(extractPredicates(operator, logicalBinaryExpression.getLeft()))
                    .addAll(extractPredicates(operator, logicalBinaryExpression.getRight()))
                    .build();
        }

        return ImmutableList.of(expression);
    }

    public static Expression and(Expression... expressions)
    {
        return and(Arrays.asList(expressions));
    }

    public static Expression and(Collection<Expression> expressions)
    {
        return binaryExpression(LogicalBinaryExpression.Operator.AND, expressions);
    }

    public static Expression or(Expression... expressions)
    {
        return or(Arrays.asList(expressions));
    }

    public static Expression or(Collection<Expression> expressions)
    {
        return binaryExpression(LogicalBinaryExpression.Operator.OR, expressions);
    }

    public static Expression binaryExpression(LogicalBinaryExpression.Operator operator, Collection<Expression> expressions)
    {
        requireNonNull(operator, "operator is null");
        requireNonNull(expressions, "expressions is null");

        if (expressions.isEmpty()) {
            switch (operator) {
                case AND:
                    return TRUE_LITERAL;
                case OR:
                    return FALSE_LITERAL;
                default:
                    throw new IllegalArgumentException("Unsupported LogicalBinaryExpression operator");
            }
        }

        // Build balanced tree for efficient recursive processing that
        // preserves the evaluation order of the input expressions.
        //
        // The tree is built bottom up by combining pairs of elements into
        // binary AND expressions.
        //
        // Example:
        //
        // Initial state:
        //  a b c d e
        //
        // First iteration:
        //
        //  /\    /\   e
        // a  b  c  d
        //
        // Second iteration:
        //
        //    / \    e
        //  /\   /\
        // a  b c  d
        //
        //
        // Last iteration:
        //
        //      / \
        //    / \  e
        //  /\   /\
        // a  b c  d

        Queue<Expression> queue = new ArrayDeque<>(expressions);
        while (queue.size() > 1) {
            Queue<Expression> buffer = new ArrayDeque<>();

            // combine pairs of elements
            while (queue.size() >= 2) {
                buffer.add(new LogicalBinaryExpression(operator, queue.remove(), queue.remove()));
            }

            // if there's and odd number of elements, just append the last one
            if (!queue.isEmpty()) {
                buffer.add(queue.remove());
            }

            // continue processing the pairs that were just built
            queue = buffer;
        }

        return queue.remove();
    }

    public static Expression combinePredicates(LogicalBinaryExpression.Operator operator, Expression... expressions)
    {
        return combinePredicates(operator, Arrays.asList(expressions));
    }

    public static Expression combinePredicates(LogicalBinaryExpression.Operator operator, Collection<Expression> expressions)
    {
        if (operator == LogicalBinaryExpression.Operator.AND) {
            return combineConjuncts(expressions);
        }

        return combineDisjuncts(expressions);
    }

    public static Expression combineConjuncts(Expression... expressions)
    {
        return combineConjuncts(Arrays.asList(expressions));
    }

    public static Expression combineConjuncts(Collection<Expression> expressions)
    {
        requireNonNull(expressions, "expressions is null");

        List<Expression> conjuncts = expressions.stream()
                .flatMap(e -> ExpressionUtils.extractConjuncts(e).stream())
                .filter(e -> !e.equals(TRUE_LITERAL))
                .collect(toList());

        conjuncts = removeDuplicates(conjuncts);

        if (conjuncts.contains(FALSE_LITERAL)) {
            return FALSE_LITERAL;
        }

        return and(conjuncts);
    }

    public static Expression combineDisjuncts(Expression... expressions)
    {
        return combineDisjuncts(Arrays.asList(expressions));
    }

    public static Expression combineDisjuncts(Collection<Expression> expressions)
    {
        return combineDisjunctsWithDefault(expressions, FALSE_LITERAL);
    }

    public static Expression combineDisjunctsWithDefault(Collection<Expression> expressions, Expression emptyDefault)
    {
        requireNonNull(expressions, "expressions is null");

        List<Expression> disjuncts = expressions.stream()
                .flatMap(e -> ExpressionUtils.extractDisjuncts(e).stream())
                .filter(e -> !e.equals(FALSE_LITERAL))
                .collect(toList());

        disjuncts = removeDuplicates(disjuncts);

        if (disjuncts.contains(TRUE_LITERAL)) {
            return TRUE_LITERAL;
        }

        return disjuncts.isEmpty() ? emptyDefault : or(disjuncts);
    }

    public static Expression filterDeterministicConjuncts(Expression expression)
    {
        return filterConjuncts(expression, ExpressionDeterminismEvaluator::isDeterministic);
    }

    public static Expression filterNonDeterministicConjuncts(Expression expression)
    {
        return filterConjuncts(expression, not(ExpressionDeterminismEvaluator::isDeterministic));
    }

    public static Expression filterConjuncts(Expression expression, Predicate<Expression> predicate)
    {
        List<Expression> conjuncts = extractConjuncts(expression).stream()
                .filter(predicate)
                .collect(toList());

        return combineConjuncts(conjuncts);
    }

    public static Function<Expression, Expression> expressionOrNullVariables(TypeProvider types, final java.util.function.Predicate<VariableReferenceExpression>... nullVariableScopes)
    {
        return expression -> {
            ImmutableList.Builder<Expression> resultDisjunct = ImmutableList.builder();
            resultDisjunct.add(expression);

            for (java.util.function.Predicate<VariableReferenceExpression> nullVariableScope : nullVariableScopes) {
                List<VariableReferenceExpression> variables = VariablesExtractor.extractUnique(expression, types).stream()
                        .filter(nullVariableScope)
                        .collect(Collectors.toList());

                if (Iterables.isEmpty(variables)) {
                    continue;
                }

                ImmutableList.Builder<Expression> nullConjuncts = ImmutableList.builder();
                for (VariableReferenceExpression variable : variables) {
                    nullConjuncts.add(new IsNullPredicate(new SymbolReference(variable.getName())));
                }

                resultDisjunct.add(and(nullConjuncts.build()));
            }

            return or(resultDisjunct.build());
        };
    }

    /**
     * Removes duplicate deterministic expressions. Preserves the relative order
     * of the expressions in the list.
     */
    private static List<Expression> removeDuplicates(List<Expression> expressions)
    {
        Set<Expression> seen = new HashSet<>();

        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (Expression expression : expressions) {
            if (!ExpressionDeterminismEvaluator.isDeterministic(expression)) {
                result.add(expression);
            }
            else if (!seen.contains(expression)) {
                result.add(expression);
                seen.add(expression);
            }
        }

        return result.build();
    }

    public static Expression normalize(Expression expression)
    {
        if (expression instanceof NotExpression) {
            NotExpression not = (NotExpression) expression;
            if (not.getValue() instanceof ComparisonExpression && ((ComparisonExpression) not.getValue()).getOperator() != IS_DISTINCT_FROM) {
                ComparisonExpression comparison = (ComparisonExpression) not.getValue();
                return new ComparisonExpression(comparison.getOperator().negate(), comparison.getLeft(), comparison.getRight());
            }
            if (not.getValue() instanceof NotExpression) {
                return normalize(((NotExpression) not.getValue()).getValue());
            }
        }
        return expression;
    }

    public static Expression rewriteIdentifiersToSymbolReferences(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SymbolReference(node.getValue());
            }
        }, expression);
    }
}