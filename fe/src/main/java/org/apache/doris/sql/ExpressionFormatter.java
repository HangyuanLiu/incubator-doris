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

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.ExistsPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.sql.tree.AllColumns;
import org.apache.doris.sql.tree.ArithmeticBinaryExpression;
import org.apache.doris.sql.tree.ArithmeticUnaryExpression;
import org.apache.doris.sql.tree.AstVisitor;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.tree.Cube;
import org.apache.doris.sql.tree.DereferenceExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.FieldReference;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.GroupingElement;
import org.apache.doris.sql.tree.GroupingOperation;
import org.apache.doris.sql.tree.GroupingSets;
import org.apache.doris.sql.tree.Identifier;
import org.apache.doris.sql.tree.InListExpression;
import org.apache.doris.sql.tree.InPredicate;
import org.apache.doris.sql.tree.IntervalLiteral;
import org.apache.doris.sql.tree.IsNotNullPredicate;
import org.apache.doris.sql.tree.IsNullPredicate;
import org.apache.doris.sql.tree.LogicalBinaryExpression;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.Node;
import org.apache.doris.sql.tree.NotExpression;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.OrderBy;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.tree.QuantifiedComparisonExpression;
import org.apache.doris.sql.tree.Rollup;
import org.apache.doris.sql.tree.SimpleGroupBy;
import org.apache.doris.sql.tree.SortItem;
import org.apache.doris.sql.tree.StringLiteral;
import org.apache.doris.sql.tree.SubqueryExpression;
import org.apache.doris.sql.tree.SymbolReference;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class ExpressionFormatter
{
    private static final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression, Optional<List<Expression>> parameters)
    {
        return new Formatter(parameters).process(expression, null);
    }

    public static String formatQualifiedName(QualifiedName name)
    {
        return name.getParts().stream()
                .map(ExpressionFormatter::formatIdentifier)
                .collect(joining("."));
    }

    /*
    public static String formatIdentifier(String s)
    {
        return '"' + s.replace("\"", "\"\"") + '"';
    }
     */
    public static String formatIdentifier(String s) {
        return s;
    }

    public static class Formatter
            extends AstVisitor<String, Void>
    {
        private final Optional<List<Expression>> parameters;

        public Formatter(Optional<List<Expression>> parameters)
        {
            this.parameters = parameters;
        }

        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException(format("not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return Long.toString(node.getValue());
        }


        @Override
        protected String visitNullLiteral(NullLiteral node, Void context)
        {
            return "null";
        }

        @Override
        protected String visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "- " : "";
            StringBuilder builder = new StringBuilder()
                    .append("INTERVAL ")
                    .append(sign)
                    .append(" '").append(node.getValue()).append("' ")
                    .append(node.getStartField());

            if (node.getEndField().isPresent()) {
                builder.append(" TO ").append(node.getEndField().get());
            }
            return builder.toString();
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            //return "(" + formatSql(node.getQuery(), parameters) + ")";
            return "subquery";
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            if (!node.isDelimited()) {
                return node.getValue();
            }
            else {
                return '"' + node.getValue().replace("\"", "\"\"") + '"';
            }
        }

        @Override
        protected String visitSymbolReference(SymbolReference node, Void context)
        {
            return formatIdentifier(node.getName());
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            String baseString = process(node.getBase(), context);
            return baseString + "." + process(node.getField());
        }

        @Override
        public String visitFieldReference(FieldReference node, Void context)
        {
            // add colon so this won't parse
            return ":input(" + node.getFieldIndex() + ")";
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(formatQualifiedName(node.getName()))
                    .append('(').append(arguments);

            if (node.getOrderBy().isPresent()) {
                builder.append(' ').append(formatOrderBy(node.getOrderBy().get(), parameters));
            }

            builder.append(')');

            if (node.isIgnoreNulls()) {
                builder.append(" IGNORE NULLS");
            }

            if (node.getFilter().isPresent()) {
                builder.append(" FILTER ").append(visitFilter(node.getFilter().get(), context));
            }

            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().toString(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context)
        {
            return "(NOT " + process(node.getValue(), context) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IS NOT NULL)";
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            String value = process(node.getValue(), context);

            switch (node.getSign()) {
                case MINUS:
                    // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
                    String separator = value.startsWith("-") ? " " : "";
                    return "-" + separator + value;
                case PLUS:
                    return "+" + value;
                default:
                    throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
            }
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context)
        {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return (node.isSafe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.getExpression(), context) + " AS " + node.getType() + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context)
        {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        private String visitFilter(Expression node, Void context)
        {
            return "(WHERE " + process(node, context) + ')';
        }

        @Override
        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context)
        {
            return new StringBuilder()
                    .append("(")
                    .append(process(node.getValue(), context))
                    .append(' ')
                    .append(node.getOperator().getValue())
                    .append(' ')
                    .append(node.getQuantifier().toString())
                    .append(' ')
                    .append(process(node.getSubquery(), context))
                    .append(")")
                    .toString();
        }

        public String visitGroupingOperation(GroupingOperation node, Void context)
        {
            return "GROUPING (" + joinExpressions(node.getGroupingColumns()) + ")";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions)
        {
            return Joiner.on(", ").join(expressions.stream()
                    .map((e) -> process(e, null))
                    .iterator());
        }
    }

    static String formatStringLiteral(String s)
    {
        s = s.replace("'", "''");
        if (CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(s)) {
            return "'" + s + "'";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("U&'");
        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            }
            else if (codePoint <= 0xFFFF) {
                builder.append('\\');
                builder.append(String.format("%04X", codePoint));
            }
            else {
                builder.append("\\+");
                builder.append(String.format("%06X", codePoint));
            }
        }
        builder.append("'");
        return builder.toString();
    }

    static String formatOrderBy(OrderBy orderBy, Optional<List<Expression>> parameters)
    {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems(), parameters);
    }

    static String formatSortItems(List<SortItem> sortItems, Optional<List<Expression>> parameters)
    {
        return Joiner.on(", ").join(sortItems.stream()
                .map(sortItemFormatterFunction(parameters))
                .iterator());
    }

    static String formatGroupBy(List<GroupingElement> groupingElements)
    {
        return formatGroupBy(groupingElements, Optional.empty());
    }

    static String formatGroupBy(List<GroupingElement> groupingElements, Optional<List<Expression>> parameters)
    {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                List<Expression> columns = ((SimpleGroupBy) groupingElement).getExpressions();
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns), parameters);
                }
                else {
                    result = formatGroupingSet(columns, parameters);
                }
            }
            else if (groupingElement instanceof GroupingSets) {
                result = format("GROUPING SETS (%s)", Joiner.on(", ").join(
                        ((GroupingSets) groupingElement).getSets().stream()
                                .map(e -> formatGroupingSet(e, parameters))
                                .iterator()));
            }
            else if (groupingElement instanceof Cube) {
                result = format("CUBE %s", formatGroupingSet(((Cube) groupingElement).getExpressions(), parameters));
            }
            else if (groupingElement instanceof Rollup) {
                result = format("ROLLUP %s", formatGroupingSet(((Rollup) groupingElement).getExpressions(), parameters));
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }

    private static boolean isAsciiPrintable(int codePoint)
    {
        if (codePoint >= 0x7F || codePoint < 0x20) {
            return false;
        }
        return true;
    }

    private static String formatGroupingSet(List<Expression> groupingSet, Optional<List<Expression>> parameters)
    {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(e -> formatExpression(e, parameters))
                .iterator()));
    }

    private static Function<SortItem, String> sortItemFormatterFunction(Optional<List<Expression>> parameters)
    {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey(), parameters));

            switch (input.getOrdering()) {
                case ASCENDING:
                    builder.append(" ASC");
                    break;
                case DESCENDING:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                    break;
                case UNDEFINED:
                    // no op
                    break;
                default:
                    throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }
}
