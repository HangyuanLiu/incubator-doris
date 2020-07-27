package org.apache.doris.sql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class ExpressionTreeRewriter<C> {
    private final ExpressionRewriter<C> rewriter;
    private final AstVisitor<Expression, ExpressionTreeRewriter.Context<C>> visitor;

    public static <C, T extends Expression> T rewriteWith(ExpressionRewriter<C> rewriter, T node)
    {
        return new ExpressionTreeRewriter<>(rewriter).rewrite(node, null);
    }

    public static <C, T extends Expression> T rewriteWith(ExpressionRewriter<C> rewriter, T node, C context)
    {
        return new ExpressionTreeRewriter<>(rewriter).rewrite(node, context);
    }

    public ExpressionTreeRewriter(ExpressionRewriter<C> rewriter)
    {
        this.rewriter = rewriter;
        this.visitor = new RewritingVisitor();
    }

    private List<Expression> rewrite(List<Expression> items, Context<C> context)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Expression expression : items) {
            builder.add(rewrite(expression, context.get()));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public <T extends Expression> T rewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, false));
    }

    /**
     * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the expression rewriter for the provided node.
     */
    @SuppressWarnings("unchecked")
    public <T extends Expression> T defaultRewrite(T node, C context)
    {
        return (T) visitor.process(node, new Context<>(context, true));
    }

    private class RewritingVisitor
            extends AstVisitor<Expression, ExpressionTreeRewriter.Context<C>>
    {
        @Override
        protected Expression visitExpression(Expression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            throw new UnsupportedOperationException("not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass().getName());
        }

        @Override
        protected Expression visitArithmeticUnary(ArithmeticUnaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArithmeticUnary(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression child = rewrite(node.getValue(), context.get());
            if (child != node.getValue()) {
                return new ArithmeticUnaryExpression(node.getSign(), child);
            }

            return node;
        }

        @Override
        public Expression visitArithmeticBinary(ArithmeticBinaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteArithmeticBinary(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());

            if (left != node.getLeft() || right != node.getRight()) {
                return new ArithmeticBinaryExpression(node.getOperator(), left, right);
            }

            return node;
        }


        @Override
        public Expression visitLiteral(Literal node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLiteral(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Expression visitIdentifier(Identifier node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIdentifier(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        public Expression visitDereferenceExpression(DereferenceExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteDereferenceExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression base = rewrite(node.getBase(), context.get());
            if (base != node.getBase()) {
                return new DereferenceExpression(base, node.getField());
            }

            return node;
        }

        @Override
        public Expression visitComparisonExpression(ComparisonExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteComparisonExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());

            if (left != node.getLeft() || right != node.getRight()) {
                return new ComparisonExpression(node.getOperator(), left, right);
            }

            return node;
        }

        @Override
        protected Expression visitBetweenPredicate(BetweenPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteBetweenPredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression min = rewrite(node.getMin(), context.get());
            Expression max = rewrite(node.getMax(), context.get());

            if (value != node.getValue() || min != node.getMin() || max != node.getMax()) {
                return new BetweenPredicate(value, min, max);
            }

            return node;
        }

        @Override
        public Expression visitLogicalBinaryExpression(LogicalBinaryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLogicalBinaryExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression left = rewrite(node.getLeft(), context.get());
            Expression right = rewrite(node.getRight(), context.get());

            if (left != node.getLeft() || right != node.getRight()) {
                return new LogicalBinaryExpression(node.getOperator(), left, right);
            }

            return node;
        }

        @Override
        public Expression visitNotExpression(NotExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteNotExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            if (value != node.getValue()) {
                return new NotExpression(value);
            }

            return node;
        }

        @Override
        protected Expression visitIsNullPredicate(IsNullPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIsNullPredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            if (value != node.getValue()) {
                return new IsNullPredicate(value);
            }

            return node;
        }

        @Override
        protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteIsNotNullPredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());

            if (value != node.getValue()) {
                return new IsNotNullPredicate(value);
            }

            return node;
        }

        @Override
        public Expression visitCast(Cast node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteCast(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression expression = rewrite(node.getExpression(), context.get());

            if (node.getExpression() != expression) {
                return new Cast(expression, node.getType(), node.isSafe(), node.isTypeOnly());
            }

            return node;
        }

        @Override
        protected Expression visitFieldReference(FieldReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteFieldReference(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }

        @Override
        protected Expression visitSymbolReference(SymbolReference node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSymbolReference(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            return node;
        }


        @Override
        public Expression visitFunctionCall(FunctionCall node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteFunctionCall(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Optional<Expression> filter = node.getFilter();
            if (filter.isPresent()) {
                Expression filterExpression = filter.get();
                Expression newFilterExpression = rewrite(filterExpression, context.get());
                filter = Optional.of(newFilterExpression);
            }

            Optional<Window> rewrittenWindow = node.getWindow();

            List<Expression> arguments = rewrite(node.getArguments(), context);

            if (!sameElements(node.getArguments(), arguments) || !sameElements(rewrittenWindow, node.getWindow())
                    || !sameElements(filter, node.getFilter())) {
                return new FunctionCall(node.getName(), rewrittenWindow, filter, node.getOrderBy().map(orderBy -> rewriteOrderBy(orderBy, context)), node.isDistinct(), node.isIgnoreNulls(), arguments);
            }
            return node;
        }

        // Since OrderBy contains list of SortItems, we want to process each SortItem's key, which is an expression
        private OrderBy rewriteOrderBy(OrderBy orderBy, Context<C> context)
        {
            List<SortItem> rewrittenSortItems = rewriteSortItems(orderBy.getSortItems(), context);
            if (sameElements(orderBy.getSortItems(), rewrittenSortItems)) {
                return orderBy;
            }

            return new OrderBy(rewrittenSortItems);
        }

        private List<SortItem> rewriteSortItems(List<SortItem> sortItems, Context<C> context)
        {
            ImmutableList.Builder<SortItem> rewrittenSortItems = ImmutableList.builder();
            for (SortItem sortItem : sortItems) {
                Expression sortKey = rewrite(sortItem.getSortKey(), context.get());
                if (sortItem.getSortKey() != sortKey) {
                    rewrittenSortItems.add(new SortItem(sortKey, sortItem.getOrdering(), sortItem.getNullOrdering()));
                }
                else {
                    rewrittenSortItems.add(sortItem);
                }
            }
            return rewrittenSortItems.build();
        }

        @Override
        public Expression visitLikePredicate(LikePredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteLikePredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression pattern = rewrite(node.getPattern(), context.get());
            Optional<Expression> rewrittenEscape = node.getEscape()
                    .map(escape -> rewrite(escape, context.get()));

            if (value != node.getValue() || pattern != node.getPattern() || !sameElements(node.getEscape(), rewrittenEscape)) {
                return new LikePredicate(value, pattern, rewrittenEscape);
            }

            return node;
        }

        @Override
        public Expression visitInPredicate(InPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteInPredicate(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression value = rewrite(node.getValue(), context.get());
            Expression list = rewrite(node.getValueList(), context.get());

            if (node.getValue() != value || node.getValueList() != list) {
                return new InPredicate(value, list);
            }

            return node;
        }

        @Override
        protected Expression visitInListExpression(InListExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteInListExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            List<Expression> values = rewrite(node.getValues(), context);

            if (!sameElements(node.getValues(), values)) {
                return new InListExpression(values);
            }

            return node;
        }

        @Override
        protected Expression visitExists(ExistsPredicate node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteExists(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            Expression subquery = node.getSubquery();
            subquery = rewrite(subquery, context.get());

            if (subquery != node.getSubquery()) {
                return new ExistsPredicate(subquery);
            }

            return node;
        }

        @Override
        public Expression visitSubqueryExpression(SubqueryExpression node, Context<C> context)
        {
            if (!context.isDefaultRewrite()) {
                Expression result = rewriter.rewriteSubqueryExpression(node, context.get(), ExpressionTreeRewriter.this);
                if (result != null) {
                    return result;
                }
            }

            // No default rewrite for SubqueryExpression since we do not want to traverse subqueries
            return node;
        }
    }

    public static class Context<C>
    {
        private final boolean defaultRewrite;
        private final C context;

        private Context(C context, boolean defaultRewrite)
        {
            this.context = context;
            this.defaultRewrite = defaultRewrite;
        }

        public C get()
        {
            return context;
        }

        public boolean isDefaultRewrite()
        {
            return defaultRewrite;
        }
    }

    private static <T> boolean sameElements(Optional<T> a, Optional<T> b)
    {
        if (!a.isPresent() && !b.isPresent()) {
            return true;
        }
        else if (a.isPresent() != b.isPresent()) {
            return false;
        }

        return a.get() == b.get();
    }

    private static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b)
    {
        if (Iterables.size(a) != Iterables.size(b)) {
            return false;
        }

        Iterator<? extends T> first = a.iterator();
        Iterator<? extends T> second = b.iterator();

        while (first.hasNext() && second.hasNext()) {
            if (first.next() != second.next()) {
                return false;
            }
        }

        return true;
    }
}
