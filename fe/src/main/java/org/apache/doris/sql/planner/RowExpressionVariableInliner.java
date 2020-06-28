package org.apache.doris.sql.planner;

import org.apache.doris.sql.expressions.RowExpressionRewriter;
import org.apache.doris.sql.expressions.RowExpressionTreeRewriter;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;

public final class RowExpressionVariableInliner
        extends RowExpressionRewriter<Void>
{
    private final Set<String> excludedNames = new HashSet<>();
    private final Function<VariableReferenceExpression, ? extends RowExpression> mapping;

    private RowExpressionVariableInliner(Function<VariableReferenceExpression, ? extends RowExpression> mapping)
    {
        this.mapping = mapping;
    }

    public static RowExpression inlineVariables(Function<VariableReferenceExpression, ? extends RowExpression> mapping, RowExpression expression)
    {
        return RowExpressionTreeRewriter.rewriteWith(new RowExpressionVariableInliner(mapping), expression);
    }

    public static RowExpression inlineVariables(Map<VariableReferenceExpression, ? extends RowExpression> mapping, RowExpression expression)
    {
        return inlineVariables(mapping::get, expression);
    }

    @Override
    public RowExpression rewriteVariableReference(VariableReferenceExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        if (!excludedNames.contains(node.getName())) {
            RowExpression result = mapping.apply(node);
            checkState(result != null, "Cannot resolve symbol %s", node.getName());
            return result;
        }
        return null;
    }
}