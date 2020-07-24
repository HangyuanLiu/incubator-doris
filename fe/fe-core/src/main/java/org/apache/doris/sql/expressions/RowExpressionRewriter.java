package org.apache.doris.sql.expressions;

import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.InputReferenceExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

public class RowExpressionRewriter<C>
{
    public RowExpression rewriteRowExpression(RowExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return null;
    }

    public RowExpression rewriteInputReference(InputReferenceExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteCall(CallExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteConstant(ConstantExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteVariableReference(VariableReferenceExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }

    public RowExpression rewriteSpecialForm(SpecialFormExpression node, C context, RowExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteRowExpression(node, context, treeRewriter);
    }
}