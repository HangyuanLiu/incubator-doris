package org.apache.doris.sql.relational;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.InputReferenceExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.RowExpressionVisitor;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RowExpressionToExpr {
    public RowExpressionToExpr()
    {
    }

    public static Expr formatRowExpression(RowExpression expression, DescriptorTable descTbl)
    {
        return expression.accept(new Formatter(), descTbl);
    }

    public static class Formatter
            implements RowExpressionVisitor<Expr, DescriptorTable>
    {
        @Override
        public Expr visitCall(CallExpression node, DescriptorTable context) {
            RowExpression left = node.getArguments().get(0);
            RowExpression right = node.getArguments().get(1);
            Expr binaryPred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                    formatRowExpression(left), formatRowExpression(right));
            return binaryPred;
        }

        @Override
        public Expr visitInputReference(InputReferenceExpression node, DescriptorTable context) {
            return null;
        }

        @Override
        public Expr visitConstant(ConstantExpression node, DescriptorTable context) {
            return null;
        }

        @Override
        public Expr visitVariableReference(VariableReferenceExpression node, DescriptorTable context) {
            context.getSlotDesc()
            Expr SlotRef = new SlotRef();
        }

        @Override
        public Expr visitSpecialForm(SpecialFormExpression node, DescriptorTable context) {
            return null;
        }
    }
}
