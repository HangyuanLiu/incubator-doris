package org.apache.doris.sql.relational;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.InputReferenceExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.RowExpressionVisitor;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.type.BigintType;

import java.util.Map;

public class RowExpressionToExpr {
    public static Expr formatRowExpression(RowExpression expression, FormatterContext descTbl)
    {
        return expression.accept(new Formatter(), descTbl);
    }

    public static class FormatterContext {
        private DescriptorTable descTbl;
        private Map<String, SlotId> variableToSlotRef;

        public FormatterContext(DescriptorTable descTbl, Map<String, SlotId> variableToSlotRef) {
            this.descTbl = descTbl;
            this.variableToSlotRef = variableToSlotRef;
        }
    }

    public static class Formatter
            implements RowExpressionVisitor<Expr, FormatterContext>
    {
        @Override
        public Expr visitCall(CallExpression node, FormatterContext context) {
            RowExpression left = node.getArguments().get(0);
            RowExpression right = node.getArguments().get(1);
            Expr binaryPred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                    formatRowExpression(left, context), formatRowExpression(right, context));
            binaryPred.setType(ScalarType.BOOLEAN);

            return binaryPred;
        }

        @Override
        public Expr visitInputReference(InputReferenceExpression node, FormatterContext context) {
            return null;
        }

        @Override
        public Expr visitConstant(ConstantExpression node, FormatterContext context) {
            try {
                if (node.getType().equals(BigintType.BIGINT)) {
                    return new IntLiteral((Long) node.getValue(), Type.INT);
                } else {
                    throw new UnsupportedOperationException("not yet implemented");
                }
            } catch (Exception ex) {
                throw new UnsupportedOperationException("not yet implemented");
            }
        }

        @Override
        public Expr visitVariableReference(VariableReferenceExpression node, FormatterContext context) {
            return new SlotRef(context.descTbl.getSlotDesc(context.variableToSlotRef.get(node.getName())));
        }

        @Override
        public Expr visitSpecialForm(SpecialFormExpression node, FormatterContext context) {
            return null;
        }
    }
}
