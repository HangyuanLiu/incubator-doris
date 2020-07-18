package org.apache.doris.sql.relational;

import com.google.common.collect.Lists;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.InputReferenceExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relation.RowExpressionVisitor;
import org.apache.doris.sql.relation.SpecialFormExpression;
import org.apache.doris.sql.relation.VariableReferenceExpression;
import org.apache.doris.sql.type.BigintType;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            String fnName = node.getFunctionHandle().getFunctionName().toLowerCase();
            Expr callExpr;
            switch (fnName) {
                case "add":
                    callExpr = new ArithmeticExpr(
                            ArithmeticExpr.Operator.ADD,
                            formatRowExpression(node.getArguments().get(0), context),
                            formatRowExpression(node.getArguments().get(1), context));
                    break;
                case "eq":
                    callExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                            formatRowExpression(node.getArguments().get(0), context),
                            formatRowExpression(node.getArguments().get(1), context));
                    break;
                default:
                    List<Expr> arg = node.getArguments().stream().map(expr -> formatRowExpression(expr, context)).collect(Collectors.toList());
                    FunctionHandle fnHandle = node.getFunctionHandle();
                    callExpr = new FunctionCallExpr(fnHandle.getFunctionName(), new FunctionParams(false, arg));
                    callExpr.setFn(fnHandle.getResolevedFn());
            }
            callExpr.setType(node.getFunctionHandle().getReturnType().toDorisType());
            return callExpr;
        }

        @Override
        public Expr visitInputReference(InputReferenceExpression node, FormatterContext context) {
            return null;
        }

        @Override
        public Expr visitConstant(ConstantExpression node, FormatterContext context) {
            try {
                switch (node.getType().getTypeSignature().getBase().toLowerCase()) {
                    case "bigint":
                        return new IntLiteral((Long) node.getValue(), Type.BIGINT);
                    case "varchar":
                        return new StringLiteral((String) node.getValue());
                    default:
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
