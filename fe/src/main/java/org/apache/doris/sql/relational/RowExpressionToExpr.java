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
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.sql.metadata.BuiltInFunctionHandle;
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
            Expr callExpr;
            if (((BuiltInFunctionHandle)node.getFunctionHandle()).getFunctionName().equalsIgnoreCase("add")) {
                Expr left = formatRowExpression(node.getArguments().get(0), context);
                Expr right = formatRowExpression(node.getArguments().get(1), context);
                callExpr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, left, right);
                callExpr.setType(ScalarType.BIGINT);
            } else if (((BuiltInFunctionHandle)node.getFunctionHandle()).getFunctionName().equalsIgnoreCase("sum")) {
                Expr left = formatRowExpression(node.getArguments().get(0), context);
                callExpr = new FunctionCallExpr(new FunctionName("sum"), new FunctionParams(false, Lists.newArrayList(left)));
                callExpr.setType(ScalarType.BIGINT);

                FunctionName fnName = new FunctionName("sum");
                Function searchDesc = new Function(fnName, Lists.newArrayList(ScalarType.BIGINT), Type.INVALID, false);
                Function fn = Catalog.getCurrentCatalog().getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

                callExpr.setFn(fn);
            } else if (((BuiltInFunctionHandle)node.getFunctionHandle()).getFunctionName().equalsIgnoreCase("count")) {
                Expr left = formatRowExpression(node.getArguments().get(0), context);
                callExpr = new FunctionCallExpr(new FunctionName("count"), new FunctionParams(false, Lists.newArrayList(left)));
                callExpr.setType(ScalarType.BIGINT);

                FunctionName fnName = new FunctionName("count");
                Function searchDesc = new Function(fnName, Lists.newArrayList(ScalarType.BIGINT), Type.INVALID, false);
                Function fn = Catalog.getCurrentCatalog().getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

                callExpr.setFn(fn);
            } else if (((BuiltInFunctionHandle)node.getFunctionHandle()).getFunctionName().equalsIgnoreCase("avg")) {
                Expr left = formatRowExpression(node.getArguments().get(0), context);
                callExpr = new FunctionCallExpr(new FunctionName("avg"), new FunctionParams(false, Lists.newArrayList(left)));
                callExpr.setType(ScalarType.DOUBLE);

                FunctionName fnName = new FunctionName("avg");
                Function searchDesc = new Function(fnName, Lists.newArrayList(ScalarType.BIGINT), Type.INVALID, false);
                Function fn = Catalog.getCurrentCatalog().getFunction(searchDesc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

                callExpr.setFn(fn);
            }
            else {
                Expr left = formatRowExpression(node.getArguments().get(0), context);
                Expr right = formatRowExpression(node.getArguments().get(1), context);
                callExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ, left,right);
                callExpr.setType(ScalarType.BOOLEAN);
            }

            return callExpr;
        }

        @Override
        public Expr visitInputReference(InputReferenceExpression node, FormatterContext context) {
            return null;
        }

        @Override
        public Expr visitConstant(ConstantExpression node, FormatterContext context) {
            try {
                if (node.getType().equals(BigintType.BIGINT)) {
                    return new IntLiteral((Long) node.getValue(), Type.BIGINT);
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
