package org.apache.doris.sql;

import javassist.compiler.ast.StringL;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Function;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.planner.LiteralEncoder;
import org.apache.doris.sql.relation.CallExpression;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.relational.RowExpressionToExpr;
import org.apache.doris.sql.tree.Literal;
import org.apache.doris.sql.type.DateType;
import org.apache.doris.sql.type.TimestampType;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.TypeManager;
import org.apache.doris.sql.type.TypeRegistry;
import org.apache.doris.sql.type.TypeSignature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class InterpretedFunctionInvoker {
    private final FunctionManager functionManager;
    private final TypeManager typeManager = new TypeRegistry();

    public InterpretedFunctionInvoker(FunctionManager functionManager) {
        this.functionManager = requireNonNull(functionManager, "registry is null");
    }

    public Object invoke(FunctionHandle functionHandle, Object... arguments) {
        return invoke(functionHandle, Arrays.asList(arguments));
    }

    public Object invoke(FunctionHandle functionHandle, List<Object> arguments) {
        //FIXME
        if (functionHandle.getFunctionName().startsWith("cast")) {
            /*
            Expr literal = RowExpressionToExpr.formatRowExpression(
                    new ConstantExpression(arguments.get(0), typeManager.getType(functionHandle.getArgumentTypes().get(0))), null);
            try {
                literal = literal.castTo(functionHandle.getReturnType().toDorisType());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            if (literal instanceof DateLiteral) {
                return ((DateLiteral) literal).getStringValue();
            } else if (literal instanceof FloatLiteral) {
                return ((FloatLiteral) literal).getValue();
            } else if (literal instanceof StringLiteral) {
                return ((StringLiteral) literal).getStringValue();
            }
            return ((LiteralExpr) literal).getRealValue();

             */
            if (arguments.get(0) instanceof java.lang.Boolean) {
                if (((java.lang.Boolean)arguments.get(0)).equals(java.lang.Boolean.TRUE)) {
                    return 1;
                } else return 0;
            }
            return arguments.get(0);
        }

        int argSize = arguments.size();
        List<RowExpression> arg = new ArrayList<>();
        for (int i = 0; i < argSize; ++i) {
            //if (functionHandle.getArgumentTypes().get(i).equals(DateType.DATE.getTypeSignature())) {
            //    arg.add(LiteralEncoder.toRowExpression(arguments.get(i), TimestampType.TIMESTAMP));
            //} else {
                arg.add(LiteralEncoder.toRowExpression(arguments.get(i),
                        typeManager.getType(functionHandle.getArgumentTypes().get(i))));
            //}
        }

        Function searchDesc = new Function(
                new FunctionName(functionHandle.getFunctionName()),
                functionHandle.getArgumentTypes().stream().map(TypeSignature::toDorisType).collect(Collectors.toList()),
                functionHandle.getReturnType().toDorisType(), false);

        //return invoke(functionManager.getBuiltInScalarFunctionImplementation(functionHandle), arguments);
        Expr expr = RowExpressionToExpr.formatRowExpression(
                new CallExpression(
                        functionHandle.getFunctionName(),
                        functionHandle,
                        typeManager.getType(functionHandle.getReturnType()),
                        arg
                ), null);
        expr.setFn(searchDesc);

        try {
            Expr resultExpr = expr.getResultValue();
            Object value;
            if (resultExpr instanceof FloatLiteral) {
                value = ((FloatLiteral) resultExpr).getValue();
            } else if (resultExpr instanceof DateLiteral) {
                value = ((DateLiteral) resultExpr).getStringValue();
            } else {
                value = ((LiteralExpr) resultExpr).getRealValue();
            }
            return value;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
}