package org.apache.doris.sql.relational;

import com.google.common.collect.Lists;
import org.apache.doris.sql.analyzer.TypeSignatureProvider;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.tree.ComparisonExpression;
import org.apache.doris.sql.type.OperatorType;
import org.apache.doris.sql.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.type.OperatorType.*;

public final class FunctionResolution {

    private final FunctionManager functionManager;

    public FunctionResolution(FunctionManager functionManager) {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    public FunctionHandle comparisonFunction(OperatorType operator, Type leftType, Type rightType)
    {
        checkArgument(operator.isComparisonOperator(), format("unexpected comparison type %s", operator));
        return functionManager.resolveOperator(operator, Lists.newArrayList(leftType, rightType));
    }

    public FunctionHandle comparisonFunction(ComparisonExpression.Operator operator, Type leftType, Type rightType)
    {
        OperatorType operatorType;
        switch (operator) {
            case EQUAL:
                operatorType = EQUAL;
                break;
            case NOT_EQUAL:
                operatorType = NOT_EQUAL;
                break;
            case LESS_THAN:
                operatorType = LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                operatorType = LESS_THAN_OR_EQUAL;
                break;
            case GREATER_THAN:
                operatorType = GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                operatorType = GREATER_THAN_OR_EQUAL;
                break;
            case IS_DISTINCT_FROM:
                operatorType = IS_DISTINCT_FROM;
                break;
            default:
                throw new IllegalStateException("Unsupported comparison operator type: " + operator);
        }

        return comparisonFunction(operatorType, leftType, rightType);
    }
}