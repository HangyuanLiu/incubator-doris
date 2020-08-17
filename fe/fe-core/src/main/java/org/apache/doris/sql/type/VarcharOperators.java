package org.apache.doris.sql.type;

import org.apache.doris.sql.function.ScalarOperator;
import org.apache.doris.sql.function.SqlType;

import static org.apache.doris.sql.type.OperatorType.CAST;

public final class VarcharOperators {
    private VarcharOperators()
    {
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DATE)
    public static String castToDate(@SqlType("varchar") String value)
    {
        //FIXME
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.CHAR)
    public static String castToChar(@SqlType("varchar") String value)
    {
        //FIXME
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType("varchar") String value)
    {
        //FIXME
        return Long.parseLong(value);
    }
}