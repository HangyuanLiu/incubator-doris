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
    public static String castToDate(@SqlType("varchar(x)") String value)
    {
        //FIXME
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.CHAR)
    public static String castToChar(@SqlType("varchar(x)") String value)
    {
        //FIXME
        return value;
    }
}