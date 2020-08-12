package org.apache.doris.sql.function;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.sql.type.StandardTypes;

public final class ScalarFunctions {

    @ScalarFunction("DAYS_ADD")
    @SqlType(StandardTypes.DATE)
    public static String daysAdd(@SqlType(StandardTypes.DATE) String date, @SqlType(StandardTypes.BIGINT) long day) {
        try {
            DateLiteral dateLiteral = new DateLiteral(date, Type.DATE);
            return dateLiteral.plusDays((int) day).getStringValue();
        } catch (AnalysisException ex) {
            ex.printStackTrace();
            return null;
        }
    }
}
