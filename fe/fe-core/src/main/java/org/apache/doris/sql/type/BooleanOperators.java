/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.doris.sql.type;


import org.apache.doris.sql.function.LiteralParameters;
import org.apache.doris.sql.function.ScalarFunction;
import org.apache.doris.sql.function.ScalarOperator;
import org.apache.doris.sql.function.SqlNullable;
import org.apache.doris.sql.function.SqlType;

import static org.apache.doris.sql.function.SqlFunctionVisibility.HIDDEN;
import static org.apache.doris.sql.type.OperatorType.BETWEEN;
import static org.apache.doris.sql.type.OperatorType.CAST;
import static org.apache.doris.sql.type.OperatorType.EQUAL;
import static org.apache.doris.sql.type.OperatorType.GREATER_THAN;
import static org.apache.doris.sql.type.OperatorType.GREATER_THAN_OR_EQUAL;
import static org.apache.doris.sql.type.OperatorType.HASH_CODE;
import static org.apache.doris.sql.type.OperatorType.INDETERMINATE;
import static org.apache.doris.sql.type.OperatorType.IS_DISTINCT_FROM;
import static org.apache.doris.sql.type.OperatorType.LESS_THAN;
import static org.apache.doris.sql.type.OperatorType.LESS_THAN_OR_EQUAL;
import static org.apache.doris.sql.type.OperatorType.NOT_EQUAL;
import static java.lang.Float.floatToRawIntBits;

public final class BooleanOperators
{
    private BooleanOperators()
    {
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return !left && right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return !left || right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left && !right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.BOOLEAN) boolean left, @SqlType(StandardTypes.BOOLEAN) boolean right)
    {
        return left || !right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.BOOLEAN) boolean value, @SqlType(StandardTypes.BOOLEAN) boolean min, @SqlType(StandardTypes.BOOLEAN) boolean max)
    {
        return (value && max) || (!value && !min);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? floatToRawIntBits(1.0f) : floatToRawIntBits(0.0f);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @SqlType(StandardTypes.BOOLEAN)
    @ScalarFunction(visibility = HIDDEN, value = "NOT") // TODO: this should not be callable from SQL
    public static boolean not(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return !value;
    }
}
