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

import org.apache.doris.sql.function.ScalarOperator;
import org.apache.doris.sql.function.SqlNullable;
import org.apache.doris.sql.function.SqlType;

import static org.apache.doris.sql.type.OperatorType.ADD;
import static org.apache.doris.sql.type.OperatorType.BETWEEN;
import static org.apache.doris.sql.type.OperatorType.CAST;
import static org.apache.doris.sql.type.OperatorType.DIVIDE;
import static org.apache.doris.sql.type.OperatorType.EQUAL;
import static org.apache.doris.sql.type.OperatorType.GREATER_THAN;
import static org.apache.doris.sql.type.OperatorType.GREATER_THAN_OR_EQUAL;
import static org.apache.doris.sql.type.OperatorType.LESS_THAN;
import static org.apache.doris.sql.type.OperatorType.LESS_THAN_OR_EQUAL;
import static org.apache.doris.sql.type.OperatorType.MODULUS;
import static org.apache.doris.sql.type.OperatorType.MULTIPLY;
import static org.apache.doris.sql.type.OperatorType.NEGATION;
import static org.apache.doris.sql.type.OperatorType.NOT_EQUAL;
import static org.apache.doris.sql.type.OperatorType.SUBTRACT;
import static java.lang.Float.floatToRawIntBits;

public final class IntegerOperators
{
    private IntegerOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.INTEGER)
    public static long add(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right) {
        return Math.addExact((int) left, (int) right);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTEGER)
    public static long subtract(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right) {
        return Math.subtractExact((int) left, (int) right);
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.INTEGER)
    public static long multiply(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right) {
        return Math.multiplyExact((int) left, (int) right);
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.INTEGER)
    public static long divide(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right) {
            return left / right;
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.INTEGER)
    public static long modulus(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right) {
            return left % right;
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.INTEGER)
    public static long negate(@SqlType(StandardTypes.INTEGER) long value) {
        return Math.negateExact((int) value);
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.INTEGER) long left, @SqlType(StandardTypes.INTEGER) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.INTEGER) long value, @SqlType(StandardTypes.INTEGER) long min, @SqlType(StandardTypes.INTEGER) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.INTEGER) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.INTEGER) long value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.INTEGER) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DECIMAL)
    public static String castToDecimal(@SqlType(StandardTypes.INTEGER) long value)
    {
        return String.valueOf(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.INTEGER) long value)
    {
        return (long) floatToRawIntBits((float) value);
    }

    @ScalarOperator(CAST)
    @SqlType("varchar(x)")
    public static String castToVarchar(@SqlType(StandardTypes.INTEGER) long value)
    {
        return String.valueOf(value);
    }

}
