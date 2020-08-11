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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.sql.function.LiteralParameters;
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
import static org.apache.doris.sql.type.OperatorType.HASH_CODE;
import static org.apache.doris.sql.type.OperatorType.INDETERMINATE;
import static org.apache.doris.sql.type.OperatorType.IS_DISTINCT_FROM;
import static org.apache.doris.sql.type.OperatorType.LESS_THAN;
import static org.apache.doris.sql.type.OperatorType.LESS_THAN_OR_EQUAL;
import static org.apache.doris.sql.type.OperatorType.MODULUS;
import static org.apache.doris.sql.type.OperatorType.MULTIPLY;
import static org.apache.doris.sql.type.OperatorType.NEGATION;
import static org.apache.doris.sql.type.OperatorType.NOT_EQUAL;
import static org.apache.doris.sql.type.OperatorType.SATURATED_FLOOR_CAST;
import static org.apache.doris.sql.type.OperatorType.SUBTRACT;
import static org.apache.doris.sql.type.OperatorType.XX_HASH_64;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class BigintOperators
{
    private BigintOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGINT)
    public static long add(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right) throws AnalysisException {
        try {
            return Math.addExact(left, right);
        }
        catch (ArithmeticException e) {
            throw new AnalysisException(format("bigint addition overflow: %s + %s", left, right));
        }
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGINT)
    public static long subtract(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right) throws AnalysisException {
        try {
            return Math.subtractExact(left, right);
        }
        catch (ArithmeticException e) {
            throw new AnalysisException(format("bigint subtraction overflow: %s - %s", left, right));
        }
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.BIGINT)
    public static long multiply(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right) throws AnalysisException {
        try {
            return Math.multiplyExact(left, right);
        }
        catch (ArithmeticException e) {
            throw new AnalysisException(format("bigint multiplication overflow: %s * %s", left, right));
        }
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.BIGINT)
    public static long divide(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right) throws AnalysisException {
        try {
            if (left == Long.MIN_VALUE && right == -1) {
                throw new AnalysisException(format("bigint division overflow: %s / %s", left, right));
            }
            return left / right;
        }
        catch (ArithmeticException e) {
            throw new AnalysisException("divid zero");
        }
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.BIGINT)
    public static long modulus(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right) throws AnalysisException {
        try {
            return left % right;
        }
        catch (ArithmeticException e) {
            throw new AnalysisException("divid zero");
        }
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.BIGINT)
    public static long negate(@SqlType(StandardTypes.BIGINT) long value) throws AnalysisException {
        try {
            return Math.negateExact(value);
        }
        catch (ArithmeticException e) {
            throw new AnalysisException("bigint negation overflow: " + value);
        }
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.BIGINT) long min, @SqlType(StandardTypes.BIGINT) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.BIGINT) long value)
    {
        return value != 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.BIGINT) long value) throws AnalysisException {
        try {
            return toIntExact(value);
        }
        catch (ArithmeticException e) {
            throw new AnalysisException("Out of range for integer: " + value);
        }
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long saturatedFloorCastToInteger(@SqlType(StandardTypes.BIGINT) long value)
    {
        return Ints.saturatedCast(value);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long saturatedFloorCastToSmallint(@SqlType(StandardTypes.BIGINT) long value)
    {
        return Shorts.saturatedCast(value);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long saturatedFloorCastToTinyint(@SqlType(StandardTypes.BIGINT) long value)
    {
        return SignedBytes.saturatedCast(value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.BIGINT) long value) throws AnalysisException {
        try {
            return Shorts.checkedCast(value);
        }
        catch (IllegalArgumentException e) {
            throw new AnalysisException("Out of range for smallint: " + value);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.BIGINT) long value) throws AnalysisException {
        try {
            return SignedBytes.checkedCast(value);
        }
        catch (IllegalArgumentException e) {
            throw new AnalysisException("Out of range for tinyint: " + value);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.BIGINT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DECIMAL)
    public static String castToDecimal(@SqlType(StandardTypes.DECIMAL) long value) {
        return String.valueOf(value);
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static String castToVarchar(@SqlType(StandardTypes.BIGINT) long value)
    {
        return String.valueOf(value);
    }

    /*
    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.BIGINT) long value)
    {
        return AbstractLongType.hash(value);
    }

     */
}
