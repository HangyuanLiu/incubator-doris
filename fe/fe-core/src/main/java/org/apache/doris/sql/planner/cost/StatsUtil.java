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
package org.apache.doris.sql.planner.cost;

import org.apache.doris.sql.InterpretedFunctionInvoker;
import org.apache.doris.sql.metadata.ConnectorSession;
import org.apache.doris.sql.metadata.FunctionHandle;
import org.apache.doris.sql.metadata.FunctionManager;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.metadata.Session;
import org.apache.doris.sql.planner.statistics.ColumnStatistics;
import org.apache.doris.sql.planner.statistics.TableStatistics;
import org.apache.doris.sql.type.BigintType;
import org.apache.doris.sql.type.BooleanType;
import org.apache.doris.sql.type.DateType;
import org.apache.doris.sql.type.DecimalType;
import org.apache.doris.sql.type.DoubleType;
import org.apache.doris.sql.type.IntegerType;
import org.apache.doris.sql.type.SmallintType;
import org.apache.doris.sql.type.TinyintType;
import org.apache.doris.sql.type.Type;

import java.util.OptionalDouble;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.type.OperatorType.CAST;

final class StatsUtil
{
    private StatsUtil() {}

    static OptionalDouble toStatsRepresentation(Metadata metadata, Session session, Type type, Object value)
    {
        return toStatsRepresentation(metadata.getFunctionManager(), session.toConnectorSession(), type, value);
    }

    static OptionalDouble toStatsRepresentation(FunctionManager functionManager, ConnectorSession session, Type type, Object value)
    {
        requireNonNull(value, "value is null");

        if (convertibleToDoubleWithCast(type)) {
            InterpretedFunctionInvoker functionInvoker = new InterpretedFunctionInvoker(functionManager);
            FunctionHandle cast = functionManager.lookupCast(type.getTypeSignature(), DoubleType.DOUBLE.getTypeSignature());

            return OptionalDouble.of((double) functionInvoker.invoke(cast, singletonList(value)));
        }

        if (DateType.DATE.equals(type)) {
            return OptionalDouble.of(((Long) value).doubleValue());
        }

        return OptionalDouble.empty();
    }

    private static boolean convertibleToDoubleWithCast(Type type)
    {
        //FIXME
        return false;
        /*
        return type instanceof DecimalType
                || DoubleType.DOUBLE.equals(type)
                || BigintType.BIGINT.equals(type)
                || IntegerType.INTEGER.equals(type)
                || SmallintType.SMALLINT.equals(type)
                || TinyintType.TINYINT.equals(type)
                || BooleanType.BOOLEAN.equals(type);

         */
    }

    public static VariableStatsEstimate toVariableStatsEstimate(TableStatistics tableStatistics, ColumnStatistics columnStatistics)
    {
        double nullsFraction = columnStatistics.getNullsFraction().getValue();
        double nonNullRowsCount = tableStatistics.getRowCount().getValue() * (1.0 - nullsFraction);
        double averageRowSize = nonNullRowsCount == 0 ? 0 : columnStatistics.getDataSize().getValue() / nonNullRowsCount;
        VariableStatsEstimate.Builder result = VariableStatsEstimate.builder();
        result.setNullsFraction(nullsFraction);
        result.setDistinctValuesCount(columnStatistics.getDistinctValuesCount().getValue());
        result.setAverageRowSize(averageRowSize);
        columnStatistics.getRange().ifPresent(range -> {
            result.setLowValue(range.getMin());
            result.setHighValue(range.getMax());
        });
        return result.build();
    }
}
