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
package org.apache.doris.sql.planner;

import org.apache.doris.sql.metadata.ConnectorSession;
import org.apache.doris.sql.metadata.Metadata;
import org.apache.doris.sql.relation.ConstantExpression;
import org.apache.doris.sql.type.BigintType;
import org.apache.doris.sql.type.BooleanType;
import org.apache.doris.sql.type.CharType;
import org.apache.doris.sql.type.DateType;
import org.apache.doris.sql.type.DecimalType;
import org.apache.doris.sql.type.Decimals;
import org.apache.doris.sql.type.DoubleType;
import org.apache.doris.sql.type.IntegerType;
import org.apache.doris.sql.type.SmallintType;
import org.apache.doris.sql.type.TimestampType;
import org.apache.doris.sql.type.TinyintType;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.VarcharType;
import org.apache.doris.sql.analyzer.SemanticException;
import org.apache.doris.sql.tree.AstVisitor;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.CharLiteral;
import org.apache.doris.sql.tree.DecimalLiteral;
import org.apache.doris.sql.tree.DoubleLiteral;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.GenericLiteral;
import org.apache.doris.sql.tree.IntervalLiteral;
import org.apache.doris.sql.tree.Literal;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import static org.apache.doris.sql.type.VarcharType.VARCHAR;
import static org.apache.doris.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static org.apache.doris.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static org.apache.doris.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public final class LiteralInterpreter
{
    private LiteralInterpreter() {}

    public static Object evaluate(Metadata metadata, ConnectorSession session, Expression node)
    {
        if (!(node instanceof Literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }
        //return new LiteralVisitor(metadata).process(node, session);
        return null;
    }
    /*
    public static Object evaluate(ConnectorSession session, ConstantExpression node)
    {
        Type type = node.getType();
        SqlFunctionProperties properties = session.getSqlFunctionProperties();

        if (node.getValue() == null) {
            return null;
        }
        if (type instanceof BooleanType) {
            return node.getValue();
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            return node.getValue();
        }
        if (type instanceof DoubleType) {
            return node.getValue();
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                checkState(node.getValue() instanceof Long);
                return decodeDecimal(BigInteger.valueOf((long) node.getValue()), decimalType);
            }
            checkState(node.getValue() instanceof Slice);
            Slice value = (Slice) node.getValue();
            return decodeDecimal(decodeUnscaledValue(value), decimalType);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return ((Slice) node.getValue()).toStringUtf8();
        }
        if (type instanceof VarbinaryType) {
            return new SqlVarbinary(((Slice) node.getValue()).getBytes());
        }
        if (type instanceof DateType) {
            return new SqlDate(((Long) node.getValue()).intValue());
        }
        if (type instanceof TimeType) {
            if (properties.isLegacyTimestamp()) {
                return new SqlTime((long) node.getValue(), properties.getTimeZoneKey());
            }
            return new SqlTime((long) node.getValue());
        }
        if (type instanceof TimestampType) {
            try {
                if (properties.isLegacyTimestamp()) {
                    return new SqlTimestamp((long) node.getValue(), properties.getTimeZoneKey());
                }
                return new SqlTimestamp((long) node.getValue());
            }
            catch (RuntimeException e) {
                throw new PrestoException(GENERIC_USER_ERROR, format("'%s' is not a valid timestamp literal", (String) node.getValue()));
            }
        }
        if (type instanceof IntervalDayTimeType) {
            return new SqlIntervalDayTime((long) node.getValue());
        }
        if (type instanceof IntervalYearMonthType) {
            return new SqlIntervalYearMonth(((Long) node.getValue()).intValue());
        }
        if (type.getJavaType().equals(Slice.class)) {
            // DO NOT ever remove toBase64. Calling toString directly on Slice whose base is not byte[] will cause JVM to crash.
            return "'" + VarbinaryFunctions.toBase64((Slice) node.getValue()).toStringUtf8() + "'";
        }

        // We should not fail at the moment; just return the raw value (block, regex, etc) to the user
        return node.getValue();
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    private static class LiteralVisitor
            extends AstVisitor<Object, ConnectorSession>
    {
        private final Metadata metadata;
        private final InterpretedFunctionInvoker functionInvoker;

        private LiteralVisitor(Metadata metadata)
        {
            this.metadata = metadata;
            this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionManager());
        }

        @Override
        protected Object visitLiteral(Literal node, ConnectorSession session)
        {
            throw new UnsupportedOperationException("Unhandled literal type: " + node);
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Long visitLongLiteral(LongLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Double visitDoubleLiteral(DoubleLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Object visitDecimalLiteral(DecimalLiteral node, ConnectorSession context)
        {
            return Decimals.parse(node.getValue()).getObject();
        }

        @Override
        protected Slice visitStringLiteral(StringLiteral node, ConnectorSession session)
        {
            return node.getSlice();
        }

        @Override
        protected Object visitCharLiteral(CharLiteral node, ConnectorSession context)
        {
            return node.getSlice();
        }

        @Override
        protected Object visitGenericLiteral(GenericLiteral node, ConnectorSession session)
        {
            Type type = metadata.getType(parseTypeSignature(node.getType()));
            if (type == null) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            if (JSON.equals(type)) {
                FunctionHandle functionHandle = metadata.getFunctionManager().lookupFunction("json_parse", fromTypes(VARCHAR));
                return functionInvoker.invoke(functionHandle, session.getSqlFunctionProperties(), ImmutableList.of(utf8Slice(node.getValue())));
            }

            try {
                FunctionHandle functionHandle = metadata.getFunctionManager().lookupCast(CAST, VARCHAR.getTypeSignature(), type.getTypeSignature());
                return functionInvoker.invoke(functionHandle, session.getSqlFunctionProperties(), ImmutableList.of(utf8Slice(node.getValue())));
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "No literal form for type %s", type);
            }
        }

        @Override
        protected Long visitIntervalLiteral(IntervalLiteral node, ConnectorSession session)
        {
            if (node.isYearToMonth()) {
                return node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                return node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
        }

        @Override
        protected Object visitNullLiteral(NullLiteral node, ConnectorSession session)
        {
            return null;
        }
    }

     */
}
