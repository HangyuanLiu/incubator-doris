package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.tree.ArithmeticUnaryExpression;
import org.apache.doris.sql.tree.BooleanLiteral;
import org.apache.doris.sql.tree.Cast;
import org.apache.doris.sql.tree.DecimalLiteral;
import org.apache.doris.sql.tree.DoubleLiteral;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.tree.FunctionCall;
import org.apache.doris.sql.tree.GenericLiteral;
import org.apache.doris.sql.tree.LongLiteral;
import org.apache.doris.sql.tree.NullLiteral;
import org.apache.doris.sql.tree.QualifiedName;
import org.apache.doris.sql.tree.StringLiteral;
import org.apache.doris.sql.type.CharType;
import org.apache.doris.sql.type.DecimalType;
import org.apache.doris.sql.type.Decimals;
import org.apache.doris.sql.type.Type;
import org.apache.doris.sql.type.VarcharType;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.math.BigDecimal;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relational.Expressions.constant;
import static org.apache.doris.sql.relational.Expressions.constantNull;
import static org.apache.doris.sql.type.BigintType.BIGINT;
import static org.apache.doris.sql.type.BooleanType.BOOLEAN;
import static org.apache.doris.sql.type.DateType.DATE;
import static org.apache.doris.sql.type.DoubleType.DOUBLE;
import static org.apache.doris.sql.type.IntegerType.INTEGER;
import static org.apache.doris.sql.type.UnknownType.UNKNOWN;
import static org.apache.doris.sql.type.SmallintType.SMALLINT;
import static org.apache.doris.sql.type.TinyintType.TINYINT;

public final class LiteralEncoder {

    public List<Expression> toExpressions(List<?> objects, List<? extends Type> types)
    {
        requireNonNull(objects, "objects is null");
        requireNonNull(types, "types is null");
        checkArgument(objects.size() == types.size(), "objects and types do not have the same size");

        ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
        for (int i = 0; i < objects.size(); i++) {
            Object object = objects.get(i);
            Type type = types.get(i);
            expressions.add(toExpression(object, type));
        }
        return expressions.build();
    }

    // Unlike toExpression, toRowExpression should be very straightforward given object is serializable
    public static RowExpression toRowExpression(Object object, Type type) {
        requireNonNull(type, "type is null");

        if (object instanceof RowExpression) {
            return (RowExpression) object;
        }

        if (object == null) {
            return constantNull(type);
        }

        return constant(object, type);
    }

    @Deprecated
    public Expression toExpression(Object object, Type type)
    {
        requireNonNull(type, "type is null");

        if (object instanceof Expression) {
            return (Expression) object;
        }

        if (object == null) {
            if (type.equals(UNKNOWN)) {
                return new NullLiteral();
            }
            return new Cast(new NullLiteral(), type.getTypeSignature().toString(), false, true);
        }

        if (type.equals(TINYINT)) {
            return new GenericLiteral("TINYINT", object.toString());
        }

        if (type.equals(SMALLINT)) {
            return new GenericLiteral("SMALLINT", object.toString());
        }

        if (type.equals(INTEGER)) {
            return new LongLiteral(object.toString());
        }

        if (type.equals(BIGINT)) {
            LongLiteral expression = new LongLiteral(object.toString());
            /*
            //FIXME
            if (expression.getValue() >= Integer.MIN_VALUE && expression.getValue() <= Integer.MAX_VALUE) {
                return new GenericLiteral("BIGINT", object.toString());
            }
             */
            return new LongLiteral(object.toString());
        }

        //checkArgument(Primitives.wrap(type.getJavaType()).isInstance(object), "object.getClass (%s) and type.getJavaType (%s) do not agree", object.getClass(), type.getJavaType());

        if (type.equals(DOUBLE)) {
            Double value = (Double) object;
            // WARNING: the ORC predicate code depends on NaN and infinity not appearing in a tuple domain, so
            // if you remove this, you will need to update the TupleDomainOrcPredicate
            // When changing this, don't forget about similar code for REAL below
            if (value.isNaN()) {
                return new FunctionCall(QualifiedName.of("nan"), ImmutableList.of());
            }
            if (value.equals(Double.NEGATIVE_INFINITY)) {
                return ArithmeticUnaryExpression.negative(new FunctionCall(QualifiedName.of("infinity"), ImmutableList.of()));
            }
            if (value.equals(Double.POSITIVE_INFINITY)) {
                return new FunctionCall(QualifiedName.of("infinity"), ImmutableList.of());
            }
            return new DoubleLiteral(object.toString());
        }

        if (type.equals(DATE)) {
            return new GenericLiteral("DATE", (String) object);
        }
        if (type instanceof DecimalType) {
            if (object instanceof BigDecimal) {
                String string = Decimals.toString(((BigDecimal) object).toString(), ((DecimalType) type).getScale());
                return new DecimalLiteral(string);
            } else {
                return new DecimalLiteral(String.valueOf(object));
            }
        }


        /*
        if (type instanceof DecimalType) {
            String string;
            if (isShortDecimal(type)) {
                string = Decimals.toString((long) object, ((DecimalType) type).getScale());
            }
            else {
                string = Decimals.toString((Slice) object, ((DecimalType) type).getScale());
            }
            return new Cast(new DecimalLiteral(string), type.getDisplayName());
        }

         */

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            StringLiteral stringLiteral = new StringLiteral((String) object);

            if (!varcharType.isUnbounded()) {
                return stringLiteral;
            }
            return new Cast(stringLiteral, type.getDisplayName(), false, true);
        }

        if (type instanceof CharType) {
            StringLiteral stringLiteral = new StringLiteral((String) object);
            return new Cast(stringLiteral, type.getDisplayName(), false, true);
        }

        if (type.equals(BOOLEAN)) {
            return new BooleanLiteral(object.toString());
        }
        /*




        Signature signature = getMagicLiteralFunctionSignature(type);
        if (object instanceof Slice) {
            // HACK: we need to serialize VARBINARY in a format that can be embedded in an expression to be
            // able to encode it in the plan that gets sent to workers.
            // We do this by transforming the in-memory varbinary into a call to from_base64(<base64-encoded value>)
            FunctionCall fromBase64 = new FunctionCall(QualifiedName.of("from_base64"), ImmutableList.of(new StringLiteral(VarbinaryFunctions.toBase64((Slice) object).toStringUtf8())));
            return new FunctionCall(QualifiedName.of(signature.getNameSuffix()), ImmutableList.of(fromBase64));
        }
        Expression rawLiteral = toExpression(object, typeForMagicLiteral(type));

        return new FunctionCall(QualifiedName.of(signature.getNameSuffix()), ImmutableList.of(rawLiteral));
         */
        return null;
    }

    public static boolean isSupportedLiteralType(Type type)
    {
        //return SUPPORTED_LITERAL_TYPES.contains(type.getJavaType());
        return true;
    }
}