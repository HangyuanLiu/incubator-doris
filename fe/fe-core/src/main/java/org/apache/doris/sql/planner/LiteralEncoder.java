package org.apache.doris.sql.planner;

import com.google.common.collect.ImmutableList;
import org.apache.doris.sql.relation.RowExpression;
import org.apache.doris.sql.tree.Expression;
import org.apache.doris.sql.type.Type;

import static java.util.Objects.requireNonNull;
import static org.apache.doris.sql.relational.Expressions.constant;
import static org.apache.doris.sql.relational.Expressions.constantNull;

public final class LiteralEncoder {
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
}