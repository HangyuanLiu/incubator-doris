package org.apache.doris.sql.relation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.doris.sql.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class VariableReferenceExpression extends RowExpression
        implements Comparable<VariableReferenceExpression> {
    private final String name;
    private final Type type;

    @JsonCreator
    public VariableReferenceExpression(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitVariableReference(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        VariableReferenceExpression other = (VariableReferenceExpression) obj;
        return Objects.equals(this.name, other.name) && Objects.equals(this.type, other.type);
    }

    @Override
    public int compareTo(VariableReferenceExpression o)
    {
        int nameComparison = name.compareTo(o.name);
        if (nameComparison != 0) {
            return nameComparison;
        }
        return type.getTypeSignature().toString().compareTo(o.type.getTypeSignature().toString());
    }
}
