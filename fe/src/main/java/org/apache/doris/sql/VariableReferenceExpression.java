package org.apache.doris.sql;

import org.apache.doris.sql.type.Type;

public class VariableReferenceExpression {
    private final String name;
    private final Type type;

    public VariableReferenceExpression(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }
}
