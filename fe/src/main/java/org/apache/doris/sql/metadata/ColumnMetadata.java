package org.apache.doris.sql.metadata;

import org.apache.doris.sql.type.Type;

public class ColumnMetadata {
    private final String name;
    private final Type type;
    private final boolean nullable;

    public ColumnMetadata(String name, Type type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public boolean isHidden() {
        return false;
    }
}
