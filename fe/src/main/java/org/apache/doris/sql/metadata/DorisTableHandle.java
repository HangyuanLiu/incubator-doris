package org.apache.doris.sql.metadata;;

import static java.util.Objects.requireNonNull;

public final class DorisTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;

    public DorisTableHandle(
            String schemaName,
            String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }
}
