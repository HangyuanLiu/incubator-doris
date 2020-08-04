package org.apache.doris.sql.metadata;;

import org.apache.doris.catalog.Table;

import static java.util.Objects.requireNonNull;

public final class DorisTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;
    private final Table table;

    public DorisTableHandle(
            String schemaName,
            String tableName,
            Table table)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.table = table;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public Table getTable() {
        return table;
    }

    @Override
    public String toString() {
        return tableName;
    }
}
