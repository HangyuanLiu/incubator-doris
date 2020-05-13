package org.apache.doris.sql.metadata;

import java.util.Map;

public interface ConnectorMetadata {

    /**
     * Checks if a schema exists. The connector may have schemas that exist
     * but are not enumerable via {@link #listSchemaNames}.
     */
    boolean schemaExists(ConnectorSession session, String schemaName);

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     */
    ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName);

    /**
     * Gets all of the columns on the specified table, or an empty map if the columns can not be enumerated.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle);

    /**
     * Return the metadata for the specified table handle.
     *
     * @throws RuntimeException if table handle is no longer valid
     */
    ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table);
}
