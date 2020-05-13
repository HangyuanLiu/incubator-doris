package org.apache.doris.sql.metadata;

import com.google.common.collect.ImmutableMap;
import org.apache.doris.catalog.Catalog;

import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataManager implements Metadata {
    //目前只有DorisMetadata，未来可以拓展
    private ConnectorMetadata metadata;

    public MetadataManager() {
        this.metadata = new DorisMetadata(Catalog.getInstance());
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema) {
        return false;
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table) {
        requireNonNull(table, "table is null");

        //CatalogMetadata catalogMetadata = catalog.get();
        ConnectorId connectorId = new ConnectorId("doris");
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session.toConnectorSession(connectorId), table.asSchemaTableName());
        if (tableHandle != null) {
            return Optional.of(new TableHandle(connectorId, tableHandle));
        }
        return Optional.empty();
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle) {
        ConnectorId connectorId = new ConnectorId("doris");
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
        return new TableMetadata(tableMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle) {
        ConnectorId connectorId = tableHandle.getConnectorId();
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());

        ImmutableMap.Builder<String, ColumnHandle> map = ImmutableMap.builder();
        for (Map.Entry<String, ColumnHandle> mapEntry : handles.entrySet()) {
            map.put(mapEntry.getKey().toLowerCase(ENGLISH), mapEntry.getValue());
        }
        return map.build();
    }
}
