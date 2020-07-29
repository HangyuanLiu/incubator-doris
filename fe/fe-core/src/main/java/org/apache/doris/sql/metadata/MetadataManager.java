package org.apache.doris.sql.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.sql.planner.statistics.ColumnStatistics;
import org.apache.doris.sql.planner.statistics.Estimate;
import org.apache.doris.sql.planner.statistics.TableStatistics;
import org.apache.doris.sql.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataManager implements Metadata {
    private final FunctionManager functions;
    private final TypeManager typeManager;
    //目前只有DorisMetadata，未来可以拓展
    private ConnectorMetadata metadata;

    public MetadataManager(
            TypeManager typeManager,
            FunctionManager functionManager,
            Catalog catalog) {
        this.typeManager = typeManager;
        this.functions = functionManager;
        this.metadata = new DorisMetadata(catalog, typeManager);
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

    @Override
    public FunctionManager getFunctionManager()
    {
        // TODO: transactional when FunctionManager is made transactional
        return functions;
    }

    @Override
    public TypeManager getTypeManager()
    {
        // TODO: make this transactional when we allow user defined types
        return typeManager;
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, List<ColumnHandle> columnHandles) {
        Map<ColumnHandle, ColumnStatistics> columnStatisticsMap = Maps.newHashMap();

        OlapTable olapTable = (OlapTable)(((DorisTableHandle)tableHandle.getConnectorHandle()).getTable());
        int rowCount = new Random().nextInt(10000);
        System.out.println(rowCount);
        return new TableStatistics(Estimate.of(rowCount), columnStatisticsMap);
    }
}
