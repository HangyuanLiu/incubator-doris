package org.apache.doris.sql.metadata;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.sql.type.UnknownType;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class DorisMetadata implements ConnectorMetadata{

    private final Catalog dorisCatalog;

    public DorisMetadata(Catalog dorisCatalog)
    {
        this.dorisCatalog = requireNonNull(dorisCatalog, "client is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schema) {
        return true;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        Table table = dorisCatalog.getDb(tableName.getSchemaName()).getTable(tableName.getTableName());
        return new DorisTableHandle(tableName.getSchemaName(), tableName.getTableName(), table);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) tableHandle;

        Database database = dorisCatalog.getDb(dorisTableHandle.getSchemaName());
        Table table = database.getTable(dorisTableHandle.getTableName());
        List<Column> columnList = table.getBaseSchema();

        HashMap<String, ColumnHandle> columnHandles = new HashMap<>();
        for(Column column : columnList) {
            columnHandles.put(column.getName(), new DorisColumnHandle(column.getName(), UnknownType.UNKNOWN, 0));
        }

        System.out.println("getColumnHandles : " + columnHandles);

        return columnHandles;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) tableHandle;
        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);

        ArrayList<ColumnMetadata> arrayList = new ArrayList<>();
        columnHandles.forEach((key, value) -> arrayList.add(
                new ColumnMetadata(((DorisColumnHandle) value).getColumnName(), ((DorisColumnHandle) value).getColumnType())
        ));
        return new ConnectorTableMetadata(new SchemaTableName(dorisTableHandle.getSchemaName(), dorisTableHandle.getTableName()), arrayList);
    }
}
