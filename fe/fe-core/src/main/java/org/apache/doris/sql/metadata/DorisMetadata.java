package org.apache.doris.sql.metadata;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.sql.type.BigintType;
import org.apache.doris.sql.type.TypeManager;
import org.apache.doris.sql.type.TypeSignature;
import org.apache.doris.sql.type.UnknownType;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class DorisMetadata implements ConnectorMetadata{

    private final Catalog dorisCatalog;
    private final TypeManager typeManager;

    public DorisMetadata(Catalog dorisCatalog, TypeManager typeManager)
    {
        this.dorisCatalog = requireNonNull(dorisCatalog, "client is null");
        this.typeManager = typeManager;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schema) {
        return true;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        Database db = dorisCatalog.getDb("default_cluster:" + tableName.getSchemaName());
        Table table = db.getTable(tableName.getTableName());
        return new DorisTableHandle(tableName.getSchemaName(), tableName.getTableName(), table);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        DorisTableHandle dorisTableHandle = (DorisTableHandle) tableHandle;

        Database database = dorisCatalog.getDb("default_cluster:" + dorisTableHandle.getSchemaName());
        Table table = database.getTable(dorisTableHandle.getTableName());
        List<Column> columnList = table.getBaseSchema();

        HashMap<String, ColumnHandle> columnHandles = new HashMap<>();
        for(Column column : columnList) {

            columnHandles.put(column.getName(), new DorisColumnHandle(column.getName(),
                    typeManager.getType(TypeSignature.create(column.getType())), 0));
        }

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
