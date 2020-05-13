package org.apache.doris.sql.metadata;

import java.util.List;

public class TableMetadata {
    private final ConnectorTableMetadata metadata;

    public TableMetadata(ConnectorTableMetadata metadata) {
        this.metadata = metadata;
    }

    public List<ColumnMetadata> getColumns()
    {
        return metadata.getColumns();
    }
}
