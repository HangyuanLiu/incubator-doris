package org.apache.doris.sql.metadata;

import static java.util.Objects.requireNonNull;

public class ConnectorId {
    private final String catalogName;

    public ConnectorId(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        if (catalogName.isEmpty()) {
            throw new IllegalArgumentException("catalogName is empty");
        }
    }

    public String getCatalogName()
    {
        return catalogName;
    }
}
