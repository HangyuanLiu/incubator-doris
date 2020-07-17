package org.apache.doris.sql.metadata;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedFunctionName {
    // TODO: Move out this class. Ideally this class should not be in presto-common module.

    private final CatalogSchemaName functionNamespace;
    private final String functionName;

    public QualifiedFunctionName(CatalogSchemaName functionNamespace, String functionName)
    {
        this.functionNamespace = functionNamespace;
        this.functionName = requireNonNull(functionName, "name is null").toLowerCase(ENGLISH);
    }

    public static QualifiedFunctionName of(CatalogSchemaName functionNamespace, String name)
    {
        return new QualifiedFunctionName(functionNamespace, name);
    }

    public CatalogSchemaName getFunctionNamespace()
    {
        return functionNamespace;
    }

    // TODO: Examine all callers to limit the usage of the method
    public String getFunctionName()
    {
        return functionName;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QualifiedFunctionName o = (QualifiedFunctionName) obj;
        return Objects.equals(functionNamespace, o.functionNamespace) &&
                Objects.equals(functionName, o.functionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionNamespace, functionName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return functionNamespace + "." + functionName;
    }
}