package org.apache.doris.sql.analyzer;


import org.apache.doris.sql.metadata.Type;

import static java.util.Objects.requireNonNull;

public class ResolvedField
{
    private final Scope scope;
    private final Field field;
    private final int hierarchyFieldIndex;
    private final int relationFieldIndex;
    private final boolean local;

    public ResolvedField(Scope scope, Field field, int hierarchyFieldIndex, int relationFieldIndex, boolean local)
    {
        this.scope = requireNonNull(scope, "scope is null");
        this.field = requireNonNull(field, "field is null");
        this.hierarchyFieldIndex = hierarchyFieldIndex;
        this.relationFieldIndex = relationFieldIndex;
        this.local = local;
    }

    public Type getType()
    {
        return field.getType();
    }

    public Scope getScope()
    {
        return scope;
    }

    public boolean isLocal()
    {
        return local;
    }

    public int getHierarchyFieldIndex()
    {
        return hierarchyFieldIndex;
    }

    public int getRelationFieldIndex()
    {
        return relationFieldIndex;
    }

    public Field getField()
    {
        return field;
    }
}
