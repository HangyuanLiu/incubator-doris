package org.apache.doris.sql.tree;

import java.util.Optional;

public abstract class Relation
        extends Node
{
    String s;

    protected Relation(Optional<NodeLocation> location)
    {
        super(location);
        s = "Hello world";
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRelation(this, context);
    }
}
