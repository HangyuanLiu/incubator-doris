package org.apache.doris.sql.tree;

import java.util.Optional;

public abstract class QueryBody
        extends Relation
{
    protected QueryBody(Optional<NodeLocation> location)
    {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQueryBody(this, context);
    }
}