package org.apache.doris.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class NaturalJoin
        extends JoinCriteria
{
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        return (obj != null) && (getClass() == obj.getClass());
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }

    @Override
    public List<Node> getNodes()
    {
        return ImmutableList.of();
    }
}
