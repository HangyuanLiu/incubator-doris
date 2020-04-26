package org.apache.doris.sql.analyzer;

import org.apache.doris.sql.tree.Node;

import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public class RelationId
{
    /**
     * Creates {@link RelationId} equal to any {@link RelationId} created from exactly the same source.
     */
    public static RelationId of(Node sourceNode)
    {
        return new RelationId(requireNonNull(sourceNode, "source cannot be null"));
    }

    /**
     * Creates {@link RelationId} equal only to itself
     */
    public static RelationId anonymous()
    {
        return new RelationId(null);
    }

    private final Node sourceNode;

    private RelationId(Node sourceNode)
    {
        this.sourceNode = sourceNode;
    }

    public boolean isAnonymous()
    {
        return sourceNode == null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RelationId that = (RelationId) o;
        return sourceNode != null && sourceNode == that.sourceNode;
    }

    @Override
    public int hashCode()
    {
        return identityHashCode(sourceNode);
    }

    @Override
    public String toString()
    {
        return "RelationId";
    }
}
