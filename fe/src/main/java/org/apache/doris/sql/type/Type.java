package org.apache.doris.sql.type;

public interface Type
{
    /**
     * True if the type supports equalTo and hash.
     */
    boolean isComparable();
}
