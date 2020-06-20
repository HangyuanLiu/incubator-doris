package org.apache.doris.sql.type;

public interface Type
{

    /**
     * Gets the name of this type which must be case insensitive globally unique.
     * The name of a user defined type must be a legal identifier in Presto.
     */
    TypeSignature getTypeSignature();
    /**
     * True if the type supports equalTo and hash.
     */
    boolean isComparable();
}
