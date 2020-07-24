package org.apache.doris.sql.type;

public interface Type
{

    /**
     * Gets the name of this type which must be case insensitive globally unique.
     * The name of a user defined type must be a legal identifier in Presto.
     */
    TypeSignature getTypeSignature();

    /**
     * Returns the name of this type that should be displayed to end-users.
     */
    String getDisplayName();

    /**
     * True if the type supports equalTo and hash.
     */
    boolean isComparable();

    /**
     * True if the type supports compareTo.
     */
    boolean isOrderable();
}
