package org.apache.doris.sql.type;

public final class UnknownType
        extends AbstractType
        implements FixedWidthType
{
    public static final UnknownType UNKNOWN = new UnknownType();
    @Override
    public int getFixedSize() {
        return 0;
    }

    @Override
    public boolean isComparable() {
        return false;
    }
}
