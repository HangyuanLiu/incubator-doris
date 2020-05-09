package org.apache.doris.sql.type;

public final class BooleanType
        extends AbstractType
        implements FixedWidthType
{
    public static final BooleanType BOOLEAN = new BooleanType();

    @Override
    public int getFixedSize() {
        return 1;
    }

    @Override
    public boolean isComparable() {
        return true;
    }
}