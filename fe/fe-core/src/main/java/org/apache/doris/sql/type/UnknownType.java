package org.apache.doris.sql.type;

public final class UnknownType
        extends AbstractType
        implements FixedWidthType
{
    public static final UnknownType UNKNOWN = new UnknownType();
    public static final String NAME = "unknown";

    protected UnknownType() {
        super(new TypeSignature(NAME));
    }

    @Override
    public int getFixedSize()
    {
        return Byte.BYTES;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }
}
