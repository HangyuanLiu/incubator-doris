package org.apache.doris.sql.type;

public final class TinyintType
        extends AbstractType
        implements FixedWidthType {
    public static final TinyintType TINYINT = new TinyintType();

    private TinyintType() {
        super(new TypeSignature(StandardTypes.TINYINT));
    }

    @Override
    public int getFixedSize() {
        return Byte.BYTES;
    }

    @Override
    public boolean isComparable() {
        return true;
    }

    @Override
    public boolean isOrderable() {
        return true;
    }
}