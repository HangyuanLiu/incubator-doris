package org.apache.doris.sql.type;

public final class DoubleType
        extends AbstractType
        implements FixedWidthType {
    public static final DoubleType DOUBLE = new DoubleType();

    private DoubleType() {
        super(new TypeSignature(StandardTypes.DOUBLE));
    }

    @Override
    public final int getFixedSize() {
        return Double.BYTES;
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