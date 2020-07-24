package org.apache.doris.sql.type;

public abstract class AbstractIntType
        extends AbstractType
        implements FixedWidthType {
    protected AbstractIntType(TypeSignature signature) {
        super(signature);
    }

    @Override
    public final int getFixedSize() {
        return Integer.BYTES;
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