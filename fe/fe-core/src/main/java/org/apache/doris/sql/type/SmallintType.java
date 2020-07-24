package org.apache.doris.sql.type;

public final class SmallintType
        extends AbstractType
        implements FixedWidthType {
    public static final SmallintType SMALLINT = new SmallintType();

    private SmallintType() {
        super(new TypeSignature(StandardTypes.SMALLINT));
    }

    @Override
    public int getFixedSize() {
        return Short.BYTES;
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