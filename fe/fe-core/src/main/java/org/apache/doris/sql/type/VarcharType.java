package org.apache.doris.sql.type;

import static java.util.Collections.singletonList;

public final class VarcharType
        extends AbstractVariableWidthType {
    public static final int UNBOUNDED_LENGTH = Integer.MAX_VALUE;
    public static final int MAX_LENGTH = Integer.MAX_VALUE - 1;
    public static final VarcharType VARCHAR = new VarcharType(UNBOUNDED_LENGTH);

    public static VarcharType createUnboundedVarcharType() {
        return VARCHAR;
    }

    public static VarcharType createVarcharType(int length) {
        if (length > MAX_LENGTH || length < 0) {
            // Use createUnboundedVarcharType for unbounded VARCHAR.
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        return new VarcharType(length);
    }

    public static TypeSignature getParametrizedVarcharSignature(String param) {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of(param));
    }

    private final int length;

    private VarcharType(int length) {
        super(
                new TypeSignature(
                        StandardTypes.VARCHAR,
                        singletonList(TypeSignatureParameter.of((long) length))));

        if (length < 0) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        this.length = length;
    }

    @Deprecated
    public int getLength() {
        return length;
    }

    public int getLengthSafe() {
        if (isUnbounded()) {
            throw new IllegalStateException("Cannot get size of unbounded VARCHAR.");
        }
        return length;
    }

    public boolean isUnbounded() {
        return length == UNBOUNDED_LENGTH;
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