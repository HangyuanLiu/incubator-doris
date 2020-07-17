package org.apache.doris.sql.type;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public final class CharType
        extends AbstractVariableWidthType {
    public static final int MAX_LENGTH = 65_536;

    private final int length;

    public static CharType createCharType(long length) {
        return new CharType(length);
    }

    private CharType(long length) {
        super(
                new TypeSignature(
                        StandardTypes.CHAR,
                        singletonList(TypeSignatureParameter.of(length))));

        if (length < 0 || length > MAX_LENGTH) {
            //throw new InvalidFunctionArgumentException(format("CHAR length scale must be in range [0, %s]", MAX_LENGTH));
        }
        this.length = (int) length;
    }

    public int getLength() {
        return length;
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