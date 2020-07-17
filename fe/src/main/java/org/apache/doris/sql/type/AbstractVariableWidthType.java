package org.apache.doris.sql.type;

public abstract class AbstractVariableWidthType
        extends AbstractType
        implements VariableWidthType {
    private static final int EXPECTED_BYTES_PER_ENTRY = 32;

    protected AbstractVariableWidthType(TypeSignature signature) {
        super(signature);
    }
}