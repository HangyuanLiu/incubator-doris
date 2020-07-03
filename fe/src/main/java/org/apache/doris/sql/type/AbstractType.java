package org.apache.doris.sql.type;

public abstract class AbstractType
        implements Type {
    private final TypeSignature signature;

    protected AbstractType(TypeSignature signature)
    {
        this.signature = signature;
    }

    @Override
    public final TypeSignature getTypeSignature()
    {
        return signature;
    }

    @Override
    public String getDisplayName()
    {
        return signature.toString();
    }
}