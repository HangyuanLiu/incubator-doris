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

    @Override
    public String toString()
    {
        return getTypeSignature().toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return this.getTypeSignature().equals(((Type) o).getTypeSignature());
    }
}