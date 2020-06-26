package org.apache.doris.sql.type;

import java.util.Locale;
import java.util.Objects;

public class TypeSignature {
    private final String base;

    public TypeSignature(String type) {
        this.base = type;
    }

    public String getBase() {
        return base;
    }

    @Override
    public boolean equals(Object o)
    {
        TypeSignature other = (TypeSignature) o;
        return Objects.equals(this.base.toLowerCase(Locale.ENGLISH), other.base.toLowerCase(Locale.ENGLISH));
    }
    @Override
    public int hashCode()
    {
        return Objects.hash(base.toLowerCase(Locale.ENGLISH));
    }
}