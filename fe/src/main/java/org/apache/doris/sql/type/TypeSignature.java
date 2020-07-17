package org.apache.doris.sql.type;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class TypeSignature {
    private final String base;
    private final List<TypeSignatureParameter> parameters;

    public TypeSignature(String base, TypeSignatureParameter... parameters)
    {
        this(base, asList(parameters));
    }

    public TypeSignature(String base, List<TypeSignatureParameter> parameters)
    {
        this.base = base;
        this.parameters = unmodifiableList(new ArrayList<>(parameters));
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