package org.apache.doris.sql.type;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;

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

    public List<TypeSignatureParameter> getParameters()
    {
        return parameters;
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

    public ScalarType toDorisType() {
        switch (this.getBase().toLowerCase()) {
            case "bigint":
                return ScalarType.BIGINT;
            case "int":
                return ScalarType.INT;
            case "double":
                return ScalarType.DOUBLE;
            case "varchar":
                return ScalarType.VARCHAR;
            default:
                throw new UnsupportedOperationException(this.getBase() +" not yet implemented");
        }
    }

    public static TypeSignature create(org.apache.doris.catalog.Type dorisType) {
        switch (dorisType.getPrimitiveType()) {
            case BIGINT:
                return new TypeSignature(StandardTypes.BIGINT);
            case DOUBLE:
                return new TypeSignature(StandardTypes.DOUBLE);
            case VARCHAR:
                return new TypeSignature(StandardTypes.VARCHAR);
            default:
                throw new UnsupportedOperationException(dorisType.toString() + " not yet implemented");
        }
    }
}