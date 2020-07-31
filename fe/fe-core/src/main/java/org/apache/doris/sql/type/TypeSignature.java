package org.apache.doris.sql.type;

import com.baidu.jprotobuf.com.squareup.protoparser.DataType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.NotImplementedException;

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
            case StandardTypes.BOOLEAN:
                return ScalarType.BOOLEAN;
            case StandardTypes.TINYINT:
                return ScalarType.TINYINT;
            case StandardTypes.INTEGER:
            case StandardTypes.INTERVAL_DAY_TO_SECOND:
                return ScalarType.INT;
            case StandardTypes.BIGINT:
                return ScalarType.BIGINT;
            case StandardTypes.DOUBLE:
                return ScalarType.DOUBLE;
            case "char":
                return ScalarType.CHAR;
            case "varchar":
                return ScalarType.VARCHAR;
            case "date":
                return ScalarType.DATE;
            case "timestamp":
                return ScalarType.DATETIME;
            case "decimal":
                return ScalarType.DECIMALV2;
            case "unknown":
                return ScalarType.NULL;

            case "day":
            case "month":
            case "year":
                return ScalarType.INT;
            default:
                throw new UnsupportedOperationException(this.getBase() +" not yet implemented");
        }
    }

    public static TypeSignature create(org.apache.doris.catalog.Type dorisType) {
        switch (dorisType.getPrimitiveType()) {
            case TINYINT:
                return new TypeSignature(StandardTypes.TINYINT);
            case INT:
                return new TypeSignature(StandardTypes.INTEGER);
            case BIGINT:
                return new TypeSignature(StandardTypes.BIGINT);
            case DOUBLE:
                return new TypeSignature(StandardTypes.DOUBLE);
            case CHAR:
                return new TypeSignature(StandardTypes.CHAR);
            case VARCHAR:
                return new TypeSignature(StandardTypes.VARCHAR);
            case DATE:
                return new TypeSignature(StandardTypes.DATE);
            case DATETIME:
                return new TypeSignature(StandardTypes.TIMESTAMP);
            case BOOLEAN:
                return new TypeSignature(StandardTypes.BOOLEAN);
            case DECIMALV2:
                return new TypeSignature(StandardTypes.DECIMAL);
            default:
                throw new UnsupportedOperationException(dorisType.toString() + " not yet implemented");
        }
    }

    @Override
    public String toString(){
        return getBase();
        /*
        try {
            throw new NotImplementedException("toString is no implement");
        } catch (NotImplementedException ex) {
            ex.printStackTrace();
        }
        return "Not Implement";

         */
    }
}