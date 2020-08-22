package org.apache.doris.sql.type;

import com.baidu.jprotobuf.com.squareup.protoparser.DataType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.NotImplementedException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Character.isDigit;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class TypeSignature {
    private final String base;
    private final List<TypeSignatureParameter> parameters;

    private static final Map<String, String> BASE_NAME_ALIAS_TO_CANONICAL =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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

    public static TypeSignature parseTypeSignature(String signature)
    {
        return parseTypeSignature(signature, new HashSet<>());
    }

    public static TypeSignature parseTypeSignature(String signature, Set<String> literalCalculationParameters)
    {
        if (!signature.contains("<") && !signature.contains("(")) {
            if (signature.equalsIgnoreCase(StandardTypes.VARCHAR)) {
                return VarcharType.createUnboundedVarcharType().getTypeSignature();
            }
            checkArgument(!literalCalculationParameters.contains(signature), "Bad type signature: '%s'");
            return new TypeSignature(canonicalizeBaseName(signature), new ArrayList<>());
        }
        /*
        if (signature.toLowerCase(Locale.ENGLISH).startsWith(StandardTypes.ROW + "(")) {
            return parseRowTypeSignature(signature, literalCalculationParameters);
        }
        */
        String baseName = null;
        List<TypeSignatureParameter> parameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            // TODO: remove angle brackets support once ROW<TYPE>(name) will be dropped
            // Angle brackets here are checked not for the support of ARRAY<> and MAP<>
            // but to correctly parse ARRAY(row<BIGINT, BIGINT>('a','b'))
            if (c == '(' || c == '<') {
                if (bracketCount == 0) {
                    verify(baseName == null, "Expected baseName to be null");
                    verify(parameterStart == -1, "Expected parameter start to be -1");
                    baseName = canonicalizeBaseName(signature.substring(0, i));
                    checkArgument(!literalCalculationParameters.contains(baseName), "Bad type signature: '%s'", signature);
                    parameterStart = i + 1;
                }
                bracketCount++;
            }
            else if (c == ')' || c == '>') {
                bracketCount--;
                checkArgument(bracketCount >= 0, "Bad type signature: '%s'", signature);
                if (bracketCount == 0) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignatureParameter(signature, parameterStart, i, literalCalculationParameters));
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return new TypeSignature(baseName, parameters);
                    }
                }
            }
            else if (c == ',') {
                if (bracketCount == 1) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignatureParameter(signature, parameterStart, i, literalCalculationParameters));
                    parameterStart = i + 1;
                }
            }
        }

        throw new IllegalArgumentException(format("Bad type signature: '%s'", signature));
    }

    private static TypeSignatureParameter parseTypeSignatureParameter(
            String signature,
            int begin,
            int end,
            Set<String> literalCalculationParameters)
    {
        String parameterName = signature.substring(begin, end).trim();
        if (isDigit(signature.charAt(begin))) {
            return TypeSignatureParameter.of(Long.parseLong(parameterName));
        }
        else if (literalCalculationParameters.contains(parameterName)) {
            return TypeSignatureParameter.of(parameterName);
        }
        else {
            return TypeSignatureParameter.of(parseTypeSignature(parameterName, literalCalculationParameters));
        }
    }

    private static String canonicalizeBaseName(String baseName)
    {
        String canonicalBaseName = BASE_NAME_ALIAS_TO_CANONICAL.get(baseName);
        if (canonicalBaseName == null) {
            return baseName;
        }
        return canonicalBaseName;
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

        TypeSignature other = (TypeSignature) o;

        return Objects.equals(this.base.toLowerCase(Locale.ENGLISH), other.base.toLowerCase(Locale.ENGLISH)) &&
                Objects.equals(this.parameters, other.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base.toLowerCase(Locale.ENGLISH), parameters);
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
            case StandardTypes.ROW:
                return ScalarType.VARCHAR;
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
        if (parameters.isEmpty()) {
            return base;
        }

        if (base.equalsIgnoreCase(StandardTypes.VARCHAR) &&
                (parameters.size() == 1) &&
                parameters.get(0).isLongLiteral() &&
                parameters.get(0).getLongLiteral() == VarcharType.UNBOUNDED_LENGTH) {
            return base;
        }

        if (base.equalsIgnoreCase(StandardTypes.VARBINARY)) {
            return base;
        }

        StringBuilder typeName = new StringBuilder(base);
        typeName.append("(").append(parameters.get(0));
        for (int i = 1; i < parameters.size(); i++) {
            typeName.append(",").append(parameters.get(i));
        }
        typeName.append(")");
        return typeName.toString();
    }
}