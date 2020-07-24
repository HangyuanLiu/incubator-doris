package org.apache.doris.sql.type;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.apache.doris.sql.type.Decimals.MAX_PRECISION;
import static org.apache.doris.sql.type.Decimals.MAX_SHORT_PRECISION;

public class DecimalType
        extends AbstractType
        implements FixedWidthType {
    public static final int DEFAULT_SCALE = 0;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;

    public static DecimalType createDecimalType(int precision, int scale) {
            return new DecimalType(precision, scale);
    }

    public static DecimalType createDecimalType(int precision) {
        return createDecimalType(precision, DEFAULT_SCALE);
    }

    public static DecimalType createDecimalType() {
        return createDecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    private final int precision;
    private final int scale;

    DecimalType(int precision, int scale) {
        super(new TypeSignature(StandardTypes.DECIMAL, buildTypeParameters(precision, scale)));
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public final boolean isComparable() {
        return true;
    }

    @Override
    public final boolean isOrderable() {
        return true;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public int getFixedSize() {
        return 8;
    }

    private static List<TypeSignatureParameter> buildTypeParameters(int precision, int scale)
    {
        List<TypeSignatureParameter> typeParameters = new ArrayList<>();
        typeParameters.add(TypeSignatureParameter.of(precision));
        typeParameters.add(TypeSignatureParameter.of(scale));
        return unmodifiableList(typeParameters);
    }
}