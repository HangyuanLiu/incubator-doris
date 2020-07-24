package org.apache.doris.sql.type;

import java.math.BigInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.doris.sql.type.DecimalType.createDecimalType;

public final class Decimals {
    private Decimals() {
    }

    public static final int MAX_PRECISION = 38;
    public static final int MAX_SHORT_PRECISION = 18;

    private static final Pattern DECIMAL_PATTERN = Pattern.compile("(\\+?|-?)((0*)(\\d*))(\\.(\\d*))?");

    public static DecimalParseResult parse(String stringValue)
    {
        return parse(stringValue, false);
    }

    private static DecimalParseResult parse(String stringValue, boolean includeLeadingZerosInPrecision)
    {
        Matcher matcher = DECIMAL_PATTERN.matcher(stringValue);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid decimal value '" + stringValue + "'");
        }

        String sign = getMatcherGroup(matcher, 1);
        if (sign.isEmpty()) {
            sign = "+";
        }
        String leadingZeros = getMatcherGroup(matcher, 3);
        String integralPart = getMatcherGroup(matcher, 4);
        String fractionalPart = getMatcherGroup(matcher, 6);

        if (leadingZeros.isEmpty() && integralPart.isEmpty() && fractionalPart.isEmpty()) {
            throw new IllegalArgumentException("Invalid decimal value '" + stringValue + "'");
        }

        int scale = fractionalPart.length();
        int precision;
        if (includeLeadingZerosInPrecision) {
            precision = leadingZeros.length() + integralPart.length() + scale;
        }
        else {
            precision = integralPart.length() + scale;
            if (precision == 0) {
                precision = 1;
            }
        }

        String unscaledValue = sign + leadingZeros + integralPart + fractionalPart;
        Object value;
        if (precision <= MAX_SHORT_PRECISION) {
            value = Long.parseLong(unscaledValue);
        }
        else {
            //FIXME
            value = 0;
            //value = encodeUnscaledValue(new BigInteger(unscaledValue));
        }
        return new DecimalParseResult(value, createDecimalType(precision, scale));
    }

    private static String getMatcherGroup(Matcher matcher, int group)
    {
        String groupValue = matcher.group(group);
        if (groupValue == null) {
            groupValue = "";
        }
        return groupValue;
    }
}