package org.apache.doris.sql.type;

import org.apache.doris.sql.type.AbstractLongType;

public final class BigintType
        extends AbstractLongType
{
    public static final BigintType BIGINT = new BigintType();

    private BigintType()
    {
        super(new TypeSignature("BIGINT"));
        //super(parseTypeSignature(StandardTypes.BIGINT));
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == BIGINT;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
