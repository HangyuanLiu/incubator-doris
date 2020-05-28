package org.apache.doris.sql.type;

import org.apache.doris.sql.type.AbstractLongType;

public final class BigintType
        extends AbstractLongType
{
    public static final BigintType BIGINT = new BigintType();

    @Override
    public int getFixedSize() {
        return 0;
    }

    @Override
    public boolean isComparable() {
        return false;
    }
}
