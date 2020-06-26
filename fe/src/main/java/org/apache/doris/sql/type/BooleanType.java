package org.apache.doris.sql.type;

import scala.reflect.api.StandardDefinitions;

public final class BooleanType
        extends AbstractType
        implements FixedWidthType
{
    public static final BooleanType BOOLEAN = new BooleanType();

    private BooleanType()
    {
        super(new TypeSignature("BOOLEAN"));
        //super(parseTypeSignature(StandardDefinitions.StandardTypes.BOOLEAN), boolean.class);
    }

    @Override
    public int getFixedSize() {
        return 1;
    }

    @Override
    public boolean isComparable() {
        return true;
    }
}