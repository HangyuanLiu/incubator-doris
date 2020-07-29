package org.apache.doris.sql.util;

public class DataSize
        implements Comparable<DataSize> {

    private double value;

    public long toBytes()
    {
        return (long) value;
    }

    @Override
    public int compareTo(DataSize o) {
        return 0;
    }
}