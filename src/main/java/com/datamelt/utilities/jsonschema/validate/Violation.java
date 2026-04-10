package com.datamelt.utilities.duckdb.jsonschema;

public class Violation
{
    private final long recordIndex;
    private final String field;
    private final String message;

    public Violation(long recordIndex, String field, String message)
    {
        this.recordIndex = recordIndex;
        this.field = field;
        this.message = message;
    }

    public long getRecordIndex()
    {
        return recordIndex;
    }

    public String getField()
    {
        return field;
    }

    public String getMessage()
    {
        return message;
    }

    public String toString() {
        return "[record #%d] field '%s': %s".formatted(recordIndex + 1, field, message);
    }
}
