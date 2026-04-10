package com.datamelt.utilities.jsonschema.validate;

public class Violation
{
    public enum Severity { WARNING, ERROR }

    private final long     recordIndex;
    private final String   field;
    private final String   message;
    private final Severity severity;

    public Violation(long recordIndex, String field, String message, Severity severity)
    {
        this.recordIndex = recordIndex;
        this.field       = field;
        this.message     = message;
        this.severity    = severity;
    }

    // Convenience constructor — defaults to ERROR for backwards compatibility
    public Violation(long recordIndex, String field, String message)
    {
        this(recordIndex, field, message, Severity.ERROR);
    }

    public long     getRecordIndex() { return recordIndex; }
    public String   getField()       { return field;       }
    public String   getMessage()     { return message;     }
    public Severity getSeverity()    { return severity;    }

    public String toString()
    {
        return "[%s] [record #%d] field '%s': %s"
                .formatted(severity, recordIndex + 1, field, message);
    }
}