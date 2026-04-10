package com.datamelt.utilities.duckdb;

/**
 * Represents a warning or error encountered during DDL generation.
 *
 * WARNING severity: the DDL was generated with a fallback type that may not be optimal
 *                   but is unlikely to be incorrect (e.g. tuple-style array, object without properties).
 * ERROR severity:   the DDL was generated with a fallback type that is likely incorrect
 *                   (e.g. unresolvable $ref, unknown type, allOf with no common type).
 */
public class DdlWarning
{
    public enum Severity { WARNING, ERROR }

    private final String   field;
    private final String   message;
    private final Severity severity;

    public DdlWarning(String field, String message, Severity severity)
    {
        this.field    = field;
        this.message  = message;
        this.severity = severity;
    }

    public String   getField()    { return field;    }
    public String   getMessage()  { return message;  }
    public Severity getSeverity() { return severity; }

    @Override
    public String toString()
    {
        return "[%s] field '%s': %s".formatted(severity, field, message);
    }
}