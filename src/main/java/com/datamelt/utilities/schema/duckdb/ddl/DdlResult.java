package com.datamelt.utilities.duckdb;

import java.util.List;

/**
 * The result of a DDL generation attempt.
 *
 * Always contains a DDL string — even when warnings or errors are present,
 * the DDL is generated using VARCHAR fallbacks for problematic fields.
 *
 * Callers should check isClean() or getErrors() before using the DDL in production.
 */
public class DdlResult
{
    private final String          ddl;
    private final List<DdlWarning> warnings;

    public DdlResult(String ddl, List<DdlWarning> warnings)
    {
        this.ddl      = ddl;
        this.warnings = warnings;
    }

    /**
     * The generated CREATE TABLE statement.
     * Always present, even if errors or warnings were encountered.
     */
    public String getDdl()
    {
        return ddl;
    }

    /**
     * All warnings and errors encountered during generation.
     */
    public List<DdlWarning> getWarnings()
    {
        return warnings;
    }

    /**
     * Errors only — fallbacks that are likely to produce incorrect DDL.
     */
    public List<DdlWarning> getErrors()
    {
        return warnings.stream()
                .filter(w -> w.getSeverity() == DdlWarning.Severity.ERROR)
                .toList();
    }

    /**
     * Returns true if no warnings or errors were encountered during generation.
     */
    public boolean isClean()
    {
        return warnings.isEmpty();
    }
}