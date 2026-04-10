package com.datamelt.utilities.duckdb.jsonschema;


import java.util.List;

public class ValidationResult
{
    private final long totalRecords;
    private final List<Violation> violations;

    public ValidationResult(long totalRecords, List<Violation> violations)
    {
        this.totalRecords = totalRecords;
        this.violations = violations;
    }

    public long getTotalRecords()
    {
        return totalRecords;
    }

    public List<Violation> getViolations()
    {
        return violations;
    }

    public boolean isValid()
    {
        return violations.isEmpty();
    }
}
