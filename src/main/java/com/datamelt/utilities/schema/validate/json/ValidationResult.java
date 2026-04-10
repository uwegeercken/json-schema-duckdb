package com.datamelt.utilities.schema.validate.json;

import java.util.List;

public class ValidationResult
{
    private final long          totalRecords;
    private final List<Violation> violations;

    public ValidationResult(long totalRecords, List<Violation> violations)
    {
        this.totalRecords = totalRecords;
        this.violations   = violations;
    }

    public long getTotalRecords()
    {
        return totalRecords;
    }

    public List<Violation> getViolations()
    {
        return violations;
    }

    public List<Violation> getErrors()
    {
        return violations.stream()
                .filter(v -> v.getSeverity() == Violation.Severity.ERROR)
                .toList();
    }

    public List<Violation> getWarnings()
    {
        return violations.stream()
                .filter(v -> v.getSeverity() == Violation.Severity.WARNING)
                .toList();
    }

    public boolean isValid()
    {
        return violations.stream()
                .noneMatch(v -> v.getSeverity() == Violation.Severity.ERROR);
    }
}