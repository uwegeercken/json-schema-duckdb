package com.datamelt.utilities.duckdb.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates JSON data records against a JSON Schema.
 *
 * Checks performed per record:
 *   - All fields declared in "required" must be present and non-null
 *   - All present fields must match their declared type
 *   - Unknown fields (not in "properties") are reported as warnings
 *
 * This is a structural validator — it does not evaluate format constraints,
 * pattern, minimum/maximum, or enum values.
 */
public class JsonSchemaValidator {

    public record ValidationResult(int totalRecords, List<Violation> violations) {
        public boolean isValid() { return violations.isEmpty(); }

        public void print() {
            System.out.printf("  Total records : %d%n", totalRecords);
            System.out.printf("  Violations    : %d%n", violations.size());
            if (!violations.isEmpty()) {
                violations.forEach(v -> System.out.println("  " + v));
            }
        }
    }

    public record Violation(int recordIndex, String field, String message) {
        @Override
        public String toString() {
            return "[record #%d] field '%s': %s".formatted(recordIndex + 1, field, message);
        }
    }

    /**
     * Validate all records in a JSON array against the given schema.
     *
     * @param records    top-level JSON array of records
     * @param rootSchema the JSON Schema document
     * @return a ValidationResult containing all violations found
     */
    public static ValidationResult validate(JsonNode records, JsonNode rootSchema) {
        List<Violation> violations = new ArrayList<>();
        JsonNode properties = rootSchema.get("properties");

        // Collect required field names
        List<String> requiredFields = new ArrayList<>();
        JsonNode requiredNode = rootSchema.get("required");
        if (requiredNode != null && requiredNode.isArray()) {
            requiredNode.forEach(n -> requiredFields.add(n.asText()));
        }

        int index = 0;
        for (JsonNode record : records) {
            if (!record.isObject()) {
                violations.add(new Violation(index, "(record)", "Expected a JSON object but got: "
                        + record.getNodeType()));
                index++;
                continue;
            }

            // ── Check required fields are present and non-null ────────────────
            for (String required : requiredFields) {
                JsonNode val = record.get(required);
                if (val == null || val.isNull()) {
                    violations.add(new Violation(index, required,
                            "required field is missing or null"));
                }
            }

            // ── Check type of all present fields ──────────────────────────────
            if (properties != null) {
                final int recordIndex = index; // effectively final capture for lambda
                record.fieldNames().forEachRemaining(fieldName -> {
                    JsonNode fieldSchema = properties.get(fieldName);
                    if (fieldSchema == null) {
                        violations.add(new Violation(recordIndex, fieldName,
                                "unknown field (not declared in schema properties)"));
                        return;
                    }
                    JsonNode val = record.get(fieldName);
                    if (val == null || val.isNull()) return; // null is allowed for optional fields

                    String typeError = checkType(val, fieldSchema, rootSchema);
                    if (typeError != null) {
                        violations.add(new Violation(recordIndex, fieldName, typeError));
                    }
                });
            }

            index++;
        }

        return new ValidationResult(index, violations);
    }

    /**
     * Check whether a value matches its schema's declared type.
     * Returns an error message string, or null if the value is valid.
     */
    private static String checkType(JsonNode val, JsonNode schema, JsonNode rootSchema) {
        if (schema == null || schema.isNull()) return null;

        // Resolve $ref
        if (schema.has("$ref")) {
            JsonNode resolved = resolveRef(schema.get("$ref").asText(), rootSchema);
            if (resolved == null) return null; // unresolvable ref — skip type check
            return checkType(val, resolved, rootSchema);
        }

        // anyOf / oneOf — valid if it matches any non-null variant
        if (schema.has("anyOf") || schema.has("oneOf")) {
            JsonNode variants = schema.has("anyOf") ? schema.get("anyOf") : schema.get("oneOf");
            for (JsonNode variant : variants) {
                String t = variant.has("type") ? variant.get("type").asText() : "";
                if ("null".equals(t)) continue;
                if (checkType(val, variant, rootSchema) == null) return null;
            }
            return "value does not match any declared variant in anyOf/oneOf";
        }

        // allOf — must match all sub-schemas
        if (schema.has("allOf")) {
            for (JsonNode sub : schema.get("allOf")) {
                JsonNode resolved = sub.has("$ref")
                        ? resolveRef(sub.get("$ref").asText(), rootSchema) : sub;
                if (resolved == null) continue;
                String err = checkType(val, resolved, rootSchema);
                if (err != null) return err;
            }
            return null;
        }

        if (!schema.has("type")) return null;
        String expectedType = schema.get("type").asText();

        return switch (expectedType) {
            case "string"  -> val.isTextual()  ? null : typeMismatch("string",  val);
            case "integer" -> val.isIntegralNumber() ? null : typeMismatch("integer", val);
            case "number"  -> val.isNumber()   ? null : typeMismatch("number",  val);
            case "boolean" -> val.isBoolean()  ? null : typeMismatch("boolean", val);
            case "array"   -> val.isArray()    ? null : typeMismatch("array",   val);
            case "object"  -> val.isObject()   ? null : typeMismatch("object",  val);
            default        -> null; // unknown type — skip
        };
    }

    private static String typeMismatch(String expected, JsonNode actual) {
        return "expected type '%s' but got '%s'".formatted(expected, actual.getNodeType().name().toLowerCase());
    }

    private static JsonNode resolveRef(String ref, JsonNode rootSchema) {
        if (!ref.startsWith("#/")) return null;
        String[] parts = ref.substring(2).split("/");
        JsonNode node = rootSchema;
        for (String part : parts) {
            part = part.replace("~1", "/").replace("~0", "~");
            node = node.get(part);
            if (node == null) return null;
        }
        return node;
    }
}