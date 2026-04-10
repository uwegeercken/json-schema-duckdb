package com.datamelt.utilities.schema.validate.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates JSON data records against a JSON Schema.
 *
 * Checks performed per record:
 *   - All fields declared in "required" must be present and non-null
 *   - All present fields must match their declared type
 *   - Unknown fields (not in "properties") are reported as WARNING or ERROR
 *     depending on the failOnUnknownFields flag
 *   - Nested objects and arrays are recursively validated
 *   - $ref is resolved, with detection of circular references and external refs
 *   - allOf, anyOf, oneOf are structurally validated
 *
 * This is a structural validator — it does not evaluate format constraints,
 * pattern, minimum/maximum, or enum values.
 */
public class FileValidator
{
    private final Schema rootSchema;
    private final boolean    failOnUnknownFields;

    /**
     * Tracks $ref strings currently being resolved, per thread.
     * Used to detect circular $ref cycles.
     * LinkedHashSet preserves insertion order for readable error messages.
     */
    private final ThreadLocal<Set<String>> activeRefs = ThreadLocal.withInitial(LinkedHashSet::new);

    /**
     * Creates a validator that treats unknown fields as warnings.
     */
    public FileValidator(Schema schema) throws IOException, IllegalArgumentException
    {
        this(schema, false);
    }

    /**
     * Creates a validator with explicit control over unknown field handling.
     *
     * @param schema               the JSON Schema to validate against
     * @param failOnUnknownFields  if true, unknown fields produce ERROR violations;
     *                             if false, they produce WARNING violations
     */
    public FileValidator(Schema schema, boolean failOnUnknownFields) throws IOException, IllegalArgumentException
    {
        this.rootSchema          = schema;
        this.failOnUnknownFields = failOnUnknownFields;
    }

    public ValidationResult validate(String dataFileName) throws IOException, IllegalArgumentException
    {
        Path dataFilePath = Path.of(dataFileName).toAbsolutePath().normalize();
        return validate(dataFilePath);
    }

    public ValidationResult validate(Path path) throws IOException, IllegalArgumentException
    {
        ObjectMapper mapper = new ObjectMapper();
        if (Files.exists(path) && Files.isRegularFile(path) && Files.isReadable(path))
        {
            JsonNode records;
            JsonNode oneNode = mapper.readTree(path.toFile());
            if (!oneNode.isArray() && oneNode.isObject())
            {
                ArrayNode arr = mapper.createArrayNode();
                records = arr.add(oneNode);
            }
            else
            {
                records = oneNode;
            }
            return validate(records);
        }
        else
        {
            throw new IOException(String.format(
                    "the data file [%s] was not found or is not readable", path));
        }
    }

    /**
     * Validate all records in a JSON array against the given schema.
     *
     * @param records    top-level JSON array of records
     * @return a ValidationResult containing all violations found
     */
    public ValidationResult validate(JsonNode records)
    {
        activeRefs.get().clear();
        try
        {
            List<Violation> violations = new ArrayList<>();
            JsonNode        properties = rootSchema.getProperties();
            List<String>    required   = rootSchema.getRequiredFields();

            int index = 0;
            for (JsonNode record : records)
            {
                if (!record.isObject())
                {
                    violations.add(new Violation(index, "(record)",
                            "expected a JSON object but got: " + record.getNodeType()));
                    index++;
                    continue;
                }
                validateObject(record, toMap(properties), required, index, "(root)", violations);
                index++;
            }
            return new ValidationResult(index, violations);
        }
        finally
        {
            activeRefs.get().clear();
        }
    }

    // ── Core validation ───────────────────────────────────────────────────────

    private void validateObject(JsonNode record, Map<String, JsonNode> properties, List<String> requiredFields,
                                int recordIndex, String path, List<Violation> violations)
    {
        // ── Required fields present and non-null ──────────────────────────
        if (requiredFields != null)
        {
            for (String required : requiredFields)
            {
                JsonNode val = record.get(required);
                if (val == null || val.isNull())
                {
                    violations.add(new Violation(recordIndex, path + "." + required,
                            "required field is missing or null"));
                }
            }
        }

        // ── Type-check all present fields ─────────────────────────────────
        if (properties != null)
        {
            record.fieldNames().forEachRemaining(fieldName -> {
                JsonNode fieldSchema = properties.get(fieldName);
                if (fieldSchema == null)
                {
                    Violation.Severity severity = failOnUnknownFields
                            ? Violation.Severity.ERROR
                            : Violation.Severity.WARNING;
                    violations.add(new Violation(recordIndex, path + "." + fieldName,
                            "unknown field (not declared in schema properties)", severity));
                    return;
                }
                JsonNode val = record.get(fieldName);
                if (val == null || val.isNull()) return;

                // Resolve $ref before further checks
                RefResult refResult = resolveRef(fieldSchema);
                if (!refResult.isResolved())
                {
                    violations.add(new Violation(recordIndex, path + "." + fieldName, refResult.error()));
                    return;
                }
                JsonNode resolvedSchema = refResult.node();

                String typeError = checkType(val, resolvedSchema);
                if (typeError != null)
                {
                    violations.add(new Violation(recordIndex, path + "." + fieldName, typeError));
                    return;
                }

                // ── Recurse into nested objects ───────────────────────────
                if (val.isObject())
                {
                    recurseIntoObject(val, resolvedSchema, recordIndex,
                            path + "." + fieldName, violations);
                }

                // ── Recurse into arrays ───────────────────────────────────
                if (val.isArray() && resolvedSchema.has("items"))
                {
                    JsonNode  itemsSchema  = resolvedSchema.get("items");
                    RefResult itemsResult  = resolveRef(itemsSchema);
                    if (!itemsResult.isResolved())
                    {
                        violations.add(new Violation(recordIndex, path + "." + fieldName, itemsResult.error()));
                        return;
                    }
                    JsonNode resolvedItems = itemsResult.node();

                    // Tuple-style: items is an array of schemas
                    if (resolvedItems.isArray())
                    {
                        for (int i = 0; i < val.size(); i++)
                        {
                            JsonNode element       = val.get(i);
                            JsonNode elementSchema = i < resolvedItems.size() ? resolvedItems.get(i) : null;
                            if (elementSchema == null) break;
                            validateElement(element, elementSchema, recordIndex,
                                    path + "." + fieldName + "[" + i + "]", violations);
                        }
                    }
                    // Single schema: all elements must match
                    else
                    {
                        for (int i = 0; i < val.size(); i++)
                        {
                            validateElement(val.get(i), resolvedItems, recordIndex,
                                    path + "." + fieldName + "[" + i + "]", violations);
                        }
                    }
                }
            });
        }
    }

    private void validateElement(JsonNode element, JsonNode elementSchema,
                                 int recordIndex, String path, List<Violation> violations)
    {
        if (element == null || element.isNull()) return;

        RefResult refResult = resolveRef(elementSchema);
        if (!refResult.isResolved())
        {
            violations.add(new Violation(recordIndex, path, refResult.error()));
            return;
        }
        JsonNode resolved = refResult.node();

        String typeError = checkType(element, resolved);
        if (typeError != null)
        {
            violations.add(new Violation(recordIndex, path, typeError));
            return;
        }

        // If the element is itself an object, recurse fully
        if (element.isObject())
        {
            recurseIntoObject(element, resolved, recordIndex, path, violations);
        }

        // If the element is itself an array, recurse into it (nested arrays)
        if (element.isArray() && resolved.has("items"))
        {
            JsonNode innerItems = resolved.get("items");
            for (int i = 0; i < element.size(); i++)
            {
                validateElement(element.get(i), innerItems, recordIndex,
                        path + "[" + i + "]", violations);
            }
        }
    }

    // ── Object recursion ──────────────────────────────────────────────────────

    private void recurseIntoObject(JsonNode val, JsonNode resolvedSchema,
                                   int recordIndex, String path, List<Violation> violations)
    {
        Map<String, JsonNode> propsToUse;
        List<String>          requiredToUse;

        if (resolvedSchema.has("allOf"))
        {
            MergedSchema merged = mergeAllOf(resolvedSchema, recordIndex, path, violations);
            propsToUse    = merged.properties();
            requiredToUse = merged.required();
        }
        else if (resolvedSchema.has("anyOf") || resolvedSchema.has("oneOf"))
        {
            JsonNode variants = resolvedSchema.has("anyOf")
                    ? resolvedSchema.get("anyOf") : resolvedSchema.get("oneOf");
            MergedSchema matched = firstMatchingVariant(val, variants, recordIndex, path, violations);
            if (matched == null) return;
            propsToUse    = matched.properties();
            requiredToUse = matched.required();
        }
        else
        {
            propsToUse    = toMap(resolvedSchema.get("properties"));
            requiredToUse = new ArrayList<>();
            if (resolvedSchema.has("required"))
            {
                resolvedSchema.get("required").forEach(n -> requiredToUse.add(n.asText()));
            }
        }

        if (propsToUse != null)
        {
            validateObject(val, propsToUse, requiredToUse, recordIndex, path, violations);
        }
    }

    // ── allOf merging ─────────────────────────────────────────────────────────

    private MergedSchema mergeAllOf(JsonNode schema, int recordIndex, String path, List<Violation> violations)
    {
        Map<String, JsonNode> properties = new LinkedHashMap<>();
        List<String>          required   = new ArrayList<>();

        for (JsonNode sub : schema.get("allOf"))
        {
            RefResult refResult = resolveRef(sub);
            if (!refResult.isResolved())
            {
                violations.add(new Violation(recordIndex, path, refResult.error()));
                continue;
            }
            JsonNode resolved = refResult.node();

            if (resolved.has("properties"))
            {
                resolved.get("properties").fieldNames()
                        .forEachRemaining(name -> properties.put(name, resolved.get("properties").get(name)));
            }
            if (resolved.has("required"))
            {
                resolved.get("required").forEach(n -> required.add(n.asText()));
            }
        }
        return new MergedSchema(properties, required);
    }

    // ── anyOf / oneOf — first matching variant ────────────────────────────────

    private MergedSchema firstMatchingVariant(JsonNode val, JsonNode variants,
                                              int recordIndex, String path, List<Violation> violations)
    {
        for (JsonNode variant : variants)
        {
            RefResult refResult = resolveRef(variant);
            if (!refResult.isResolved())
            {
                violations.add(new Violation(recordIndex, path, refResult.error()));
                continue;
            }
            JsonNode resolved = refResult.node();

            String t = resolved.has("type") ? resolved.get("type").asText() : "";
            if ("null".equals(t)) continue;
            if (checkType(val, resolved) != null) continue;

            // This variant matches — extract its properties and required
            if (resolved.has("allOf"))
            {
                return mergeAllOf(resolved, recordIndex, path, violations);
            }
            List<String> required = new ArrayList<>();
            if (resolved.has("required"))
            {
                resolved.get("required").forEach(n -> required.add(n.asText()));
            }
            return new MergedSchema(toMap(resolved.get("properties")), required);
        }
        return null;
    }

    // ── Records ───────────────────────────────────────────────────────────────

    private record MergedSchema(Map<String, JsonNode> properties, List<String> required) {}

    private record RefResult(JsonNode node, String error)
    {
        static RefResult resolved(JsonNode node) { return new RefResult(node, null);  }
        static RefResult failed(String error)    { return new RefResult(null, error); }
        boolean isResolved()                     { return node != null;               }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Resolves a $ref in the given schema node.
     * If the node has no $ref, returns it unchanged.
     * Detects circular references and reports unresolvable or external refs.
     */
    private RefResult resolveRef(JsonNode schema)
    {
        if (schema != null && schema.has("$ref"))
        {
            String ref = schema.get("$ref").asText();
            if (!activeRefs.get().add(ref))
            {
                throw new IllegalStateException(
                        "Circular $ref detected: " + activeRefs.get() + " -> " + ref);
            }
            try
            {
                JsonNode resolved = rootSchema.resolveRef(ref);
                if (resolved == null)
                {
                    return RefResult.failed(
                            "$ref '%s' is external or could not be resolved — field skipped".formatted(ref));
                }
                return RefResult.resolved(resolved);
            }
            finally
            {
                activeRefs.get().remove(ref);
            }
        }
        return schema != null ? RefResult.resolved(schema) : RefResult.failed("null schema node");
    }

    /**
     * Converts a JsonNode properties object to a Map for uniform access
     * alongside Map-based merged schemas from allOf.
     */
    private Map<String, JsonNode> toMap(JsonNode properties)
    {
        if (properties == null || properties.isNull()) return null;
        Map<String, JsonNode> map = new LinkedHashMap<>();
        properties.fieldNames().forEachRemaining(name -> map.put(name, properties.get(name)));
        return map;
    }

    // ── Type checking ─────────────────────────────────────────────────────────

    private String checkType(JsonNode val, JsonNode schema)
    {
        if (schema == null || schema.isNull()) return null;

        if (schema.has("$ref"))
        {
            RefResult refResult = resolveRef(schema);
            if (!refResult.isResolved()) return refResult.error();
            return checkType(val, refResult.node());
        }

        if (schema.has("anyOf") || schema.has("oneOf"))
        {
            JsonNode variants = schema.has("anyOf") ? schema.get("anyOf") : schema.get("oneOf");
            for (JsonNode variant : variants)
            {
                String t = variant.has("type") ? variant.get("type").asText() : "";
                if ("null".equals(t)) continue;
                if (checkType(val, variant) == null) return null;
            }
            return "value does not match any declared variant in anyOf/oneOf";
        }

        if (schema.has("allOf"))
        {
            for (JsonNode sub : schema.get("allOf"))
            {
                RefResult refResult = resolveRef(sub);
                if (!refResult.isResolved()) return refResult.error();
                String err = checkType(val, refResult.node());
                if (err != null) return err;
            }
            return null;
        }

        if (!schema.has("type")) return null;
        String expectedType = schema.get("type").asText();

        return switch (expectedType) {
            case "string"  -> val.isTextual()        ? null : typeMismatch("string",  val);
            case "integer" -> val.isIntegralNumber() ? null : typeMismatch("integer", val);
            case "number"  -> val.isNumber()         ? null : typeMismatch("number",  val);
            case "boolean" -> val.isBoolean()        ? null : typeMismatch("boolean", val);
            case "array"   -> val.isArray()          ? null : typeMismatch("array",   val);
            case "object"  -> val.isObject()         ? null : typeMismatch("object",  val);
            default        -> null;
        };
    }

    private String typeMismatch(String expected, JsonNode actual)
    {
        return "expected type '%s' but got '%s'".formatted(
                expected, actual.getNodeType().name().toLowerCase());
    }
}