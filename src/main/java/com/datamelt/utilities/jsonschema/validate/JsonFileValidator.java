package com.datamelt.utilities.jsonschema.validate;

import com.datamelt.utilities.FileUtility;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.datamelt.utilities.FileUtility.isValidDirectory;
import static com.datamelt.utilities.FileUtility.isValidFile;

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

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaValidator.class);

    private final JsonNode rootSchema;
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonSchemaValidator(String schemaFileName) throws IOException, IllegalArgumentException
    {
        this(Path.of(schemaFileName).toAbsolutePath().normalize());
    }

    public JsonSchemaValidator(Path schemaFilePath) throws IOException, IllegalArgumentException
    {
        boolean schemaFilePathOk = isValidFile(schemaFilePath);
        if (schemaFilePathOk)
        {
            rootSchema = mapper.readTree(schemaFilePath.toFile());
            if(!validateSchemaContent()) {
                throw new IllegalArgumentException(String.format("the schema file [%s] has no top-level 'properties' — not a valid object schema", schemaFilePath));
            }
        }
        else
        {
            throw new IllegalArgumentException(String.format("the schema file [%s] was not found or is not readable", schemaFilePath));
        }
    }

    public List<ValidationResult> validate(String dataFileName) throws IOException, IllegalArgumentException
    {
        Path dataFilePath = Path.of(dataFileName).toAbsolutePath().normalize();
        return validate(dataFilePath);
    }

    public List<ValidationResult> validate(Path dataFilePath) throws IOException, IllegalArgumentException
    {
        ObjectMapper mapper = new ObjectMapper();

        List<ValidationResult> results = new ArrayList<>();
        for(Path path : getFiles(dataFilePath))
        {
            JsonNode records = null;
            JsonNode dataFileNode = mapper.readTree(dataFilePath.toFile());
            if (!dataFileNode.isArray() && dataFileNode.isObject())
            {
                ArrayNode arr = mapper.createArrayNode();
                records = arr.add(dataFileNode);
            }
            else
            {
                records = dataFileNode;
            }
            results.add(validate(records));
        }
        return results;
    }

    private List<Path> getFiles(Path dataFilePath) throws IOException
    {
        if (isValidFile(dataFilePath))
        {
            return List.of(dataFilePath);
        }
        else if(isValidDirectory(dataFilePath))
        {
            return FileUtility.getFilesFromDirectory(dataFilePath);
        }
        else
        {
            return List.of();
        }
    }

    private boolean validateSchemaContent() {
        return rootSchema.has("properties") || rootSchema.get("properties").isEmpty();
    }


    /**
     * Validate all records in a JSON array against the given schema.
     *
     * @param records    top-level JSON array of records
     * @return a ValidationResult containing all violations found
     */
    public ValidationResult validate(JsonNode records) {
        List<Violation> violations  = new ArrayList<>();
        JsonNode        properties  = rootSchema.get("properties");

        List<String> requiredFields = new ArrayList<>();
        JsonNode requiredNode = rootSchema.get("required");
        if (requiredNode != null && requiredNode.isArray()) {
            requiredNode.forEach(n -> requiredFields.add(n.asText()));
        }

        LOG.debug("Validating {} record(s), {} required field(s)", records.size(), requiredFields.size());

        int index = 0;
        for (JsonNode record : records) {
            if (!record.isObject()) {
                LOG.warn("Record #{} is not a JSON object ({})", index + 1, record.getNodeType());
                violations.add(new Violation(index, "(record)",
                        "Expected a JSON object but got: " + record.getNodeType()));
                index++;
                continue;
            }

            // ── Required fields present and non-null ──────────────────────────
            for (String required : requiredFields) {
                JsonNode val = record.get(required);
                if (val == null || val.isNull()) {
                    LOG.debug("Record #{}: required field '{}' is missing or null", index + 1, required);
                    violations.add(new Violation(index, required,
                            "required field is missing or null"));
                }
            }

            // ── Type-check all present fields ─────────────────────────────────
            if (properties != null) {
                final int recordIndex = index; // effectively final for lambda
                record.fieldNames().forEachRemaining(fieldName -> {
                    JsonNode fieldSchema = properties.get(fieldName);
                    if (fieldSchema == null) {
                        LOG.debug("Record #{}: unknown field '{}'", recordIndex + 1, fieldName);
                        violations.add(new Violation(recordIndex, fieldName,
                                "unknown field (not declared in schema properties)"));
                        return;
                    }
                    JsonNode val = record.get(fieldName);
                    if (val == null || val.isNull()) return;

                    String typeError = checkType(val, fieldSchema, rootSchema);
                    if (typeError != null) {
                        LOG.debug("Record #{}: field '{}' type error — {}", recordIndex + 1, fieldName, typeError);
                        violations.add(new Violation(recordIndex, fieldName, typeError));
                    }
                });
            }

            index++;
        }

        LOG.info("Validation complete: {} record(s), {} violation(s)", index, violations.size());
        return new ValidationResult(index, violations);
    }

    // ── Type checking ─────────────────────────────────────────────────────────

    private String checkType(JsonNode val, JsonNode schema, JsonNode rootSchema) {
        if (schema == null || schema.isNull()) return null;

        if (schema.has("$ref")) {
            JsonNode resolved = resolveRef(schema.get("$ref").asText(), rootSchema);
            if (resolved == null) return null;
            return checkType(val, resolved, rootSchema);
        }

        if (schema.has("anyOf") || schema.has("oneOf")) {
            JsonNode variants = schema.has("anyOf") ? schema.get("anyOf") : schema.get("oneOf");
            for (JsonNode variant : variants) {
                String t = variant.has("type") ? variant.get("type").asText() : "";
                if ("null".equals(t)) continue;
                if (checkType(val, variant, rootSchema) == null) return null;
            }
            return "value does not match any declared variant in anyOf/oneOf";
        }

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
            case "string"  -> val.isTextual()        ? null : typeMismatch("string",  val);
            case "integer" -> val.isIntegralNumber() ? null : typeMismatch("integer", val);
            case "number"  -> val.isNumber()         ? null : typeMismatch("number",  val);
            case "boolean" -> val.isBoolean()        ? null : typeMismatch("boolean", val);
            case "array"   -> val.isArray()          ? null : typeMismatch("array",   val);
            case "object"  -> val.isObject()         ? null : typeMismatch("object",  val);
            default        -> null;
        };
    }

    private String typeMismatch(String expected, JsonNode actual) {
        return "expected type '%s' but got '%s'".formatted(
                expected, actual.getNodeType().name().toLowerCase());
    }

    private JsonNode resolveRef(String ref, JsonNode rootSchema) {
        if (!ref.startsWith("#/")) return null;
        String[] parts = ref.substring(2).split("/");
        JsonNode node  = rootSchema;
        for (String part : parts) {
            part = part.replace("~1", "/").replace("~0", "~");
            node = node.get(part);
            if (node == null) return null;
        }
        return node;
    }
}