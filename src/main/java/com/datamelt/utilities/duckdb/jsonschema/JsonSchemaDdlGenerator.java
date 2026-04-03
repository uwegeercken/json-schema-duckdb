package com.datamelt.utilities.duckdb.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Generates a DuckDB CREATE TABLE statement from a JSON Schema.
 *
 * The root schema is passed through to JsonSchemaToDuckDbType so that
 * $ref references can be resolved anywhere in the schema tree.
 */
public class JsonSchemaDdlGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Generate DDL from a JSON Schema file path. */
    public static String generateDdl(String tableName, Path schemaPath) throws IOException {
        JsonNode schema = MAPPER.readTree(Files.readString(schemaPath));
        return generateDdl(tableName, schema);
    }

    /** Generate DDL from a JSON Schema input stream. */
    public static String generateDdl(String tableName, InputStream schemaStream) throws IOException {
        JsonNode schema = MAPPER.readTree(schemaStream);
        return generateDdl(tableName, schema);
    }

    /** Generate DDL from a parsed JSON Schema node. */
    public static String generateDdl(String tableName, JsonNode schema) {
        // Resolve top-level $ref if the root itself is a reference
        JsonNode rootSchema = schema;
        JsonNode resolved   = resolveTopLevel(schema, rootSchema);

        JsonNode properties = resolved.get("properties");
        if (properties == null || properties.isEmpty()) {
            throw new IllegalArgumentException(
                    "JSON Schema has no 'properties' defined at top level.");
        }

        // Collect required fields
        Set<String> requiredFields = new HashSet<>();
        JsonNode requiredNode = resolved.get("required");
        if (requiredNode != null && requiredNode.isArray()) {
            requiredNode.forEach(n -> requiredFields.add(n.asText()));
        }

        List<String> columnDefs = new ArrayList<>();
        properties.fieldNames().forEachRemaining(fieldName -> {
            JsonNode fieldSchema = properties.get(fieldName);
            // Pass rootSchema so nested $refs can be resolved
            String duckDbType  = JsonSchemaToDuckDbType.toDuckDbType(fieldSchema, rootSchema);
            String nullability = requiredFields.contains(fieldName) ? " NOT NULL" : "";
            columnDefs.add("    %s %s%s".formatted(fieldName, duckDbType, nullability));
        });

        return """
                CREATE TABLE IF NOT EXISTS %s (
                %s
                );""".formatted(tableName, String.join(",\n", columnDefs));
    }

    /**
     * If the root schema node itself is a $ref, follow it once.
     * (Uncommon but valid in JSON Schema draft-07+)
     */
    private static JsonNode resolveTopLevel(JsonNode schema, JsonNode rootSchema) {
        if (schema.has("$ref")) {
            String ref    = schema.get("$ref").asText();
            String[] parts = ref.startsWith("#/") ? ref.substring(2).split("/") : null;
            if (parts != null) {
                JsonNode node = rootSchema;
                for (String part : parts) {
                    part = part.replace("~1", "/").replace("~0", "~");
                    node = node.get(part);
                    if (node == null) return schema;
                }
                return node;
            }
        }
        return schema;
    }
}