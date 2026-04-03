package com.datamelt.utilities.duckdb.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Intended for use both as a command-line tool (via Main) and as a library.
 *
 * Programmatic API:
 * <pre>
 *   // From a file path
 *   String ddl = JsonSchemaDdlGenerator.generateDdl("orders", Path.of("order.json"));
 *
 *   // From an InputStream
 *   String ddl = JsonSchemaDdlGenerator.generateDdl("orders", getClass().getResourceAsStream("/order.json"));
 *
 *   // From a pre-parsed JsonNode
 *   JsonNode schema = new ObjectMapper().readTree(file);
 *   String ddl = JsonSchemaDdlGenerator.generateDdl("orders", schema);
 * </pre>
 */
public class JsonSchemaDdlGenerator {

    private static final Logger       LOG    = LoggerFactory.getLogger(JsonSchemaDdlGenerator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Generate a DuckDB CREATE TABLE statement from a JSON Schema file.
     *
     * @param tableName  name of the table in the generated DDL
     * @param schemaPath path to the JSON Schema file
     * @return the CREATE TABLE statement as a String
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if the schema has no top-level properties
     */
    public static String generateDdl(String tableName, Path schemaPath) throws IOException {
        LOG.info("Reading schema from file: {}", schemaPath);
        JsonNode schema = MAPPER.readTree(Files.readString(schemaPath));
        return generateDdl(tableName, schema);
    }

    /**
     * Generate a DuckDB CREATE TABLE statement from a JSON Schema input stream.
     *
     * @param tableName    name of the table in the generated DDL
     * @param schemaStream input stream of the JSON Schema
     * @return the CREATE TABLE statement as a String
     * @throws IOException              if the stream cannot be read
     * @throws IllegalArgumentException if the schema has no top-level properties
     */
    public static String generateDdl(String tableName, InputStream schemaStream) throws IOException {
        LOG.info("Reading schema from input stream");
        JsonNode schema = MAPPER.readTree(schemaStream);
        return generateDdl(tableName, schema);
    }

    /**
     * Generate a DuckDB CREATE TABLE statement from a pre-parsed JSON Schema node.
     *
     * @param tableName  name of the table in the generated DDL
     * @param schema     the root JSON Schema node
     * @return the CREATE TABLE statement as a String
     * @throws IllegalArgumentException if the schema has no top-level properties
     */
    public static String generateDdl(String tableName, JsonNode schema) {
        LOG.debug("Generating DDL for table '{}'", tableName);

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
        LOG.debug("Required fields: {}", requiredFields);

        List<String> columnDefs = new ArrayList<>();
        properties.fieldNames().forEachRemaining(fieldName -> {
            JsonNode fieldSchema  = properties.get(fieldName);
            String   duckDbType   = JsonSchemaToDuckDbType.toDuckDbType(fieldSchema, rootSchema);
            String   nullability  = requiredFields.contains(fieldName) ? " NOT NULL" : "";
            LOG.debug("  Column '{}' -> {} {}", fieldName, duckDbType,
                    nullability.isBlank() ? "(nullable)" : "(NOT NULL)");
            columnDefs.add("    %s %s%s".formatted(fieldName, duckDbType, nullability));
        });

        String ddl = """
                CREATE TABLE IF NOT EXISTS %s (
                %s
                );""".formatted(tableName, String.join(",\n", columnDefs));

        LOG.info("DDL generated for table '{}' with {} column(s)", tableName, columnDefs.size());
        return ddl;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /**
     * If the root schema node itself is a $ref, follow it once.
     * (Uncommon but valid in JSON Schema draft-07+)
     */
    private static JsonNode resolveTopLevel(JsonNode schema, JsonNode rootSchema) {
        if (schema.has("$ref")) {
            String   ref   = schema.get("$ref").asText();
            String[] parts = ref.startsWith("#/") ? ref.substring(2).split("/") : null;
            if (parts != null) {
                JsonNode node = rootSchema;
                for (String part : parts) {
                    part = part.replace("~1", "/").replace("~0", "~");
                    node = node.get(part);
                    if (node == null) {
                        LOG.warn("Could not resolve top-level $ref '{}' — using schema as-is", ref);
                        return schema;
                    }
                }
                LOG.debug("Resolved top-level $ref '{}'", ref);
                return node;
            }
        }
        return schema;
    }
}