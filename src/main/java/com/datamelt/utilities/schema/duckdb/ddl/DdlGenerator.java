package com.datamelt.utilities.duckdb;

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
 * Intended for use both as a command-line tool (via Main) and as a library.
 *
 * Field names are always double-quoted in the generated DDL to handle names that
 * are DuckDB reserved words or contain special characters (e.g. @type, $ref).
 *
 * JSON Schema metadata keywords ($schema, $vocabulary, etc.) are excluded from
 * the generated DDL even if they appear under "properties".
 *
 * All fallbacks and issues encountered during generation are returned in the
 * DdlResult rather than logged. Callers should check DdlResult.isClean() or
 * DdlResult.getErrors() before using the DDL in production.
 *
 * Programmatic API:
 * <pre>
 *   // From a file path (IF NOT EXISTS by default)
 *   DdlResult result = JsonSchemaDdlGenerator.generateDdl("orders", Path.of("order.json"));
 *
 *   // From a file path without IF NOT EXISTS
 *   DdlResult result = JsonSchemaDdlGenerator.generateDdl("orders", Path.of("order.json"), false);
 *
 *   // From an InputStream
 *   DdlResult result = JsonSchemaDdlGenerator.generateDdl("orders", getClass().getResourceAsStream("/order.json"));
 *
 *   // From a pre-parsed JsonNode
 *   JsonNode schema = new ObjectMapper().readTree(file);
 *   DdlResult result = JsonSchemaDdlGenerator.generateDdl("orders", schema);
 *
 *   // Using the result
 *   if (!result.isClean()) {
 *       result.getWarnings().forEach(System.out::println);
 *   }
 *   String ddl = result.getDdl();
 * </pre>
 */
public class JsonSchemaDdlGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * JSON Schema metadata keywords that must never become DDL columns,
     * even if they appear under "properties" in the schema.
     */
    static final Set<String> METADATA_FIELDS = Set.of(
            "$schema", "$vocabulary", "$id", "$comment", "$anchor", "$dynamicAnchor"
    );

    // ── Public API ────────────────────────────────────────────────────────────

    public static DdlResult generateDdl(String tableName, Path schemaPath) throws IOException {
        return generateDdl(tableName, schemaPath, true);
    }

    public static DdlResult generateDdl(String tableName, Path schemaPath, boolean ifNotExists) throws IOException {
        JsonNode schema = MAPPER.readTree(Files.readString(schemaPath));
        return generateDdl(tableName, schema, ifNotExists);
    }

    public static DdlResult generateDdl(String tableName, InputStream schemaStream) throws IOException {
        return generateDdl(tableName, schemaStream, true);
    }

    public static DdlResult generateDdl(String tableName, InputStream schemaStream, boolean ifNotExists) throws IOException {
        JsonNode schema = MAPPER.readTree(schemaStream);
        return generateDdl(tableName, schema, ifNotExists);
    }

    public static DdlResult generateDdl(String tableName, JsonNode schema) {
        return generateDdl(tableName, schema, true);
    }

    /**
     * Generate a DuckDB CREATE TABLE statement from a pre-parsed JSON Schema node.
     *
     * @param tableName   name of the table in the generated DDL
     * @param schema      the root JSON Schema node
     * @param ifNotExists if true, emits CREATE TABLE IF NOT EXISTS
     * @return a DdlResult containing the DDL string and any warnings or errors encountered
     * @throws IllegalArgumentException if the schema has no top-level properties
     */
    public static DdlResult generateDdl(String tableName, JsonNode schema, boolean ifNotExists) {
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

        List<DdlWarning> warnings   = new ArrayList<>();
        List<String>     columnDefs = new ArrayList<>();

        properties.fieldNames().forEachRemaining(fieldName -> {

            // Skip JSON Schema metadata keywords — they are not data columns
            if (METADATA_FIELDS.contains(fieldName)) {
                return;
            }

            JsonNode fieldSchema = properties.get(fieldName);
            String   duckDbType  = JsonSchemaToDuckDbType.toDuckDbType(
                    fieldSchema, rootSchema, fieldName, warnings);
            String   nullability = requiredFields.contains(fieldName) ? " NOT NULL" : "";
            String   quotedName  = "\"" + fieldName + "\"";

            columnDefs.add("    %s %s%s".formatted(quotedName, duckDbType, nullability));
        });

        String ifNotExistsClause = ifNotExists ? "IF NOT EXISTS " : "";
        String ddl = """
                CREATE TABLE %s%s (
                %s
                );""".formatted(ifNotExistsClause, tableName, String.join(",\n", columnDefs));

        return new DdlResult(ddl, warnings);
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
                    if (node == null) return schema;
                }
                return node;
            }
        }
        return schema;
    }
}