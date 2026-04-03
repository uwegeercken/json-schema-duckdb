package com.datamelt.utilities.duckdb.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps JSON Schema type definitions to DuckDB SQL types.
 *
 * Supported features:
 *   - Primitives: string, integer, number, boolean (with format modifiers)
 *   - Objects with properties           -> STRUCT(field TYPE, ...)
 *   - Objects with additionalProperties -> MAP(VARCHAR, valueType)
 *   - Arrays                            -> elementType[]
 *   - Nested arrays                     -> elementType[][] (recursive)
 *   - $ref resolution                   -> resolved from root $defs / definitions
 *   - anyOf / oneOf                     -> common-type merge or VARCHAR fallback
 *   - allOf                             -> merged STRUCT of all sub-schemas
 */
public class JsonSchemaToDuckDbType {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaToDuckDbType.class);

    // ── Public entry point ────────────────────────────────────────────────────

    /**
     * Convert a JSON Schema node to its DuckDB type string.
     *
     * @param schema     the schema node to convert
     * @param rootSchema the top-level schema document (needed for $ref resolution)
     * @return DuckDB type string, e.g. "VARCHAR", "BIGINT", "STRUCT(x VARCHAR, y BIGINT)"
     */
    public static String toDuckDbType(JsonNode schema, JsonNode rootSchema) {
        if (schema == null || schema.isNull()) {
            LOG.warn("Null schema node encountered — falling back to VARCHAR");
            return "VARCHAR";
        }

        if (schema.has("$ref")) {
            return resolveRef(schema.get("$ref").asText(), rootSchema);
        }

        if (schema.has("allOf")) {
            return mergeAllOf(schema.get("allOf"), rootSchema);
        }

        if (schema.has("anyOf")) {
            return mergeAnyOf(schema.get("anyOf"), "anyOf", rootSchema);
        }
        if (schema.has("oneOf")) {
            return mergeAnyOf(schema.get("oneOf"), "oneOf", rootSchema);
        }

        String type   = schema.has("type")   ? schema.get("type").asText()   : "";
        String format = schema.has("format") ? schema.get("format").asText() : "";

        return switch (type) {
            case "string"  -> mapStringType(format);
            case "integer" -> "BIGINT";
            case "number"  -> "DOUBLE";
            case "boolean" -> "BOOLEAN";
            case "object"  -> mapObjectType(schema, rootSchema);
            case "array"   -> mapArrayType(schema, rootSchema);
            default -> {
                if (schema.has("properties") || schema.has("additionalProperties")) {
                    yield mapObjectType(schema, rootSchema);
                }
                LOG.warn("Unknown/missing type '{}' — falling back to VARCHAR", type);
                yield "VARCHAR";
            }
        };
    }

    // ── $ref resolution ───────────────────────────────────────────────────────

    private static String resolveRef(String ref, JsonNode rootSchema) {
        JsonNode node = resolveRefNode(ref, rootSchema);
        if (node == null) {
            LOG.warn("$ref '{}' could not be resolved — falling back to VARCHAR", ref);
            return "VARCHAR";
        }
        LOG.debug("Resolved $ref '{}'", ref);
        return toDuckDbType(node, rootSchema);
    }

    private static JsonNode resolveRefNode(String ref, JsonNode rootSchema) {
        if (!ref.startsWith("#/")) {
            LOG.warn("External $ref '{}' is not supported — falling back to VARCHAR", ref);
            return null;
        }
        String[] parts = ref.substring(2).split("/");
        JsonNode node  = rootSchema;
        for (String part : parts) {
            part = part.replace("~1", "/").replace("~0", "~");
            node = node.get(part);
            if (node == null) return null;
        }
        return node;
    }

    // ── allOf → merged STRUCT ─────────────────────────────────────────────────

    private static String mergeAllOf(JsonNode allOf, JsonNode rootSchema) {
        List<String> fields = new ArrayList<>();
        for (JsonNode subSchema : allOf) {
            JsonNode resolved = subSchema.has("$ref")
                    ? resolveRefNode(subSchema.get("$ref").asText(), rootSchema)
                    : subSchema;
            if (resolved == null) continue;
            JsonNode props = resolved.get("properties");
            if (props != null) {
                props.fieldNames().forEachRemaining(name ->
                        fields.add(name + " " + toDuckDbType(props.get(name), rootSchema))
                );
            }
        }
        if (fields.isEmpty()) {
            LOG.warn("allOf produced no fields — falling back to VARCHAR");
            return "VARCHAR";
        }
        return "STRUCT(" + String.join(", ", fields) + ")";
    }

    // ── anyOf / oneOf → common type or VARCHAR ────────────────────────────────

    private static String mergeAnyOf(JsonNode variants, String keyword, JsonNode rootSchema) {
        List<JsonNode> nonNull = new ArrayList<>();
        for (JsonNode v : variants) {
            String t = v.has("type") ? v.get("type").asText() : "";
            if (!"null".equals(t)) nonNull.add(v);
        }

        if (nonNull.size() == 1) {
            return toDuckDbType(nonNull.get(0), rootSchema);
        }

        List<String> resolved = nonNull.stream()
                .map(v -> toDuckDbType(v, rootSchema))
                .distinct()
                .toList();

        if (resolved.size() == 1) {
            return resolved.get(0);
        }

        LOG.warn("'{}' has {} distinct types {} — falling back to VARCHAR", keyword, resolved.size(), resolved);
        return "VARCHAR";
    }

    // ── object mapping ────────────────────────────────────────────────────────

    private static String mapObjectType(JsonNode schema, JsonNode rootSchema) {
        JsonNode properties           = schema.get("properties");
        JsonNode additionalProperties = schema.get("additionalProperties");

        boolean hasProps      = properties != null && !properties.isEmpty();
        boolean hasAdditional = additionalProperties != null
                && !additionalProperties.isNull()
                && !additionalProperties.isBoolean();

        if (hasProps && hasAdditional) {
            LOG.warn("Object has both 'properties' and 'additionalProperties' — mapping as STRUCT");
        }

        if (hasProps) {
            return buildStruct(properties, rootSchema);
        }

        if (hasAdditional) {
            String valueType = toDuckDbType(additionalProperties, rootSchema);
            return "MAP(VARCHAR, %s)".formatted(valueType);
        }

        LOG.warn("Object has no typed properties — falling back to VARCHAR (store as JSON text)");
        return "VARCHAR";
    }

    private static String buildStruct(JsonNode properties, JsonNode rootSchema) {
        List<String> fields = new ArrayList<>();
        properties.fieldNames().forEachRemaining(name ->
                fields.add(name + " " + toDuckDbType(properties.get(name), rootSchema))
        );
        return "STRUCT(" + String.join(", ", fields) + ")";
    }

    // ── array mapping ─────────────────────────────────────────────────────────

    private static String mapArrayType(JsonNode schema, JsonNode rootSchema) {
        JsonNode items = schema.get("items");
        if (items == null || items.isNull()) {
            LOG.warn("Array has no 'items' definition — falling back to VARCHAR[]");
            return "VARCHAR[]";
        }
        return toDuckDbType(items, rootSchema) + "[]";
    }

    // ── string format mapping ─────────────────────────────────────────────────

    private static String mapStringType(String format) {
        return switch (format) {
            case "date"      -> "DATE";
            case "date-time" -> "TIMESTAMP";
            case "time"      -> "TIME";
            case "uuid"      -> "UUID";
            default          -> "VARCHAR";
        };
    }
}