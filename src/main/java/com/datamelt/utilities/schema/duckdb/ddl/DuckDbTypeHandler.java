package com.datamelt.utilities.schema.duckdb.ddl;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps JSON Schema type definitions to DuckDB SQL types.
 *
 * Supported features:
 *   - Primitives: string, integer, number, boolean (with format modifiers)
 *   - integer formats: int8 -> TINYINT, int16 -> SMALLINT, int32 -> INTEGER, int64/default -> BIGINT
 *   - number formats:  float -> FLOAT, decimal/default -> DOUBLE
 *   - Objects with properties           -> STRUCT(field TYPE, ...)
 *   - Objects with additionalProperties -> MAP(VARCHAR, valueType)
 *   - Arrays                            -> elementType[]
 *   - Nested arrays                     -> elementType[][] (recursive)
 *   - Tuple-style arrays                -> VARCHAR[] (fallback, WARNING)
 *   - $ref resolution                   -> resolved from root $defs / definitions
 *   - anyOf / oneOf                     -> common-type merge or VARCHAR fallback (WARNING)
 *   - allOf                             -> primitive passthrough or merged STRUCT
 *
 * All fallbacks are recorded in the provided warnings list rather than logged.
 */
public class SchemaToDuckDbType
{

    // ── Public entry point ────────────────────────────────────────────────────

    /**
     * Convert a JSON Schema node to its DuckDB type string.
     *
     * @param schema     the schema node to convert
     * @param rootSchema the top-level schema document (needed for $ref resolution)
     * @param fieldPath  dotted path to this field, used in warning messages
     * @param warnings   list to accumulate any warnings or errors encountered
     * @return DuckDB type string, e.g. "VARCHAR", "BIGINT", "STRUCT(x VARCHAR, y BIGINT)"
     */
    public static String toDuckDbType(JsonNode schema, JsonNode rootSchema,
                                      String fieldPath, List<DdlWarning> warnings) {
        if (schema == null || schema.isNull()) {
            warnings.add(new DdlWarning(fieldPath,
                    "null schema node — falling back to VARCHAR",
                    DdlWarning.Severity.ERROR));
            return "VARCHAR";
        }

        if (schema.has("$ref")) {
            return resolveRef(schema.get("$ref").asText(), rootSchema, fieldPath, warnings);
        }

        if (schema.has("allOf")) {
            return mergeAllOf(schema.get("allOf"), rootSchema, fieldPath, warnings);
        }

        if (schema.has("anyOf")) {
            return mergeAnyOf(schema.get("anyOf"), "anyOf", rootSchema, fieldPath, warnings);
        }
        if (schema.has("oneOf")) {
            return mergeAnyOf(schema.get("oneOf"), "oneOf", rootSchema, fieldPath, warnings);
        }

        String type   = schema.has("type")   ? schema.get("type").asText()   : "";
        String format = schema.has("format") ? schema.get("format").asText() : "";

        return switch (type) {
            case "string"  -> mapStringType(format);
            case "integer" -> mapIntegerType(format);
            case "number"  -> mapNumberType(format);
            case "boolean" -> "BOOLEAN";
            case "object"  -> mapObjectType(schema, rootSchema, fieldPath, warnings);
            case "array"   -> mapArrayType(schema, rootSchema, fieldPath, warnings);
            default -> {
                if (schema.has("properties") || schema.has("additionalProperties")) {
                    yield mapObjectType(schema, rootSchema, fieldPath, warnings);
                }
                warnings.add(new DdlWarning(fieldPath,
                        "unknown or missing type '%s' — falling back to VARCHAR".formatted(type),
                        DdlWarning.Severity.ERROR));
                yield "VARCHAR";
            }
        };
    }

    // ── $ref resolution ───────────────────────────────────────────────────────

    private static String resolveRef(String ref, JsonNode rootSchema,
                                     String fieldPath, List<DdlWarning> warnings) {
        JsonNode node = resolveRefNode(ref, rootSchema);
        if (node == null) {
            warnings.add(new DdlWarning(fieldPath,
                    "$ref '%s' could not be resolved — falling back to VARCHAR".formatted(ref),
                    DdlWarning.Severity.ERROR));
            return "VARCHAR";
        }
        return toDuckDbType(node, rootSchema, fieldPath, warnings);
    }

    private static JsonNode resolveRefNode(String ref, JsonNode rootSchema) {
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

    // ── allOf → primitive passthrough or merged STRUCT ────────────────────────

    private static String mergeAllOf(JsonNode allOf, JsonNode rootSchema,
                                     String fieldPath, List<DdlWarning> warnings) {
        List<String> fields         = new ArrayList<>();
        List<String> primitiveTypes = new ArrayList<>();
        boolean      hasProperties  = false;

        for (JsonNode subSchema : allOf) {
            JsonNode resolved = subSchema.has("$ref")
                    ? resolveRefNode(subSchema.get("$ref").asText(), rootSchema)
                    : subSchema;
            if (resolved == null) continue;

            if (resolved.has("properties")) {
                hasProperties = true;
                resolved.get("properties").fieldNames().forEachRemaining(name -> {
                    String quotedName = "\"" + name + "\"";
                    fields.add(quotedName + " " + toDuckDbType(
                            resolved.get("properties").get(name), rootSchema,
                            fieldPath + "." + name, warnings));
                });
            }

            if (resolved.has("type")) {
                String type = resolved.get("type").asText();
                if (!type.equals("object") && !type.equals("array")) {
                    primitiveTypes.add(toDuckDbType(resolved, rootSchema, fieldPath, warnings));
                }
            }
        }

        // If there are properties, build a STRUCT
        if (hasProperties) {
            if (fields.isEmpty()) {
                warnings.add(new DdlWarning(fieldPath,
                        "allOf produced no fields — falling back to VARCHAR",
                        DdlWarning.Severity.ERROR));
                return "VARCHAR";
            }
            return "STRUCT(" + String.join(", ", fields) + ")";
        }

        // If all sub-schemas agree on a single primitive type, return it directly
        List<String> distinctTypes = primitiveTypes.stream().distinct().toList();
        if (distinctTypes.size() == 1) {
            return distinctTypes.get(0);
        }

        warnings.add(new DdlWarning(fieldPath,
                "allOf produced no fields and no common primitive type — falling back to VARCHAR",
                DdlWarning.Severity.ERROR));
        return "VARCHAR";
    }

    // ── anyOf / oneOf → common type or VARCHAR ────────────────────────────────

    private static String mergeAnyOf(JsonNode variants, String keyword, JsonNode rootSchema,
                                     String fieldPath, List<DdlWarning> warnings) {
        List<JsonNode> nonNull = new ArrayList<>();
        for (JsonNode v : variants) {
            String t = v.has("type") ? v.get("type").asText() : "";
            if (!"null".equals(t)) nonNull.add(v);
        }

        if (nonNull.size() == 1) {
            return toDuckDbType(nonNull.get(0), rootSchema, fieldPath, warnings);
        }

        List<String> resolved = nonNull.stream()
                .map(v -> toDuckDbType(v, rootSchema, fieldPath, warnings))
                .distinct()
                .toList();

        if (resolved.size() == 1) {
            return resolved.get(0);
        }

        warnings.add(new DdlWarning(fieldPath,
                "'%s' has %d distinct types %s — falling back to VARCHAR"
                        .formatted(keyword, resolved.size(), resolved),
                DdlWarning.Severity.WARNING));
        return "VARCHAR";
    }

    // ── object mapping ────────────────────────────────────────────────────────

    private static String mapObjectType(JsonNode schema, JsonNode rootSchema,
                                        String fieldPath, List<DdlWarning> warnings) {
        JsonNode properties           = schema.get("properties");
        JsonNode additionalProperties = schema.get("additionalProperties");

        boolean hasProps      = properties != null && !properties.isEmpty();
        boolean hasAdditional = additionalProperties != null
                && !additionalProperties.isNull()
                && !additionalProperties.isBoolean();

        if (hasProps) {
            return buildStruct(schema, rootSchema, fieldPath, warnings);
        }

        if (hasAdditional) {
            String valueType = toDuckDbType(additionalProperties, rootSchema, fieldPath, warnings);
            return "MAP(VARCHAR, %s)".formatted(valueType);
        }

        warnings.add(new DdlWarning(fieldPath,
                "object has no typed properties — falling back to VARCHAR (store as JSON text)",
                DdlWarning.Severity.WARNING));
        return "VARCHAR";
    }

    private static String buildStruct(JsonNode objectSchema, JsonNode rootSchema,
                                      String fieldPath, List<DdlWarning> warnings) {
        JsonNode     properties = objectSchema.get("properties");
        List<String> fields     = new ArrayList<>();

        properties.fieldNames().forEachRemaining(name -> {
            String quotedName = "\"" + name + "\"";
            String type       = toDuckDbType(properties.get(name), rootSchema,
                    fieldPath + "." + name, warnings);
            fields.add(quotedName + " " + type);
        });
        return "STRUCT(" + String.join(", ", fields) + ")";
    }

    // ── array mapping ─────────────────────────────────────────────────────────

    private static String mapArrayType(JsonNode schema, JsonNode rootSchema,
                                       String fieldPath, List<DdlWarning> warnings) {
        JsonNode items = schema.get("items");
        if (items == null || items.isNull()) {
            warnings.add(new DdlWarning(fieldPath,
                    "array has no 'items' definition — falling back to VARCHAR[]",
                    DdlWarning.Severity.WARNING));
            return "VARCHAR[]";
        }
        if (items.isArray()) {
            warnings.add(new DdlWarning(fieldPath,
                    "tuple-style array 'items' is not supported — falling back to VARCHAR[]",
                    DdlWarning.Severity.WARNING));
            return "VARCHAR[]";
        }
        return toDuckDbType(items, rootSchema, fieldPath, warnings) + "[]";
    }

    // ── type format mappings ──────────────────────────────────────────────────

    private static String mapStringType(String format) {
        return switch (format) {
            case "date"      -> "DATE";
            case "date-time" -> "TIMESTAMP";
            case "time"      -> "TIME";
            case "uuid"      -> "UUID";
            default          -> "VARCHAR";
        };
    }

    private static String mapIntegerType(String format) {
        return switch (format) {
            case "int8"  -> "TINYINT";
            case "int16" -> "SMALLINT";
            case "int32" -> "INTEGER";
            default      -> "BIGINT";
        };
    }

    private static String mapNumberType(String format) {
        return switch (format) {
            case "float" -> "FLOAT";
            default      -> "DOUBLE";
        };
    }
}