package com.datamelt.utilities.duckdb.jsonschema;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Derives a DuckDB CREATE TABLE statement from a JSON Schema.
 *
 * Modes:
 *   --schema only               → print CREATE TABLE statement
 *   --schema + --validate-file  → validate a single JSON file against the schema
 *   --schema + --validate-dir   → validate all *.json files in a folder against the schema
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    static final String VERSION       = "1.0.0";
    static final String DEFAULT_TABLE = "data";

    public static void main(String[] args) {
        if (args.length == 0 || hasFlag(args, "--help")) {
            printHelp();
            System.exit(0);
        }

        String schemaArg       = argValue(args, "--schema");
        String validateFileArg = argValue(args, "--validate-file");
        String validateDirArg  = argValue(args, "--validate-dir");
        String tableArg        = argValue(args, "--table");

        String tableName = (tableArg != null && !tableArg.isBlank()) ? tableArg : DEFAULT_TABLE;

        if (schemaArg == null || schemaArg.isBlank()) {
            LOG.error("--schema <file> is required");
            System.err.println("ERROR: --schema <file> is required.");
            System.err.println("Run with --help for usage information.");
            System.exit(1);
        }

        if (validateFileArg != null && validateDirArg != null) {
            LOG.error("--validate-file and --validate-dir are mutually exclusive");
            System.err.println("ERROR: --validate-file and --validate-dir are mutually exclusive.");
            System.err.println("Run with --help for usage information.");
            System.exit(1);
        }

        // ── Load schema ───────────────────────────────────────────────────────
        Path schemaPath = Path.of(schemaArg).toAbsolutePath().normalize();
        validateFile(schemaPath, "schema");

        ObjectMapper mapper     = new ObjectMapper();
        JsonNode     rootSchema = parseJsonFile(mapper, schemaPath, "schema");
        validateSchemaContent(rootSchema, schemaPath);

        // ── Always print DDL ──────────────────────────────────────────────────
        LOG.info("Generating DDL for table '{}' from schema '{}'", tableName, schemaPath.getFileName());
        String ddl = JsonSchemaDdlGenerator.generateDdl(tableName, rootSchema);
        System.out.println(ddl);

        // ── Optionally validate data files ────────────────────────────────────
        if (validateFileArg != null || validateDirArg != null) {
            System.out.println();
            List<Path> dataFiles = resolveDataFiles(validateFileArg, validateDirArg);
            runValidation(mapper, rootSchema, dataFiles);
        }
    }

    // ── Validation ────────────────────────────────────────────────────────────

    private static void runValidation(ObjectMapper mapper, JsonNode rootSchema, List<Path> dataFiles) {
        System.out.println("=== File(s) to Schema Validation ===");
        LOG.info("Starting validation of {} file(s)", dataFiles.size());
        System.out.printf("Validating %d file(s)...%n%n", dataFiles.size());

        int totalFiles  = 0;
        int failedFiles = 0;

        for (Path dataFile : dataFiles) {
            totalFiles++;
            System.out.println("File: " + dataFile.getFileName());
            LOG.info("Validating file: {}", dataFile);

            Optional<JsonNode> records = getAsArray(mapper, dataFile);
            if (records.isEmpty()) {
                LOG.warn("File '{}' is not a JSON array or object — skipping", dataFile.getFileName());
                System.err.println("  ERROR: Not a JSON array or object — skipping.");
                failedFiles++;
                continue;
            }

            JsonSchemaValidator.ValidationResult result =
                    JsonSchemaValidator.validate(records.get(), rootSchema);
            result.print();

            if (!result.isValid()) {
                LOG.warn("File '{}' has {} violation(s)", dataFile.getFileName(),
                        result.violations().size());
                failedFiles++;
            } else {
                LOG.info("File '{}' is valid", dataFile.getFileName());
            }
            System.out.println();
        }

        System.out.println("─".repeat(40));
        System.out.printf("Files checked : %d%n", totalFiles);
        System.out.printf("Files valid   : %d%n", totalFiles - failedFiles);
        System.out.printf("Files invalid : %d%n", failedFiles);
        LOG.info("Validation summary — checked: {}, valid: {}, invalid: {}",
                totalFiles, totalFiles - failedFiles, failedFiles);

        if (failedFiles > 0) System.exit(1);
    }

    // ── Normalise input to a JSON array ───────────────────────────────────────

    /**
     * Read a data file and return its contents as a JSON array.
     * Accepts both a top-level array [{...},{...}] and a single object {...}.
     * Returns empty if the content is neither.
     */
    private static Optional<JsonNode> getAsArray(ObjectMapper mapper, Path dataFile) {
        JsonNode records = parseJsonFile(mapper, dataFile, "data");
        if (!records.isArray() && records.isObject()) {
            LOG.debug("File '{}' contains a single object — wrapping in array", dataFile.getFileName());
            ArrayNode arr = mapper.createArrayNode();
            arr.add(records);
            return Optional.of(arr);
        } else if (records.isArray()) {
            return Optional.of(records);
        } else {
            return Optional.empty();
        }
    }

    // ── File resolution ───────────────────────────────────────────────────────

    private static List<Path> resolveDataFiles(String fileArg, String dirArg) {
        if (fileArg != null) {
            Path p = Path.of(fileArg).toAbsolutePath().normalize();
            validateFile(p, "data");
            return List.of(p);
        }

        Path dir = Path.of(dirArg).toAbsolutePath().normalize();
        if (!Files.exists(dir)) {
            LOG.error("Validation directory does not exist: {}", dir);
            System.err.println("ERROR: The validation directory does not exist: " + dir);
            System.exit(1);
        }
        if (!Files.isDirectory(dir)) {
            LOG.error("--validate-dir path is not a directory: {}", dir);
            System.err.println("ERROR: --validate-dir path is not a directory: " + dir);
            System.exit(1);
        }

        try (Stream<Path> stream = Files.list(dir)) {
            List<Path> files = stream
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".json"))
                    .filter(Files::isRegularFile)
                    .sorted()
                    .toList();

            if (files.isEmpty()) {
                LOG.error("No *.json files found in directory: {}", dir);
                System.err.println("ERROR: No *.json files found in directory: " + dir);
                System.exit(1);
            }
            LOG.debug("Found {} JSON file(s) in {}", files.size(), dir);
            return files;

        } catch (IOException e) {
            LOG.error("Could not read directory '{}': {}", dir, e.getMessage());
            System.err.println("ERROR: Could not read directory '%s': %s".formatted(dir, e.getMessage()));
            System.exit(1);
            return List.of();
        }
    }

    // ── Guards ────────────────────────────────────────────────────────────────

    private static void validateSchemaContent(JsonNode schema, Path path) {
        if (!schema.has("properties") || schema.get("properties").isEmpty()) {
            LOG.error("Schema file '{}' has no top-level 'properties'", path);
            System.err.println(
                    "ERROR: '%s' has no top-level 'properties' — not a valid object schema."
                            .formatted(path));
            System.exit(1);
        }
    }

    private static void validateFile(Path path, String role) {
        if (!Files.exists(path)) {
            LOG.error("The {} file does not exist: {}", role, path);
            System.err.println("ERROR: The %s file does not exist: %s".formatted(role, path));
            System.exit(1);
        }
        if (!Files.isRegularFile(path)) {
            LOG.error("The {} path is not a regular file: {}", role, path);
            System.err.println("ERROR: The %s path is not a regular file: %s".formatted(role, path));
            System.exit(1);
        }
        if (!Files.isReadable(path)) {
            LOG.error("The {} file is not readable: {}", role, path);
            System.err.println("ERROR: The %s file is not readable (check permissions): %s"
                    .formatted(role, path));
            System.exit(1);
        }
    }

    private static JsonNode parseJsonFile(ObjectMapper mapper, Path path, String role) {
        try {
            return mapper.readTree(path.toFile());
        } catch (NoSuchFileException e) {
            LOG.error("The {} file was not found: {}", role, path);
            System.err.println("ERROR: The %s file was not found: %s".formatted(role, path));
            System.exit(1);
        } catch (JsonParseException e) {
            LOG.error("The {} file is not valid JSON: {} — {}", role, path, e.getOriginalMessage());
            System.err.println("ERROR: The %s file is not valid JSON: %s".formatted(role, path));
            System.err.println("       " + e.getOriginalMessage());
            System.exit(1);
        } catch (IOException e) {
            LOG.error("Could not read {} file '{}': {}", role, path, e.getMessage());
            System.err.println("ERROR: Could not read %s file '%s': %s"
                    .formatted(role, path, e.getMessage()));
            System.exit(1);
        }
        return null; // unreachable
    }

    // ── Arg parsing ───────────────────────────────────────────────────────────

    private static String argValue(String[] args, String flag) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equalsIgnoreCase(flag)) return args[i + 1];
        }
        return null;
    }

    private static boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (arg.equalsIgnoreCase(flag)) return true;
        }
        return false;
    }

    // ── Help ──────────────────────────────────────────────────────────────────

    private static void printHelp() {
        System.out.println("""
                json-schema-duckdb  v%s
                Derive a DuckDB CREATE TABLE statement from a JSON Schema.

                USAGE
                  java -jar json-schema-duckdb.jar --schema <file> [OPTIONS]

                REQUIRED
                  --schema <file>          Path to the JSON Schema file.

                OPTIONS
                  --table <name>           Table name in the generated DDL (default: "data").

                  --validate-file <file>   Validate a single JSON data file against the schema.
                                           Accepts a JSON array or a single JSON object.
                                           Prints violations and exits with code 1 if invalid.

                  --validate-dir <folder>  Validate all *.json files in a folder against the schema.
                                           Each file may be a JSON array or a single JSON object.
                                           Files are processed in alphabetical order.
                                           Exits with code 1 if any file has violations.

                  --help                   Print this message and exit.

                Note: --validate-file and --validate-dir are mutually exclusive.
                      Validation never modifies data — it only checks structure.

                EXAMPLES
                  # Print CREATE TABLE for a schema
                  java -jar json-schema-duckdb.jar --schema order.json

                  # Print CREATE TABLE with a specific table name
                  java -jar json-schema-duckdb.jar --schema order.json --table orders

                  # Print DDL and validate a single file
                  java -jar json-schema-duckdb.jar --schema order.json --validate-file data.json

                  # Print DDL and validate all files in a folder
                  java -jar json-schema-duckdb.jar --schema order.json --validate-dir ./data/

                TYPE MAPPING
                  string                 -> VARCHAR
                  string (date)          -> DATE
                  string (date-time)     -> TIMESTAMP
                  string (uuid)          -> UUID
                  integer                -> BIGINT
                  number                 -> DOUBLE
                  boolean                -> BOOLEAN
                  object (properties)    -> STRUCT(...)
                  object (addtlProps)    -> MAP(VARCHAR, valueType)
                  array                  -> elementType[]
                  anyOf/oneOf (nullable) -> resolved non-null type
                  anyOf/oneOf (mixed)    -> VARCHAR  [WARN logged]
                  allOf                  -> merged STRUCT
                  $ref                   -> resolved from $defs / definitions

                VALIDATION CHECKS
                  - Required fields are present and non-null
                  - Field types match the schema declaration
                  - No unknown fields (not declared in 'properties')
                  - Top-level records are JSON objects

                LOGGING
                  Configure via log4j2.xml on the classpath or with:
                  -Dlog4j.configurationFile=/path/to/log4j2.xml
                  Default level: INFO for com.datamelt.*, WARN for everything else.

                EXIT CODES
                  0  Success
                  1  Argument error, invalid schema/file, or validation failure
                """.formatted(VERSION));
    }
}