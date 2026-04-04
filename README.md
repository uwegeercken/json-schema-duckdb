# JSON Schema to DuckDB

Generates a DuckDB `CREATE TABLE` statement from a JSON Schema and optionally validates data files.

## Requirements

- Java 21+
- Maven 3.8+

## Usage from Code
You may also use the tool programmatically by adding it as a dependency to your project.

Add the following dependency to your Maven pom.xml:

```xml
<dependency>
    <groupId>io.github.uwegeercken</groupId>
    <artifactId>json-schema-duckdb</artifactId>
    <version>1.0.0</version>
</dependency>
```

The artifact is available on Maven Central: https://central.sonatype.com/artifact/io.github.uwegeercken/json-schema-duckdb

Example usage:

```java
// From a path
String ddl = JsonSchemaDdlGenerator.generateDdl("orders", Path.of("order.json"));

// From a stream (e.g. from classpath resource)
String ddl = JsonSchemaDdlGenerator.generateDdl("orders", getClass().getResourceAsStream("/order.json"));
```

## Usage from Main Program

    USAGE
      java -jar json-schema-duckdb.jar --schema <file> [OPTIONS]

    REQUIRED
      --schema <file>          Path to the JSON Schema file.

    OPTIONS
      --table <name>           Table name in the generated DDL (default: "data").

      --validate-file <file>   Validate a single JSON data file against the schema.
                               Prints violations and exits with code 1 if invalid.

      --validate-dir <folder>  Validate all *.json files in a folder against the schema.
                               Files are processed in alphabetical order.
                               Exits with code 1 if any file has violations.

      --help                   Print this message and exit.

    Note: --validate-file and --validate-dir are mutually exclusive.
          Validation never loads data — it only checks structure.

    EXAMPLES
      # Print CREATE TABLE for a schema
      java -jar json-schema-duckdb.jar --schema order.json

      # Print CREATE TABLE with a specific table name
      java -jar json-schema-duckdb.jar --schema order.json --table orders

      # Print DDL and validate a single file
      java -jar json-schema-duckdb.jar --schema order.json --validate-file data.json

      # Print DDL and validate all files in a folder
      java -jar json-schema-duckdb.jar --schema order.json --validate-dir ./data/

## Type Mapping

| JSON Schema              | DuckDB Type                  |
|--------------------------|------------------------------|
| `string`                 | `VARCHAR`                    |
| `string` + `date`        | `DATE`                       |
| `string` + `date-time`   | `TIMESTAMP`                  |
| `string` + `time`        | `TIME`                       |
| `string` + `uuid`        | `UUID`                       |
| `integer`                | `BIGINT`                     |
| `number`                 | `DOUBLE`                     |
| `boolean`                | `BOOLEAN`                    |
| `object` with properties | `STRUCT(field TYPE, ...)`    |
| `object` no properties   | `VARCHAR` (raw JSON text)    |
| `array`                  | `<inner_type>[]`             |
| `anyOf` / `oneOf`        | `VARCHAR` (fallback)         |

## Key Design Decisions

- **Optional fields** → nullable columns (no `NOT NULL` constraint)
- **Required fields** → `NOT NULL` constraint (from JSON Schema `required` array)
- **Nested objects** → DuckDB `STRUCT` type
- **Arrays** → DuckDB native array type (e.g. `VARCHAR[]`)
- **`anyOf`/`oneOf`** → `VARCHAR` fallback (store as JSON text)
- **Unknown object** (no `properties`) → `VARCHAR` (store raw JSON)

