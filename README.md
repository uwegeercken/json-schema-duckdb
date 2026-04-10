# JSON Schema to DuckDB

Generates a DuckDB `CREATE TABLE` statement from a JSON Schema and optionally validates JSON data files against the schema.

## Requirements

- Java 21+
- Maven 3.8+

## Maven Dependency

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.uwegeercken</groupId>
    <artifactId>json-schema-duckdb</artifactId>
    <version>1.0.3</version>
</dependency>
```

The artifact is available on Maven Central: https://central.sonatype.com/artifact/io.github.uwegeercken/json-schema-duckdb

---

## DDL Generation
Specify the name of the table and pass the schema file to generate a duckdb create table statement.

### From a file path

```java
// CREATE TABLE IF NOT EXISTS (default)
String ddl = JsonSchemaDdlGenerator.generateDdl("orders", Path.of("order.json"));

// CREATE TABLE (without IF NOT EXISTS)
String ddl = JsonSchemaDdlGenerator.generateDdl("orders", Path.of("order.json"), false);
```

### From a classpath resource stream

```java
// CREATE TABLE IF NOT EXISTS (default)
String ddl = JsonSchemaDdlGenerator.generateDdl("orders", getClass().getResourceAsStream("/order.json"));

// CREATE TABLE (without IF NOT EXISTS)
String ddl = JsonSchemaDdlGenerator.generateDdl("orders", getClass().getResourceAsStream("/order.json"), false);
```

### From a pre-parsed JsonNode

```java
JsonNode schema = new ObjectMapper().readTree(Path.of("order.json").toFile());

// CREATE TABLE IF NOT EXISTS (default)
String ddl = JsonSchemaDdlGenerator.generateDdl("orders", schema);

// CREATE TABLE (without IF NOT EXISTS)
String ddl = JsonSchemaDdlGenerator.generateDdl("orders", schema, false);
```

---

## Validation

Validates JSON data records against a JSON Schema. Checks performed per record:

- All fields declared in `required` are present and non-null
- All present fields match their declared type
- Nested objects and arrays are recursively validated
- `$ref`, `allOf`, `anyOf`, `oneOf` are structurally validated
- Unknown fields (not declared in `properties`) are reported as warnings or errors

This is a structural validator — it does not evaluate `format`, `pattern`, `minimum`/`maximum`, or `enum` constraints.

### Basic usage

```java
JsonSchema schema = new JsonSchema("order.json");
JsonFileValidator validator = new JsonFileValidator(schema);

// Validate a single file
ValidationResult result = validator.validate(Path.of("data.json"));

// Validate from a file path string
ValidationResult result = validator.validate("data.json");

// Validate multiple files in a loop
for (Path file : files) {
ValidationResult result = validator.validate(file);
}

// Validate a pre-parsed JsonNode array
ValidationResult result = validator.validate(recordsNode);

System.out.println("Records validated: " + result.getTotalRecords());
System.out.println("Valid: "             + result.isValid());
```

### Strict mode — fail on unknown fields

By default, unknown fields (fields present in the data but not declared in the schema) produce
a `WARNING` violation and do not affect `isValid()`. Pass `true` to treat them as errors instead:

```java
JsonSchema schema = new JsonSchema("order.json");
JsonFileValidator validator = new JsonFileValidator(schema, true);

ValidationResult result = validator.validate(Path.of("data.json"));
System.out.println("Valid: " + result.isValid()); // false if any unknown fields found
```

### Working with violations

```java
ValidationResult result = validator.validate(Path.of("data.json"));

// All violations (errors and warnings)
result.getViolations().forEach(System.out::println);

// Errors only (affect isValid())
result.getErrors().forEach(v ->
    System.out.printf("ERROR   [record #%d] %s: %s%n",
        v.getRecordIndex() + 1, v.getField(), v.getMessage()));

// Warnings only (do not affect isValid())
result.getWarnings().forEach(v ->
    System.out.printf("WARNING [record #%d] %s: %s%n",
        v.getRecordIndex() + 1, v.getField(), v.getMessage()));
```

Each `Violation` contains:

| Method              | Description                              |
|---------------------|------------------------------------------|
| `getRecordIndex()`  | Zero-based index of the offending record |
| `getField()`        | Dotted path to the field, e.g. `(root).person.address.city` |
| `getMessage()`      | Human-readable description of the problem |
| `getSeverity()`     | `ERROR` or `WARNING`                     |


## Type Mapping

| JSON Schema type             | Format / notes                  | DuckDB type               |
|------------------------------|---------------------------------|---------------------------|
| `string`                     |                                 | `VARCHAR`                 |
| `string`                     | `format: date`                  | `DATE`                    |
| `string`                     | `format: date-time`             | `TIMESTAMP`               |
| `string`                     | `format: time`                  | `TIME`                    |
| `string`                     | `format: uuid`                  | `UUID`                    |
| `integer`                    |                                 | `BIGINT`                  |
| `integer`                    | `format: int32`                 | `INTEGER`                 |
| `integer`                    | `format: int16`                 | `SMALLINT`                |
| `integer`                    | `format: int8`                  | `TINYINT`                 |
| `number`                     |                                 | `DOUBLE`                  |
| `number`                     | `format: float`                 | `FLOAT`                   |
| `boolean`                    |                                 | `BOOLEAN`                 |
| `object` with `properties`   |                                 | `STRUCT(field TYPE, ...)` |
| `object` with `additionalProperties` |                         | `MAP(VARCHAR, valueType)` |
| `object` without properties  |                                 | `VARCHAR` (raw JSON text) |
| `array`                      | single `items` schema           | `<elementType>[]`         |
| `array`                      | tuple-style `items` array       | `VARCHAR[]` (fallback)    |
| `anyOf` / `oneOf`            | single non-null variant         | resolved type             |
| `anyOf` / `oneOf`            | multiple distinct types         | `VARCHAR` (fallback)      |
| `allOf`                      | sub-schemas with `properties`   | `STRUCT(field TYPE, ...)` |
| `allOf`                      | sub-schemas with primitive type | resolved primitive type   |
| `$ref`                       | internal (`#/definitions/...`)  | resolved type             |
| `$ref`                       | external                        | `VARCHAR` (fallback)      |

---

## Key Design Decisions

- **Optional fields** → nullable columns (no `NOT NULL` constraint)
- **Required fields** → `NOT NULL` constraint, propagated into nested `STRUCT` fields
- **Nested objects** → DuckDB `STRUCT` type, recursively resolved
- **Arrays** → DuckDB native array type (e.g. `VARCHAR[]`), recursively resolved
- **`allOf` with primitives** → resolved to the common primitive type, not a `STRUCT`
- **`anyOf`/`oneOf`** → resolved if all non-null variants share a common type, otherwise `VARCHAR`
- **Unknown object** (no `properties`) → `VARCHAR` (store raw JSON)
- **Circular `$ref`** → detected at validation time, throws `IllegalStateException`
- **External `$ref`** → reported as a violation with a clear message, field is skipped