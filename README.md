# JSON Schema → DuckDB (Java 21)

Generates a DuckDB `CREATE TABLE` statement from a JSON Schema, then loads
JSON records into it — handling optional fields as nullable columns.

## Requirements

- Java 21+
- Maven 3.8+

## Build & Run

```bash
mvn package -DskipTests
java -jar target/json-schema-duckdb-1.0-SNAPSHOT.jar
```

## Expected Output

```
=== Generated DDL ===
CREATE TABLE IF NOT EXISTS orders (
    id BIGINT NOT NULL,
    customer_name VARCHAR NOT NULL,
    email VARCHAR,
    total_amount DOUBLE NOT NULL,
    discount_pct DOUBLE,
    order_date DATE NOT NULL,
    shipped_at TIMESTAMP,
    is_paid BOOLEAN,
    item_count BIGINT,
    address STRUCT(street VARCHAR, city VARCHAR, country VARCHAR),
    tags VARCHAR[]
);

✓ Table created.
✓ Inserted 4 records.

=== Table Contents ===
...

=== Optional Field NULL Stats ===
  total_rows          : 4
  has_email           : 2
  has_discount        : 2
  has_shipped_at      : 1
  has_address         : 2
```

## Project Structure

```
src/main/java/com/example/
  ├── Main.java                     # Full workflow demo
  ├── JsonSchemaDdlGenerator.java   # Schema → DDL generator
  └── JsonSchemaToDuckDbType.java   # JSON Schema type → DuckDB type mapper

src/main/resources/
  ├── schema.json   # Your JSON Schema (edit this)
  └── data.json     # Sample records (edit this)
```

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

## Using a Persistent Database File

Change the connection string in `Main.java`:

```java
// In-memory (default)
DriverManager.getConnection("jdbc:duckdb:")

// Persistent file
DriverManager.getConnection("jdbc:duckdb:/path/to/my.duckdb")
```

## Loading from JSON Files Directly

Once the table exists, you can also load JSON files via DuckDB SQL:

```java
stmt.execute("""
    INSERT INTO orders
    SELECT * FROM read_json('data.json',
        columns = {
            id: 'BIGINT',
            customer_name: 'VARCHAR',
            ...
        }
    )
""");
```