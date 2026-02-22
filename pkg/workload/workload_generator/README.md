# Workload Generator

The workload generator is a tool that automatically generates realistic workloads from CockroachDB debug zips. It analyzes schema definitions and SQL statements from debug logs to recreate both the database schema and query patterns, enabling you to replay production-like workloads in testing or development environments.

## Overview

The workload generator extracts information from debug zips to create:
- **Schema YAML files** with table definitions, row counts, and relationships
- **SQL workload files** (read and write queries) extracted from statement statistics
- **Initialized tables** with synthetic data that respects foreign key constraints
- **Executable workloads** that replay production query patterns

This is useful for:
- Performance testing with production-like schemas and query patterns
- Reproducing issues from customer environments
- Load testing with realistic data distributions
- Benchmarking schema changes

## Prerequisites

1. **Debug zip**: An unzipped CockroachDB debug bundle containing:
   - `crdb_internal.create_statements.txt` (DDL statements)
   - `nodes/*/crdb_internal.node_statement_statistics.txt` (SQL queries)

   Generate a debug zip with:
   ```bash
   cockroach debug zip /tmp/debug.zip --url='postgresql://...'
   unzip /tmp/debug.zip -d /tmp/debug_logs
   ```

   **Note**: Schema is generally always present in debug zips. However, in rare cases where
   the schema information is missing from the debug zip, you can provide a DDL file as a fallback.

2. **Target database**: A running CockroachDB cluster where you want to initialize the workload

3. **DDL file** (fallback if schema missing from debug zip):

   If the debug zip is missing schema information, you can create a DDL file manually:
   ```bash
   cockroach sql --url='postgresql://...' \
     --execute="show create all tables;" > ddl_file.sql
   ```

## Quick Start

### List Available Databases

Before running the full init, discover which databases are in the debug logs:

```bash
./cockroach workload init workload_generator \
  'postgresql://root@localhost:26257?sslmode=disable' \
  --debug-logs=/path/to/debug_logs \
  --list-dbs
```

Output:
```
Databases found in debug logs:
  - my_app_db
  - analytics_db
```

### Initialize Schema and Data

Generate schema, load data, and create SQL workload files:

```bash
./cockroach workload init workload_generator \
  'postgresql://root@localhost:26257/my_app_db?sslmode=disable' \
  --debug-logs=/path/to/debug_logs \
  --rows=10000
```

This will:
1. Parse DDL from debug logs
2. Create tables in the target database
3. Generate synthetic data respecting FK constraints
4. Extract SQL queries from statement statistics
5. Write `my_app_db_read.sql` and `my_app_db_write.sql`
6. Write `schema_my_app_db.yaml`

### Run the Workload

Execute the extracted workload against your database:

```bash
./cockroach workload run workload_generator \
  'postgresql://root@localhost:26257/my_app_db?sslmode=disable' \
  --input-yaml=schema_my_app_db.yaml \
  --duration=5m \
  --concurrency=50 \
  --read-pct=70
```

## Common Use Cases

### Schema-Only Mode

Generate schema files without initializing tables (useful for reviewing before loading data):

```bash
./cockroach workload init workload_generator \
  'postgresql://root@localhost:26257/test_db?sslmode=disable' \
  --debug-logs=/path/to/debug_logs \
  --schema-only
```

### Using a DDL File (Fallback for Missing Schema)

Schema is generally always present in debug zips. Use the `--ddl-file` option only in rare
cases where the schema is missing from the debug zip:

```bash
# First, create the DDL file from the source database
cockroach sql --url='postgresql://source-db:26257/mydb' \
  --execute="show create all tables;" > ddl_export.sql

# Then use it with workload_generator
./cockroach workload init workload_generator \
  'postgresql://root@localhost:26257/test_db?sslmode=disable' \
  --ddl-file=ddl_export.sql \
  --rows=5000
```

**Note**: When using `--ddl-file`, SQL workload extraction still requires the debug zip's
statement statistics. The DDL file only provides the schema information.

### Custom Output Directory

Write generated files to a specific location:

```bash
./cockroach workload init workload_generator \
  'postgresql://root@localhost:26257/test_db?sslmode=disable' \
  --debug-logs=/path/to/debug_logs \
  --output-dir=/path/to/workloads
```

Output files:
- `/path/to/workloads/schema_test_db.yaml`
- `/path/to/workloads/test_db_read.sql`
- `/path/to/workloads/test_db_write.sql`

### Using Pre-Generated Schema YAML

Skip schema generation and use an existing YAML file:

```bash
./cockroach workload init workload_generator \
  'postgresql://root@localhost:26257/test_db?sslmode=disable' \
  --input-yaml=schema_test_db.yaml
```

## Key Flags

### Init Command

| Flag | Default | Description |
|------|---------|-------------|
| `--debug-logs` | "" | Path to unzipped debug logs directory |
| `--ddl-file` | "" | Path to DDL file (fallback if schema missing from debug zip) |
| `--list-dbs` | false | List databases in debug logs and exit |
| `--rows` | 1000 | Base row count for tables without FKs |
| `--input-yaml` | "" | Use existing schema YAML instead of generating |
| `--output-dir` | "." | Directory for generated files |
| `--schema-only` | false | Generate schema files only, skip table init |
| `--db` | "" | Override database name |
| `--drop` | false | Drop existing database before init |

### Run Command

| Flag | Default | Description |
|------|---------|-------------|
| `--input-yaml` | "" | Path to schema YAML file (required for run) |
| `--duration` | 0 | How long to run (0 = forever) |
| `--concurrency` | 24 | Number of concurrent workers |
| `--read-pct` | 50 | Percentage of operations that are reads (0-100) |
| `--max-rate` | 0 | Max operations per second (0 = unlimited) |
| `--tx-timeout` | 5m | Per-transaction timeout |
| `--histograms` | "" | File to write latency histogram data |

## How It Works

### Schema Generation (Init Phase)

1. **Parse DDL**: Extracts table definitions from `crdb_internal.create_statements.txt`
   - Identifies columns, types, constraints, foreign keys
   - Builds dependency graph for table ordering

2. **Build Schema**: Creates YAML schema with metadata for each table:
   - Row counts scaled by FK depth and fanout
   - Column generators (sequences, UUIDs, foreign key refs, etc.)
   - Constraint information (PKs, unique, check constraints)

3. **Generate Data**: Populates tables with synthetic data:
   - Respects foreign key relationships
   - Honors unique and check constraints
   - Uses appropriate generators per column type
   - Loads in topological order (parents before children)

4. **Extract Workload**: Parses SQL queries from statement statistics:
   - Rewrites placeholders (`_`, `__more__`) with column-aware placeholders
   - Groups queries by transaction fingerprint
   - Separates into read vs write workloads
   - Writes to `<dbname>_read.sql` and `<dbname>_write.sql`

### Workload Execution (Run Phase)

1. **Load Schema**: Reads YAML to understand table structure and generators
2. **Parse Queries**: Loads SQL files and identifies placeholders
3. **Execute Transactions**: For each operation:
   - Selects read or write transaction based on `--read-pct`
   - Generates data for placeholders using column generators
   - Executes transaction with proper isolation level
   - Records latency metrics

## Output Files

### Schema YAML (`schema_<dbname>.yaml`)

Contains complete schema metadata:

```yaml
users:
  count: 10000
  columns:
    id:
      type: sequence
      isPrimaryKey: true
    email:
      type: string
      args:
        pattern: "email"
      isUnique: true
    created_at:
      type: timestamp
  pk: [id]
  original_table: public.users
  column_order: [id, email, created_at]

orders:
  count: 50000  # Scaled by FK fanout
  columns:
    id:
      type: sequence
      isPrimaryKey: true
    user_id:
      type: int
      hasForeignKey: true
      fk: users.id
      fanout: 5
  pk: [id]
```

### Read SQL (`<dbname>_read.sql`)

Contains SELECT queries extracted from statement statistics:

```sql
-- Transaction 1
select id, email from users where id = $1;
select count(*) from orders where user_id = $2;

-- Transaction 2
select * from orders where created_at > $1 limit $2;
```

### Write SQL (`<dbname>_write.sql`)

Contains INSERT, UPDATE, DELETE queries:

```sql
-- Transaction 1
insert into users (email, created_at) values ($1, $2);

-- Transaction 2
update orders set status = $1 where id = $2;
```

## Data Generation

The workload generator creates synthetic data that respects your schema:

- **Sequences**: Auto-incrementing integers for `SERIAL`, `INT` PKs
- **UUIDs**: `gen_random_uuid()` for UUID columns
- **Strings**: Pattern-based generation (emails, names, addresses, etc.)
- **Timestamps**: Realistic distributions within time ranges
- **Foreign Keys**: Valid references to parent table PKs
- **Unique Constraints**: Guaranteed unique value generation
- **Check Constraints**: Values that satisfy check expressions
- **Defaults**: Uses column defaults when specified

### Row Count Scaling

Row counts are automatically scaled based on foreign key relationships:

```
Base table (no FKs):     1000 rows  (--rows value)
1 level deep (FKs):      5000 rows  (1000 × fanout of 5)
2 levels deep:          25000 rows  (5000 × fanout of 5)
```

Override with `--rows` flag to scale the entire workload.

## Troubleshooting

### Error: "missing database_name column in TSV file"

The debug zip is incomplete or corrupted. Ensure:
- Debug zip is from CockroachDB v20.2+
- File `crdb_internal.create_statements.txt` exists and is valid
- File was properly unzipped

### Error: "failed to open TSV file"

Check that `--debug-logs` points to the **unzipped** directory, not the .zip file:

```bash
# Wrong
--debug-logs=/tmp/debug.zip

# Correct
--debug-logs=/tmp/debug_logs
```

### Error: "no tables found for database"

The database name doesn't match what's in the debug logs. Use `--list-dbs` to see available databases:

```bash
./cockroach workload init workload_generator \
  'postgresql://...' \
  --debug-logs=/path/to/debug_logs \
  --list-dbs
```

### Schema missing from debug zip

In rare cases, the debug zip may not contain schema information in `crdb_internal.create_statements.txt`.
If this happens, create a DDL file manually from the source database:

```bash
# Extract DDL from the source database
cockroach sql --url='postgresql://source-db:26257/mydb' \
  --execute="show create all tables;" > ddl_export.sql

# Use the DDL file with workload_generator
./cockroach workload init workload_generator \
  'postgresql://...' \
  --ddl-file=ddl_export.sql \
  --debug-logs=/path/to/debug_logs \
  --rows=10000
```

The `--ddl-file` flag provides the schema while the debug logs still provide SQL workload queries.

### Tables loaded in wrong order (FK violations)

This indicates a bug in topological sorting. As a workaround:
1. Use `--schema-only` to generate files
2. Manually load DDL in correct order
3. Use `--input-yaml` to run workload

### Workload runs slowly

- Increase `--concurrency` (default: 24)
- Reduce `--rows` for faster initialization
- Use `--max-rate` to limit throughput if overwhelming cluster
- Check `--tx-timeout` if queries are timing out

### Memory issues during data generation

Large tables can consume significant memory. Solutions:
- Reduce `--rows` value
- Use `--schema-only` then manually load smaller batches
- Increase available memory for the workload process

## Advanced Usage

### Multi-Database Workloads

If your debug logs contain multiple databases:

```bash
# List all databases
./cockroach workload init workload_generator \
  'postgresql://...' --debug-logs=/path/to/logs --list-dbs

# Initialize each separately
for db in db1 db2 db3; do
  ./cockroach workload init workload_generator \
    "postgresql://.../$db?sslmode=disable" \
    --debug-logs=/path/to/logs \
    --rows=5000
done
```

### Combining with Other Tools

Export histograms for analysis:

```bash
./cockroach workload run workload_generator \
  'postgresql://...' \
  --input-yaml=schema_mydb.yaml \
  --duration=10m \
  --histograms=latency.json
```

Integrate with monitoring:

```bash
./cockroach workload run workload_generator \
  'postgresql://...' \
  --input-yaml=schema_mydb.yaml \
  --prometheus-port=2112 \
  --duration=1h
```

### Customizing Generated Data

Edit the `schema_<dbname>.yaml` file to customize data generation:

```yaml
users:
  count: 50000  # Change row count
  columns:
    email:
      type: string
      args:
        pattern: "custom@example.com"  # Custom pattern
    status:
      type: enum
      args:
        values: ["active", "inactive", "pending"]  # Custom enum values
```

Then reinitialize with the modified YAML:

```bash
./cockroach workload init workload_generator \
  'postgresql://...' \
  --input-yaml=schema_mydb.yaml \
  --drop  # Drop and recreate with new settings
```

## Performance Considerations

- **Initialization time** scales with row count and FK complexity
  - Small workload (1K rows): ~1 minute
  - Medium workload (100K rows): ~10 minutes
  - Large workload (1M+ rows): 30+ minutes

- **Run performance** depends on query complexity and concurrency
  - Start with default concurrency (24)
  - Increase gradually based on cluster capacity
  - Monitor cluster metrics (CPU, memory, disk I/O)

- **Memory usage** during init scales with batch size and table count
  - Expect ~100MB per 100K rows during data generation
  - Reduce `--rows` if memory constrained

## Examples

### Complete Workflow Example

```bash
# 1. Generate debug zip from production
cockroach debug zip /tmp/prod_debug.zip \
  --url='postgresql://root@prod-cluster:26257'

# 2. Unzip
unzip /tmp/prod_debug.zip -d /tmp/prod_logs

# 3. List available databases
./cockroach workload init workload_generator \
  'postgresql://root@test-cluster:26257?sslmode=disable' \
  --debug-logs=/tmp/prod_logs \
  --list-dbs

# 4. Initialize workload (10K base rows)
./cockroach workload init workload_generator \
  'postgresql://root@test-cluster:26257/myapp?sslmode=disable' \
  --debug-logs=/tmp/prod_logs \
  --rows=10000 \
  --output-dir=./workloads

# 5. Run workload for 1 hour with metrics
./cockroach workload run workload_generator \
  'postgresql://root@test-cluster:26257/myapp?sslmode=disable' \
  --input-yaml=./workloads/schema_myapp.yaml \
  --duration=1h \
  --concurrency=50 \
  --read-pct=80 \
  --histograms=./workloads/metrics.json \
  --prometheus-port=2112
```

### Testing Schema Changes

```bash
# Generate baseline workload from production
./cockroach workload init workload_generator \
  'postgresql://root@test1:26257/app?sslmode=disable' \
  --debug-logs=/tmp/prod_logs \
  --rows=50000

# Run baseline performance test
./cockroach workload run workload_generator \
  'postgresql://root@test1:26257/app?sslmode=disable' \
  --input-yaml=schema_app.yaml \
  --duration=10m \
  --histograms=baseline.json

# Apply schema changes to test2
# ... make schema modifications ...

# Run workload against modified schema
./cockroach workload run workload_generator \
  'postgresql://root@test2:26257/app?sslmode=disable' \
  --input-yaml=schema_app.yaml \
  --duration=10m \
  --histograms=modified.json

# Compare metrics
diff baseline.json modified.json
```

## Limitations

- **Computed columns**: Not supported; removed from generated DDL
- **Materialized views**: Not captured in schema generation
- **Stored procedures**: Not extracted from debug logs
- **Sequences**: Current values not preserved; start from 1
- **Partial indexes**: Supported but data may not optimize for them
- **Internal schemas**: `information_schema`, `crdb_internal` are filtered out
- **Cross-database queries**: Limited support; may be skipped during extraction

## Contributing

When making changes to the workload generator:

1. Update tests in `*_test.go` files
2. Run `./dev test pkg/workload/workload_generator`
3. Update this README if adding new flags or functionality
4. Format code with `crlfmt -w -tab 2 *.go`

## See Also

- [CockroachDB Workload Framework](../README.md)
- [Debug Zip Documentation](https://www.cockroachlabs.com/docs/stable/cockroach-debug-zip.html)
- [Statement Statistics](https://www.cockroachlabs.com/docs/stable/ui-statements-page.html)