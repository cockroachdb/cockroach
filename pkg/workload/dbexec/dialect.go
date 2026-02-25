// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbexec

import (
	"fmt"
	"strings"
)

// SchemaDialect generates DDL for table creation during workload init.
// All Dialect implementations satisfy this interface. Standalone
// implementations exist for databases (like Spanner) that don't use
// the full Dialect for queries.
type SchemaDialect interface {
	// SchemaFragment returns the CREATE TABLE body (columns, constraints)
	// for use as workload.Table.Schema. The result must NOT include the
	// "CREATE TABLE <name>" prefix -- the workload framework adds that.
	//
	// keySize controls the primary key type:
	//   - keySize == 0: integer key (INT/BIGINT/INT64 depending on dialect)
	//   - keySize > 0: string key of that width (STRING(N)/VARCHAR(N))
	//
	// If secondaryIndex is true, the schema includes a secondary index on v
	// (inline for CRDB, separate statement via SecondaryIndexStmts for others).
	//
	// numShards specifies hash bucket count for hash-sharded primary keys.
	// Ignored by dialects that don't support hash sharding.
	SchemaFragment(keySize int, secondaryIndex bool, numShards int) string

	// SecondaryIndexStmts returns DDL to create secondary indexes as separate
	// statements (outside CREATE TABLE), or nil if the index is included
	// inline in SchemaFragment. Each statement must be valid for the target
	// dialect. Only called when secondaryIndex is true.
	SecondaryIndexStmts(table string) []string

	// PostLoadStmts returns DDL to execute after table creation and data
	// loading. Used for operations that must happen after the table exists,
	// such as adding computed columns or scattering ranges.
	// Returns nil if no post-load DDL is needed.
	PostLoadStmts(table string, enum, scatter bool) []string

	// Capabilities returns what features this dialect supports.
	Capabilities() Capabilities
}

// Dialect defines SQL syntax variations between databases. Each database
// family has different syntax for upserts, follower reads, schema features,
// and other operations. Implementations generate the appropriate SQL for
// their target database.
type Dialect interface {
	SchemaDialect

	// Name returns a short identifier for this dialect (e.g., "crdb", "postgres").
	Name() string

	// UpsertStmt returns an upsert statement for batchSize rows.
	// If hasEnum is true, the statement includes the enum column.
	UpsertStmt(table string, batchSize int, hasEnum bool) string

	// ReadStmt returns a batch read statement for batchSize keys.
	// If hasEnum is true, the statement selects the enum column.
	ReadStmt(table string, batchSize int, hasEnum bool) string

	// FollowerReadStmt returns a stale/replica read statement for batchSize keys.
	// If hasEnum is true, the statement selects the enum column.
	// Returns the same as ReadStmt if follower reads are not supported.
	FollowerReadStmt(table string, batchSize int, hasEnum bool) string

	// DeleteStmt returns a batch delete statement for batchSize keys.
	DeleteStmt(table string, batchSize int) string

	// SpanStmt returns a spanning query statement with the given limit.
	// The statement counts values starting from a given key.
	SpanStmt(table string, limit int) string

	// SelectForUpdateStmt returns a SELECT ... FOR UPDATE statement for
	// batchSize keys.
	SelectForUpdateStmt(table string, batchSize int) string
}

// placeholders generates "$1, $2, ..., $n" for parameterized queries.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(parts, ", ")
}

// valueTuples generates "($1, $2), ($3, $4), ..." for batch inserts.
// Each tuple has tupleSize placeholders, and there are count tuples.
func valueTuples(count, tupleSize int) string {
	if count <= 0 || tupleSize <= 0 {
		return ""
	}
	tuples := make([]string, count)
	for i := range tuples {
		parts := make([]string, tupleSize)
		for j := range parts {
			parts[j] = fmt.Sprintf("$%d", i*tupleSize+j+1)
		}
		tuples[i] = "(" + strings.Join(parts, ", ") + ")"
	}
	return strings.Join(tuples, ", ")
}

// CockroachDialect implements Dialect for CockroachDB. It supports all
// CockroachDB-specific features including follower reads, hash-sharded
// primary keys, and transaction QoS.
type CockroachDialect struct{}

var _ Dialect = CockroachDialect{}

// Name returns "crdb" as the dialect identifier.
func (CockroachDialect) Name() string {
	return "crdb"
}

// UpsertStmt generates a CockroachDB UPSERT statement. The hasEnum parameter
// is ignored because CockroachDB's enum column is a computed stored column
// (AS ('v') STORED) that cannot be written to directly.
//
// Example (batchSize=2):
//
//	UPSERT INTO kv (k, v) VALUES ($1, $2), ($3, $4)
func (CockroachDialect) UpsertStmt(table string, batchSize int, hasEnum bool) string {
	// hasEnum is intentionally ignored: the e column is a computed stored column
	// in CockroachDB and cannot be included in UPSERT statements.
	return fmt.Sprintf("UPSERT INTO %s (k, v) VALUES %s", table, valueTuples(batchSize, 2))
}

// ReadStmt generates a batch SELECT statement.
//
// Example without enum (batchSize=3):
//
//	SELECT k, v FROM kv WHERE k IN ($1, $2, $3)
//
// Example with enum (batchSize=3):
//
//	SELECT k, v, e FROM kv WHERE k IN ($1, $2, $3)
func (CockroachDialect) ReadStmt(table string, batchSize int, hasEnum bool) string {
	cols := "k, v"
	if hasEnum {
		cols = "k, v, e"
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE k IN (%s)", cols, table, placeholders(batchSize))
}

// FollowerReadStmt generates a CockroachDB follower read statement using
// AS OF SYSTEM TIME follower_read_timestamp().
//
// Example (batchSize=2):
//
//	SELECT k, v FROM kv AS OF SYSTEM TIME follower_read_timestamp() WHERE k IN ($1, $2)
func (CockroachDialect) FollowerReadStmt(table string, batchSize int, hasEnum bool) string {
	cols := "k, v"
	if hasEnum {
		cols = "k, v, e"
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s AS OF SYSTEM TIME follower_read_timestamp() WHERE k IN (%s)",
		cols, table, placeholders(batchSize),
	)
}

// DeleteStmt generates a batch DELETE statement.
//
// Example (batchSize=3):
//
//	DELETE FROM kv WHERE k IN ($1, $2, $3)
func (CockroachDialect) DeleteStmt(table string, batchSize int) string {
	return fmt.Sprintf("DELETE FROM %s WHERE k IN (%s)", table, placeholders(batchSize))
}

// SpanStmt generates a spanning query using CockroachDB's scalar subquery syntax.
// When limit > 0, the statement counts values starting from key $1 with the
// given limit. When limit == 0, it performs a full table scan with no arguments.
//
// Example (limit=100):
//
//	SELECT count(v) FROM [SELECT v FROM kv WHERE k >= $1 ORDER BY k LIMIT 100]
//
// Example (limit=0):
//
//	SELECT count(v) FROM [SELECT v FROM kv]
func (CockroachDialect) SpanStmt(table string, limit int) string {
	if limit == 0 {
		return fmt.Sprintf("SELECT count(v) FROM [SELECT v FROM %s]", table)
	}
	return fmt.Sprintf(
		"SELECT count(v) FROM [SELECT v FROM %s WHERE k >= $1 ORDER BY k LIMIT %d]",
		table, limit,
	)
}

// SelectForUpdateStmt generates a SELECT ... FOR UPDATE statement.
//
// Example (batchSize=2):
//
//	SELECT k, v FROM kv WHERE k IN ($1, $2) FOR UPDATE
func (CockroachDialect) SelectForUpdateStmt(table string, batchSize int) string {
	return fmt.Sprintf("SELECT k, v FROM %s WHERE k IN (%s) FOR UPDATE", table, placeholders(batchSize))
}

// SchemaFragment generates the CockroachDB CREATE TABLE body for use as
// workload.Table.Schema. The output matches the production schema constants
// in kv.go (kvSchema, kvSchemaWithIndex, shardedKvSchema, etc.).
//
// Example (keySize=0, no shards, no index):
//
//	(
//		k BIGINT NOT NULL PRIMARY KEY,
//		v BYTES NOT NULL
//	)
//
// Example (keySize=0, numShards=8, secondaryIndex=true):
//
//	(
//		k BIGINT NOT NULL PRIMARY KEY USING HASH WITH (bucket_count = 8),
//		v BYTES NOT NULL,
//		INDEX (v)
//	)
func (CockroachDialect) SchemaFragment(keySize int, secondaryIndex bool, numShards int) string {
	keyType := "BIGINT"
	if keySize > 0 {
		keyType = "STRING"
	}

	var sb strings.Builder
	sb.WriteString("(\n")

	if numShards > 0 {
		sb.WriteString(fmt.Sprintf(
			"\t\tk %s NOT NULL PRIMARY KEY USING HASH WITH (bucket_count = %d),\n",
			keyType, numShards))
	} else {
		sb.WriteString(fmt.Sprintf("\t\tk %s NOT NULL PRIMARY KEY,\n", keyType))
	}

	sb.WriteString("\t\tv BYTES NOT NULL")
	if secondaryIndex {
		sb.WriteString(",\n\t\tINDEX (v)")
	}
	sb.WriteString("\n\t)")
	return sb.String()
}

// SecondaryIndexStmts returns nil because CockroachDB includes secondary
// indexes inline in the CREATE TABLE statement via SchemaFragment.
func (CockroachDialect) SecondaryIndexStmts(table string) []string {
	return nil
}

// PostLoadStmts returns DDL to execute after table creation and data loading.
// For CockroachDB, this includes adding computed enum columns and scattering
// ranges across the cluster.
//
// The enum DDL combines CREATE TYPE and ALTER TABLE into a single string
// because they are executed together as a multi-statement batch via db.Exec().
// This matches the original behavior in kv.go's PostLoad hook.
func (CockroachDialect) PostLoadStmts(table string, enum, scatter bool) []string {
	var stmts []string
	if enum {
		stmts = append(stmts, fmt.Sprintf(
			"CREATE TYPE enum_type AS ENUM ('v');\n"+
				"ALTER TABLE %s ADD COLUMN e enum_type NOT NULL AS ('v') STORED;",
			table))
	}
	if scatter {
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s SCATTER", table))
	}
	return stmts
}

// Capabilities returns CockroachDB's full feature set.
func (CockroachDialect) Capabilities() Capabilities {
	return Capabilities{
		FollowerReads:       true,
		Scatter:             true,
		Splits:              true,
		HashShardedPK:       true,
		TransactionQoS:      true,
		SelectForUpdate:     true,
		TransactionPriority: true,
		SerializationRetry:  true,
		Select1:             true,
		EnumColumn:          true,
	}
}

// PostgresDialect implements Dialect for PostgreSQL-compatible databases.
// This includes PostgreSQL, Aurora PostgreSQL, and other compatible databases.
// It does not support CockroachDB-specific features like follower reads or
// hash-sharded primary keys.
type PostgresDialect struct{}

var _ Dialect = PostgresDialect{}

// Name returns "postgres" as the dialect identifier.
func (PostgresDialect) Name() string {
	return "postgres"
}

// UpsertStmt generates a PostgreSQL INSERT ... ON CONFLICT statement.
//
// Example without enum (batchSize=2):
//
//	INSERT INTO kv (k, v) VALUES ($1, $2), ($3, $4)
//	ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
//
// Example with enum (batchSize=2):
//
//	INSERT INTO kv (k, v, e) VALUES ($1, $2, $3), ($4, $5, $6)
//	ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, e = EXCLUDED.e
func (PostgresDialect) UpsertStmt(table string, batchSize int, hasEnum bool) string {
	if hasEnum {
		return fmt.Sprintf(
			"INSERT INTO %s (k, v, e) VALUES %s ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, e = EXCLUDED.e",
			table, valueTuples(batchSize, 3),
		)
	}
	return fmt.Sprintf(
		"INSERT INTO %s (k, v) VALUES %s ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
		table, valueTuples(batchSize, 2),
	)
}

// ReadStmt generates a batch SELECT statement.
//
// Example without enum (batchSize=3):
//
//	SELECT k, v FROM kv WHERE k IN ($1, $2, $3)
//
// Example with enum (batchSize=3):
//
//	SELECT k, v, e FROM kv WHERE k IN ($1, $2, $3)
func (PostgresDialect) ReadStmt(table string, batchSize int, hasEnum bool) string {
	cols := "k, v"
	if hasEnum {
		cols = "k, v, e"
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE k IN (%s)", cols, table, placeholders(batchSize))
}

// FollowerReadStmt returns the same as ReadStmt since PostgreSQL does not
// support follower reads.
func (d PostgresDialect) FollowerReadStmt(table string, batchSize int, hasEnum bool) string {
	return d.ReadStmt(table, batchSize, hasEnum)
}

// DeleteStmt generates a batch DELETE statement.
//
// Example (batchSize=3):
//
//	DELETE FROM kv WHERE k IN ($1, $2, $3)
func (PostgresDialect) DeleteStmt(table string, batchSize int) string {
	return fmt.Sprintf("DELETE FROM %s WHERE k IN (%s)", table, placeholders(batchSize))
}

// SpanStmt generates a spanning query using standard SQL derived table syntax.
// When limit > 0, the statement counts values starting from key $1 with the
// given limit. When limit == 0, it performs a full table scan with no arguments.
//
// Example (limit=100):
//
//	SELECT count(v) FROM (SELECT v FROM kv WHERE k >= $1 ORDER BY k LIMIT 100) AS t
//
// Example (limit=0):
//
//	SELECT count(v) FROM (SELECT v FROM kv) AS t
func (PostgresDialect) SpanStmt(table string, limit int) string {
	if limit == 0 {
		return fmt.Sprintf("SELECT count(v) FROM (SELECT v FROM %s) AS t", table)
	}
	return fmt.Sprintf(
		"SELECT count(v) FROM (SELECT v FROM %s WHERE k >= $1 ORDER BY k LIMIT %d) AS t",
		table, limit,
	)
}

// SelectForUpdateStmt generates a SELECT ... FOR UPDATE statement.
//
// Example (batchSize=2):
//
//	SELECT k, v FROM kv WHERE k IN ($1, $2) FOR UPDATE
func (PostgresDialect) SelectForUpdateStmt(table string, batchSize int) string {
	return fmt.Sprintf("SELECT k, v FROM %s WHERE k IN (%s) FOR UPDATE", table, placeholders(batchSize))
}

// SchemaFragment generates the PostgreSQL CREATE TABLE body for use as
// workload.Table.Schema. Uses BYTEA for binary data and standard SQL
// types for keys. Hash sharding (numShards) is ignored because PostgreSQL
// does not support hash-sharded primary keys.
//
// Example (keySize=0):
//
//	(k BIGINT NOT NULL PRIMARY KEY, v BYTEA NOT NULL)
//
// Example (keySize=100):
//
//	(k VARCHAR(100) NOT NULL PRIMARY KEY, v BYTEA NOT NULL)
func (PostgresDialect) SchemaFragment(keySize int, secondaryIndex bool, numShards int) string {
	// numShards and secondaryIndex are ignored: PostgreSQL doesn't support
	// hash-sharded PKs, and secondary indexes are created via
	// SecondaryIndexStmts as separate DDL statements.
	keyType := "BIGINT"
	if keySize > 0 {
		keyType = fmt.Sprintf("VARCHAR(%d)", keySize)
	}
	return fmt.Sprintf("(k %s NOT NULL PRIMARY KEY, v BYTEA NOT NULL)", keyType)
}

// SecondaryIndexStmts returns a CREATE INDEX statement for the secondary
// index on column v. Uses IF NOT EXISTS for idempotency.
func (PostgresDialect) SecondaryIndexStmts(table string) []string {
	return []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_v_idx ON %s (v)", table, table),
	}
}

// PostLoadStmts returns nil because PostgreSQL does not require any
// post-load DDL for the kv workload.
func (PostgresDialect) PostLoadStmts(table string, enum, scatter bool) []string {
	return nil
}

// Capabilities returns PostgreSQL's feature set. SelectForUpdate and Select1
// are supported because PGXExecutor implements ExtendedTx with prepared
// statement access for transactional write workflows.
func (PostgresDialect) Capabilities() Capabilities {
	return Capabilities{
		FollowerReads:       false,
		Scatter:             false,
		Splits:              false,
		HashShardedPK:       false,
		TransactionQoS:      false,
		SelectForUpdate:     true,
		TransactionPriority: false,
		SerializationRetry:  false,
		Select1:             true,
		EnumColumn:          false,
	}
}

// SpannerSchemaDialect implements SchemaDialect for Google Cloud Spanner.
// Unlike CockroachDialect and PostgresDialect, SpannerSchemaDialect only
// implements SchemaDialect (not the full Dialect) because SpannerExecutor
// uses the Spanner mutations API for queries instead of SQL.
type SpannerSchemaDialect struct{}

var _ SchemaDialect = SpannerSchemaDialect{}

// SchemaFragment generates the Spanner CREATE TABLE body for use as
// workload.Table.Schema. Spanner uses INT64 for integer keys and
// BYTES(MAX) for binary data. The PRIMARY KEY clause appears outside the
// column definition parentheses, matching Spanner's DDL syntax.
//
// Example (keySize=0):
//
//	(k INT64 NOT NULL, v BYTES(MAX) NOT NULL) PRIMARY KEY (k)
//
// Example (keySize=100):
//
//	(k STRING(100) NOT NULL, v BYTES(MAX) NOT NULL) PRIMARY KEY (k)
func (SpannerSchemaDialect) SchemaFragment(keySize int, secondaryIndex bool, numShards int) string {
	// numShards and secondaryIndex are ignored: Spanner does not support
	// hash-sharded PKs, and secondary indexes are created via
	// SecondaryIndexStmts as separate DDL statements.
	if keySize > 0 {
		return fmt.Sprintf(
			"(k STRING(%d) NOT NULL, v BYTES(MAX) NOT NULL) PRIMARY KEY (k)",
			keySize)
	}
	return "(k INT64 NOT NULL, v BYTES(MAX) NOT NULL) PRIMARY KEY (k)"
}

// SecondaryIndexStmts returns a CREATE INDEX statement for the secondary
// index on column v. Spanner does not support IF NOT EXISTS on indexes.
func (SpannerSchemaDialect) SecondaryIndexStmts(table string) []string {
	return []string{
		fmt.Sprintf("CREATE INDEX %s_v_idx ON %s (v)", table, table),
	}
}

// PostLoadStmts returns nil because Spanner does not require any post-load DDL
// for the kv workload. Although Spanner supports enum columns (EnumColumn
// capability is true), enum values are stored as regular strings by
// SpannerExecutor and don't require DDL changes like CockroachDB's computed
// stored columns.
func (SpannerSchemaDialect) PostLoadStmts(table string, enum, scatter bool) []string {
	return nil
}

// Capabilities returns Spanner's feature set. Spanner supports stale reads
// (follower reads) and enum columns (stored as strings) but does not support
// CockroachDB-specific features like scatter, splits, or hash-sharded
// primary keys.
func (SpannerSchemaDialect) Capabilities() Capabilities {
	return Capabilities{
		FollowerReads:       true, // via stale reads
		Scatter:             false,
		Splits:              false,
		HashShardedPK:       false,
		TransactionQoS:      false,
		SelectForUpdate:     false,
		TransactionPriority: false,
		SerializationRetry:  false,
		Select1:             false,
		EnumColumn:          true, // stored as strings
	}
}
