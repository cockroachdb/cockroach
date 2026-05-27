// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationtestutils

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// RowDiff represents a difference between two tables. If RowA is nil, the row
// exists only in table B. If RowB is nil, the row exists only in table A. If
// both are non-nil, the rows have the same primary key but different column
// values. The MVCC and origin timestamps are included to help diagnose
// replication convergence issues.
type RowDiff struct {
	ColNames         []string
	RowA             []any
	MVCCTimestampA   hlc.Timestamp // crdb_internal_mvcc_timestamp from table A
	OriginTimestampA hlc.Timestamp // crdb_internal_origin_timestamp from table A
	RowB             []any
	MVCCTimestampB   hlc.Timestamp // crdb_internal_mvcc_timestamp from table B
	OriginTimestampB hlc.Timestamp // crdb_internal_origin_timestamp from table B
}

func (d RowDiff) formatRow(row []any) string {
	if row == nil {
		return "<nil>"
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := range row {
		if i != 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s: '%v'", d.ColNames[i], row[i])
	}
	b.WriteByte('}')
	return b.String()
}

func (d RowDiff) String() string {
	return fmt.Sprintf(
		"RowA=%s (mvcc=%s, origin=%s) RowB=%s (mvcc=%s, origin=%s)",
		d.formatRow(d.RowA), d.MVCCTimestampA, d.OriginTimestampA,
		d.formatRow(d.RowB), d.MVCCTimestampB, d.OriginTimestampB,
	)
}

// diffSchema is the minimal schema information Diff needs: column names, types,
// and primary key indices.
//
// We deliberately do not reuse pkg/workload/rand.Table here. That type is
// loaded with skip rules tailored to the rand workload's data-generation needs
// (e.g. drop columns whose default is unique_rowid()), which would silently
// exclude columns from a comparison.
type diffSchema struct {
	cols       []diffCol
	primaryKey []int
}

type diffCol struct {
	name string
	typ  *types.T
}

// indexableColumns returns the indices of columns whose types support
// crdb_internal.datums_to_bytes (used for row hashing). Types like TSVECTOR or
// PGVECTOR don't support keyside encoding and must be excluded.
func (s *diffSchema) indexableColumns() []int {
	var out []int
	for i := range s.cols {
		switch s.cols[i].typ.Family() {
		case types.TSVectorFamily,
			types.TSQueryFamily,
			types.JsonpathFamily,
			types.PGVectorFamily,
			types.RefCursorFamily:
			continue
		}
		out = append(out, i)
	}
	return out
}

// loadDiffSchema reads the column list and primary key for a table via
// pg_catalog. The conn passed in must already have its session database set to
// the table's database (see openSessionForTable); pg_catalog virtual tables in
// CRDB only return rows for the connection's current database.
func loadDiffSchema(
	ctx context.Context, conn *gosql.Conn, tableName string,
) (_ *diffSchema, retErr error) {
	var relid int
	if err := conn.QueryRowContext(ctx, "SELECT $1::REGCLASS::OID", tableName).Scan(&relid); err != nil {
		return nil, errors.Wrapf(err, "resolving OID for %s", tableName)
	}

	rows, err := conn.QueryContext(ctx, `
SELECT attname, atttypid
FROM pg_catalog.pg_attribute
WHERE attrelid=$1 AND NOT attisdropped AND attnum > 0`, relid)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()

	var cols []diffCol
	for rows.Next() {
		var name string
		var typOid int
		if err := rows.Scan(&name, &typOid); err != nil {
			return nil, err
		}
		typ, err := typeForOid(ctx, conn, oid.Oid(typOid), relid, name)
		if err != nil {
			return nil, errors.Wrapf(err, "resolving type for column %s", name)
		}
		cols = append(cols, diffCol{name: name, typ: typ})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, errors.Newf("no columns found for table %s "+
			"(connection's current database may not contain the table)", tableName)
	}

	pkRows, err := conn.QueryContext(ctx, `
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = $1 AND i.indisprimary`, relid)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.CombineErrors(retErr, pkRows.Close()) }()

	var pk []int
	for pkRows.Next() {
		var name string
		if err := pkRows.Scan(&name); err != nil {
			return nil, err
		}
		for i, c := range cols {
			if c.name == name {
				pk = append(pk, i)
				break
			}
		}
	}
	if err = pkRows.Err(); err != nil {
		return nil, err
	}

	return &diffSchema{cols: cols, primaryKey: pk}, nil
}

// typeForOid returns the *types.T for a column. BIT and CHAR widths are not
// captured in the OID, so we look them up from information_schema.
func typeForOid(
	ctx context.Context, conn *gosql.Conn, typeOid oid.Oid, relid int, columnName string,
) (*types.T, error) {
	base := types.OidToType[typeOid]
	if base == nil {
		return nil, errors.Newf("unknown type oid %d", typeOid)
	}
	t := *base
	if typeOid == oid.T_bit || typeOid == oid.T_char {
		var width int32
		if err := conn.QueryRowContext(ctx,
			`SELECT IFNULL(character_maximum_length, 0)
			FROM information_schema.columns
			JOIN pg_catalog.pg_class ON pg_class.relname = table_name
			WHERE pg_class.oid = $1 AND column_name = $2`,
			relid, columnName).Scan(&width); err != nil {
			return nil, err
		}
		t.InternalType.Width = width
	}
	return &t, nil
}

// fetchedRow holds the data for a single row fetched during diff phase 2.
type fetchedRow struct {
	row             []any
	mvccTimestamp   hlc.Timestamp
	originTimestamp hlc.Timestamp
}

// buildPKHashExpr returns the SQL expression for the primary key hash, using
// fnv64 to produce a compact int64 instead of raw bytes.
func buildPKHashExpr(schema *diffSchema) string {
	var b strings.Builder
	b.WriteString("fnv64(crdb_internal.datums_to_bytes(")
	for i, pkIdx := range schema.primaryKey {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.NameString(schema.cols[pkIdx].name))
	}
	b.WriteString("))")
	return b.String()
}

// buildOrderByClause returns an ORDER BY clause using the primary key columns,
// ensuring deterministic row ordering for hash and fetch queries.
func buildOrderByClause(schema *diffSchema) string {
	var b strings.Builder
	b.WriteString(" ORDER BY ")
	for i, pkIdx := range schema.primaryKey {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.NameString(schema.cols[pkIdx].name))
	}
	return b.String()
}

// buildHashQuery returns a lightweight query that computes only pk_hash and
// row_hash for every row as int64 values using fnv64.
//
// Example for a table with PK (id) and columns (id, name, value):
//
//	SELECT fnv64(crdb_internal.datums_to_bytes(id)) AS pk_hash,
//	  fnv64(crdb_internal.datums_to_bytes(id, name, value)) AS row_hash
//	FROM my_table
func buildHashQuery(schema *diffSchema, tableName string) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(buildPKHashExpr(schema))
	b.WriteString(" AS pk_hash, fnv64(crdb_internal.datums_to_bytes(")
	for i, colIdx := range schema.indexableColumns() {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.NameString(schema.cols[colIdx].name))
	}
	b.WriteString(")) AS row_hash FROM ")
	b.WriteString(tableName)
	b.WriteString(buildOrderByClause(schema))
	return b.String()
}

// queryHashes executes the hash query and returns a map from pk_hash to
// row_hash. Both are int64 values produced by fnv64.
func queryHashes(ctx context.Context, conn *gosql.Conn, query string) (map[int64]int64, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "loading hashes")
	}
	defer rows.Close()

	result := make(map[int64]int64)
	for rows.Next() {
		var pkHash, rowHash int64
		if err := rows.Scan(&pkHash, &rowHash); err != nil {
			return nil, errors.Wrap(err, "scanning hash row")
		}
		result[pkHash] = rowHash
	}
	return result, errors.Wrap(rows.Err(), "iterating hash rows")
}

// buildFetchQuery returns a query that retrieves all columns plus MVCC/origin
// timestamps and the pk_hash for every row.
//
// Example for a table with PK (id) and columns (id, name, value):
//
//	SELECT id, name, value,
//	  crdb_internal_mvcc_timestamp, crdb_internal_origin_timestamp,
//	  fnv64(crdb_internal.datums_to_bytes(id)) AS pk_hash
//	FROM my_table
func buildFetchQuery(schema *diffSchema, pkHashExpr string, tableName string) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, col := range schema.cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.NameString(col.name))
	}
	b.WriteString(", crdb_internal_mvcc_timestamp, crdb_internal_origin_timestamp, ")
	b.WriteString(pkHashExpr)
	b.WriteString(" AS pk_hash FROM ")
	b.WriteString(tableName)
	b.WriteString(buildOrderByClause(schema))
	return b.String()
}

// fetchDiffRows executes a full table scan but only retains rows whose pk_hash
// is in the diffPKs set, returning a map from pk_hash to the fetched row data.
func fetchDiffRows(
	ctx context.Context, conn *gosql.Conn, query string, numCols int, diffPKs map[int64]struct{},
) (map[int64]fetchedRow, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "fetching diff rows")
	}
	defer rows.Close()

	result := make(map[int64]fetchedRow)
	for rows.Next() {
		// The fetch query (see buildFetchQuery) appends three columns after the
		// table's own columns: dest[numCols] is crdb_internal_mvcc_timestamp,
		// dest[numCols+1] is crdb_internal_origin_timestamp, and dest[numCols+2]
		// is pk_hash.
		dest := make([]any, numCols+3)
		ptrs := make([]any, numCols+3)
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, errors.Wrap(err, "scanning diff row")
		}
		pk, ok := dest[numCols+2].(int64)
		if !ok {
			return nil, errors.Newf("expected int64 for pk_hash, got %T", dest[numCols+2])
		}
		if _, needed := diffPKs[pk]; !needed {
			continue
		}
		fr := fetchedRow{row: dest[:numCols]}
		if fr.mvccTimestamp, err = decimalToHLC(dest[numCols]); err != nil {
			return nil, errors.Wrap(err, "parsing mvcc timestamp")
		}
		if fr.originTimestamp, err = decimalToHLC(dest[numCols+1]); err != nil {
			return nil, errors.Wrap(err, "parsing origin timestamp")
		}
		result[pk] = fr
	}
	return result, errors.Wrap(rows.Err(), "iterating diff rows")
}

// Diff compares the contents of two tables across two database connections and
// returns up to resultLimit row differences. Both tables must have the same
// schema. The schema is loaded from connA/tableA and used for both sides.
//
// tableA and tableB must be fully qualified as database.schema.table.
// CRDB's pg_catalog virtual tables only return rows for the connection's
// current database, so Diff pins a session connection per side and SETs its
// database from the qualified name. Connections passed in may be on any
// database; Diff does not modify them.
//
// The comparison uses two queries per table. The first is a lightweight hash
// query that computes pk_hash and row_hash server-side using
// crdb_internal.datums_to_bytes, minimizing data sent over pgwire. The second
// query fetches full row data (all columns plus MVCC/origin timestamps) only
// for rows whose pk_hash appeared in the diff set.
//
// Limitations:
//   - Columns with types that cannot be keyside-encoded (e.g. TSVECTOR,
//     PGVECTOR) are excluded from the row hash. Differences in only those
//     columns will not be detected.
//   - Primary key hashes use FNV64, so hash collisions would cause rows to be
//     silently dropped. This is negligible for reasonable table sizes.
func Diff(
	ctx context.Context,
	connA *gosql.DB,
	tableA string,
	connB *gosql.DB,
	tableB string,
	resultLimit int,
) ([]RowDiff, error) {
	sessA, releaseA, err := openSessionForTable(ctx, connA, tableA)
	if err != nil {
		return nil, errors.Wrap(err, "opening session for table A")
	}
	defer releaseA()
	sessB, releaseB, err := openSessionForTable(ctx, connB, tableB)
	if err != nil {
		return nil, errors.Wrap(err, "opening session for table B")
	}
	defer releaseB()

	schema, err := loadDiffSchema(ctx, sessA, tableA)
	if err != nil {
		return nil, errors.Wrap(err, "loading table schema")
	}
	if len(schema.primaryKey) == 0 {
		return nil, errors.Newf("table %s has no primary key", tableA)
	}

	pkHashExpr := buildPKHashExpr(schema)

	// Phase 1: load hashes from both tables.
	hashesA, err := queryHashes(ctx, sessA, buildHashQuery(schema, tableA))
	if err != nil {
		return nil, err
	}
	hashesB, err := queryHashes(ctx, sessB, buildHashQuery(schema, tableB))
	if err != nil {
		return nil, err
	}

	// Compute the set of pk_hashes that differ between the two tables.
	diffPKs := make(map[int64]struct{})
	for pk, hashA := range hashesA {
		hashB, ok := hashesB[pk]
		if !ok || hashA != hashB {
			diffPKs[pk] = struct{}{}
		}
	}
	for pk := range hashesB {
		if _, ok := hashesA[pk]; !ok {
			diffPKs[pk] = struct{}{}
		}
	}

	if len(diffPKs) == 0 {
		return nil, nil
	}

	// Phase 2: fetch full row data only for differing rows.
	numCols := len(schema.cols)
	dataA, err := fetchDiffRows(ctx, sessA, buildFetchQuery(schema, pkHashExpr, tableA), numCols, diffPKs)
	if err != nil {
		return nil, err
	}
	dataB, err := fetchDiffRows(ctx, sessB, buildFetchQuery(schema, pkHashExpr, tableB), numCols, diffPKs)
	if err != nil {
		return nil, err
	}

	colNames := make([]string, len(schema.cols))
	for i, col := range schema.cols {
		colNames[i] = col.name
	}

	var results []RowDiff
	for pk := range diffPKs {
		if len(results) >= resultLimit {
			break
		}
		diff := RowDiff{ColNames: colNames}
		if entryA, inA := dataA[pk]; inA {
			diff.RowA = entryA.row
			diff.MVCCTimestampA = entryA.mvccTimestamp
			diff.OriginTimestampA = entryA.originTimestamp
		}
		if entryB, inB := dataB[pk]; inB {
			diff.RowB = entryB.row
			diff.MVCCTimestampB = entryB.mvccTimestamp
			diff.OriginTimestampB = entryB.originTimestamp
		}
		results = append(results, diff)
	}

	return results, nil
}

// openSessionForTable returns a single-connection session pinned to the
// database extracted from a fully qualified table name (database.schema.table).
// The caller must call the returned release function.
func openSessionForTable(
	ctx context.Context, db *gosql.DB, tableName string,
) (*gosql.Conn, func(), error) {
	dbName, err := databaseFromQualifiedName(tableName)
	if err != nil {
		return nil, nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	release := func() { _ = conn.Close() }
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET database = %s", tree.NameString(dbName))); err != nil {
		release()
		return nil, nil, errors.Wrapf(err, "setting database to %q", dbName)
	}
	return conn, release, nil
}

// databaseFromQualifiedName returns the database part of a fully qualified
// table name. It returns an error if the name is not 3-part qualified, since
// Diff requires a database qualifier to set the session database for
// pg_catalog lookups.
func databaseFromQualifiedName(tableName string) (string, error) {
	u, err := parser.ParseTableName(tableName)
	if err != nil {
		return "", errors.Wrapf(err, "parsing table name %q", tableName)
	}
	if u.NumParts != 3 {
		return "", errors.Newf(
			"table name %q must be fully qualified as database.schema.table", tableName)
	}
	// UnresolvedObjectName stores parts in reverse order: object, schema, catalog.
	return u.Parts[2], nil
}

// decimalToHLC converts a scanned crdb_internal_mvcc_timestamp or
// crdb_internal_origin_timestamp value (a DECIMAL) into an hlc.Timestamp.
// Returns the zero timestamp if the value is nil (e.g. origin timestamp for
// locally-written data).
func decimalToHLC(v any) (hlc.Timestamp, error) {
	if v == nil {
		return hlc.Timestamp{}, nil
	}
	s, ok := v.([]byte)
	if !ok {
		return hlc.Timestamp{}, errors.Newf("expected []byte for timestamp, got %T", v)
	}
	return hlc.ParseHLC(string(s))
}
