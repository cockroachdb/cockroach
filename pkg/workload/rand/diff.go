// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rand

import (
	gosql "database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
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

// formatRow returns a pretty-printed representation of a row as
// {col1: 'val1', col2: 'val2', ...}.
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

// fetchedRow holds the data for a single row fetched during diff phase 2.
type fetchedRow struct {
	row             []any
	mvccTimestamp   hlc.Timestamp
	originTimestamp hlc.Timestamp
}

// buildPKHashExpr returns the SQL expression for the primary key hash, using
// fnv64 to produce a compact int64 instead of raw bytes.
func buildPKHashExpr(schema *Table) string {
	var b strings.Builder
	b.WriteString("fnv64(crdb_internal.datums_to_bytes(")
	for i, pkIdx := range schema.PrimaryKey {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.NameString(schema.Cols[pkIdx].Name))
	}
	b.WriteString("))")
	return b.String()
}

// buildOrderByClause returns an ORDER BY clause using the primary key columns,
// ensuring deterministic row ordering for hash and fetch queries.
func buildOrderByClause(schema *Table) string {
	var b strings.Builder
	b.WriteString(" ORDER BY ")
	for i, pkIdx := range schema.PrimaryKey {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.NameString(schema.Cols[pkIdx].Name))
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
func buildHashQuery(schema *Table, tableName string) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(buildPKHashExpr(schema))
	b.WriteString(" AS pk_hash, fnv64(crdb_internal.datums_to_bytes(")
	for i, colIdx := range schema.IndexableColumns() {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.NameString(schema.Cols[colIdx].Name))
	}
	b.WriteString(")) AS row_hash FROM ")
	b.WriteString(tableName)
	b.WriteString(buildOrderByClause(schema))
	return b.String()
}

// queryHashes executes the hash query and returns a map from pk_hash to
// row_hash. Both are int64 values produced by fnv64.
func queryHashes(conn *gosql.DB, query string) (map[int64]int64, error) {
	rows, err := conn.Query(query)
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
func buildFetchQuery(schema *Table, pkHashExpr string, tableName string) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, col := range schema.Cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.NameString(col.Name))
	}
	b.WriteString(", crdb_internal_mvcc_timestamp, crdb_internal_origin_timestamp, ")
	b.WriteString(pkHashExpr)
	b.WriteString(" AS pk_hash FROM ")
	b.WriteString(tableName)
	b.WriteString(buildOrderByClause(schema))
	return b.String()
}

// fetchDiffRows executes a full table scan but only retains rows whose
// pk_hash is in the diffPKs set, returning a map from pk_hash to the
// fetched row data.
func fetchDiffRows(
	conn *gosql.DB, query string, numCols int, diffPKs map[int64]struct{},
) (map[int64]fetchedRow, error) {
	rows, err := conn.Query(query)
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
			return nil, errors.Newf(
				"expected int64 for pk_hash, got %T", dest[numCols+2],
			)
		}
		if _, needed := diffPKs[pk]; !needed {
			continue
		}
		fr := fetchedRow{
			row: dest[:numCols],
		}
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
// returns up to resultLimit differences. Both tables must have the same schema.
// The schema is loaded from connA/tableA and used for both sides.
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
	connA *gosql.DB, tableA string, connB *gosql.DB, tableB string, resultLimit int,
) ([]RowDiff, error) {
	schema, err := LoadTable(connA, tableA)
	if err != nil {
		return nil, errors.Wrap(err, "loading table schema")
	}

	if len(schema.PrimaryKey) == 0 {
		return nil, errors.Newf("table %s has no primary key", tableA)
	}

	pkHashExpr := buildPKHashExpr(&schema)

	// Phase 1: load hashes from both tables.
	hashesA, err := queryHashes(connA, buildHashQuery(&schema, tableA))
	if err != nil {
		return nil, err
	}
	hashesB, err := queryHashes(connB, buildHashQuery(&schema, tableB))
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
	numCols := len(schema.Cols)
	fetchQueryA := buildFetchQuery(&schema, pkHashExpr, tableA)
	dataA, err := fetchDiffRows(connA, fetchQueryA, numCols, diffPKs)
	if err != nil {
		return nil, err
	}
	fetchQueryB := buildFetchQuery(&schema, pkHashExpr, tableB)
	dataB, err := fetchDiffRows(connB, fetchQueryB, numCols, diffPKs)
	if err != nil {
		return nil, err
	}

	// Build diff results from the fetched rows.
	colNames := make([]string, len(schema.Cols))
	for i, col := range schema.Cols {
		colNames[i] = col.Name
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
