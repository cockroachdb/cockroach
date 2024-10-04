// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file contains constants and types used by pg_catalog diff tool
// that are also re-used in /pkg/cmd/generate-postgres-metadata-tables

package sql

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// RDBMS options
const (
	MySQL    = "mysql"
	Postgres = "postgres"
)

// GetPGMetadataSQL is a query uses udt_name::regtype instead of data_type column because
// data_type only says "ARRAY" but does not say which kind of array it is.
const GetPGMetadataSQL = `
	SELECT
		c.relname AS table_name,
		a.attname AS column_name,
		t.typname AS data_type,
		t.oid AS data_type_oid
	FROM pg_class c
	JOIN pg_attribute a ON a.attrelid = c.oid
	JOIN pg_type t ON t.oid = a.atttypid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1
	AND a.attnum > 0
  AND c.relkind != 'i';
`

// Summary will keep accountability for any unexpected difference and report it in the log.
type Summary struct {
	TotalTables        int
	TotalColumns       int
	MissingTables      int
	MissingColumns     int
	DatatypeMismatches int
}

// PGMetadataColumnDiff describes diffs information for a column type. Fields are exported for marshaling purposes.
type PGMetadataColumnDiff struct {
	Oid              uint32 `json:"oid"`
	DataType         string `json:"dataType"`
	ExpectedOid      uint32 `json:"expectedOid"`
	ExpectedDataType string `json:"expectedDataType"`
}

// PGMetadataColumnDiffs maps column names to datatype diffs.
type PGMetadataColumnDiffs map[string]*PGMetadataColumnDiff

// PGMetadataTableDiffs is used to store and load expected diffs:
//   - A table name pointing to a zero length PGMetadataColumnDiffs means that we expect this table to be missing
//     in cockroach db.
//   - If PGMetadataColumnDiffs is not empty but columnName points to null, we expect that column to be missing in that table in
//     cockroach db.
//   - If column Name points to a not null PGMetadataColumnDiff, the test column describes how we expect that data type to be
//     different between cockroach db and postgres.
type PGMetadataTableDiffs map[string]PGMetadataColumnDiffs

// PGMetadataColumnType represents a column type from postgres/mysql.
type PGMetadataColumnType struct {
	Oid      uint32 `json:"oid"`
	DataType string `json:"dataType"`
}

// PGMetadataColumns maps columns names to datatypes.
type PGMetadataColumns map[string]*PGMetadataColumnType

// PGMetadataTableInfo represents a table with column mapping and column names in insertion order.
type PGMetadataTableInfo struct {
	ColumnNames []string          `json:"columnNames"`
	Columns     PGMetadataColumns `json:"columns"`
}

// PGMetadataTables maps tables with columns.
type PGMetadataTables map[string]PGMetadataTableInfo

// PGMetadataFile stores the schema gotten from postgres/mysql.
type PGMetadataFile struct {
	Version    string           `json:"version"`
	PGMetadata PGMetadataTables `json:"tables"`
}

// PGMetadataDiffFile is used to store expected diffs or by the diff tool to validate a diff is an expected diff.
type PGMetadataDiffFile struct {
	Version            string               `json:"version"`
	DiffSummary        Summary              `json:"diffSummary"`
	Diffs              PGMetadataTableDiffs `json:"diffs"`
	UnimplementedTypes map[oid.Oid]string   `json:"unimplementedTypes"`
}

func (d PGMetadataTableDiffs) addColumn(
	tableName, columnName string, column *PGMetadataColumnDiff,
) {
	columns, ok := d[tableName]

	if !ok {
		columns = make(PGMetadataColumnDiffs)
		d[tableName] = columns
	}

	columns[columnName] = column
}

func (p PGMetadataTables) addColumn(
	tableName string, columnName string, column *PGMetadataColumnType,
) {
	tableInfo, ok := p[tableName]

	if !ok {
		tableInfo = PGMetadataTableInfo{
			ColumnNames: []string{},
			Columns:     make(PGMetadataColumns),
		}
		p[tableName] = tableInfo
	}

	tableInfo.ColumnNames = append(tableInfo.ColumnNames, columnName)
	tableInfo.Columns[columnName] = column
	p[tableName] = tableInfo
}

// AddColumnMetadata is used to load data from postgres or cockroach pg_catalog schema
func (p PGMetadataTables) AddColumnMetadata(
	tableName string, columnName string, dataType string, dataTypeOid uint32,
) {
	p.addColumn(tableName, columnName, &PGMetadataColumnType{
		dataTypeOid,
		dataType,
	})
}

// addDiff is for the second use case for pgTables which objective is create a datatype diff
func (d PGMetadataTableDiffs) addDiff(
	tableName string, columnName string, expected *PGMetadataColumnType, actual *PGMetadataColumnType,
) {
	d.addColumn(tableName, columnName, &PGMetadataColumnDiff{
		actual.Oid,
		actual.DataType,
		expected.Oid,
		expected.DataType,
	})
}

// Not using error interface because message is not relevant
type compareResult int

const (
	success compareResult = 1 + iota
	expectedDiffError
	diffError
)

// compareColumns verifies if there is a datatype mismatch or if the diff is an expected diff
func (d PGMetadataTableDiffs) compareColumns(
	tableName string, columnName string, expected *PGMetadataColumnType, actual *PGMetadataColumnType,
) compareResult {
	// MySQL don't have oid as they are in postgres so we can't compare oids.
	if expected.Oid == 0 {
		return 0
	}

	expectedDiff := d.getExpectedDiff(tableName, columnName)

	if actual.Oid == expected.Oid {
		if expectedDiff != nil {
			// Need to update JSON file
			return expectedDiffError
		}
	} else if expectedDiff == nil || expectedDiff.Oid != actual.Oid ||
		expectedDiff.ExpectedOid != expected.Oid {
		// This diff is not expected
		return diffError
	}

	return success
}

// If there is an expected diff for a table.column it will return it
func (d PGMetadataTableDiffs) getExpectedDiff(tableName, columnName string) *PGMetadataColumnDiff {
	columns, ok := d[tableName]
	if !ok {
		return nil
	}

	return columns[columnName]
}

// isExpectedMissingTable is used by the diff PGMetadataTableDiffs to verify whether missing a table in cockroach is expected
// or not
func (d PGMetadataTableDiffs) isExpectedMissingTable(tableName string) bool {
	if columns, ok := d[tableName]; !ok || len(columns) > 0 {
		return false
	}

	return true
}

// isExpectedMissingColumn is similar to isExpectedMissingTable to verify column expected misses
func (d PGMetadataTableDiffs) isExpectedMissingColumn(tableName string, columnName string) bool {
	columns, ok := d[tableName]
	if !ok {
		return false
	}

	diff, ok := columns[columnName]
	if !ok {
		return false
	}

	return diff == nil
}

// addMissingTable adds a tablename when it is not found in cockroach db
func (d PGMetadataTableDiffs) addMissingTable(tableName string) {
	d[tableName] = make(PGMetadataColumnDiffs)
}

// addMissingColumn adds a column when it is not found in cockroach db
func (d PGMetadataTableDiffs) addMissingColumn(tableName string, columnName string) {
	columns, ok := d[tableName]

	if !ok {
		columns = make(PGMetadataColumnDiffs)
		d[tableName] = columns
	}

	columns[columnName] = nil
}

// rewriteDiffs creates pg_catalog_test-diffs.json
func (d PGMetadataTableDiffs) rewriteDiffs(
	source PGMetadataFile, sum Summary, diffFile string,
) error {
	f, err := os.OpenFile(diffFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	mf := &PGMetadataDiffFile{
		Version:            source.Version,
		Diffs:              d,
		DiffSummary:        sum,
		UnimplementedTypes: source.PGMetadata.getUnimplementedTypes(),
	}
	mf.Save(f)
	return nil
}

// Save stores the diff file in a JSON format.
func (df *PGMetadataDiffFile) Save(writer io.Writer) {
	Save(writer, df)
}

// Save stores the table metadata in a JSON format.
func (f *PGMetadataFile) Save(writer io.Writer) {
	Save(writer, f)
}

// Save stores any file into the writer in JSON format
func Save(writer io.Writer, file interface{}) {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(file); err != nil {
		panic(err)
	}
}

// getUnimplementedTables retrieves the tables that are not yet part of CRDB.
func (d PGMetadataTableDiffs) getUnimplementedTables(source PGMetadataTables) PGMetadataTables {
	unimplementedTables := make(PGMetadataTables)
	for tableName := range d {
		if len(d[tableName]) == 0 && len(source[tableName].Columns.getUnimplementedTypes()) == 0 {
			unimplementedTables[tableName] = source[tableName]
		}
	}
	return unimplementedTables
}

// getUnimplementedColumns is used by diffs as it might not be in sync with
// already implemented columns.
func (d PGMetadataTableDiffs) getUnimplementedColumns(target PGMetadataTables) PGMetadataTables {
	unimplementedColumns := make(PGMetadataTables)
	for tableName, columns := range d {
		for columnName, columnType := range columns {
			if columnType != nil {
				// dataType mismatch (Not a new column).
				continue
			}
			sourceType, ok := target[tableName].Columns[columnName]
			if !ok {
				continue
			}
			typeOid := oid.Oid(sourceType.Oid)
			if _, ok := types.OidToType[typeOid]; !ok || typeOid == oid.T_anyarray {
				// can't implement this column due to missing type.
				continue
			}
			unimplementedColumns.AddColumnMetadata(tableName, columnName, sourceType.DataType, sourceType.Oid)
		}
	}
	return unimplementedColumns
}

// removeImplementedColumns removes diff columns that are marked as expected
// diff (or unimplemented column) but is already implemented in CRDB.
func (d PGMetadataTableDiffs) removeImplementedColumns(source PGMetadataTables) {
	for tableName, tableInfo := range source {
		pColumns, exists := d[tableName]
		if !exists {
			continue
		}
		for _, columnName := range tableInfo.ColumnNames {
			columnType, exists := pColumns[columnName]
			if !exists {
				continue
			}
			if columnType != nil {
				continue
			}

			delete(pColumns, columnName)
		}
	}
}

// getUnimplementedTypes verifies that all the types are implemented in cockroach db.
func (c PGMetadataColumns) getUnimplementedTypes() map[oid.Oid]string {
	unimplemented := make(map[oid.Oid]string)
	for _, column := range c {
		typeOid := oid.Oid(column.Oid)
		if _, ok := types.OidToType[typeOid]; !ok || typeOid == oid.T_anyarray {
			unimplemented[typeOid] = column.DataType
		}
	}

	return unimplemented
}

func (p PGMetadataTables) getUnimplementedTypes() map[oid.Oid]string {
	unimplemented := make(map[oid.Oid]string)
	for _, tableInfo := range p {
		for typeOid, dataType := range tableInfo.Columns.getUnimplementedTypes() {
			unimplemented[typeOid] = dataType
		}
	}

	return unimplemented
}

// TablesMetadataFilename give the appropriate name where to store or read
// any schema description from a specific database.
func TablesMetadataFilename(path, rdbms, schema string) string {
	return filepath.Join(
		path,
		fmt.Sprintf("%s_tables_from_%s.json", schema, rdbms),
	)
}
