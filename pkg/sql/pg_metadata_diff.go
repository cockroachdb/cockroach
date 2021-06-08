// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
  AND c.relkind != 'i'
  AND c.relname NOT LIKE 'pg_stat%'
	AND c.relname NOT LIKE '_pg_%'
	ORDER BY 1, 2;
`

// Summary will keep accountability for any unexpected difference and report it in the log.
type Summary struct {
	TotalTables        int
	TotalColumns       int
	MissingTables      int
	MissingColumns     int
	DatatypeMismatches int
}

// PGMetadataColumnType is a structure which contains a small description about the datatype of a column, but this can also be
// used as a diff information if populating ExpectedOid. Fields are exported for Marshaling purposes.
type PGMetadataColumnType struct {
	Oid              uint32  `json:"oid"`
	DataType         string  `json:"dataType"`
	ExpectedOid      *uint32 `json:"expectedOid"`
	ExpectedDataType *string `json:"expectedDataType"`
}

// PGMetadataColumns maps column names to datatype description
type PGMetadataColumns map[string]*PGMetadataColumnType

// PGMetadataTables have 2 use cases:
// First: This is used to model pg_schema for postgres and cockroach db for comparison purposes by mapping tableNames
// to columns.
// Second: This is used to store and load expected diffs:
// - Using it this way, a table name pointing to a zero length PGMetadataColumns means that we expect this table to be missing
//   in cockroach db
// - If PGMetadataColumns is not empty but columnName points to null, we expect that column to be missing in that table in
//   cockroach db
// - If column Name points to a not null PGMetadataColumnType, the test column describes how we expect that data type to be
//   different between cockroach db and postgres
type PGMetadataTables map[string]PGMetadataColumns

// PGMetadataFile is used to export pg_catalog from postgres and store the representation of this structure as a
// json file
type PGMetadataFile struct {
	PGVersion          string             `json:"pgVersion"`
	DiffSummary        Summary            `json:"diffSummary"`
	PGMetadata         PGMetadataTables   `json:"pgMetadata"`
	UnimplementedTypes map[oid.Oid]string `json:"unimplementedTypes"`
}

func (p PGMetadataTables) addColumn(tableName, columnName string, column *PGMetadataColumnType) {
	columns, ok := p[tableName]

	if !ok {
		columns = make(PGMetadataColumns)
		p[tableName] = columns
	}

	columns[columnName] = column
}

// AddColumnMetadata is used to load data from postgres or cockroach pg_catalog schema
func (p PGMetadataTables) AddColumnMetadata(
	tableName string, columnName string, dataType string, dataTypeOid uint32,
) {
	p.addColumn(tableName, columnName, &PGMetadataColumnType{
		dataTypeOid,
		dataType,
		nil,
		nil,
	})
}

// addDiff is for the second use case for pgTables which objective is create a datatype diff
func (p PGMetadataTables) addDiff(
	tableName string, columnName string, expected *PGMetadataColumnType, actual *PGMetadataColumnType,
) {
	p.addColumn(tableName, columnName, &PGMetadataColumnType{
		actual.Oid,
		actual.DataType,
		&expected.Oid,
		&expected.DataType,
	})
}

// isDiffOid verifies if there is a datatype mismatch or if the diff is an expected diff
func (p PGMetadataTables) isDiffOid(
	tableName string, columnName string, expected *PGMetadataColumnType, actual *PGMetadataColumnType,
) bool {
	// MySQL don't have oid as they are in postgres so we can't compare oids.
	if expected.Oid == 0 || expected.Oid == actual.Oid {
		return false
	}

	columns, ok := p[tableName]
	if !ok {
		return true
	}

	// For columns that are expected to be missing, the diff is stored as nil
	// and is present in the map.
	diff, ok := columns[columnName]
	if !ok || diff == nil {
		return true
	}

	return !(diff.Oid == actual.Oid && *diff.ExpectedOid == expected.Oid)
}

// isExpectedMissingTable is used by the diff PGMetadataTables to verify whether missing a table in cockroach is expected
// or not
func (p PGMetadataTables) isExpectedMissingTable(tableName string) bool {
	if columns, ok := p[tableName]; !ok || len(columns) > 0 {
		return false
	}

	return true
}

// isExpectedMissingColumn is similar to isExpectedMissingTable to verify column expected misses
func (p PGMetadataTables) isExpectedMissingColumn(tableName string, columnName string) bool {
	columns, ok := p[tableName]
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
func (p PGMetadataTables) addMissingTable(tableName string) {
	p[tableName] = make(PGMetadataColumns)
}

// addMissingColumn adds a column when it is not found in cockroach db
func (p PGMetadataTables) addMissingColumn(tableName string, columnName string) {
	columns, ok := p[tableName]

	if !ok {
		columns = make(PGMetadataColumns)
		p[tableName] = columns
	}

	columns[columnName] = nil
}

// rewriteDiffs creates pg_catalog_test-diffs.json
func (p PGMetadataTables) rewriteDiffs(source PGMetadataFile, sum Summary, diffFile string) error {
	f, err := os.OpenFile(diffFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	mf := &PGMetadataFile{
		PGVersion:          source.PGVersion,
		PGMetadata:         p,
		DiffSummary:        sum,
		UnimplementedTypes: source.UnimplementedTypes,
	}
	mf.Save(f)
	return nil
}

// Save have the purpose of storing all the data retrieved from postgres and
// useful information as postgres version.
func (f *PGMetadataFile) Save(writer io.Writer) {
	byteArray, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		panic(err)
	}

	if _, err = writer.Write(byteArray); err != nil {
		panic(err)
	}
}

//getUnimplementedTables retrieves the tables that are not yet part of CRDB.
func (p PGMetadataTables) getUnimplementedTables(source PGMetadataTables) PGMetadataTables {
	notImplemented := make(PGMetadataTables)
	for tableName := range p {
		if len(p[tableName]) == 0 && len(source[tableName].getUnimplementedTypes()) == 0 {
			notImplemented[tableName] = source[tableName]
		}
	}
	return notImplemented
}

// getUnimplementedColumns is used by diffs as it might not be in sync with
// already implemented columns.
func (p PGMetadataTables) getUnimplementedColumns(target PGMetadataTables) PGMetadataTables {
	unimplementedColumns := make(PGMetadataTables)
	for tableName, columns := range p {
		for columnName, columnType := range columns {
			if columnType != nil {
				// dataType mismatch (Not a new column).
				continue
			}
			sourceType := target[tableName][columnName]
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
func (p PGMetadataTables) removeImplementedColumns(source PGMetadataTables) {
	for tableName, columns := range source {
		pColumns, exists := p[tableName]
		if !exists {
			continue
		}
		for columnName := range columns {
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

// AddUnimplementedType reports a type that is not implemented in cockroachdb.
func (f *PGMetadataFile) AddUnimplementedType(columnType *PGMetadataColumnType) {
	typeOid := oid.Oid(columnType.Oid)
	if f.UnimplementedTypes == nil {
		f.UnimplementedTypes = make(map[oid.Oid]string)
	}

	f.UnimplementedTypes[typeOid] = columnType.DataType
}

// IsImplemented determines whether the type is implemented or not in
// cockroachdb.
func (t *PGMetadataColumnType) IsImplemented() bool {
	typeOid := oid.Oid(t.Oid)
	_, ok := types.OidToType[typeOid]
	// Cannot use type oid.T_anyarray in CREATE TABLE
	return ok && typeOid != oid.T_anyarray
}

// TablesMetadataFilename give the appropriate name where to store or read
// any schema description from a specific database.
func TablesMetadataFilename(path, rdbms, schema string) string {
	return filepath.Join(
		path,
		fmt.Sprintf("%s_tables_from_%s.json", schema, rdbms),
	)
}
