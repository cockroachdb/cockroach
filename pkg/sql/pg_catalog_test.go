// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This test compares a dump generated from PostgreSQL with the pg_catalog
// and compares it with the current pg_catalog at cockroach db skipping
// all the known diffs:
//
// cd pkg/sql
// go test -run TestPGCatalog
//
// If you want to re-create the known (expected) diffs with the current differences
// add -rewrite-diffs flag when running this test:
//
// cd pkg/sql
// go test -run TestPGCatalog -rewrite-diffs
//
// To create the postgres dump file see pkg/cmd/generate-pg-catalog/main.go:
//
// cd pkg/cmd/generate-pg-catalog/
// go run main.go > ../../sql/testdata/pg_catalog_tables
package sql

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors/oserror"
)

// This constants enumerates indexes in pg_catalog_tables
const (
	testTableIndex = iota
	testColumnIndex
	testTypeIndex
	testTypeOidIndex
)

// This query uses udt_name::regtype instead of data_type column because
// data_type only says "ARRAY" but does not say which kind of array it is.
const getPgCatalogSQL = `
	SELECT table_name, column_name, udt_name::regtype data_type, udt_name::regtype::Oid data_type_oid
	FROM information_schema.columns 
	WHERE table_schema = 'pg_catalog'
	ORDER BY 1, 2;
`

// Test data files
const (
	pgCatalogDump = "pg_catalog_tables"          // PostgreSQL pg_catalog schema
	expectedDiffs = "pg_catalog_test-diffs.json" // Contains expected difference between postgres and cockroach
	testdata      = "testdata"                   // testdata directory
)

// When running test with -rewrite-diffs test will pass and re-create pg_catalog_test-diffs.json
var rewriteFlag = flag.Bool("rewrite-diffs", false, "This will re-create the expected diffs file")

// summary will keep accountability for any unexpected difference and report it in the log
type summary struct {
	missingTables        int
	missingColumns       int
	mismatchDatatypesOid int
}

// TestColumn is a structure which contains a small description about the datatype of a column, but this can also be
// used as a diff information if populating ExpOid and ExpDataType. Fields are exported for Marshaling purposes.
type TestColumn struct {
	Oid         uint32  `json:"oid"`
	DataType    string  `json:"dataType"`
	ExpOid      *uint32 `json:"expOid"`
	ExpDataType *string `json:"expDataType"`
}

// testColumns maps column names with datatype description
type testColumns map[string]*TestColumn

// testTable have 2 use cases:
// First: This is used to model pg_schema for postgres and cockroach db for comparing purposes by mapping tableNames
// with columns.
// Second: This is used to store and load expected diffs:
// - Using it this way, a table name pointing to a zero length testColumns means that we expect this table to be missing
//   in cockroach db
// - If testColumns is not empty but columnName points to null, we expect that column to be missing in that table in
//   cockroach db
// - If column Name points to a not null TestColumn, the test column describes how we expect that data type to be
//   different between cockroach db and postgres
type testTable map[string]testColumns

// addRow is used to load data from postgres or cockroach pg_catalog schema
func (pgTables testTable) addRow(
	tableName string, columnName string, dataType string, dataTypeOid uint32,
) {
	columns, ok := pgTables[tableName]

	if !ok {
		columns = make(testColumns)
		pgTables[tableName] = columns
	}

	columns[columnName] = &TestColumn{dataTypeOid, dataType, nil, nil}
}

// addDiff is for the second use case for pgTables which objective is create a datatype diff
func (pgTables testTable) addDiff(
	tableName string, columnName string, expected *TestColumn, got *TestColumn,
) {
	columns, ok := pgTables[tableName]

	if !ok {
		columns = make(testColumns)
		pgTables[tableName] = columns
	}

	columns[columnName] = &TestColumn{
		Oid:         got.Oid,
		DataType:    got.DataType,
		ExpOid:      &expected.Oid,
		ExpDataType: &expected.DataType,
	}
}

// isDiffOid verifies if there is a datatype mismatch or if the diff is an expected diff
func (pgTables testTable) isDiffOid(
	tableName string, columnName string, expected *TestColumn, got *TestColumn,
) bool {
	if expected.Oid == got.Oid {
		return false
	}

	columns, ok := pgTables[tableName]
	if !ok {
		return true
	}

	diff, ok := columns[columnName]
	if !ok {
		return true
	}

	return !(diff.Oid == got.Oid && *diff.ExpOid == expected.Oid)
}

// isExpectedMissingTable is used by the diff testTable to verify whether missing a table in cockroach is expected
// or not
func (pgTables testTable) isExpectedMissingTable(tableName string) bool {
	if columns, ok := pgTables[tableName]; !ok || len(columns) > 0 {
		return false
	}

	return true
}

// isExpectedMissingColumn is similar as isExpectedMissingTable to verify column expected misses
func (pgTables testTable) isExpectedMissingColumn(tableName string, columnName string) bool {
	columns, ok := pgTables[tableName]
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
func (pgTables testTable) addMissingTable(tableName string) {
	pgTables[tableName] = make(testColumns)
}

// addMissingColumn adds a column when it is not found in cockroach db
func (pgTables testTable) addMissingColumn(tableName string, columnName string) {
	columns, ok := pgTables[tableName]

	if !ok {
		columns = make(testColumns)
		pgTables[tableName] = columns
	}

	columns[columnName] = nil
}

// dump creates pg_catalog_test-diffs.json when -rewrite-diffs is selected
func (pgTables testTable) dump(t *testing.T) {
	if !*rewriteFlag {
		return
	}

	diffFile := filepath.Join(testdata, expectedDiffs)
	f, err := os.OpenFile(diffFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	byteArray, err := json.MarshalIndent(pgTables, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	if _, err = f.Write(byteArray); err != nil {
		t.Fatal(err)
	}
}

// report will log the amount of diffs for missing table and columns and data type mismatches
func (sum *summary) report(t *testing.T) {
	if sum.missingTables != 0 {
		errorf(t, "Missing %d tables", sum.missingTables)
	}

	if sum.missingColumns != 0 {
		errorf(t, "Missing %d columns", sum.missingColumns)
	}

	if sum.mismatchDatatypesOid != 0 {
		errorf(t, "Column datatype mismatches: %d", sum.mismatchDatatypesOid)
	}
}

// loadTestData retrieves the pg_catalog from the dumpfile generated from Postgres
func loadTestData(t testing.TB) testTable {
	pgTables := make(testTable)
	testdataFile := filepath.Join(testdata, pgCatalogDump)
	f, err := os.Open(testdataFile)
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()
	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		t.Logf("PostgreSQL version: %s", scanner.Text())
	}

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		dataTypeOid, err := strconv.ParseUint(row[testTypeOidIndex], 10, 32)
		if err != nil {
			t.Fatal(err)
		}
		pgTables.addRow(row[testTableIndex], row[testColumnIndex], row[testTypeIndex], uint32(dataTypeOid))
	}

	return pgTables
}

// loadCockroachPgCatalog retrieves pg_catalog schema from cockroach db
func loadCockroachPgCatalog(t testing.TB) testTable {
	crdbTables := make(testTable)
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(db)
	rows := sqlRunner.Query(t, getPgCatalogSQL)
	defer rows.Close()

	for rows.Next() {
		var tableName, columnName, dataType string
		var dataTypeOid uint32
		if err := rows.Scan(&tableName, &columnName, &dataType, &dataTypeOid); err != nil {
			t.Fatal(err)
		}
		crdbTables.addRow(tableName, columnName, dataType, dataTypeOid)
	}
	return crdbTables
}

// loadExpectedDiffs get all differences that will be skipped by the this test
func loadExpectedDiffs(t *testing.T) (diffs testTable) {
	diffs = make(testTable)

	if *rewriteFlag {
		// For rewrite we want this to be empty and get populated
		return
	}

	diffFile := filepath.Join(testdata, expectedDiffs)
	if _, err := os.Stat(diffFile); err != nil {
		if oserror.IsNotExist(err) {
			// File does not exists it means diffs are not expected
			return
		}

		t.Fatal(err)
	}
	f, err := os.Open(diffFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(bytes, &diffs); err != nil {
		t.Fatal(err)
	}

	return
}

// errorf wraps *testing.T Errorf to report fails only when the test doesn't run in rewrite mode
func errorf(t *testing.T, format string, args ...interface{}) {
	if !*rewriteFlag {
		t.Errorf(format, args...)
	}
}

// TestPGCatalog is the pg_catalog diff tool test which compares pg_catalog with postgres and cockroach
func TestPGCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	pgTables := loadTestData(t)
	crdbTables := loadCockroachPgCatalog(t)
	diffs := loadExpectedDiffs(t)
	sum := &summary{}

	for pgTable, pgColumns := range pgTables {
		t.Run(fmt.Sprintf("Table=%s", pgTable), func(t *testing.T) {
			crdbColumns, ok := crdbTables[pgTable]
			if !ok {
				if !diffs.isExpectedMissingTable(pgTable) {
					errorf(t, "Missing table `%s`", pgTable)
					diffs.addMissingTable(pgTable)
					sum.missingTables++
				}
				return
			}

			for expColumnName, expColumn := range pgColumns {
				gotColumn, ok := crdbColumns[expColumnName]
				if !ok {
					if !diffs.isExpectedMissingColumn(pgTable, expColumnName) {
						errorf(t, "Missing column `%s`", expColumnName)
						diffs.addMissingColumn(pgTable, expColumnName)
						sum.missingColumns++
					}
					continue
				}

				if diffs.isDiffOid(pgTable, expColumnName, expColumn, gotColumn) {
					sum.mismatchDatatypesOid++
					errorf(t, "Column `%s` expected data type oid `%d` (%s) but found `%d` (%s)",
						expColumnName,
						expColumn.Oid,
						expColumn.DataType,
						gotColumn.Oid,
						gotColumn.DataType,
					)
					diffs.addDiff(pgTable, expColumnName, expColumn, gotColumn)
				}
			}
		})
	}

	sum.report(t)
	diffs.dump(t)
}
