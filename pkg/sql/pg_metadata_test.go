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
// go run main.go > ../../sql/testdata/pg_catalog_tables.json
package sql

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors/oserror"
)

// Test data files
const (
	catalogDump   = "%s_tables.json"              // PostgreSQL pg_catalog schema
	expectedDiffs = "%s_test_expected_diffs.json" // Contains expected difference between postgres and cockroach
	testdata      = "testdata"                    // testdata directory
)

// When running test with -rewrite-diffs test will pass and re-create pg_catalog_test-diffs.json
var (
	rewriteFlag = flag.Bool("rewrite-diffs", false, "This will re-create the expected diffs file")
	catalogName = flag.String("catalog", "pg_catalog", "Catalog or namespace, default: pg_catalog")
)

// summary will keep accountability for any unexpected difference and report it in the log
type summary struct {
	missingTables        int
	missingColumns       int
	mismatchDatatypesOid int
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
func loadTestData(t testing.TB) PGMetadataTables {
	var pgCatalogFile PGMetadataFile
	testdataFile := filepath.Join(testdata, fmt.Sprintf(catalogDump, *catalogName))
	f, err := os.Open(testdataFile)
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	if err = json.Unmarshal(bytes, &pgCatalogFile); err != nil {
		t.Fatal(err)
	}

	return pgCatalogFile.PGMetadata
}

// loadCockroachPgCatalog retrieves pg_catalog schema from cockroach db
func loadCockroachPgCatalog(t testing.TB) PGMetadataTables {
	crdbTables := make(PGMetadataTables)
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(db)
	rows := sqlRunner.Query(t, GetPGMetadataSQL, *catalogName)
	defer rows.Close()

	for rows.Next() {
		var tableName, columnName, dataType string
		var dataTypeOid uint32
		if err := rows.Scan(&tableName, &columnName, &dataType, &dataTypeOid); err != nil {
			t.Fatal(err)
		}
		crdbTables.AddColumnMetadata(tableName, columnName, dataType, dataTypeOid)
	}
	return crdbTables
}

// loadExpectedDiffs get all differences that will be skipped by the this test
func loadExpectedDiffs(t *testing.T) (diffs PGMetadataTables) {
	diffs = PGMetadataTables{}

	if *rewriteFlag {
		// For rewrite we want this to be empty and get populated
		return
	}

	diffFile := filepath.Join(testdata, fmt.Sprintf(expectedDiffs, *catalogName))
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

func rewriteDiffs(t *testing.T, diffs PGMetadataTables, diffsFile string) {
	if !*rewriteFlag {
		return
	}

	if err := diffs.rewriteDiffs(diffsFile); err != nil {
		t.Fatal(err)
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
	rewriteDiffs(t, diffs, filepath.Join(testdata, fmt.Sprintf(expectedDiffs, *catalogName)))
}
