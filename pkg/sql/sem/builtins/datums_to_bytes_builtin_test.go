// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// Ensure that we can generate a bunch of rows of all of the relevant data
// types and get reasonable values out of them with no errors. We do this by
// first creating tables with a single column, one table per type and add
// values to that table, ensuring that we don't get an error and that we get
// unique values. Then we exercise random combinations of these types in the
// same way.
func TestCrdbInternalDatumsToBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	types := []string{
		"INT2", "INT4", "INT8",
		"FLOAT4", "FLOAT8",
		"STRING", "CHAR", "BYTES",
		"DECIMAL",
		"INTERVAL",
		"OID",
		"TIMESTAMPTZ", "TIMESTAMP", "DATE",
		"INET",
		"VARBIT",
		"STRING[]",
		"INT[]",
	}
	rng, _ := randutil.NewTestRand()
	createTable := func(t *testing.T, tdb *sqlutils.SQLRunner, typ []string) (columnNames []string) {
		columnNames = make([]string, len(typ))
		columnSpecs := make([]string, len(typ))
		for i := range typ {
			columnNames[i] = fmt.Sprintf("c%d", i)
			columnSpecs[i] = fmt.Sprintf("c%d %s", i, typ[i])
		}
		tdb.Exec(t, "SET experimental_enable_unique_without_index_constraints = true")

		// Create the table. It will look like:
		//
		//  CREATE TABLE (c0 <typ[0]>, c1 <typ[1]>, UNIQUE WITHOUT INDEX (c0, c1))
		createStmt := fmt.Sprintf(`CREATE TABLE "%s" (%s, UNIQUE WITHOUT INDEX (%s))`,
			t.Name(),
			strings.Join(columnSpecs, ", "),
			strings.Join(columnNames, ", "))
		tdb.Exec(t, createStmt)
		return columnNames
	}
	insertRows := func(t *testing.T, tdb *sqlutils.SQLRunner, columnNames []string) {
		// Insert numRows rows of random data with the first row being all NULL.
		numRows := 100 // arbitrary
		if util.RaceEnabled {
			numRows = 2
		}
		tab := desctestutils.TestingGetPublicTableDescriptor(kvDB, srv.ApplicationLayer().Codec(), "defaultdb", t.Name())
		for i := 0; i < numRows; i++ {
			var row []string
			for _, col := range tab.WritableColumns() {
				if col.GetName() == "rowid" {
					continue
				}
				var d tree.Datum
				if i == 0 {
					d = tree.DNull
				} else {
					const nullOk = false
					d = randgen.RandDatum(rng, col.GetType(), nullOk)
				}
				row = append(row, tree.AsStringWithFlags(d, tree.FmtParsable))
			}
			tdb.Exec(t, fmt.Sprintf(`INSERT INTO "%s" VALUES (%s) ON CONFLICT (%s) DO NOTHING`,
				t.Name(), strings.Join(row, ", "), strings.Join(columnNames, ", ")))
		}
	}

	testTableWithColumnTypes := func(t *testing.T, typ ...string) {
		conn, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		tdb := sqlutils.MakeSQLRunner(conn)
		columnNames := createTable(t, tdb, typ)
		insertRows(t, tdb, columnNames)
		// Validate that every row maps to a unique encoded value.
		read := fmt.Sprintf(`
WITH t AS (
          SELECT (t.*) AS cols, crdb_internal.datums_to_bytes(t.*) AS encoded
            FROM "%s" AS t
         )
SELECT (SELECT count(DISTINCT (cols)) FROM t) -
       (SELECT count(DISTINCT (encoded)) FROM t);`,
			t.Name())
		tdb.CheckQueryResults(t, read, [][]string{{"0"}})
	}
	t.Run("single type and nulls", func(t *testing.T) {
		for i := range types {
			typ := types[i]
			t.Run(typ, func(t *testing.T) {
				testTableWithColumnTypes(t, typ)
			})
		}
	})
	t.Run("various type combinations", func(t *testing.T) {
		const numCombinations = 10
		for i := 0; i < numCombinations; i++ {
			t.Run("", func(t *testing.T) {
				numColumns := rng.Intn(len(types)*3) + 1 // arbitrary, at least 1
				colTypes := make([]string, numColumns)
				for i := range colTypes {
					colTypes[i] = types[rng.Intn(len(types))]
				}
				testTableWithColumnTypes(t, colTypes...)
			})
		}
	})
}

// Test that some data types cannot be key encoded.
func TestCrdbInternalDatumsToBytesIllegalType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	for _, val := range []string{
		"'foo:1,2 bar:3'::tsvector",
	} {
		t.Run(val, func(t *testing.T) {
			tdb.ExpectErr(t, ".*illegal argument.*",
				fmt.Sprintf("SELECT crdb_internal.datums_to_bytes(%s)", val))
		})
	}
}
