// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// Tests for the format() SQL function.
func TestFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	typesToTest := make([]*types.T, 0, 256)
	// Types we don't support that are present in types.OidToType
	var skipType func(typ *types.T) bool
	skipType = func(typ *types.T) bool {
		switch typ.Family() {
		case types.AnyFamily, types.OidFamily:
			return true
		case types.ArrayFamily:
			if !randgen.IsAllowedForArray(typ.ArrayContents()) {
				return true
			}
			if skipType(typ.ArrayContents()) {
				return true
			}
		}
		return !randgen.IsLegalColumnType(typ)
	}
	for _, typ := range types.OidToType {
		if !skipType(typ) {
			typesToTest = append(typesToTest, typ)
		}
	}

	seed := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	createTable := func(t *testing.T, tdb *sqlutils.SQLRunner, typ *types.T) (tableNamer func(string) string) {
		columnSpec := fmt.Sprintf("c %s", typ.SQLString())
		tableName := fmt.Sprintf("%s_table_%d", strings.Replace(typ.String(), "\"", "", -1), seed.Int())
		tableName = strings.Replace(tableName, `[]`, `_array`, -1)

		// Create the table.
		createStmt := fmt.Sprintf(`CREATE TABLE %s (%s)`, tableName, columnSpec)
		tdb.Exec(t, createStmt)
		return func(s string) string { return strings.Replace(s, "tablename", tableName, -1) }
	}
	insertRows := func(t *testing.T, tdb *sqlutils.SQLRunner, r func(string) string) {
		// Insert numRows rows of random data with the first row being all NULL.
		numRows := 10 // arbitrary
		if util.RaceEnabled {
			numRows = 2
		}
		tab := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "defaultdb", r(`tablename`))
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
					d = randgen.RandDatum(seed, col.GetType(), nullOk)
				}
				row = append(row, tree.AsStringWithFlags(d, tree.FmtParsable))
			}
			tdb.Exec(t, fmt.Sprintf(r(`INSERT INTO tablename VALUES (%s)`),
				strings.Join(row, ", ")))
		}
	}

	for _, typ := range typesToTest {
		t.Run(typ.String(), func(t *testing.T) {
			conn, err := sqlDB.Conn(ctx)
			require.NoError(t, err)
			tdb := sqlutils.MakeSQLRunner(conn)
			r := createTable(t, tdb, typ)
			insertRows(t, tdb, r)
			var values string
			tdb.QueryRow(t, r(`SELECT array_to_string(array_agg(c::string),', ') FROM tablename`)).Scan(&values)
			t.Log(values)
			t.Run("%s does not error", func(t *testing.T) {
				tdb.Exec(t, r(`SELECT format('%s',c) from tablename`))
			})
			t.Run("%I creates a valid identifier", func(t *testing.T) {
				stmts := tdb.Query(t,
					r(`SELECT format('CREATE TABLE IF NOT EXISTS %I (i int)', c) FROM tablename WHERE c IS NOT NULL`),
				)
				var shouldBeValidStmt string
				for stmts.Next() {
					require.NoError(t, stmts.Scan(&shouldBeValidStmt))
					tdb.Exec(t, shouldBeValidStmt)
				}
			})
			t.Run("%L creates a literal logically equivalent to the value", func(t *testing.T) {
				if typ.Family() == types.ArrayFamily {
					skip.WithIssue(t, 84274)
				}
				stmts := tdb.Query(t, r(`SELECT rowid, format('%L', c) FROM tablename WHERE c IS NOT NULL`))
				var literal string
				var rowid int
				queries := make([]string, 0, 10)
				for stmts.Next() {
					require.NoError(t, stmts.Scan(&rowid, &literal))
					queries = append(queries,
						fmt.Sprintf(r(`SELECT count(*) FROM tablename WHERE rowid=%d AND c=%s`),
							rowid, literal))
				}
				for _, query := range queries {
					tdb.CheckQueryResults(t, query, [][]string{{`1`}})
				}
			})

		})
	}

}
