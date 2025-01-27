// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgformat_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/pgformat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// Tests for the format() SQL function.
func TestFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "mitigating the tendency to time out at the sql package level under race")

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
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
		if typ.Oid() == oid.T_refcursor {
			// REFCURSOR doesn't support comparison operators.
			return true
		}

		// Skip jsonpath because we don't support <jsonpath> = <string> comparisons.
		if typ.Family() == types.JsonpathFamily {
			return true
		}
		return !randgen.IsLegalColumnType(typ)
	}
	for _, typ := range types.OidToType {
		if !skipType(typ) {
			typesToTest = append(typesToTest, typ)
		}
	}

	rng, _ := randutil.NewTestRand()
	createTable := func(t *testing.T, tdb *sqlutils.SQLRunner, typ *types.T) (tableNamer func(string) string) {
		columnSpec := fmt.Sprintf("c %s", typ.SQLString())
		tableName := fmt.Sprintf("%s_table_%d", strings.Replace(typ.String(), "\"", "", -1), rng.Int())
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
		tab := desctestutils.TestingGetPublicTableDescriptor(kvDB, srv.ApplicationLayer().Codec(), "defaultdb", r(`tablename`))
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
					switch typ.InternalType.ArrayContents {
					case types.Geometry, types.Geography, types.Box2D:
						// Casts from strings to arrays of these types are not currently supported.
						skip.WithIssue(t, 49203)
					case types.Float4:
						// Arrays of float4s are tricky, see issue for details.
						skip.WithIssue(t, 84326)
					}
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

func TestFormatWithWeirdFormatStrings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	specialFormatChars := []byte{'%', '$', '-', '*', '0', '1', 's', 'I', 'L'}
	numSpecial := len(specialFormatChars)
	specialFreq := 0.2
	evalContext := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	datums := make(tree.Datums, 10)
	for i := range datums {
		datums[i] = tree.NewDInt(tree.DInt(i))
	}
	for i := 0; i < 1000; i++ {
		b := make([]byte, rand.Intn(100))
		for j := 0; j < len(b); j++ {
			if rand.Float64() < specialFreq {
				b[j] = specialFormatChars[rand.Intn(numSpecial)]
			} else {
				b[j] = byte(rand.Intn(256))
			}
		}
		str := string(b)
		// Mostly just making sure no panics
		_, err := pgformat.Format(context.Background(), evalContext, str, datums...)
		if err != nil {
			require.Regexp(t, `position|width|not enough arguments|unrecognized verb|unterminated format`, err.Error(),
				"input string was %s", str)
		}
	}
}
