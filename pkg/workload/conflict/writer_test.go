// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package conflict

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/ldrrandgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	workloadrand "github.com/cockroachdb/cockroach/pkg/workload/rand"
	"github.com/stretchr/testify/require"
)

func TestWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE test`)
	sqlDB.Exec(t, `CREATE TABLE test_writer (id INT PRIMARY KEY, data TEXT NOT NULL)`)

	table, err := workloadrand.LoadTable(db, "test_writer")
	require.NoError(t, err)

	writer, err := newWriter(ctx, db, table)
	require.NoError(t, err)

	row := []any{1, "test data"}
	err = writer.upsertRow(ctx, row)
	require.NoError(t, err)

	sqlDB.CheckQueryResults(t,
		`SELECT id, data FROM test_writer WHERE id = 1`,
		[][]string{{"1", "test data"}},
	)

	err = writer.deleteRow(ctx, row)
	require.NoError(t, err)

	sqlDB.CheckQueryResults(t,
		`SELECT id, data FROM test_writer WHERE id = 1`,
		[][]string{},
	)
}

func TestWriterRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)

	rndSrc, _ := randutil.NewTestRand()
	stmt := ldrrandgen.GenerateLDRTable(ctx, rndSrc, "test_writer", true)

	sqlDB.Exec(t, tree.AsStringWithFlags(stmt, tree.FmtParsable))

	table, err := workloadrand.LoadTable(db, "test_writer")
	require.NoError(t, err)

	writer, err := newWriter(ctx, db, table)
	require.NoError(t, err)

	t.Logf("stmt: %s\n", tree.AsStringWithFlags(stmt, tree.FmtParsable))
	t.Logf("upsert: %s\n", writer.upsertStmt)
	t.Logf("delete: %s\n", writer.deleteStmt)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// NOTE: we retry the insert if it contains computed columns because its
	// possible the table contains computed columns that combine to overflow
	// (e.g. a + b). It's tricky to fix randgen so that it only generates values
	// that do not overflow.
	shouldRetryUpdate := func(err error, table workloadrand.Table) bool {
		hasComputedColumns := false
		for _, col := range table.Cols {
			if col.IsComputed {
				hasComputedColumns = true
				break
			}
		}

		// Ignore "pq: integer out of range" errors if the table has computed columns
		// since randgen doesn't know to constrain columns based on computed expressions
		if hasComputedColumns && strings.Contains(err.Error(), "pq: integer out of range") {
			return true
		}

		return false
	}

	var row []any
	require.Eventually(t, func() bool {
		var err error
		row, err = table.RandomRow(rng, 10)
		if err != nil {
			return false
		}
		t.Logf("inserting row: %+v", row)
		err = writer.upsertRow(ctx, row)
		if err != nil && !shouldRetryUpdate(err, table) {
			require.NoError(t, err)
		}
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "failed to insert random row after retries")

	require.Eventually(t, func() bool {
		var err error
		row, err = table.MutateRow(rng, 10, row)
		if err != nil {
			return false
		}
		t.Logf("mutating row: %+v", row)
		err = writer.upsertRow(ctx, row)
		if err != nil && !shouldRetryUpdate(err, table) {
			require.NoError(t, err)
		}
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "failed to mutate row after retries")

	// check there is one row in the table
	sqlDB.CheckQueryResults(t,
		`SELECT count(*) FROM test_writer`,
		[][]string{{"1"}},
	)

	t.Logf("deleting row: %+v", row)
	require.NoError(t, writer.deleteRow(ctx, row))

	rows := sqlDB.QueryStr(t, `SELECT * FROM test_writer`)
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}
	require.Equal(t, rows, [][]string{}, "failed to delete row (%+v)", row)
}
