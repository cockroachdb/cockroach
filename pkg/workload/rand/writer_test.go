// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rand

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/ldrrandgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

	table, err := LoadTable(db, "test_writer")
	require.NoError(t, err)

	writer := NewTableWriter(db, table)

	row := []any{1, "test data"}
	err = writer.UpsertRow(ctx, row)
	require.NoError(t, err)

	sqlDB.CheckQueryResults(t,
		`SELECT id, data FROM test_writer WHERE id = 1`,
		[][]string{{"1", "test data"}},
	)

	err = writer.DeleteRow(ctx, row)
	require.NoError(t, err)

	sqlDB.CheckQueryResults(t,
		`SELECT id, data FROM test_writer WHERE id = 1`,
		[][]string{},
	)
}

// runWriterTest executes a CREATE TABLE statement and tests that the writer can
// insert, mutate, and delete a random row.
func runWriterTest(
	t *testing.T, ctx context.Context, db *gosql.DB, tableName string, createStmt string,
) {
	t.Helper()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, createStmt)

	t.Logf("stmt: %s\n", createStmt)

	table, err := LoadTable(db, tableName)
	require.NoError(t, err)

	writer := NewTableWriter(db, table)

	t.Logf("upsert: %s\n", writer.upsertStmt)
	t.Logf("delete: %s\n", writer.deleteStmt)

	rng, _ := randutil.NewTestRand()

	// NOTE: we retry the insert if it contains computed columns because its
	// possible the table contains computed columns that combine to overflow
	// (e.g. a + b). It's tricky to fix randgen so that it only generates values
	// that do not overflow.
	shouldRetryUpdate := func(err error, table Table) bool {
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
	testutils.SucceedsSoon(t, func() error {
		var err error
		row, err = table.RandomRow(rng, 10)
		if err != nil {
			return err
		}
		t.Logf("inserting row: %+v", row)
		err = writer.UpsertRow(ctx, row)
		if err != nil && shouldRetryUpdate(err, table) {
			return err
		}
		if err != nil {
			t.Fatal(err)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		var err error
		row, err = table.MutateRow(rng, 10, row)
		if err != nil {
			return err
		}
		t.Logf("mutating row: %+v", row)
		err = writer.UpsertRow(ctx, row)
		if err != nil && shouldRetryUpdate(err, table) {
			return err
		}
		if err != nil {
			t.Fatal(err)
		}
		return nil
	})

	// check there is one row in the table
	sqlDB.CheckQueryResults(t,
		fmt.Sprintf("SELECT count(*) FROM %s", tableName),
		[][]string{{"1"}},
	)

	t.Logf("deleting row: %+v", row)
	require.NoError(t, writer.DeleteRow(ctx, row))

	rows := sqlDB.QueryStr(t, fmt.Sprintf("SELECT * FROM %s", tableName))
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}
	require.Equal(t, rows, [][]string{}, "failed to delete row (%+v)", row)
}

func TestWriterRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	rng, _ := randutil.NewTestRand()
	writerType := sqlclustersettings.LDRWriterType(sqlclustersettings.LDRImmediateModeWriter.Default())
	stmt := ldrrandgen.GenerateLDRTable(ctx, rng, "test_writer", writerType)
	createStmt := tree.AsStringWithFlags(stmt, tree.FmtParsable)

	runWriterTest(t, ctx, db, "test_writer", createStmt)
}

func TestWriterRegression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	testCases := []struct {
		name      string
		createSQL string
	}{
		{
			// The "char" type requires the parameters to have an explicit cast to
			// "char" otherwise the type gets inferred as STRING which causes '\x00'
			// to be interpreted as a string literal instead of a byte literal. The
			// symptom of this is the delete will fail to find the row because the
			// "char" column is truncated to 1 byte on insert, but the where clause
			// casts the char to a string which is not truncated.
			//
			//> insert into table cursed values ('\x00'); -- this gets truncated to '\'
			// INSERT 0 1
			//> delete from cursed where c = '\x00'; -- this is not truncated
			// DELETE 0
			//> select * from cursed;
			//  c
			//-----
			//  \
			name:      "char_type",
			createSQL: `CREATE TABLE char_type (col0 "char" NOT NULL, PRIMARY KEY (col0))`,
		},
		{
			// Regression test for integer overflow with computed columns.
			name: "computed_column_overflow",
			createSQL: `CREATE TABLE computed_column_overflow (
				col0 SMALLINT NOT NULL,
				col1 SMALLINT NOT NULL,
				col2 SMALLINT NOT NULL AS (col0 + col1) VIRTUAL,
				PRIMARY KEY (col0)
			)`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runWriterTest(t, ctx, db, tc.name, tc.createSQL)
		})
	}
}
