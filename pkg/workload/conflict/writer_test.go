package conflict

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
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
	stmt := tree.AsStringWithFlags(
		randgen.RandCreateTable(ctx, rndSrc, "test_writer", 1, RandTableOpt),
		tree.FmtParsable)

	t.Logf("stmt: %s", stmt)

	sqlDB.Exec(t, stmt)

	table, err := workloadrand.LoadTable(db, "test_writer1")
	require.NoError(t, err)

	writer, err := newWriter(ctx, db, table)
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	row, err := table.RandomRow(rng, 10)
	require.NoError(t, err)
	require.NoError(t, writer.upsertRow(ctx, row))

	row, err = table.MutateRow(rng, 10, row)
	require.NoError(t, err)
	require.NoError(t, writer.upsertRow(ctx, row))

	// check there is one row in the table
	sqlDB.CheckQueryResults(t,
		`SELECT COUNT(*) FROM test_writer1`,
		[][]string{{"1"}},
	)

	require.NoError(t, writer.deleteRow(ctx, row))

	sqlDB.CheckQueryResults(t,
		`SELECT COUNT(*) FROM test_writer1`,
		[][]string{{"0"}},
	)
}
