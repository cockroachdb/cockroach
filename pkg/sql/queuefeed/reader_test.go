package queuefeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/queuefeed/queuebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestReaderBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, `CREATE TABLE t (a STRING, b INT)`)

	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 't'").Scan(&tableID)

	qm := NewTestManager(t, srv.ApplicationLayer())
	defer qm.Close()
	require.NoError(t, qm.CreateQueue(ctx, "test_queue", tableID))

	reader, err := qm.CreateReaderForSession(ctx, "test_queue", Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   sqlliveness.SessionID("1"),
	})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	db.Exec(t, `INSERT INTO t VALUES ('row1', 10), ('row2', 20), ('row3', 30)`)

	rows := pollForRows(t, ctx, reader, 3)

	requireRow(t, rows[0], "row1", 10)
	requireRow(t, rows[1], "row2", 20)
	requireRow(t, rows[2], "row3", 30)
	reader.ConfirmReceipt(ctx)
}

func TestReaderRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, `CREATE TABLE t (a STRING, b INT)`)

	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 't'").Scan(&tableID)

	qm := NewTestManager(t, srv.ApplicationLayer())
	defer qm.Close()
	require.NoError(t, qm.CreateQueue(ctx, "rollback_test", tableID))

	reader, err := qm.CreateReaderForSession(ctx, "rollback_test", Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   sqlliveness.SessionID("1"),
	})
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	db.Exec(t, `INSERT INTO t VALUES ('row1', 100), ('row2', 200)`)

	rows1 := pollForRows(t, ctx, reader, 2)

	requireRow(t, rows1[0], "row1", 100)
	requireRow(t, rows1[1], "row2", 200)

	reader.RollbackBatch(ctx)

	rows2, err := reader.GetRows(ctx, 10)
	require.NoError(t, err)
	require.Len(t, rows2, 2, "should get same 2 rows after rollback")

	requireRow(t, rows2[0], "row1", 100)
	requireRow(t, rows2[1], "row2", 200)

	reader.ConfirmReceipt(ctx)

	db.Exec(t, `INSERT INTO t VALUES ('row3', 300), ('row4', 400)`)

	// Verify we got the NEW data (row3, row4), NOT the old data (row1, row2).
	rows3 := pollForRows(t, ctx, reader, 2)

	requireRow(t, rows3[0], "row3", 300)
	requireRow(t, rows3[1], "row4", 400)

	reader.ConfirmReceipt(ctx)
}

func TestCheckpointRestoration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, `CREATE TABLE t (a STRING, b INT)`)

	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 't'").Scan(&tableID)

	qm := NewTestManager(t, srv.ApplicationLayer())
	defer qm.Close()
	require.NoError(t, qm.CreateQueue(ctx, "checkpoint_test", tableID))

	session := Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   sqlliveness.SessionID("1"),
	}
	reader1, err := qm.CreateReaderForSession(ctx, "checkpoint_test", session)
	require.NoError(t, err)

	db.Exec(t, `INSERT INTO t VALUES ('batch1_row1', 1), ('batch1_row2', 2)`)

	// Sleep to let the rangefeed checkpoint advance past the data timestamps.
	// This is really ugly but with 3 seconds the test failed in 100% of runs.
	time.Sleep(10 * time.Second)

	_ = pollForRows(t, ctx, reader1, 2)

	reader1.ConfirmReceipt(ctx)
	require.NoError(t, reader1.Close())

	db.Exec(t, `INSERT INTO t VALUES ('batch2_row1', 3), ('batch2_row2', 4)`)

	reader2, err := qm.CreateReaderForSession(ctx, "checkpoint_test", session)
	require.NoError(t, err)
	defer func() { _ = reader2.Close() }()

	rows2 := pollForRows(t, ctx, reader2, 2)

	// Verify we got ONLY the new data, not the old data.
	// Check that none of the rows are from batch1.
	for _, row := range rows2 {
		val := getString(row[0])
		require.NotContains(t, val, "batch1", "should not see batch1 data after checkpoint")
		require.Contains(t, val, "batch2", "should see batch2 data")
	}
}

// pollForRows waits for the reader to return expectedCount rows.
func pollForRows(
	t *testing.T, ctx context.Context, reader queuebase.Reader, expectedCount int,
) []tree.Datums {
	var rows []tree.Datums
	require.Eventually(t, func() bool {
		var err error
		rows, err = reader.GetRows(ctx, 10)
		require.NoError(t, err)
		if len(rows) < expectedCount {
			reader.RollbackBatch(ctx)
		}
		return len(rows) == expectedCount
	}, 5*time.Second, 50*time.Millisecond, "expected %d rows", expectedCount)
	return rows
}

// getString extracts a string from a tree.Datum.
func getString(d tree.Datum) string {
	return string(*d.(*tree.DString))
}

// getInt extracts an int64 from a tree.Datum.
func getInt(d tree.Datum) int64 {
	return int64(*d.(*tree.DInt))
}

// requireRow asserts that a row matches the expected string and int values.
func requireRow(t *testing.T, row tree.Datums, expectedStr string, expectedInt int64) {
	require.Equal(t, expectedStr, getString(row[0]))
	require.Equal(t, expectedInt, getInt(row[1]))
}
