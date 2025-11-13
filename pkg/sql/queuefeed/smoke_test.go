package queuefeed

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestQueuefeedSmoketest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `CREATE TABLE t (k string primary key)`)
	_, err := srv.SystemLayer().SQLConn(t).Exec(`SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	require.NoError(t, err)

	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)
	db.Exec(t, `SELECT crdb_internal.create_queue_feed('test_queue', $1)`, tableID)

	// TODO improve this test once creating the queue sets an accurate cursor. We
	// should be able to read an expected set of rows.
	ctx, cancel := context.WithCancel(ctx)
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		for i := 0; ctx.Err() == nil; i++ {
			t.Log("inserting row", i)
			db.Exec(t, `INSERT INTO t VALUES ($1::STRING)`, i)
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	})

	conn, err := srv.SQLConn(t).Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Try to read from the queue until we observe some data. The queue doesn't
	// currently track the frontier, so we need to keep inserting data because
	// there is a race between inserting and reading from the queue.
	found := 0
	for found < 1 {
		t.Log("reading from queue feed")

		cursor, err := conn.QueryContext(ctx, "SELECT * FROM crdb_internal.select_from_queue_feed('test_queue', 1)")
		require.NoError(t, err)

		for cursor.Next() {
			var k string
			require.NoError(t, cursor.Scan(&k))
			found++
		}

		require.NoError(t, cursor.Err())
		require.NoError(t, cursor.Close())
	}

	cancel()
	require.NoError(t, group.Wait())
}

func TestQueuefeedSmoketestMultipleRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)
	_, err := srv.SystemLayer().SQLConn(t).Exec(`SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	require.NoError(t, err)

	// Create table with composite primary key and split it
	db.Exec(t, `CREATE TABLE t (k1 INT, k2 INT, v string, PRIMARY KEY (k1, k2))`)
	db.Exec(t, `ALTER TABLE t SPLIT AT VALUES (1)`)

	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)
	db.Exec(t, `SELECT crdb_internal.create_queue_feed('test_multi', $1)`, tableID)

	// Create two managers for separate readers
	qm := NewTestManager(t, srv.ApplicationLayer())
	newReader := func(session uuid.UUID) *Reader {
		qm.mu.Lock()
		defer qm.mu.Unlock()

		// TODO: use the built ins once readers are properly assigned to a session.
		reader, err := qm.newReaderLocked(ctx, "test_multi", Session{
			ConnectionID: uuid.NewV4(),
			LivenessID:   sqlliveness.SessionID("1"),
		})
		require.NoError(t, err)
		return reader
	}

	ctx, cancel := context.WithCancel(ctx)
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		for ctx.Err() == nil {
			db.Exec(t, `INSERT INTO t VALUES ($1, $2, 'foo')`, rand.Intn(3), rand.Int())
			time.Sleep(10 * time.Millisecond)
		}
		t.Log("inserter stopping")
		return nil
	})

	readRows := 0
	reader := newReader(uuid.NewV4())
	for readRows < 100 {
		rows, err := reader.GetRows(ctx, 10)
		require.NoError(t, err)
		reader.ConfirmReceipt(ctx)
		t.Log("reader read", len(rows), "rows")
		readRows += len(rows)
		require.NoError(t, err)
	}

	cancel()
	_ = group.Wait()
}
