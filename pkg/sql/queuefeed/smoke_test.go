package queuefeed

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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

	// TODO(jeffswenson): rewrite this test to use normal sessions.

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
	defer qm.Close()
	newReader := func(session uuid.UUID) *Reader {
		qm.mu.Lock()
		defer qm.mu.Unlock()

		// TODO: use the built ins once readers are properly assigned to a session.
		reader, err := qm.newReaderLocked(ctx, "test_multi", Session{
			ConnectionID: session,
			LivenessID:   sqlliveness.SessionID("1"),
		})
		require.NoError(t, err)
		return reader
	}

	var rowsRead1, rowsRead2 atomic.Int64
	session1, session2 := uuid.NewV4(), uuid.NewV4()

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

	reader1 := newReader(session1)
	group.Go(func() error {
		for ctx.Err() == nil {
			rows, err := reader1.GetRows(ctx, 10)
			if err != nil {
				t.Log("reader 1 got error:", err)
				return err
			}
			reader1.ConfirmReceipt(ctx)
			t.Log("reader 1 read", len(rows), "rows")
			rowsRead1.Add(int64(len(rows)))
		}
		t.Log("reader 1 stopping")
		return nil
	})

	countPartitions := func() map[string]int {
		partitions := map[string]int{}
		rows := db.Query(t, `SELECT COALESCE(user_session::STRING, ''), COUNT(*) FROM defaultdb.queue_partition_test_multi GROUP BY 1`)
		defer rows.Close()
		for rows.Next() {
			var sessionID string
			var count int
			require.NoError(t, rows.Scan(&sessionID, &count))
			partitions[sessionID] = count
		}
		return partitions
	}

	testutils.SucceedsWithin(t, func() error {
		partitions := countPartitions()
		if partitions[session1.String()] != 2 {
			//t.Logf("expected reader '%s' to have 2 partitions, got %+v", session1.String(), partitions)
			return errors.Newf("expected reader '%s' to have 2 partitions, got %+v", session1.String(), partitions)
		}
		t.Log("reader 1 has 2 partitions:", partitions)
		return nil
	}, 10*time.Second)
	t.Log("reader 1 has 2 partitions")

	reader2 := newReader(session2)
	group.Go(func() error {
		t.Log("reader 2 started")
		for ctx.Err() == nil {
			rows, err := reader2.GetRows(ctx, 10)
			if err != nil {
				t.Log("reader 2 got error:", err)
				return err

			}
			reader2.ConfirmReceipt(ctx)
			rowsRead2.Add(int64(len(rows)))
		}
		return nil
	})

	testutils.SucceedsWithin(t, func() error {
		partitions := countPartitions()
		if partitions[session1.String()] != 1 && partitions[session2.String()] != 1 {
			return errors.Newf("expected each reader to have 1 partition found: %+v", partitions)
		}
		if rowsRead1.Load() == 0 {
			return errors.New("expected reader 1 to have read some rows")
		}
		if rowsRead2.Load() == 0 {
			return errors.New("epected reader 2 to have read some rows")
		}
		return nil
	}, 10*time.Second)
	t.Log("partitions are balanced and both readers have read some rows")

	cancel()
	err = group.Wait()
	if err != nil {
		t.Log("group finished with error:", err)
		require.ErrorIs(t, err, context.Canceled)
	}
}
