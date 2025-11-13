package queuefeed

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

func TestQueuefeedSmoketestMultipleReaders(t *testing.T) {
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
	db.Exec(t, `ALTER TABLE t SPLIT AT VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9)`)

	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)
	db.Exec(t, `SELECT crdb_internal.create_queue_feed('t_queue', $1)`, tableID)

	ctx, cancel := context.WithCancel(ctx)
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		for i := 0; ctx.Err() == nil; i++ {
			_, err := sqlDB.ExecContext(ctx, `INSERT INTO t VALUES ($1, $2)`, i%10, rand.Int())
			if err != nil {
				return errors.Wrap(err, "inserting a row")
			}
		}
		return ctx.Err()
	})

	numWriters := rand.Intn(10) + 1 // create [1, 10] writers
	rowsSeen := make([]atomic.Int64, numWriters)
	t.Logf("spawning %d readers", numWriters)
	for i := range numWriters {
		group.Go(func() error {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

			conn, err := srv.SQLConn(t).Conn(ctx)
			if err != nil {
				return err
			}

			for ctx.Err() == nil {
				cursor, err := conn.QueryContext(ctx, `SELECT * FROM crdb_internal.select_from_queue_feed('t_queue', 1000)`)
				if err != nil {
					return err
				}
				for cursor.Next() {
					var e string
					if err := cursor.Scan(&e); err != nil {
						return errors.Wrap(err, "scanning queue feed row")
					}
					rowsSeen[i].Add(1)
				}
				require.NoError(t, cursor.Close())
			}

			return ctx.Err()
		})
	}

	// Wait for every reader to observe rows and every partition to be assigned.
	testutils.SucceedsSoon(t, func() error {
		for _, row := range db.QueryStr(t, "SELECT partition_id, user_session, user_session_successor FROM defaultdb.queue_partition_t_queue") {
			t.Logf("partition row: %v", row)
		}

		seen := make([]int64, numWriters)
		for i := range numWriters {
			seen[i] = rowsSeen[i].Load()
		}
		t.Logf("row counts %v", seen)

		for i := range numWriters {
			if seen[i] == 0 {
				return errors.Newf("reader %d has not seen any rows yet", i)
			}
		}

		var unassignedPartitions int
		db.QueryRow(t, "SELECT COUNT(*) FROM defaultdb.queue_partition_t_queue WHERE user_session IS NULL").Scan(&unassignedPartitions)
		if unassignedPartitions != 0 {
			return errors.Newf("%d unassigned partitions remain", unassignedPartitions)
		}

		return nil
	})

	cancel()
	_ = group.Wait()
}
