package queuefeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewTestManager(t *testing.T, a serverutils.ApplicationLayerInterface) *Manager {
	db := a.InternalDB().(isql.DB)
	m := NewManager(context.Background(), db, a.RangeFeedFactory().(*rangefeed.Factory), a.RangeDescIteratorFactory().(rangedesc.IteratorFactory), a.Codec(), a.LeaseManager().(*lease.Manager), nil)
	require.NotNil(t, m.codec)
	t.Cleanup(m.Close)
	return m
}

func TestFeedCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	// expect an error when trying to read from a queue that doesn't exist
	qm := NewTestManager(t, srv.ApplicationLayer())
	defer qm.Close()
	_, err := qm.CreateReaderForSession(context.Background(), "test", Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   "",
	})
	require.ErrorContains(t, err, "queue feed not found")

	// expect no error when creating a queue
	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, `CREATE TABLE t (a string)`)
	// get table id
	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)
	require.NoError(t, qm.CreateQueue(context.Background(), "test", tableID))

	// now we can read from the queue
	reader, err := qm.CreateReaderForSession(context.Background(), "test", Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   "",
	})
	require.NoError(t, err)
	require.NotNil(t, reader)
	_ = reader.Close()
}

func TestQueuefeedCtxCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, `CREATE TABLE t (a string)`)
	db.Exec(t, `SELECT crdb_internal.create_queue_feed('hi', 't')`)

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	_, err := db.DB.QueryContext(ctx, `SELECT crdb_internal.select_from_queue_feed('hi', 1)`)
	require.Error(t, err)
}

func TestWatchForDeadSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	// Create a fake SQL liveness storage for testing
	fakeStorage := slstorage.NewFakeStorage()

	// Create a manager with the fake storage
	db := srv.ApplicationLayer().InternalDB().(isql.DB)
	m := NewManager(
		ctx, db, srv.ApplicationLayer().RangeFeedFactory().(*rangefeed.Factory),
		srv.ApplicationLayer().RangeDescIteratorFactory().(rangedesc.IteratorFactory), srv.ApplicationLayer().Codec(), srv.ApplicationLayer().LeaseManager().(*lease.Manager),
		fakeStorage,
	)

	// Create a queue
	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, `CREATE TABLE t (a string PRIMARY KEY)`)
	var tableID int64
	sqlDB.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)

	// Create multiple ranges BEFORE creating the queue to ensure we have enough partitions
	sqlDB.Exec(t, `INSERT INTO t (a) SELECT generate_series(1, 100)`)
	sqlDB.Exec(t, `ALTER TABLE t SPLIT AT VALUES ('10'), ('20'), ('30'), ('40'), ('50'), ('60'), ('70'), ('80'), ('90')`)
	sqlDB.Exec(t, `ALTER TABLE t SCATTER`)

	// Now create the queue - it will create partitions for all the ranges
	require.NoError(t, m.CreateQueue(ctx, "test", tableID))

	// Get partitions
	pt := &partitionTable{queueName: "test"}
	var partitions []Partition
	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		partitions, err = pt.ListPartitions(ctx, txn)
		return err
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(partitions), 4, "should have at least 4 partitions for this test")

	// Create two sessions with liveness IDs
	deadSessionID := sqlliveness.SessionID("dead-session")
	aliveSessionID := sqlliveness.SessionID("alive-session")
	successorSessionID := sqlliveness.SessionID("successor-session")

	deadSession := Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   deadSessionID,
	}
	aliveSession := Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   aliveSessionID,
	}
	successorSession := Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   successorSessionID,
	}

	// Mark sessions as alive in fake storage
	clock := srv.ApplicationLayer().Clock()
	expiration := clock.Now().Add(10*time.Second.Nanoseconds(), 0)
	require.NoError(t, fakeStorage.Insert(ctx, deadSessionID, expiration))
	require.NoError(t, fakeStorage.Insert(ctx, aliveSessionID, expiration))
	require.NoError(t, fakeStorage.Insert(ctx, successorSessionID, expiration))

	// Assign some partitions to the dead session
	deadPartition := partitions[0]
	deadPartition.Session = deadSession
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.UpdatePartition(ctx, txn, deadPartition)
	})
	require.NoError(t, err)

	// Assign a partition with a dead session and a successor
	deadWithSuccessorPartition := partitions[1]
	deadWithSuccessorPartition.Session = deadSession
	deadWithSuccessorPartition.Successor = successorSession
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.UpdatePartition(ctx, txn, deadWithSuccessorPartition)
	})
	require.NoError(t, err)

	// Assign a partition with a dead successor
	deadSuccessorPartition := partitions[2]
	deadSuccessorPartition.Session = aliveSession
	deadSuccessorPartition.Successor = deadSession
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.UpdatePartition(ctx, txn, deadSuccessorPartition)
	})
	require.NoError(t, err)

	// Assign a partition to an alive session (should remain unchanged)
	alivePartition := partitions[3]
	alivePartition.Session = aliveSession
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.UpdatePartition(ctx, txn, alivePartition)
	})
	require.NoError(t, err)

	// Mark the dead session as dead by deleting it from fake storage
	require.NoError(t, fakeStorage.Delete(ctx, deadSessionID))

	// Check for dead sessions
	require.NoError(t, m.checkQueueForDeadSessions(ctx, "test"))

	// Verify results
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		updatedPartitions, err := pt.ListPartitions(ctx, txn)
		require.NoError(t, err)

		// Find the partitions we updated
		var foundDeadPartition, foundDeadWithSuccessorPartition, foundDeadSuccessorPartition, foundAlivePartition *Partition
		for i := range updatedPartitions {
			p := &updatedPartitions[i]
			if p.ID == deadPartition.ID {
				foundDeadPartition = p
			} else if p.ID == deadWithSuccessorPartition.ID {
				foundDeadWithSuccessorPartition = p
			} else if p.ID == deadSuccessorPartition.ID {
				foundDeadSuccessorPartition = p
			} else if p.ID == alivePartition.ID {
				foundAlivePartition = p
			}
		}

		// Dead session partition should be cleared
		require.NotNil(t, foundDeadPartition, "should find dead partition")
		assert.True(t, foundDeadPartition.Session.Empty(), "dead session partition should be cleared")
		assert.True(t, foundDeadPartition.Successor.Empty(), "dead session partition should have no successor")

		// Dead session with successor should promote successor to session
		require.NotNil(t, foundDeadWithSuccessorPartition, "should find dead with successor partition")
		assert.Equal(t, successorSession, foundDeadWithSuccessorPartition.Session, "successor should be promoted to session")
		assert.True(t, foundDeadWithSuccessorPartition.Successor.Empty(), "successor should be cleared")

		// Dead successor should be cleared
		require.NotNil(t, foundDeadSuccessorPartition, "should find dead successor partition")
		assert.Equal(t, aliveSession, foundDeadSuccessorPartition.Session, "alive session should remain")
		assert.True(t, foundDeadSuccessorPartition.Successor.Empty(), "dead successor should be cleared")

		// Alive session partition should remain unchanged
		require.NotNil(t, foundAlivePartition, "should find alive partition")
		assert.Equal(t, aliveSession, foundAlivePartition.Session, "alive session should remain unchanged")
		assert.True(t, foundAlivePartition.Successor.Empty(), "alive partition should have no successor")

		return nil
	})
	require.NoError(t, err)

	// Close the manager to wait for the watchForDeadSessions goroutine to exit
	m.Close()
}
