package queuefeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewTestManager(t *testing.T, a serverutils.ApplicationLayerInterface) *Manager {
	ctx := context.Background()
	db := a.InternalDB().(isql.DB)
	m := NewManager(ctx, db, a.RangeFeedFactory().(*rangefeed.Factory), a.Codec(), a.LeaseManager().(*lease.Manager))
	require.NotNil(t, m.codec)
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
	// get table id
	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)
	db.Exec(t, `SELECT crdb_internal.create_queue_feed('hi', $1)`, tableID)

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	_, err := db.DB.QueryContext(ctx, `SELECT crdb_internal.select_from_queue_feed('hi', 1)`)
	require.Error(t, err)
}

func TestFeedCreationPartitions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	// expect no error when creating a queue
	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, `CREATE TABLE t (a string)`)
	// split into 1k ranges
	db.Exec(t, `INSERT INTO t (a) SELECT generate_series(1, 10000)`)
	db.Exec(t, `ALTER TABLE t SPLIT AT (SELECT (i/10)::int FROM generate_series(1, 10000) AS g(i))`)
	db.Exec(t, `ALTER TABLE t SCATTER`)

	// get table id
	var tableID int64
	db.QueryRow(t, "SELECT id FROM system.namespace where name = 't'").Scan(&tableID)
	qm := NewTestManager(t, srv.ApplicationLayer())
	require.NoError(t, qm.CreateQueue(ctx, "test", tableID))

	// Get the table descriptor to determine the primary index span.
	leaseMgr := srv.ApplicationLayer().LeaseManager().(*lease.Manager)
	descriptor, err := leaseMgr.Acquire(ctx, lease.TimestampToReadTimestamp(srv.ApplicationLayer().Clock().Now()), descpb.ID(tableID))
	require.NoError(t, err)
	defer descriptor.Release(ctx)
	tableDesc := descriptor.Underlying().(catalog.TableDescriptor)
	primaryIndexSpan := tableDesc.PrimaryIndexSpan(qm.codec)

	// Count the number of partitions.
	pt := &partitionTable{queueName: "test"}
	err = srv.ApplicationLayer().InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := pt.ListPartitions(ctx, txn)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(partitions), 1000, "expected at least 1000 partitions") // It could be a bit more than 1k.

		partitionIDs := make(map[int64]bool)
		var partitionSpans []roachpb.Span
		for _, partition := range partitions {
			// There should be no duplicate partition IDs.
			assert.NotZero(t, partition.ID)
			_, ok := partitionIDs[partition.ID]
			assert.False(t, ok, "duplicate partition ID: %d", partition.ID)
			partitionIDs[partition.ID] = true

			// The spans should be primary index only and not overlap and cover the entire primary index span.
			partitionSpan := partition.Span
			assert.True(t, partitionSpan.Valid())
			assert.True(t, primaryIndexSpan.Contains(partitionSpan))
			partitionSpans = append(partitionSpans, partitionSpan)

			assert.True(t, partition.Session.Empty(), "partition %d should not be assigned to a session", partition.ID)
			assert.True(t, partition.Successor.Empty(), "partition %d should not have a successor", partition.ID)
		}

		// Verify spans don't overlap by checking each pair.
		for i, span1 := range partitionSpans {
			for j, span2 := range partitionSpans {
				if i < j {
					// Spans should not overlap (they can be adjacent).
					assert.False(t, span1.Overlaps(span2),
						"partition spans should not overlap: span1=%v, span2=%v", span1, span2)
				}
			}
		}

		// Verify spans cover the entire primary index span
		var spanGroup roachpb.SpanGroup
		spanGroup.Add(partitionSpans...)
		mergedSpans := spanGroup.Slice() // should be a single span covering the entire primary index span
		assert.Equal(t, 1, len(mergedSpans))
		assert.True(t, mergedSpans[0].Equal(primaryIndexSpan))

		return nil
	})
	require.NoError(t, err)

	// Start a reader and verify it reads all the partitions.
	reader, err := qm.CreateReaderForSession(ctx, "test", Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   "",
	})
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer func() { _ = reader.Close() }()

	err = srv.ApplicationLayer().InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := pt.ListPartitions(ctx, txn)
		require.NoError(t, err)

		session := reader.session
		for _, partition := range partitions {
			assert.Equal(t, session, partition.Session)
			assert.True(t, partition.Successor.Empty(), "partition %d should not have a successor", partition.ID)
		}

		return nil
	})
	require.NoError(t, err)
}
