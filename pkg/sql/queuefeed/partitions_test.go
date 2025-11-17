package queuefeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestListPartitions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := srv.ApplicationLayer().InternalDB().(isql.DB)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	queueName := "test"

	pt := &partitionTable{queueName: queueName}

	// Create table
	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.CreateSchema(ctx, txn)
	})
	require.NoError(t, err)

	// Test empty
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := pt.ListPartitions(ctx, txn)
		require.NoError(t, err)
		require.Empty(t, partitions)
		return nil
	})
	require.NoError(t, err)

	// Insert one partition
	sessionID := uuid.MakeV4()
	connectionID := uuid.MakeV4()
	span := &roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
	spanBytes, _ := span.Marshal()

	sqlRunner.Exec(t, `
		INSERT INTO defaultdb.queue_partition_`+queueName+` 
		(partition_id, sql_liveness_session, user_session, partition_spec)
		VALUES (1, $1, $2, $3)`, sessionID, connectionID, spanBytes)

	// Test with data
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := pt.ListPartitions(ctx, txn)
		require.NoError(t, err)
		require.Len(t, partitions, 1)
		require.Equal(t, int64(1), partitions[0].ID)
		require.Equal(t, connectionID, partitions[0].Session.ConnectionID)
		return nil
	})
	require.NoError(t, err)
}

func TestUpdatePartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := srv.ApplicationLayer().InternalDB().(isql.DB)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	queueName := "test"

	pt := &partitionTable{queueName: queueName}

	// Create table
	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.CreateSchema(ctx, txn)
	})
	require.NoError(t, err)

	// Insert initial partition
	sqlRunner.Exec(t, `
		INSERT INTO defaultdb.queue_partition_`+queueName+` (partition_id) VALUES (1)`)

	// Update the partition
	newSessionID := uuid.MakeV4()
	newConnectionID := uuid.MakeV4()
	span := roachpb.Span{Key: roachpb.Key("new"), EndKey: roachpb.Key("span")}

	partition := Partition{
		ID: 1,
		Session: Session{
			LivenessID:   sqlliveness.SessionID(newSessionID.GetBytes()),
			ConnectionID: newConnectionID,
		},
		Span: span,
	}

	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.UpdatePartition(ctx, txn, partition)
	})
	require.NoError(t, err)

	// Verify update worked
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := pt.ListPartitions(ctx, txn)
		require.NoError(t, err)
		require.Len(t, partitions, 1)
		require.Equal(t, newConnectionID, partitions[0].Session.ConnectionID)
		require.Equal(t, span.Key, partitions[0].Span.Key)
		return nil
	})
	require.NoError(t, err)
}

func TestInsertPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := srv.ApplicationLayer().InternalDB().(isql.DB)
	queueName := "test"

	pt := &partitionTable{queueName: queueName}

	// Create table
	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.CreateSchema(ctx, txn)
	})
	require.NoError(t, err)

	// Insert partition
	sessionID := uuid.MakeV4()
	connectionID := uuid.MakeV4()
	span := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}

	partition := Partition{
		ID: 1,
		Session: Session{
			LivenessID:   sqlliveness.SessionID(sessionID.GetBytes()),
			ConnectionID: connectionID,
		},
		Span: span,
	}

	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.InsertPartition(ctx, txn, partition)
	})
	require.NoError(t, err)

	// Verify insertion worked
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := pt.ListPartitions(ctx, txn)
		require.NoError(t, err)
		require.Len(t, partitions, 1)
		require.Equal(t, int64(1), partitions[0].ID)
		require.Equal(t, connectionID, partitions[0].Session.ConnectionID)
		require.Equal(t, span.Key, partitions[0].Span.Key)
		require.Equal(t, span.EndKey, partitions[0].Span.EndKey)
		return nil
	})
	require.NoError(t, err)
}

func TestFetchPartitions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := srv.ApplicationLayer().InternalDB().(isql.DB)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	queueName := "test"
	pt := &partitionTable{queueName: queueName}

	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.CreateSchema(ctx, txn)
	})
	require.NoError(t, err)

	// Insert some test data
	sqlRunner.Exec(t, `INSERT INTO defaultdb.queue_partition_test (partition_id) VALUES (1), (3)`)

	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		result, err := pt.FetchPartitions(ctx, txn, []int64{1, 2, 3})
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Equal(t, int64(1), result[1].ID)
		require.True(t, result[2].Empty())
		require.Equal(t, int64(3), result[3].ID)
		return nil
	})
	require.NoError(t, err)
}

func TestGetPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := srv.ApplicationLayer().InternalDB().(isql.DB)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	pt := &partitionTable{queueName: "test"}

	// Create table
	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.CreateSchema(ctx, txn)
	})
	require.NoError(t, err)

	// Test not found
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := pt.Get(ctx, txn, 999)
		require.Error(t, err)
		return nil
	})
	require.NoError(t, err)

	// Insert test partition
	connectionID := uuid.MakeV4()
	span := roachpb.Span{Key: roachpb.Key("test"), EndKey: roachpb.Key("testend")}
	spanBytes, _ := span.Marshal()
	sqlRunner.Exec(t, `INSERT INTO defaultdb.queue_partition_test (partition_id, sql_liveness_session, user_session, partition_spec) VALUES (1, $1, $2, $3)`, []byte("test-session"), connectionID, spanBytes)

	// Test found
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partition, err := pt.Get(ctx, txn, 1)
		require.NoError(t, err)
		require.Equal(t, int64(1), partition.ID)
		require.Equal(t, connectionID, partition.Session.ConnectionID)
		return nil
	})
	require.NoError(t, err)
}

func TestUnregisterSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := srv.ApplicationLayer().InternalDB().(isql.DB)
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
	pt := &partitionTable{queueName: "test"}

	// Create table
	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pt.CreateSchema(ctx, txn)
	})
	require.NoError(t, err)

	// Create sessions
	session1 := Session{LivenessID: "session1", ConnectionID: uuid.MakeV4()}
	session2 := Session{LivenessID: "session2", ConnectionID: uuid.MakeV4()}

	// Insert partition with session1 as owner, session2 as successor
	span1 := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	span1Bytes, _ := span1.Marshal()
	sqlRunner.Exec(t, `INSERT INTO defaultdb.queue_partition_test (partition_id, sql_liveness_session, user_session, sql_liveness_session_successor, user_session_successor, partition_spec) VALUES (1, $1, $2, $3, $4, $5)`,
		[]byte(session1.LivenessID), session1.ConnectionID, []byte(session2.LivenessID), session2.ConnectionID, span1Bytes)

	// Insert partition with session1 as owner, no successor
	span2 := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}
	span2Bytes, _ := span2.Marshal()
	sqlRunner.Exec(t, `INSERT INTO defaultdb.queue_partition_test (partition_id, sql_liveness_session, user_session, partition_spec) VALUES (2, $1, $2, $3)`,
		[]byte(session1.LivenessID), session1.ConnectionID, span2Bytes)

	// Unregister session1
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		updates, err := pt.UnregisterSession(ctx, txn, session1)
		require.NoError(t, err)
		require.Len(t, updates, 2)

		// Partition 1: session2 should now be the owner
		partition1 := updates[1]
		require.Equal(t, session2.ConnectionID, partition1.Session.ConnectionID)
		require.True(t, partition1.Successor.Empty())

		// Partition 2: should be unassigned (no successor to promote)
		partition2 := updates[2]
		require.True(t, partition2.Session.Empty())
		require.True(t, partition2.Successor.Empty())
		return nil
	})
	require.NoError(t, err)
}
