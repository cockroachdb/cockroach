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
