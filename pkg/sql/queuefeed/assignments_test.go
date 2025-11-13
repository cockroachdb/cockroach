package queuefeed_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/queuefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestPartitionAssignments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE TABLE test_table (id INT PRIMARY KEY, data TEXT)")

	var tableDescID int64
	tdb.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 'test_table'").Scan(&tableDescID)

	// Create queue using QueueManager
	manager := queuefeed.NewTestManager(t, s.ApplicationLayer())
	queueName := "test_queue"
	err := manager.CreateQueue(ctx, queueName, tableDescID)
	require.NoError(t, err)

	pa := queuefeed.NewPartitionAssignments(s.ExecutorConfig().(sql.ExecutorConfig).InternalDB, queueName)

	session := queuefeed.Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   sqlliveness.SessionID("1"),
	}

	assignment, err := pa.RegisterSession(ctx, session)
	require.NoError(t, err)
	require.Len(t, assignment.Partitions, 1)
	require.Equal(t, session, assignment.Partitions[0].Session, "partition: %+v", assignment.Partitions[0])

	tdb.CheckQueryResults(t,
		"SELECT sql_liveness_session, user_session FROM defaultdb.queue_partition_"+queueName,
		[][]string{{"1", session.ConnectionID.String()}})

	require.NoError(t, pa.UnregisterSession(ctx, session))

	tdb.CheckQueryResults(t,
		"SELECT sql_liveness_session, user_session FROM defaultdb.queue_partition_"+queueName,
		[][]string{{"NULL", "NULL"}})
}

func TestPartitionReassignments(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	// tdb.Exec(t, "CREATE TABLE test_table (id INT PRIMARY KEY, data TEXT)") // TODO: why does this fail with "empty encoded value"?
	tdb.Exec(t, "CREATE TABLE test_table (a string)")

	var tableDescID int64
	tdb.QueryRow(t, "SELECT id FROM system.namespace WHERE name = 'test_table'").Scan(&tableDescID)

	// Create queue using QueueManager
	manager := queuefeed.NewTestManager(t, s.ApplicationLayer())
	queueName := "test_queue"
	err := manager.CreateQueue(ctx, queueName, tableDescID)
	require.NoError(t, err)

	pa := queuefeed.NewPartitionAssignments(s.ExecutorConfig().(sql.ExecutorConfig).InternalDB, queueName)

	session := queuefeed.Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   sqlliveness.SessionID("1"),
	}
	reader, err := manager.CreateReaderForSession(ctx, "test_queue", session)
	require.NoError(t, err)

	// get the session the reader is using
	var partition queuefeed.Partition
	pt := queuefeed.TestNewPartitionsTable(queueName)
	err = s.ExecutorConfig().(sql.ExecutorConfig).InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		partitions, err := pt.ListPartitions(ctx, txn)
		require.NoError(t, err)
		require.Len(t, partitions, 1)
		partition = partitions[0]
		return nil
	})
	require.NoError(t, err)

	// some other session tries to claim the partition
	someOtherSession := queuefeed.Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   sqlliveness.SessionID("2"),
	}
	partition, err = pa.TryClaim(ctx, someOtherSession, partition)
	require.NoError(t, err)
	require.Equal(t, someOtherSession, partition.Successor)

	// do a read from the queue so it checks for a reassignment
	tdb.Exec(t, "INSERT INTO test_table (a) VALUES ('test'), ('test2')")
	rows, err := reader.GetRows(ctx, 1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	// confirm receipt. it should then check for a reassignment and see that we disowned it
	reader.ConfirmReceipt(ctx)

	// try to read again to see that it failed
	// NOTE: we want this to not fail in the future but to sleep & poll instead.
	// sooo maybe this isnt the best way to test this. but i cant think of a
	// better way at this exact moment.

	rows, err = reader.GetRows(ctx, 1)
	require.Error(t, err)
}
