package queuefeed_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
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
	defer manager.Close()
	queueName := "test_queue"
	err := manager.CreateQueue(ctx, queueName, tableDescID)
	require.NoError(t, err)

	pa, err := queuefeed.NewPartitionAssignments(s.ExecutorConfig().(sql.ExecutorConfig).InternalDB, queueName)
	require.NoError(t, err)

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

	newAssignment, err := pa.RefreshAssignment(context.Background(), assignment, true)
	require.NoError(t, err)
	require.Nil(t, newAssignment)

	require.NoError(t, pa.UnregisterSession(ctx, session))

	tdb.CheckQueryResults(t,
		"SELECT sql_liveness_session, user_session FROM defaultdb.queue_partition_"+queueName,
		[][]string{{"NULL", "NULL"}})
}
