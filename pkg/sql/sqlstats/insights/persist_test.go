package insights_test

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPersistTxnExecInsight(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{})

	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	server := testCluster.Server(0 /* idx */).ApplicationLayer()
	sqlConn := server.SQLConn(t, "system")
	sqlRunner := sqlutils.MakeSQLRunner(sqlConn)
	cfg := server.ExecutorConfig().(sql.ExecutorConfig)
	monitor := sql.MakeInternalExecutorMemMonitor(sql.MemoryMetrics{}, cfg.Settings)
	monitor.StartNoReserved(ctx, server.SQLServer().(*sql.Server).GetBytesMonitor())
	internalDB := sql.NewInternalDB(server.SQLServer().(*sql.Server), sql.MemoryMetrics{}, monitor)

	contention := time.Duration(1000)
	stmtExecID, _ := clusterunique.IDFromString("1")
	txnID := uuid.FastMakeV4()

	txnInsight := &insights.Transaction{
		ID:               txnID,
		FingerprintID:    123,
		UserPriority:     "",
		ImplicitTxn:      false,
		Contention:       &contention,
		StartTime:        time.Now().Add(1 * time.Hour),
		EndTime:          time.Now().Add(1 * time.Minute),
		User:             "root",
		ApplicationName:  "app name",
		RowsRead:         0,
		RowsWritten:      0,
		RetryCount:       0,
		AutoRetryReason:  "auto retry reason",
		Problems:         []insights.Problem{insights.Problem_FailedExecution},
		Causes:           []insights.Cause{insights.Cause_HighContention},
		StmtExecutionIDs: []clusterunique.ID{stmtExecID},
		CPUSQLNanos:      1234,
		LastErrorCode:    "last error code",
		Status:           insights.Transaction_Completed,
	}

	insight := &insights.Insight{
		Session:     insights.Session{},
		Transaction: txnInsight,
	}

	err := internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, innerErr := insights.PersistTxnExecInsight(ctx, txn.(isql.Txn), insight)
		return innerErr
	})
	require.NoError(t, err)

	rows := sqlRunner.QueryStr(t, "select * from system.txn_exec_insights;")

	require.Equal(t, len(rows), 1)
	require.Equal(t, len(rows[0]), 21)
	require.Equal(t, rows[0][0] /*txn_id*/, txnID.String())
}
