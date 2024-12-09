// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCleanupIntentsDuringBackupPerformanceRegression tests the backup process
// in the presence of unresolved intents.
func TestCleanupIntentsDuringBackupPerformanceRegression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "measures backup times to ensure intent resolution does not regress, can't work under race")
	skip.UnderDeadlock(t, "measures backup times to ensure intent resolution does not regress, can't work under deadlock")

	const totalRowCount = 10000
	const perTransactionRowCount = 10

	// Test with unresolved intents that belong to an aborted transaction (abort =
	// true) or a pending transaction (abort = false).
	testutils.RunTrueAndFalse(t, "abort", func(t *testing.T, abort bool) {
		var numIntentResolveBatches atomic.Int32

		// Interceptor catches requests that cleanup transactions of size 10, which are
		// test data transactions. All other requests pass though.
		interceptor := func(ctx context.Context, req *kvpb.BatchRequest) *kvpb.Error {
			if req.Requests[0].GetResolveIntent() != nil {
				numIntentResolveBatches.Add(1)
			}
			endTxn := req.Requests[0].GetEndTxn()
			if endTxn != nil && !endTxn.Commit && len(endTxn.LockSpans) == perTransactionRowCount {
				// If this is a rollback of one the test's SQL transactions, allow the
				// EndTxn to proceed and mark the transaction record as ABORTED, but strip
				// the request of its lock spans so that no intents are recorded into the
				// transaction record or eagerly resolved. This is a bit of a hack, but it
				// mimics the behavior of an abandoned transaction which is aborted by a
				// pusher after expiring due to an absence of heartbeats.
				endTxn.LockSpans = nil
			}
			return nil
		}

		serverKnobs := kvserver.StoreTestingKnobs{TestingRequestFilter: interceptor}
		s, sqlDb, _ := serverutils.StartServer(t,
			base.TestServerArgs{Knobs: base.TestingKnobs{Store: &serverKnobs}})
		defer s.Stopper().Stop(context.Background())

		// These cluster settings ensure that ExportRequests issued by the backup
		// processor are not delayed for too long, especially for abort=false, when
		// only the final high-priority ExportRequest can push the slow pending txn.
		_, err := sqlDb.Exec("SET CLUSTER SETTING bulkio.backup.read_with_priority_after = '500ms'")
		require.NoError(t, err)
		_, err = sqlDb.Exec("SET CLUSTER SETTING bulkio.backup.read_retry_delay = '100ms'")
		require.NoError(t, err)

		_, err = sqlDb.Exec("create table foo(v int not null)")
		require.NoError(t, err)

		transactions := make([]*gosql.Tx, totalRowCount/perTransactionRowCount)

		for i := 0; i < totalRowCount; i += perTransactionRowCount {
			tx, err := sqlDb.Begin()
			require.NoError(t, err)
			transactions[i/perTransactionRowCount] = tx
			for j := 0; j < perTransactionRowCount; j += 1 {
				statement := fmt.Sprintf("insert into foo (v) values (%d)", i+j)
				_, err = tx.Exec(statement)
				require.NoError(t, err)
			}
			if abort {
				require.NoError(t, tx.Rollback())
			}
		}

		// Reset the counters to avoid counting pushes and intent resolutions not
		// part of the backup.
		numIntentResolveBatches.Store(0)

		_, err = sqlDb.Exec("backup table foo into 'userfile:///test.foo'")
		require.NoError(t, err, "Failed to run backup")

		// We expect each group of 10 intents to take 2 intent resolution batches:
		// - One intent gets discovered and added to the lock table, which forces the
		//   abandoned txn to be pushed and added to the txnStatusCache.
		// - The remaining nine intents get resolved together because they all belong
		//   to an abandoned txn.
		// In reality, intents get batched into even larger batches, so the actual
		// number of intent resolution batches is lower than 2,000.
		require.GreaterOrEqual(t, 2000, int(numIntentResolveBatches.Load()))
		if !abort {
			for _, tx := range transactions {
				// Ensure the long-running transactions can commit.
				require.NoError(t, tx.Commit())
			}
		}
	})
}
