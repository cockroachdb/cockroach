// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCleanupIntentsDuringBackupPerformanceRegression tests the backup process
// in the presence of unresolved intents.
func TestCleanupIntentsDuringBackupPerformanceRegression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingEnableEnterprise()()

	const totalRowCount = 10000
	const perTransactionRowCount = 10

	// Test with unresolved intents that belong to an aborted transaction (abort =
	// true) or a pending transaction (abort = false).
	testutils.RunTrueAndFalse(t, "abort", func(t *testing.T, abort bool) {
		var numIntentResolveBatches atomic.Int32
		var numPushBatches atomic.Int32

		// Interceptor catches requests that cleanup transactions of size 10, which are
		// test data transactions. All other requests pass though.
		interceptor := func(ctx context.Context, req *kvpb.BatchRequest) *kvpb.Error {
			if req.Requests[0].GetResolveIntent() != nil {
				numIntentResolveBatches.Add(1)
			}
			if req.Requests[0].GetPushTxn() != nil {
				numPushBatches.Add(1)
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

		_, err := sqlDb.Exec("create table foo(v int not null)")
		require.NoError(t, err)

		transactions := make([]*sql.Tx, totalRowCount/perTransactionRowCount)

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

		_, err = sqlDb.Exec("backup table foo to 'userfile:///test.foo'")
		require.NoError(t, err, "Failed to run backup")

		if !abort {
			for _, tx := range transactions {
				require.NoError(t, tx.Commit())
			}
		}

		// We expect each group of 10 intents to take 2 intent resolution batches:
		// - One intent gets discovered and added to the lock table, which forces the
		//   abandoned txn to be pushed and added to the txnStatusCache.
		// - The remaining nine intents get resolved together because they all belong
		//   to an abandoned txn.
		require.GreaterOrEqual(t, 2000, int(numIntentResolveBatches.Load()))
		require.GreaterOrEqual(t, 1100, int(numPushBatches.Load()))
	})
}
