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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
		numIntentResolveBatches := 0
		numPushBatches := 0

		// Interceptor catches requests that cleanup transactions of size 10, which are
		// test data transactions. All other requests pass though.
		interceptor := func(ctx context.Context, req *kvpb.BatchRequest) *kvpb.Error {
			if req.Requests[0].GetResolveIntent() != nil {
				numIntentResolveBatches++
			}
			if req.Requests[0].GetPushTxn() != nil {
				numPushBatches++
			}
			endTxn := req.Requests[0].GetEndTxn()
			if endTxn != nil && len(endTxn.LockSpans) == perTransactionRowCount {
				if endTxn.Commit {
					// If this is a commit of one of the test's transactions, return an error
					// to ensure the commit doesn't go through and the intents remain
					// unresolved. This is a bit of a hack, but it mimics the behavior of a
					// long-running PENDING transaction.
					return kvpb.NewError(errors.New("Test txn not allowed to commit."))
				} else {
					// If this is a rollback of one of the test's transactions, allow the
					// EndTxn to proceed and mark the transaction record as ABORTED, but strip
					// the request of its lock spans so that no intents are recorded into the
					// transaction record or eagerly resolved. This is a bit of a hack, but it
					// mimics the behavior of an abandoned transaction which is aborted by a
					// pusher after expiring due to an absence of heartbeats.
					endTxn.LockSpans = nil
				}
			}
			return nil
		}

		serverKnobs := kvserver.StoreTestingKnobs{TestingRequestFilter: interceptor}
		s, sqlDb, _ := serverutils.StartServer(t,
			base.TestServerArgs{Knobs: base.TestingKnobs{Store: &serverKnobs}})
		defer s.Stopper().Stop(context.Background())

		_, err := sqlDb.Exec("create table foo(v int not null)")
		require.NoError(t, err)

		for i := 0; i < totalRowCount; i += perTransactionRowCount {
			tx, err := sqlDb.Begin()
			require.NoError(t, err)
			for j := 0; j < perTransactionRowCount; j += 1 {
				statement := fmt.Sprintf("insert into foo (v) values (%d)", i+j)
				_, err = tx.Exec(statement)
				require.NoError(t, err)
			}
			if abort {
				require.NoError(t, tx.Rollback())
			} else {
				require.NoError(t, tx.Commit())
			}
		}

		_, err = sqlDb.Exec("backup table foo to 'userfile:///test.foo'")
		require.NoError(t, err, "Failed to run backup")

		// We expect each group of 10 intents to take 2 intent resolution batches:
		// - One intent gets discovered and added to the lock table, which forces the
		//   abandoned txn to be pushed and added to the txnStatusCache.
		// - The remaining nine intents get resolved together because they all belong
		//   to an abandoned txn.
		require.GreaterOrEqual(t, 2000, numIntentResolveBatches)
		require.GreaterOrEqual(t, 2000, numPushBatches)
	})
}
