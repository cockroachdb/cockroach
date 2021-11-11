// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestTransactionIDCache tests the correctness of the TxnIDCache. TxnIDCache
// isn't necessarily a ccl feature. However, since Bulk-IO statement handles
// the transaction lifetime slightly different from the regular statements,
// in order to expand test coverage to also include BACKUP statements,
// TxnIDCache tests is put into `ccl` directory in order to pull in CCL
// packages.
func TestTransactionIDCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()

	appName := "txnIDCacheTest"
	expectedTxnIDToUUIDMapping := make(map[uuid.UUID]roachpb.TransactionFingerprintID)

	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		BeforeTxnStatsRecorded: func(
			sessionData *sessiondata.SessionData,
			txnID uuid.UUID,
			txnFingerprintID roachpb.TransactionFingerprintID,
		) {
			if strings.Contains(sessionData.ApplicationName, appName) {
				expectedTxnIDToUUIDMapping[txnID] = txnFingerprintID
			}
		},
	}

	testServer, sqlConn, kvDB := serverutils.StartServer(t, params)
	defer func() {
		require.NoError(t, sqlConn.Close())
		testServer.Stopper().Stop(ctx)
	}()

	testConn := sqlutils.MakeSQLRunner(sqlConn)

	testConn.Exec(t, "CREATE DATABASE txnIDTest")
	testConn.Exec(t, "USE txnIDTest")
	testConn.Exec(t, "CREATE TABLE t AS SELECT generate_series(1, 10)")
	testConn.Exec(t, "SET application_name = $1", appName)

	testCases := []struct {
		stmts    []string
		explicit bool
	}{
		// Implicit transactions that will have same statement fingerprint.
		{
			stmts:    []string{"SELECT 1"},
			explicit: false,
		},
		{
			stmts:    []string{"SELECT 2"},
			explicit: false,
		},

		// Implicit transaction that have different statement fingerprints.
		{
			stmts:    []string{"SELECT 1, 1"},
			explicit: false,
		},
		{
			stmts:    []string{"SELECT 1, 1, 2"},
			explicit: false,
		},

		// Explicit Transactions.
		{
			stmts:    []string{"SELECT 1"},
			explicit: true,
		},
		{
			stmts:    []string{"SELECT 5"},
			explicit: true,
		},
		{
			stmts:    []string{"SELECT 5", "SELECT 6, 7"},
			explicit: true,
		},
	}

	// Send test statements into both regular SQL connection and internal
	// executor to test both code paths.
	for _, tc := range testCases {
		if tc.explicit {
			testConn.Exec(t, "BEGIN")
		}
		{
			for _, stmt := range tc.stmts {
				testConn.Exec(t, stmt)
			}
		}
		if tc.explicit {
			testConn.Exec(t, "COMMIT")
		}
	}

	ie := testServer.InternalExecutor().(*sql.InternalExecutor)

	for _, tc := range testCases {
		// Send statements one by one since internal executor doesn't support
		// sending a batch of statements.
		for _, stmt := range tc.stmts {
			var txn *kv.Txn
			if tc.explicit {
				txn = kvDB.NewTxn(ctx, "")
			}
			_, err := ie.QueryRowEx(
				ctx,
				appName,
				txn,
				sessiondata.InternalExecutorOverride{
					User: security.RootUserName(),
				},
				stmt,
			)
			require.NoError(t, err)
			if tc.explicit {
				require.NoError(t, txn.Commit(ctx))
			}

			require.NoError(t, err)
		}
	}

	// Ensure we have intercepted all transactions, the expected size is
	// calculated as:
	// # stmt executed in regular executor
	//  + # stmt executed in internal executor
	//  + 1 (`SET application_name` executed previously in regular SQL Conn)
	//  - 3 (explicit txns executed in internal executor due to
	//       https://github.com/cockroachdb/cockroach/issues/73091)
	expectedTxnIDCacheSize := len(testCases)*2 + 1 - 3
	require.Equal(t, expectedTxnIDCacheSize, len(expectedTxnIDToUUIDMapping))

	sqlServer := testServer.SQLServer().(*sql.Server)
	txnIDCache := sqlServer.GetTxnIDCache()

	for txnID, expectedTxnFingerprintID := range expectedTxnIDToUUIDMapping {
		actualTxnFingerprintID, ok := txnIDCache.Lookup(txnID)
		require.True(t, ok, "expected to found transaction fingerprint"+
			" for %s in transaction ID cache, but didn't", txnID)
		require.Equal(t, expectedTxnFingerprintID, actualTxnFingerprintID,
			"expected txnID %s to have fingerprint ID %s, but found %s",
			txnID, expectedTxnFingerprintID, actualTxnFingerprintID)
	}

	// We now test the enforcement of the cache size limit.
	previousCacheSize :=
		sqlServer.ServerMetrics.ContentionSubsystemMetrics.TxnIDCacheSize.Value()

	// Limit the size to 64 bytes.
	testConn.Exec(t, "SET CLUSTER SETTING sql.contention.txn_id_cache.max_size = '128B'")

	// Execute a statement to ensure that TxnIDCache has a chance to evict older
	// entries.
	testConn.Exec(t, "SELECT 1")

	currentCacheSize :=
		sqlServer.ServerMetrics.ContentionSubsystemMetrics.TxnIDCacheSize.Value()

	require.Lessf(t, currentCacheSize, previousCacheSize,
		"expected currentCacheSize (%d) to be less than the"+
			"previousCacheSize (%d), but it was not", currentCacheSize, previousCacheSize)
}
