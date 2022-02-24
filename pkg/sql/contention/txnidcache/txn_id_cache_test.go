// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTransactionIDCache tests the correctness of the txnidcache.Cache.
func TestTransactionIDCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()

	appName := "txnIDCacheTest"
	expectedTxnIDToUUIDMapping := make(map[uuid.UUID]roachpb.TransactionFingerprintID)
	injector := runtimeHookInjector{}

	injector.setHook(func(
		sessionData *sessiondata.SessionData,
		txnID uuid.UUID,
		txnFingerprintID roachpb.TransactionFingerprintID,
	) {
		if strings.Contains(sessionData.ApplicationName, appName) {
			expectedTxnIDToUUIDMapping[txnID] = txnFingerprintID
		}
	})

	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		BeforeTxnStatsRecorded: injector.hook,
	}

	testServer, sqlConn, kvDB := serverutils.StartServer(t, params)
	defer func() {
		require.NoError(t, sqlConn.Close())
		testServer.Stopper().Stop(ctx)
	}()

	testConn := sqlutils.MakeSQLRunner(sqlConn)

	// Set the cache size limit to a very generous amount to prevent premature
	// eviction.
	testConn.Exec(t, "SET CLUSTER SETTING sql.contention.txn_id_cache.max_size = '1GB'")

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

	txnIDCache.DrainWriteBuffer()
	t.Run("resolved_txn_id_cache_record", func(t *testing.T) {
		testutils.SucceedsWithin(t, func() error {
			for txnID, expectedTxnFingerprintID := range expectedTxnIDToUUIDMapping {
				actualTxnFingerprintID, ok := txnIDCache.Lookup(txnID)
				if !ok {
					return errors.Newf("expected to find txn(%s) with fingerprintID: "+
						"%d, but it was not found.",
						txnID, expectedTxnFingerprintID,
					)
				}
				if expectedTxnFingerprintID != actualTxnFingerprintID {
					return errors.Newf("expected to find txn(%s) with fingerprintID: %d, but the actual fingerprintID is: %d", txnID, expectedTxnFingerprintID, actualTxnFingerprintID)
				}
			}
			return nil
		}, 3*time.Second)

		sizePreEviction := txnIDCache.Size()
		testConn.Exec(t, "SET CLUSTER SETTING sql.contention.txn_id_cache.max_size = '10B'")

		// Execute additional queries to ensure we are overflowing the size limit.
		testConn.Exec(t, "SELECT 1")
		txnIDCache.DrainWriteBuffer()

		testutils.SucceedsWithin(t, func() error {
			sizePostEviction := txnIDCache.Size()
			if sizePostEviction >= sizePreEviction {
				return errors.Newf("expected txn id cache size to shrink below %d, "+
					"but it has increased to %d", sizePreEviction, sizePostEviction)
			}
			return nil
		}, 3*time.Second)
	})

	t.Run("provisional_txn_id_cache_record", func(t *testing.T) {
		testConn.Exec(t, "SET CLUSTER SETTING sql.contention.txn_id_cache.max_size = '10MB'")
		callCaptured := uint32(0)

		injector.setHook(func(
			sessionData *sessiondata.SessionData,
			txnID uuid.UUID,
			txnFingerprintID roachpb.TransactionFingerprintID) {
			if strings.Contains(sessionData.ApplicationName, appName) {
				if txnFingerprintID != roachpb.InvalidTransactionFingerprintID {
					txnIDCache.DrainWriteBuffer()

					testutils.SucceedsWithin(t, func() error {
						existingTxnFingerprintID, ok := txnIDCache.Lookup(txnID)
						if !ok {
							return errors.Newf("expected provision txn fingerprint id to be found for "+
								"txn(%s), but it was not", txnID)
						}
						if existingTxnFingerprintID != roachpb.InvalidTransactionFingerprintID {
							return errors.Newf("expected txn (%s) to have a provisional"+
								"txn fingerprint id, but this txn already has a resolved "+
								"txn fingerprint id: %d", txnID, existingTxnFingerprintID)
						}
						return nil
					}, 3*time.Second)
					atomic.StoreUint32(&callCaptured, 1)
				}
			}
		})

		testConn.Exec(t, "BEGIN")
		testConn.Exec(t, "SELECT 1")
		testConn.Exec(t, "COMMIT")

		require.NotZerof(t, atomic.LoadUint32(&callCaptured),
			"expected to found provisional txn id cache record, "+
				"but it was not found")
	})
}

// runtimeHookInjector provides a way to dynamically inject a testing knobs
// into a running cluster.
type runtimeHookInjector struct {
	syncutil.RWMutex
	op func(
		sessionData *sessiondata.SessionData,
		txnID uuid.UUID,
		txnFingerprintID roachpb.TransactionFingerprintID,
	)
}

func (s *runtimeHookInjector) hook(
	sessionData *sessiondata.SessionData,
	txnID uuid.UUID,
	txnFingerprintID roachpb.TransactionFingerprintID,
) {
	s.RLock()
	defer s.RUnlock()
	s.op(sessionData, txnID, txnFingerprintID)
}

func (s *runtimeHookInjector) setHook(
	op func(
		sessionData *sessiondata.SessionData,
		txnID uuid.UUID,
		txnFingerprintID roachpb.TransactionFingerprintID,
	),
) {
	s.Lock()
	defer s.Unlock()
	s.op = op
}
