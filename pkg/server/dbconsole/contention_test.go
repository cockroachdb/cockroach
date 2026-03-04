// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// makeContentionRow builds a tree.Datums row for the contention query
// columns: collection_ts, blocking_txn_id, blocking_txn_fingerprint_id,
// waiting_txn_id, waiting_stmt_id, contention_duration, key,
// database_name, schema_name, table_name, index_name, contention_type.
func makeContentionRow(
	collectionTS, blockingTxnID, blockingFpID, waitingTxnID, waitingStmtID, duration, key, dbName, schemaName, tableName, indexName, contentionType string,
) tree.Datums {
	return tree.Datums{
		dString(collectionTS),
		dString(blockingTxnID),
		dString(blockingFpID),
		dString(waitingTxnID),
		dString(waitingStmtID),
		dString(duration),
		dString(key),
		dString(dbName),
		dString(schemaName),
		dString(tableName),
		dString(indexName),
		dString(contentionType),
	}
}

func TestGetContentionDetails(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				makeContentionRow(
					"2024-01-15 10:00:00+00",
					"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
					"1234567890abcdef",
					"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
					"cccccccc-cccc-cccc-cccc-cccccccccccc",
					"00:00:01.5",
					"/Table/42/1/1",
					"mydb", "public", "users", "users_pkey",
					"LOCK_WAIT",
				),
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest("GET", "/api/v2/dbconsole/contention", nil)
		w := httptest.NewRecorder()
		api.GetContentionDetails(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result ContentionResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.ContentionEvents, 1)

		ev := result.ContentionEvents[0]
		require.Equal(t, "mydb", ev.DatabaseName)
		require.Equal(t, "users", ev.TableName)
		require.Equal(t, "users_pkey", ev.IndexName)
		require.Equal(t, "LOCK_WAIT", ev.ContentionType)
	})

	t.Run("with filters", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET",
			"/api/v2/dbconsole/contention?waiting_txn_id=abc&start=2024-01-15T00:00:00Z&end=2024-01-16T00:00:00Z",
			nil,
		)
		w := httptest.NewRecorder()
		api.GetContentionDetails(w, req)

		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("invalid start time", func(t *testing.T) {
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: &mockExecutor{}}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/contention?start=not-a-time", nil,
		)
		w := httptest.NewRecorder()
		api.GetContentionDetails(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx(
				nil, errors.New("query failed"),
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest("GET", "/api/v2/dbconsole/contention", nil)
		w := httptest.NewRecorder()
		api.GetContentionDetails(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestGetClusterLocks(t *testing.T) {
	// makeClusterLockRow builds a tree.Datums row for the cluster locks query
	// columns: database_name, table_name, index_name, lock_key_pretty,
	// txn_id, granted, contended, duration.
	makeClusterLockRow := func(
		dbName, tableName, indexName, lockKey, txnID string,
		granted bool, duration string,
	) tree.Datums {
		return tree.Datums{
			dString(dbName),
			dString(tableName),
			dString(indexName),
			dString(lockKey),
			dString(txnID),
			dBool(granted),
			dBool(true), // contended is always true per the WHERE clause
			dString(duration),
		}
	}

	t.Run("success with holder and waiter", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{
				// Lock holder (granted=true) first.
				makeClusterLockRow(
					"mydb", "users", "users_pkey",
					"/Table/42/1/1",
					"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
					true, "00:00:05",
				),
				// Waiter (granted=false).
				makeClusterLockRow(
					"mydb", "users", "users_pkey",
					"/Table/42/1/1",
					"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
					false, "00:00:02",
				),
			}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/cluster-locks", nil,
		)
		w := httptest.NewRecorder()
		api.GetClusterLocks(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result ClusterLocksResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Len(t, result.Locks, 1)

		lock := result.Locks[0]
		require.Equal(t, "mydb", lock.DatabaseName)
		require.Equal(t, "users", lock.TableName)
		require.Equal(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", lock.LockHolderTxnID)
		require.Equal(t, "00:00:05", lock.HoldTime)
		require.Len(t, lock.Waiters, 1)
		require.Equal(t, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", lock.Waiters[0].TxnID)
		require.Equal(t, "00:00:02", lock.Waiters[0].WaitTime)
	})

	t.Run("empty results", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx([]tree.Datums{}, nil),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/cluster-locks", nil,
		)
		w := httptest.NewRecorder()
		api.GetClusterLocks(w, req)

		require.Equal(t, http.StatusOK, w.Code)

		var result ClusterLocksResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		require.Empty(t, result.Locks)
	})

	t.Run("sql error", func(t *testing.T) {
		exec := &mockExecutor{
			queryBufferedExFn: makeStaticQueryBufferedEx(
				nil, errors.New("query failed"),
			),
		}
		api := &ApiV2DBConsole{InternalDB: &mockInternalDB{executor: exec}}

		req := newAuthenticatedRequest(
			"GET", "/api/v2/dbconsole/cluster-locks", nil,
		)
		w := httptest.NewRecorder()
		api.GetClusterLocks(w, req)

		require.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
