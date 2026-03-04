// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/errors"
)

// ContentionEvent represents a single contention event.
type ContentionEvent struct {
	// CollectionTS is the timestamp when the event was collected.
	CollectionTS string `json:"collection_ts"`
	// BlockingTxnID is the ID of the blocking transaction.
	BlockingTxnID string `json:"blocking_txn_id"`
	// BlockingTxnFingerprintID is the fingerprint ID of the blocking
	// transaction.
	BlockingTxnFingerprintID string `json:"blocking_txn_fingerprint_id"`
	// WaitingTxnID is the ID of the waiting transaction.
	WaitingTxnID string `json:"waiting_txn_id"`
	// WaitingStmtID is the ID of the waiting statement.
	WaitingStmtID string `json:"waiting_stmt_id"`
	// ContentionDuration is the duration of the contention event.
	ContentionDuration string `json:"contention_duration"`
	// Key is the contended key.
	Key string `json:"key"`
	// DatabaseName is the database where contention occurred.
	DatabaseName string `json:"database_name"`
	// TableName is the table where contention occurred.
	TableName string `json:"table_name"`
	// IndexName is the index where contention occurred.
	IndexName string `json:"index_name"`
	// ContentionType is the type of contention (e.g. LOCK_WAIT).
	ContentionType string `json:"contention_type"`
}

// ContentionResponse contains the list of contention events.
type ContentionResponse struct {
	// ContentionEvents is the list of contention events.
	ContentionEvents []ContentionEvent `json:"contention_events"`
}

// GetContentionDetails returns contention events with optional filtering.
//
// ---
// @Summary Get contention details
// @Description Returns contention events from
// crdb_internal.transaction_contention_events.
// @Tags Contention
// @Produce json
// @Param waiting_txn_id query string false "Filter by waiting transaction ID"
// @Param waiting_stmt_id query string false "Filter by waiting statement ID"
// @Param start query string false "Start time filter (RFC3339)"
// @Param end query string false "End time filter (RFC3339)"
// @Success 200 {object} ContentionResponse
// @Failure 400 {string} string "Invalid parameter"
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /contention [get]
func (api *ApiV2DBConsole) GetContentionDetails(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	query := safesql.NewQuery()
	query.Append(
		`SELECT collection_ts, blocking_txn_id,
            blocking_txn_fingerprint_id, waiting_txn_id,
            waiting_stmt_id, contention_duration,
            key, database_name, schema_name, table_name,
            index_name, contention_type
     FROM crdb_internal.transaction_contention_events
     WHERE true`,
	)

	if v := r.URL.Query().Get("waiting_txn_id"); v != "" {
		query.Append(` AND waiting_txn_id = $::UUID`, v)
	}
	if v := r.URL.Query().Get("waiting_stmt_id"); v != "" {
		query.Append(` AND waiting_stmt_id = $`, v)
	}
	if v := r.URL.Query().Get("start"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "invalid start parameter: expected RFC3339 format", http.StatusBadRequest)
			return
		}
		query.Append(` AND collection_ts >= $`, t)
	}
	if v := r.URL.Query().Get("end"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "invalid end parameter: expected RFC3339 format", http.StatusBadRequest)
			return
		}
		query.Append(` AND collection_ts <= $`, t)
	}
	query.Append(` ORDER BY collection_ts DESC`)

	if errs := query.Errors(); len(errs) > 0 {
		srverrors.APIV2InternalError(ctx, errors.CombineErrors(errs[0], errs[len(errs)-1]), w)
		return
	}

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-contention", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	events := make([]ContentionEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, ContentionEvent{
			CollectionTS:             tree.AsStringWithFlags(row[0], tree.FmtBareStrings),
			BlockingTxnID:            tree.AsStringWithFlags(row[1], tree.FmtBareStrings),
			BlockingTxnFingerprintID: tree.AsStringWithFlags(row[2], tree.FmtBareStrings),
			WaitingTxnID:             tree.AsStringWithFlags(row[3], tree.FmtBareStrings),
			WaitingStmtID:            tree.AsStringWithFlags(row[4], tree.FmtBareStrings),
			ContentionDuration:       tree.AsStringWithFlags(row[5], tree.FmtBareStrings),
			Key:                      tree.AsStringWithFlags(row[6], tree.FmtBareStrings),
			DatabaseName:             string(tree.MustBeDString(row[7])),
			TableName:                string(tree.MustBeDString(row[9])),
			IndexName:                string(tree.MustBeDString(row[10])),
			ContentionType:           string(tree.MustBeDString(row[11])),
		})
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, ContentionResponse{
		ContentionEvents: events,
	})
}

// ClusterLockWaiter represents a transaction waiting on a lock.
type ClusterLockWaiter struct {
	// TxnID is the ID of the waiting transaction.
	TxnID string `json:"txn_id"`
	// WaitTime is the duration the transaction has been waiting.
	WaitTime string `json:"wait_time"`
}

// ClusterLockInfo represents a single contended lock and its waiters.
type ClusterLockInfo struct {
	// DatabaseName is the database containing the locked key.
	DatabaseName string `json:"database_name"`
	// TableName is the table containing the locked key.
	TableName string `json:"table_name"`
	// IndexName is the index containing the locked key.
	IndexName string `json:"index_name"`
	// LockHolderTxnID is the ID of the transaction holding the lock.
	LockHolderTxnID string `json:"lock_holder_txn_id"`
	// HoldTime is the duration the lock has been held.
	HoldTime string `json:"hold_time"`
	// Waiters is the list of transactions waiting on this lock.
	Waiters []ClusterLockWaiter `json:"waiters"`
}

// ClusterLocksResponse contains the list of contended cluster locks.
type ClusterLocksResponse struct {
	// Locks is the list of contended locks, grouped by lock key.
	Locks []ClusterLockInfo `json:"locks"`
}

// GetClusterLocks returns contended cluster locks, grouped by lock key with
// the lock holder and waiters.
//
// ---
// @Summary Get cluster locks
// @Description Returns contended cluster locks from
// crdb_internal.cluster_locks, grouped by lock key.
// @Tags Contention
// @Produce json
// @Success 200 {object} ClusterLocksResponse
// @Failure 500 {object} ErrorResponse
// @Security ApiKeyAuth
// @Router /cluster-locks [get]
func (api *ApiV2DBConsole) GetClusterLocks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sqlUser := authserver.UserFromHTTPAuthInfoContext(ctx)

	ie := api.InternalDB.Executor()
	rows, err := ie.QueryBufferedEx(
		ctx, "dbconsole-get-cluster-locks", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sqlUser},
		`SELECT database_name, table_name, index_name,
            lock_key_pretty, txn_id, granted,
            contended, duration
     FROM crdb_internal.cluster_locks
     WHERE contended = true
     ORDER BY lock_key_pretty, granted DESC`,
	)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	// Group locks by lock_key_pretty. Rows are ordered so that the granted
	// (holder) row appears first for each key group.
	locksByKey := make(map[string]*ClusterLockInfo)
	var lockOrder []string
	for _, row := range rows {
		lockKey := string(tree.MustBeDString(row[3]))
		txnID := tree.AsStringWithFlags(row[4], tree.FmtBareStrings)
		granted := tree.MustBeDBool(row[5])
		duration := tree.AsStringWithFlags(row[7], tree.FmtBareStrings)

		lock, exists := locksByKey[lockKey]
		if !exists {
			lock = &ClusterLockInfo{
				DatabaseName: string(tree.MustBeDString(row[0])),
				TableName:    string(tree.MustBeDString(row[1])),
				IndexName:    string(tree.MustBeDString(row[2])),
				Waiters:      []ClusterLockWaiter{},
			}
			locksByKey[lockKey] = lock
			lockOrder = append(lockOrder, lockKey)
		}

		if bool(granted) {
			lock.LockHolderTxnID = txnID
			lock.HoldTime = duration
		} else {
			lock.Waiters = append(lock.Waiters, ClusterLockWaiter{
				TxnID:    txnID,
				WaitTime: duration,
			})
		}
	}

	locks := make([]ClusterLockInfo, 0, len(lockOrder))
	for _, key := range lockOrder {
		locks = append(locks, *locksByKey[key])
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, ClusterLocksResponse{
		Locks: locks,
	})
}
