// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stmtdiagnostics

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TxnRequest describes a transaction diagnostic request for which a diagnostic
// bundle should be collected.
type TxnRequest struct {
	txnFingerprintId    uint64
	stmtFingerprintIds  []uint64
	redacted            bool
	username            string
	expiresAt           time.Time
	minExecutionLatency time.Duration
	samplingProbability float64
}

func NewTxnRequest(
	txnFingerprintId uint64,
	stmtFingerprintIds []uint64,
	redacted bool,
	username string,
	expiresAt time.Time,
	minExecutionLatency time.Duration,
	samplingProbability float64,
) TxnRequest {
	return TxnRequest{
		txnFingerprintId:    txnFingerprintId,
		stmtFingerprintIds:  stmtFingerprintIds,
		redacted:            redacted,
		username:            username,
		expiresAt:           expiresAt,
		minExecutionLatency: minExecutionLatency,
		samplingProbability: samplingProbability,
	}
}

func (t *TxnRequest) TxnFingerprintId() uint64 {
	return t.txnFingerprintId
}

func (t *TxnRequest) StmtFingerprintIds() []uint64 {
	return t.stmtFingerprintIds
}

func (t *TxnRequest) IsRedacted() bool {
	return t.redacted
}

func (t *TxnRequest) Username() string {
	return t.username
}

func (t *TxnRequest) isExpired(now time.Time) bool {
	return !t.expiresAt.IsZero() && t.expiresAt.Before(now)
}

func (t *TxnRequest) isConditional() bool {
	return t.minExecutionLatency != 0
}

// TxnDiagnostic is a container for all the diagnostic data that has been
// collected and will be persisted for a transaction. This will be downloadable
// as a transaction diagnostic bundle
type TxnDiagnostic struct {
	stmtDiagnostics []StmtDiagnostic
	bundle          []byte
}

func NewTxnDiagnostic(stmtDiagnostics []StmtDiagnostic, bundle []byte) TxnDiagnostic {
	return TxnDiagnostic{stmtDiagnostics: stmtDiagnostics, bundle: bundle}
}

// TxnRegistry maintains a view on the transactions on which a diagnostic
// bundle should be collected. It is responsible for saving new requests
// to the transaction diagnostics requests table, deciding whether a
// diagnostic bundle should be collected for a transaction, and persisting
// recorded diagnostics to the transaction diagnostics table.
type TxnRegistry struct {
	st           *cluster.Settings
	db           isql.DB
	StmtRegistry *Registry
	ts           timeutil.TimeSource
	mu           struct {
		// NOTE: This lock can't be held while the registry runs any statements
		// internally; it'd deadlock.
		syncutil.Mutex
		// requests is a map of all the transaction diagnostic requests that are
		// pending to be collected. Requests will be removed from this map once
		// it has been fulfilled, has expired, or if moved to the
		// unconditionalOngoingRequests map.
		requests map[RequestID]TxnRequest
		// unconditionalOngoingRequests contains requests that are currently being
		// recorded and expected to be recorded unconditionally. This means that
		// these requests should be recorded on their next execution, regardless
		// of the transaction's latency or other conditions.
		unconditionalOngoingRequests map[RequestID]TxnRequest
		rand                         *rand.Rand
	}
}

func NewTxnRegistry(
	db isql.DB, st *cluster.Settings, stmtDiagnosticsRegistry *Registry, ts timeutil.TimeSource,
) *TxnRegistry {
	r := &TxnRegistry{
		db:           db,
		st:           st,
		StmtRegistry: stmtDiagnosticsRegistry,
		ts:           ts,
	}
	r.mu.rand = rand.New(rand.NewSource(ts.Now().UnixNano()))
	r.mu.requests = make(map[RequestID]TxnRequest)
	r.mu.unconditionalOngoingRequests = make(map[RequestID]TxnRequest)
	return r
}

// ShouldStartTxnDiagnostic returns the first txn request whose first
// statement fingerprint id matches the provided stmtFingerprintId. There may
// be multiple transaction diagnostic requests that the stmtFingerprintId
// matches, in which case we will stop at the first one we find.
func (r *TxnRegistry) ShouldStartTxnDiagnostic(
	ctx context.Context, stmtFingerprintId uint64,
) (shouldCollect bool, reqID RequestID, req TxnRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.requests) == 0 {
		return false, 0, req
	}

	for id, f := range r.mu.requests {
		if len(f.stmtFingerprintIds) > 0 && f.stmtFingerprintIds[0] == stmtFingerprintId {
			if f.isExpired(r.ts.Now()) {
				delete(r.mu.requests, id)
				return false, 0, req
			}
			req = f
			reqID = id
			break
		}
	}
	if reqID == 0 {
		return false, 0, TxnRequest{}
	}

	// Unconditional requests are those that will be recorded on the next
	// execution. In this case, we move the request to the unconditional
	// ongoing requests map, so that it is not considered for future
	// transactions until it is reset.
	if !req.isConditional() {
		r.mu.unconditionalOngoingRequests[reqID] = req
		delete(r.mu.requests, reqID)
	}

	if req.samplingProbability == 0 || r.mu.rand.Float64() < req.samplingProbability {
		return true, reqID, req
	}
	return false, 0, TxnRequest{}
}

func (r *TxnRegistry) InsertTxnRequest(
	ctx context.Context,
	txnFingerprintId uint64,
	stmtFingerprintIds []uint64,
	username string,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAfter time.Duration,
	redacted bool,
) error {
	_, err := r.insertTxnRequestInternal(
		ctx, txnFingerprintId, stmtFingerprintIds, username, samplingProbability, minExecutionLatency, expiresAfter, redacted)
	return err
}

func (r *TxnRegistry) insertTxnRequestInternal(
	ctx context.Context,
	txnFingerprintId uint64,
	stmtFingerprintIds []uint64,
	username string,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAfter time.Duration,
	redacted bool,
) (reqID RequestID, er error) {
	if samplingProbability != 0 {
		if samplingProbability < 0 || samplingProbability > 1 {
			return reqID, errors.Newf(
				"expected sampling probability in range [0.0, 1.0], got %f",
				samplingProbability)
		}
		if minExecutionLatency == 0 {
			return reqID, errors.Newf(
				"got non-zero sampling probability %f and empty min exec latency",
				samplingProbability)
		}
	}

	var expiresAt time.Time
	if expiresAfter != 0 {
		expiresAt = r.ts.Now().Add(expiresAfter)
	}

	// Insert the request into system.transaction_diagnostics_requests
	err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		txn.KV().SetDebugName("txn-diag-insert-request")

		now := r.ts.Now()
		insertColumns := "transaction_fingerprint_id, statement_fingerprint_ids, requested_at"
		qargs := make([]interface{}, 3, 7)

		// Convert txnFingerprintId to bytes for storage
		txnFingerprintBytes := sqlstatsutil.EncodeUint64ToBytes(txnFingerprintId)
		qargs[0] = tree.NewDBytes(tree.DBytes(txnFingerprintBytes))

		// Convert statement fingerprint IDs to byte arrays
		stmtFingerprintArray := tree.NewDArray(types.Bytes)
		for _, id := range stmtFingerprintIds {
			idBytes := sqlstatsutil.EncodeUint64ToBytes(id)
			if err := stmtFingerprintArray.Append(tree.NewDBytes(tree.DBytes(idBytes))); err != nil {
				return err
			}
		}
		qargs[1] = stmtFingerprintArray
		qargs[2] = now

		if minExecutionLatency != 0 {
			insertColumns += ", min_execution_latency"
			qargs = append(qargs, minExecutionLatency)
		}
		if !expiresAt.IsZero() {
			insertColumns += ", expires_at"
			qargs = append(qargs, expiresAt)
		}
		if samplingProbability != 0 {
			insertColumns += ", sampling_probability"
			qargs = append(qargs, samplingProbability)
		}
		if redacted {
			insertColumns += ", redacted"
			qargs = append(qargs, redacted)
		}
		if username != "" {
			insertColumns += ", username"
			qargs = append(qargs, username) // username
		}

		valuesClause := "$1, $2, $3"
		for i := range qargs[3:] {
			valuesClause += fmt.Sprintf(", $%d", i+4)
		}

		stmt := "INSERT INTO system.transaction_diagnostics_requests (" +
			insertColumns + ") VALUES (" + valuesClause + ") RETURNING id;"

		row, err := txn.QueryRowEx(
			ctx, "txn-diag-insert-request", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			stmt, qargs...,
		)
		if err != nil {
			return err
		}
		if row == nil {
			return errors.New("failed to insert transaction diagnostics request")
		}
		reqID = RequestID(*row[0].(*tree.DInt))
		return nil
	})
	if err != nil {
		return reqID, err
	}

	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.addTxnRequestInternalLocked(
			ctx, reqID, txnFingerprintId, stmtFingerprintIds, samplingProbability,
			minExecutionLatency, expiresAt, redacted, username,
		)
	}()

	return reqID, nil
}

// ResetTxnRequest moves the TxnRequest of the given requestID from the ongoing
// requests map back to the requests map, which makes it available to be picked
// up to be recorded again.
func (r *TxnRegistry) ResetTxnRequest(requestID RequestID) (req TxnRequest, ok bool) {
	if requestID == 0 {
		return TxnRequest{}, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	req, ok = r.mu.unconditionalOngoingRequests[requestID]
	if !ok {
		return TxnRequest{}, false
	}

	delete(r.mu.unconditionalOngoingRequests, requestID)
	r.mu.requests[requestID] = req

	return req, true
}

// InsertTxnDiagnostic persists the collected transaction diagnostic bundle. It
// will persist all the collected statement diagnostic bundles as well as the
// transaction trace, and update the request as completed.
func (r *TxnRegistry) InsertTxnDiagnostic(
	ctx context.Context, requestId RequestID, request TxnRequest, diagnostic TxnDiagnostic,
) (CollectedInstanceID, error) {
	var txnDiagnosticId CollectedInstanceID
	collectionTime := r.ts.Now()
	txnFingerprintBytes := sqlstatsutil.EncodeUint64ToBytes(request.txnFingerprintId)

	stmtFingerprintArray := tree.NewDArray(types.Bytes)
	for _, id := range request.stmtFingerprintIds {
		idBytes := sqlstatsutil.EncodeUint64ToBytes(id)
		if err := stmtFingerprintArray.Append(tree.NewDBytes(tree.DBytes(idBytes))); err != nil {
			return txnDiagnosticId, err
		}
	}

	var stmtsStrings = make([]string, 0, len(diagnostic.stmtDiagnostics))
	for _, sd := range diagnostic.stmtDiagnostics {
		stmtsStrings = append(stmtsStrings, sd.stmtFingerprint)
	}

	err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		txn.KV().SetDebugName("txn-diag-insert-bundle")

		// Insert the transaction diagnostic bundle
		bundleChunkIds, err := r.StmtRegistry.insertBundleChunks(ctx, diagnostic.bundle, "transaction diagnostics bundle", txn)
		if err != nil {
			fmt.Printf("failed to insert bundle chunks: %v\n", err)
			return err
		}

		// Insert the transaction diagnostics record
		row, err := txn.QueryRowEx(
			ctx, "txn-diag-insert", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"INSERT INTO system.transaction_diagnostics "+
				"(transaction_fingerprint_id, statement_fingerprint_ids, transaction_fingerprint, collected_at, bundle_chunks) "+
				"VALUES ($1, $2, $3, $4, $5) RETURNING id",
			tree.NewDBytes(tree.DBytes(txnFingerprintBytes)), stmtFingerprintArray, strings.Join(stmtsStrings, ";\n"), collectionTime, bundleChunkIds,
		)
		if err != nil {
			fmt.Printf("failed to insert transaction diagnostics: %v\n", err)
			return err
		}
		if row == nil {
			fmt.Printf("failed to insert transaction diagnostics: %v\n", err)
			return errors.New("failed to insert transaction diagnostics")
		}
		txnDiagnosticId = CollectedInstanceID(*row[0].(*tree.DInt))

		// Insert all the statement diagnostics
		stmtDiagnostics := tree.NewDArray(types.Int)
		for _, sd := range diagnostic.stmtDiagnostics {
			id, err := r.StmtRegistry.innerInsertStatementDiagnostics(ctx, sd, txn, txnDiagnosticId)
			if err != nil {
				return err
			}
			if err = stmtDiagnostics.Append(tree.NewDInt(tree.DInt(id))); err != nil {
				return err
			}
		}

		// Mark the request as completed in system.transaction_diagnostics_requests
		if requestId != 0 {
			_, err := txn.ExecEx(ctx, "txn-diag-mark-completed", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"UPDATE system.transaction_diagnostics_requests "+
					"SET completed = true, transaction_diagnostics_id = $1 WHERE id = $2",
				txnDiagnosticId, requestId)
			if err != nil {
				fmt.Printf("failed to mark request as completed: %v\n", err)
				return err
			}
		}
		return nil
	})

	return txnDiagnosticId, err
}

func (r *TxnRegistry) addTxnRequestInternalLocked(
	ctx context.Context,
	id RequestID,
	txnFingerprintId uint64,
	stmtFingerprintsId []uint64,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAt time.Time,
	redacted bool,
	username string,
) {
	if r.findTxnRequestLocked(id) {
		return
	}
	if r.mu.requests == nil {
		r.mu.requests = make(map[RequestID]TxnRequest)
	}
	request := TxnRequest{
		txnFingerprintId:    txnFingerprintId,
		stmtFingerprintIds:  stmtFingerprintsId,
		redacted:            redacted,
		username:            username,
		expiresAt:           expiresAt,
		minExecutionLatency: minExecutionLatency,
		samplingProbability: samplingProbability,
	}
	r.mu.requests[id] = request
}

func (r *TxnRegistry) findTxnRequestLocked(requestID RequestID) bool {
	f, ok := r.mu.requests[requestID]
	if ok {
		if f.isExpired(r.ts.Now()) {
			delete(r.mu.requests, requestID)
		}
		return true
	}
	_, ok = r.mu.unconditionalOngoingRequests[requestID]
	return ok
}
