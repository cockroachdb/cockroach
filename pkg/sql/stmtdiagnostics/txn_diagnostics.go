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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
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

func (t *TxnRequest) IsConditional() bool {
	return t.minExecutionLatency != 0
}

func (t *TxnRequest) MinExecutionLatency() time.Duration {
	return t.minExecutionLatency
}

// TxnRequestMatch pairs a request ID with its TxnRequest. Returned by
// ShouldStartTxnDiagnostic to allow callers to track multiple matching
// requests simultaneously.
type TxnRequestMatch struct {
	ID      RequestID
	Request TxnRequest
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
		// it has been fulfilled or has expired.
		requests map[RequestID]TxnRequest

		// epoch is observed before reading system.transaction_diagnostics_requests, and then
		// checked again before loading the tables contents. If the value changed in
		// between, then the table contents might be stale.
		epoch int

		rand *rand.Rand
	}
}

func (r *TxnRegistry) InsertRequest(
	ctx context.Context,
	txnFingerprintID appstatspb.TransactionFingerprintID,
	stmtFingerprintIDs []appstatspb.StmtFingerprintID,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAfter time.Duration,
	redacted bool,
	username string,
) (int64, error) {
	stmtFingerprintUintIDs := make([]uint64, len(stmtFingerprintIDs))
	for i, sf := range stmtFingerprintIDs {
		stmtFingerprintUintIDs[i] = uint64(sf)
	}
	id, err := r.insertTxnRequestInternal(ctx, uint64(txnFingerprintID), stmtFingerprintUintIDs, username, samplingProbability, minExecutionLatency, expiresAfter, redacted)
	return int64(id), err
}

func (r *TxnRegistry) CancelRequest(ctx context.Context, requestID int64) error {
	row, err := r.db.Executor().QueryRowEx(ctx, "txn-diag-cancel-request", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		// Rather than deleting the row from the table, we choose to mark the
		// request as "expired" by setting `expires_at` into the past. This will
		// allow any queries that are currently being traced for this request to
		// write their collected bundles.
		"UPDATE system.transaction_diagnostics_requests SET expires_at = '1970-01-01' "+
			"WHERE completed = false AND id = $1 "+
			"AND (expires_at IS NULL OR expires_at > now()) RETURNING id;",
		requestID,
	)
	if err != nil {
		return err
	}

	if row == nil {
		// There is no pending diagnostics request with the given request.
		return errors.Newf("no pending request found for the request: %s", requestID)
	}

	reqID := RequestID(requestID)
	r.RemoveFromRegistry(reqID)
	return nil
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
	return r
}

// ShouldStartTxnDiagnostic returns all txn requests whose first statement
// fingerprint id matches the provided stmtFingerprintId. Multiple transactions
// may share the same first statement fingerprint, so returning all matches
// allows callers to progressively narrow candidates as subsequent statements
// are observed.
func (r *TxnRegistry) ShouldStartTxnDiagnostic(
	ctx context.Context, stmtFingerprintId uint64,
) []TxnRequestMatch {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.mu.requests) == 0 {
		return nil
	}

	now := r.ts.Now()
	roll := r.mu.rand.Float64()
	var matches []TxnRequestMatch
	for id, f := range r.mu.requests {
		if len(f.stmtFingerprintIds) > 0 && f.stmtFingerprintIds[0] == stmtFingerprintId {
			if f.isExpired(now) {
				delete(r.mu.requests, id)
				continue
			}
			if f.samplingProbability != 0 && roll >= f.samplingProbability {
				continue
			}
			matches = append(matches, TxnRequestMatch{ID: id, Request: f})
		}
	}
	return matches
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
) (int, error) {
	reqId, err := r.insertTxnRequestInternal(
		ctx, txnFingerprintId, stmtFingerprintIds, username, samplingProbability, minExecutionLatency, expiresAfter, redacted)
	return int(reqId), err
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
		r.mu.epoch++
		r.addTxnRequestInternalLocked(
			ctx, reqID, txnFingerprintId, stmtFingerprintIds, samplingProbability,
			minExecutionLatency, expiresAt, redacted, username,
		)
	}()

	return reqID, nil
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
	joinedStmts := strings.Join(stmtsStrings, ";\n")

	var alreadyCompleted bool
	err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		txn.KV().SetDebugName("txn-diag-insert-bundle")
		if requestId != 0 {
			row, err := txn.QueryRowEx(ctx, "txn-diag-check-completed", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"SELECT count(1) FROM system.transaction_diagnostics_requests WHERE id = $1 AND completed = false",
				requestId)
			if err != nil {
				return err
			}
			if row == nil {
				return errors.New("failed to check completed transaction diagnostics")
			}
			cnt := int(*row[0].(*tree.DInt))
			if cnt == 0 {
				// Someone else already marked the request as completed. We've traced for nothing.
				// This can only happen once per node, per request since we're going to
				// remove the request from the registry.
				alreadyCompleted = true
				return nil
			}
		}

		// Insert the transaction diagnostic bundle
		bundleChunkIds, err := r.StmtRegistry.insertBundleChunks(ctx, diagnostic.bundle, "transaction diagnostics bundle", txn)
		if err != nil {
			return err
		}

		// Insert the transaction diagnostics record
		row, err := txn.QueryRowEx(
			ctx, "txn-diag-insert", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"INSERT INTO system.transaction_diagnostics "+
				"(transaction_fingerprint_id, statement_fingerprint_ids, transaction_fingerprint, collected_at, bundle_chunks) "+
				"VALUES ($1, $2, $3, $4, $5) RETURNING id",
			tree.NewDBytes(tree.DBytes(txnFingerprintBytes)), stmtFingerprintArray, joinedStmts, collectionTime, bundleChunkIds,
		)
		if err != nil {
			return err
		}
		if row == nil {
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
				return err
			}
			r.RemoveFromRegistry(requestId)
		}
		return nil
	})

	if alreadyCompleted {
		err = errors.New("transaction diagnostics request was already completed in another execution")
	}

	return txnDiagnosticId, err
}

func (r *TxnRegistry) RemoveFromRegistry(requestID RequestID) {
	if requestID == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.mu.requests, requestID)
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
	return false
}

// pollTxnRequests reads the pending rows from system.transaction_diagnostics_requests and
// updates r.mu.requests accordingly.
func (r *TxnRegistry) pollTxnRequests(ctx context.Context) error {
	if !r.st.Version.IsActive(ctx, clusterversion.V25_4) {
		return nil
	}

	var rows []tree.Datums

	// Loop until we run the query without straddling an epoch increment.
	for {
		r.mu.Lock()
		epoch := r.mu.epoch
		r.mu.Unlock()

		it, err := r.db.Executor().QueryIteratorEx(ctx, "txn-diag-poll", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			`SELECT id, transaction_fingerprint_id, statement_fingerprint_ids, min_execution_latency, expires_at, sampling_probability, redacted, username
				FROM system.transaction_diagnostics_requests
				WHERE completed = false AND (expires_at IS NULL OR expires_at > now())`,
		)
		if err != nil {
			return err
		}
		rows = rows[:0]
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			rows = append(rows, it.Cur())
		}
		if err != nil {
			return err
		}

		// If the epoch changed it means that a request was added to the registry
		// manually while the query was running. In that case, if we were to process
		// the query results normally, we might remove that manually-added request.
		if r.mu.Lock(); r.mu.epoch != epoch {
			r.mu.Unlock()
			continue
		}
		break
	}
	defer r.mu.Unlock()

	now := r.ts.Now()
	var ids intsets.Fast
	for _, row := range rows {
		id := RequestID(*row[0].(*tree.DInt))
		var txnFingerprintId uint64
		txnFingerprintId, err := sqlstatsutil.DatumToUint64(row[1])
		if err != nil {
			return err
		}

		var stmtFingerprintIds []uint64
		stmtFingerprintArray := row[2].(*tree.DArray)
		for _, elem := range stmtFingerprintArray.Array {
			stmtFpId, err := sqlstatsutil.DatumToUint64(elem)
			if err != nil {
				return err
			}
			stmtFingerprintIds = append(stmtFingerprintIds, stmtFpId)
		}

		var minExecutionLatency time.Duration
		var expiresAt time.Time
		var samplingProbability float64
		var redacted bool
		var username string

		if minExecLatency, ok := row[3].(*tree.DInterval); ok {
			minExecutionLatency = time.Duration(minExecLatency.Nanos())
		}
		if e, ok := row[4].(*tree.DTimestampTZ); ok {
			expiresAt = e.Time
		}
		if prob, ok := row[5].(*tree.DFloat); ok {
			samplingProbability = float64(*prob)
		}
		if b, ok := row[6].(*tree.DBool); ok {
			redacted = bool(*b)
		}

		if u, ok := row[7].(*tree.DString); ok {
			username = string(*u)
		}

		ids.Add(int(id))
		r.addTxnRequestInternalLocked(ctx, id, txnFingerprintId, stmtFingerprintIds, samplingProbability, minExecutionLatency, expiresAt, redacted, username)
	}

	// Remove all other requests.
	for id, req := range r.mu.requests {
		if !ids.Contains(int(id)) || req.isExpired(now) {
			delete(r.mu.requests, id)
		}
	}
	return nil
}
