// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stmtdiagnostics

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var pollingInterval = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"sql.diagnostics.poll_interval",
	"rate at which the stmtdiagnostics registries polls for requests, set to zero to disable",
	10*time.Second,
	settings.WithRetiredName("sql.stmt_diagnostics.poll_interval"),
)

var bundleChunkSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"sql.stmt_diagnostics.bundle_chunk_size",
	"chunk size for statement diagnostic bundles",
	1024*1024,
	settings.ByteSizeWithMinimum(16),
)

// collectUntilExpiration enables continuous collection of statement bundles for
// requests that declare a sampling probability and have an expiration
// timestamp.
//
// This setting should be used with some caution, enabling it would start
// accruing diagnostic bundles that meet a certain latency threshold until the
// request expires. It's worth nothing that there's no automatic GC of bundles
// today (best you can do is `cockroach statement-diag delete --all`). This
// setting also captures multiple bundles for a single diagnostic request which
// does not fit well with our current scheme of one-bundle-per-completed. What
// it does internally is refuse to mark a "continuous" request as completed
// until it has expired, accumulating bundles until that point. The UI
// integration is incomplete -- only the most recently collected bundle is shown
// once the request is marked as completed. The full set can be retrieved using
// `cockroach statement-diag download <bundle-id>`. This setting is primarily
// intended for low-overhead trace capture during tail latency investigations,
// experiments, and escalations under supervision.
//
// TODO(irfansharif): Longer term we should rip this out in favor of keeping a
// bounded set of bundles around per-request/fingerprint. See #82896 for more
// details.
//
// Deprecated: Continuous collection is now the default behavior when a request
// has both sampling_probability and expires_at set. This setting is kept for
// backward compatibility but will be removed in a future version.
var collectUntilExpiration = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stmt_diagnostics.collect_continuously.enabled",
	"deprecated: continuous collection is now the default behavior when "+
		"sampling_probability and expires_at are set; this setting will be "+
		"removed in a future version",
	true)

// maxBundlesPerRequest is the maximum number of diagnostic bundles to collect
// per continuous request. Enforced at bundle insertion time via COUNT.
var maxBundlesPerRequest = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stmt_diagnostics.max_bundles_per_request",
	"maximum number of diagnostic bundles to collect per continuous request",
	10,
	settings.PositiveInt,
)

// Registry maintains a view on the statement fingerprints
// on which data is to be collected (i.e. system.statement_diagnostics_requests)
// and provides utilities for checking a query against this list and satisfying
// the requests.
type Registry struct {
	mu struct {
		// NOTE: This lock can't be held while the registry runs any statements
		// internally; it'd deadlock.
		syncutil.Mutex
		// requests waiting for the right query to come along. The conditional
		// requests are left in this map until they either are satisfied or
		// expire (i.e. they never enter unconditionalOngoing map).
		requestFingerprints map[RequestID]Request
		// ids of unconditional requests that this node is in the process of
		// servicing.
		unconditionalOngoing map[RequestID]Request

		// epoch is observed before reading system.statement_diagnostics_requests, and then
		// checked again before loading the tables contents. If the value changed in
		// between, then the table contents might be stale.
		epoch int

		rand *rand.Rand
	}
	st *cluster.Settings
	db isql.DB
}

// Request describes a statement diagnostics request along with some conditional
// information.
type Request struct {
	fingerprint         string
	planGist            string
	antiPlanGist        bool
	samplingProbability float64
	minExecutionLatency time.Duration
	expiresAt           time.Time
	redacted            bool
	username            string
}

// IsRedacted returns whether this diagnostic request is for a redacted bundle.
func (r *Request) IsRedacted() bool {
	return r.redacted
}

// Username returns the normalized username of the user that initiated this
// request. It can be empty in which case the requester user is unknown.
func (r *Request) Username() string {
	return r.username
}

func (r *Request) isExpired(now time.Time) bool {
	return !r.expiresAt.IsZero() && r.expiresAt.Before(now)
}

func (r *Request) isConditional() bool {
	return r.minExecutionLatency != 0
}

// continueCollecting returns true if this request collects multiple bundles
// rather than completing after the first one.
func (r *Request) continueCollecting(st *cluster.Settings) bool {
	return collectUntilExpiration.Get(&st.SV) &&
		r.samplingProbability != 0 && !r.expiresAt.IsZero() &&
		!r.isExpired(timeutil.Now())
}

// NewRegistry constructs a new Registry.
func NewRegistry(db isql.DB, st *cluster.Settings) *Registry {
	r := &Registry{
		db: db,
		st: st,
	}
	r.mu.rand = rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	return r
}

type StmtDiagnostic struct {
	requestID       RequestID
	req             Request
	stmtFingerprint string
	stmt            string
	bundle          []byte
	collectionErr   error
}

func NewStmtDiagnostic(
	requestID RequestID,
	req Request,
	stmtFingerprint string,
	stmt string,
	bundle []byte,
	collectionErr error,
) StmtDiagnostic {
	return StmtDiagnostic{
		requestID:       requestID,
		req:             req,
		stmtFingerprint: stmtFingerprint,
		stmt:            stmt,
		bundle:          bundle,
		collectionErr:   collectionErr,
	}
}

// RequestID is the ID of a diagnostics request, corresponding to the id
// column in statement_diagnostics_requests.
// A zero ID is invalid.
type RequestID int

// CollectedInstanceID is the ID of an instance of collected diagnostics,
// corresponding to the id column in statement_diagnostics.
type CollectedInstanceID int

// addRequestInternalLocked adds a request to r.mu.requestFingerprints. If the
// request is already present or it has already expired, the call is a noop.
func (r *Registry) addRequestInternalLocked(
	ctx context.Context,
	id RequestID,
	queryFingerprint string,
	planGist string,
	antiPlanGist bool,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAt time.Time,
	redacted bool,
	username string,
) {
	if r.findRequestLocked(id) {
		// Request already exists.
		return
	}
	if r.mu.requestFingerprints == nil {
		r.mu.requestFingerprints = make(map[RequestID]Request)
	}
	r.mu.requestFingerprints[id] = Request{
		fingerprint:         queryFingerprint,
		planGist:            planGist,
		antiPlanGist:        antiPlanGist,
		samplingProbability: samplingProbability,
		minExecutionLatency: minExecutionLatency,
		expiresAt:           expiresAt,
		redacted:            redacted,
		username:            username,
	}
}

// findRequestLocked returns whether the request already exists. If the request
// is not ongoing and has already expired, it is removed from the registry (yet
// true is still returned).
func (r *Registry) findRequestLocked(requestID RequestID) bool {
	f, ok := r.mu.requestFingerprints[requestID]
	if ok {
		if f.isExpired(timeutil.Now()) {
			// This request has already expired.
			delete(r.mu.requestFingerprints, requestID)
		}
		return true
	}
	_, ok = r.mu.unconditionalOngoing[requestID]
	return ok
}

// cancelRequest removes the request with the given RequestID from the Registry
// if present.
func (r *Registry) cancelRequest(requestID RequestID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.mu.requestFingerprints, requestID)
	delete(r.mu.unconditionalOngoing, requestID)
}

// InsertRequest is part of the server.StmtDiagnosticsRequester interface.
func (r *Registry) InsertRequest(
	ctx context.Context,
	stmtFingerprint string,
	planGist string,
	antiPlanGist bool,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAfter time.Duration,
	redacted bool,
	username string,
) error {
	_, err := r.insertRequestInternal(
		ctx, stmtFingerprint, planGist, antiPlanGist, samplingProbability,
		minExecutionLatency, expiresAfter, redacted, username,
	)
	return err
}

func (r *Registry) insertRequestInternal(
	ctx context.Context,
	stmtFingerprint string,
	planGist string,
	antiPlanGist bool,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAfter time.Duration,
	redacted bool,
	username string,
) (RequestID, error) {
	if samplingProbability != 0 {
		if samplingProbability < 0 || samplingProbability > 1 {
			return 0, errors.Newf(
				"expected sampling probability in range [0.0, 1.0], got %f",
				samplingProbability)
		}
		if minExecutionLatency == 0 {
			return 0, errors.Newf(
				"got non-zero sampling probability %f and empty min exec latency",
				samplingProbability)
		}
	}

	var reqID RequestID
	var expiresAt time.Time
	err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		txn.KV().SetDebugName("stmt-diag-insert-request")
		// Check if there's already a pending request for this fingerprint.
		row, err := txn.QueryRowEx(ctx, "stmt-diag-check-pending", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`SELECT count(1) FROM system.statement_diagnostics_requests
				WHERE
					completed = false AND
					statement_fingerprint = $1 AND
					(expires_at IS NULL OR expires_at > now())`,
			stmtFingerprint)
		if err != nil {
			return err
		}
		if row == nil {
			return errors.New("failed to check pending statement diagnostics")
		}
		count := int(*row[0].(*tree.DInt))
		if count != 0 {
			return errors.New(
				"A pending request for the requested fingerprint already exists. " +
					"Cancel the existing request first and try again.",
			)
		}

		now := timeutil.Now()
		insertColumns := "statement_fingerprint, requested_at"
		qargs := make([]interface{}, 2, 10)
		qargs[0] = stmtFingerprint // statement_fingerprint
		qargs[1] = now             // requested_at
		if planGist != "" {
			insertColumns += ", plan_gist, anti_plan_gist"
			qargs = append(qargs, planGist)     // plan_gist
			qargs = append(qargs, antiPlanGist) // anti_plan_gist
		}
		if samplingProbability != 0 {
			insertColumns += ", sampling_probability"
			qargs = append(qargs, samplingProbability) // sampling_probability
		}
		if minExecutionLatency != 0 {
			insertColumns += ", min_execution_latency"
			qargs = append(qargs, minExecutionLatency) // min_execution_latency
		}
		if expiresAfter != 0 {
			insertColumns += ", expires_at"
			expiresAt = now.Add(expiresAfter)
			qargs = append(qargs, expiresAt) // expires_at
		}
		if redacted {
			insertColumns += ", redacted"
			qargs = append(qargs, redacted) // redacted
		}
		if username != "" {
			insertColumns += ", username"
			qargs = append(qargs, username) // username
		}
		valuesClause := "$1, $2"
		for i := range qargs[2:] {
			valuesClause += fmt.Sprintf(", $%d", i+3)
		}
		stmt := "INSERT INTO system.statement_diagnostics_requests (" +
			insertColumns + ") VALUES (" + valuesClause + ") RETURNING id;"
		row, err = txn.QueryRowEx(
			ctx, "stmt-diag-insert-request", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			stmt, qargs...,
		)
		if err != nil {
			return err
		}
		if row == nil {
			return errors.New("failed to insert statement diagnostics request")
		}
		reqID = RequestID(*row[0].(*tree.DInt))
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Manually insert the request in the (local) registry. This lets this node
	// pick up the request quickly if the right query comes around, without
	// waiting for the poller.
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.mu.epoch++
		r.addRequestInternalLocked(
			ctx, reqID, stmtFingerprint, planGist, antiPlanGist, samplingProbability,
			minExecutionLatency, expiresAt, redacted, username,
		)
	}()

	return reqID, nil
}

// CancelRequest is part of the server.StmtDiagnosticsRequester interface.
func (r *Registry) CancelRequest(ctx context.Context, requestID int64) error {
	row, err := r.db.Executor().QueryRowEx(ctx, "stmt-diag-cancel-request", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		// Rather than deleting the row from the table, we choose to mark the
		// request as "expired" by setting `expires_at` into the past. This will
		// allow any queries that are currently being traced for this request to
		// write their collected bundles.
		"UPDATE system.statement_diagnostics_requests SET expires_at = '1970-01-01' "+
			"WHERE completed = false AND id = $1 "+
			"AND (expires_at IS NULL OR expires_at > now()) RETURNING id;",
		requestID,
	)
	if err != nil {
		return err
	}

	if row == nil {
		// There is no pending diagnostics request with the given fingerprint.
		return errors.Newf("no pending request found for the fingerprint: %s", requestID)
	}

	reqID := RequestID(requestID)
	r.cancelRequest(reqID)

	return nil
}

// IsConditionSatisfied returns whether the completed request satisfies its
// condition.
func (r *Registry) IsConditionSatisfied(req Request, execLatency time.Duration) bool {
	return req.minExecutionLatency <= execLatency
}

// MaybeRemoveRequest checks whether the request needs to be removed from the
// local Registry and removes it if so. Note that the registries on other nodes
// will learn about it via polling of the system table.
func (r *Registry) MaybeRemoveRequest(requestID RequestID, req Request, execLatency time.Duration) {
	// We should remove the request from the registry if its condition is
	// satisfied unless we want to continue collecting bundles for this request.
	shouldRemove := r.IsConditionSatisfied(req, execLatency) && !req.continueCollecting(r.st)
	// Always remove the expired requests.
	if shouldRemove || req.isExpired(timeutil.Now()) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if req.isConditional() {
			delete(r.mu.requestFingerprints, requestID)
		} else {
			delete(r.mu.unconditionalOngoing, requestID)
		}
	}
}

// ShouldCollectDiagnostics checks whether any data should be collected for the
// given query, which is the case if the registry has a request for this
// statement's fingerprint (and assuming probability conditions hold); in this
// case ShouldCollectDiagnostics will return true again on this node for the
// same diagnostics request only for conditional requests.
//
// If shouldCollect is true, MaybeRemoveRequest needs to be called.
func (r *Registry) ShouldCollectDiagnostics(
	ctx context.Context, fingerprint string, planGist string,
) (shouldCollect bool, reqID RequestID, req Request) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Return quickly if we have no requests to trace.
	if len(r.mu.requestFingerprints) == 0 {
		return false, 0, req
	}

	for id, f := range r.mu.requestFingerprints {
		if f.fingerprint == fingerprint {
			if f.isExpired(timeutil.Now()) {
				delete(r.mu.requestFingerprints, id)
				return false, 0, req
			}
			// We found non-expired request that matches the fingerprint.
			if f.planGist == "" {
				// The request didn't specify the plan gist, so this execution
				// will do.
				reqID = id
				req = f
				break
			}
			if (f.planGist == planGist && !f.antiPlanGist) ||
				(planGist != "" && f.planGist != planGist && f.antiPlanGist) {
				// The execution's plan gist matches the one from the request,
				// or the execution's plan gist doesn't match the one from the
				// request and "anti-match" is requested.
				reqID = id
				req = f
				break
			}
		}
	}

	if reqID == 0 {
		return false, 0, Request{}
	}

	if !req.isConditional() {
		if r.mu.unconditionalOngoing == nil {
			r.mu.unconditionalOngoing = make(map[RequestID]Request)
		}
		r.mu.unconditionalOngoing[reqID] = req
		delete(r.mu.requestFingerprints, reqID)
	}

	if req.samplingProbability == 0 || r.mu.rand.Float64() < req.samplingProbability {
		return true, reqID, req
	}
	return false, 0, Request{}
}

// InsertStatementDiagnostics inserts a trace into system.statement_diagnostics.
//
// If requestID is not zero, it also marks the request as completed in
// system.statement_diagnostics_requests. If requestID is zero, a new entry is
// inserted.
//
// collectionErr should be any error generated during the collection or
// generation of the bundle/trace.
func (r *Registry) InsertStatementDiagnostics(
	ctx context.Context,
	requestID RequestID,
	req Request,
	stmtFingerprint string,
	stmt string,
	bundle []byte,
	collectionErr error,
) (CollectedInstanceID, error) {
	var diagID CollectedInstanceID
	err := r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		txn.KV().SetDebugName("stmt-diag-insert-bundle")
		id, err := r.innerInsertStatementDiagnostics(ctx, NewStmtDiagnostic(requestID, req, stmtFingerprint, stmt, bundle, collectionErr), txn, CollectedInstanceID(0))
		if err != nil {
			return err
		}
		diagID = id
		return nil
	})

	return diagID, err
}

func (r *Registry) insertBundleChunks(
	ctx context.Context, bundle []byte, description string, txn isql.Txn,
) (*tree.DArray, error) {
	bundleChunksVal := tree.NewDArray(types.Int)
	bundleToUpload := bundle
	for len(bundleToUpload) > 0 {
		chunkSize := int(bundleChunkSize.Get(&r.st.SV))
		chunk := bundleToUpload
		if len(chunk) > chunkSize {
			chunk = chunk[:chunkSize]
		}
		bundleToUpload = bundleToUpload[len(chunk):]

		// Insert the chunk into system.statement_bundle_chunks.
		row, err := txn.QueryRowEx(
			ctx, "stmt-bundle-chunks-insert", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"INSERT INTO system.statement_bundle_chunks(description, data) VALUES ($1, $2) RETURNING id",
			description,
			tree.NewDBytes(tree.DBytes(chunk)),
		)
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, errors.New("failed to check statement bundle chunk")
		}
		chunkID := row[0].(*tree.DInt)
		if err := bundleChunksVal.Append(chunkID); err != nil {
			return nil, err
		}
	}
	return bundleChunksVal, nil
}

func (r *Registry) innerInsertStatementDiagnostics(
	ctx context.Context, diagnostic StmtDiagnostic, txn isql.Txn, txnDiagnosticId CollectedInstanceID,
) (CollectedInstanceID, error) {
	var diagID CollectedInstanceID
	requestIDColumnAvailable := r.st.Version.IsActive(ctx, clusterversion.V26_2_StmtDiagnosticsRequestID)
	isContinuous := diagnostic.req.continueCollecting(r.st)

	if diagnostic.requestID != 0 {
		// Check if the request is still pending (not yet completed).
		row, err := txn.QueryRowEx(ctx, "stmt-diag-check-completed", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"SELECT count(1) FROM system.statement_diagnostics_requests "+
				"WHERE id = $1 AND completed = false",
			diagnostic.requestID)
		if err != nil {
			return diagID, err
		}
		if row == nil {
			return diagID, errors.New("failed to check completed statement diagnostics")
		}
		cnt := int(*row[0].(*tree.DInt))
		if cnt == 0 {
			return diagID, nil
		}
	}

	// Generate the values that will be inserted.
	errorVal := tree.DNull
	if diagnostic.collectionErr != nil {
		errorVal = tree.NewDString(diagnostic.collectionErr.Error())
	}

	bundleChunksVal, err := r.insertBundleChunks(ctx, diagnostic.bundle, "statement diagnostics bundle", txn)
	if err != nil {
		return diagID, err
	}

	collectionTime := timeutil.Now()

	insertCols := "statement_fingerprint, statement, collected_at, bundle_chunks, error"
	insertVals := "$1, $2, $3, $4, $5"
	vals := []interface{}{diagnostic.stmtFingerprint, diagnostic.stmt, collectionTime, bundleChunksVal, errorVal}
	paramIdx := 6
	if txnDiagnosticId != 0 {
		insertCols += ", transaction_diagnostics_id"
		insertVals += fmt.Sprintf(", $%d", paramIdx)
		vals = append(vals, txnDiagnosticId)
		paramIdx++
	}
	// Include request_id to link this bundle to the originating request.
	if diagnostic.requestID != 0 && requestIDColumnAvailable {
		insertCols += ", request_id"
		insertVals += fmt.Sprintf(", $%d", paramIdx)
		vals = append(vals, diagnostic.requestID)
	}
	// Insert the collection metadata into system.statement_diagnostics.
	row, err := txn.QueryRowEx(
		ctx, "stmt-diag-insert", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"INSERT INTO system.statement_diagnostics "+
			"("+insertCols+") "+
			"VALUES ("+insertVals+") RETURNING id",
		vals...,
	)
	if err != nil {
		return diagID, err
	}
	if row == nil {
		return diagID, errors.New("failed to insert statement diagnostics")
	}
	diagID = CollectedInstanceID(*row[0].(*tree.DInt))

	if diagnostic.requestID != 0 {
		if isContinuous && requestIDColumnAvailable {
			// For continuous requests, count existing bundles for this
			// request and compare against the cluster setting to decide
			// whether to mark the request as completed.
			row, err := txn.QueryRowEx(ctx, "stmt-diag-count-bundles", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				`SELECT count(*) FROM system.statement_diagnostics
				 WHERE request_id = $1`,
				diagnostic.requestID)
			if err != nil {
				return diagID, err
			}
			shouldComplete := false
			if row != nil {
				bundleCount := int(*row[0].(*tree.DInt))
				maxBundles := int(maxBundlesPerRequest.Get(&r.st.SV))
				shouldComplete = bundleCount >= maxBundles
			}
			_, err = txn.ExecEx(ctx, "stmt-diag-mark-completed", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"UPDATE system.statement_diagnostics_requests "+
					"SET statement_diagnostics_id = $1, completed = $2 WHERE id = $3",
				diagID, shouldComplete, diagnostic.requestID)
			if err != nil {
				return diagID, err
			}
		} else {
			// Non-continuous or pre-migration: mark completed immediately.
			// During mixed-version rolling upgrades, continuous requests
			// collect without limit enforcement until the migration
			// completes and request_id becomes available for COUNT-based
			// limiting. Pre-migration bundles have NULL request_id, so
			// the post-migration counter effectively resets to 0.
			shouldMarkCompleted := !isContinuous
			_, err := txn.ExecEx(ctx, "stmt-diag-mark-completed", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"UPDATE system.statement_diagnostics_requests "+
					"SET completed = $1, statement_diagnostics_id = $2 WHERE id = $3",
				shouldMarkCompleted, diagID, diagnostic.requestID)
			if err != nil {
				return diagID, err
			}
		}
	} else if txnDiagnosticId == 0 {
		// Insert a completed request into system.statement_diagnostics_request.
		// This is necessary because the UI uses this table to discover completed
		// diagnostics.
		//
		// This bundle was collected via explicit EXPLAIN ANALYZE (DEBUG).
		_, err := txn.ExecEx(ctx, "stmt-diag-add-completed", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"INSERT INTO system.statement_diagnostics_requests"+
				" (completed, statement_fingerprint, statement_diagnostics_id, requested_at)"+
				" VALUES (true, $1, $2, $3)",
			diagnostic.stmtFingerprint, diagID, collectionTime)
		if err != nil {
			return diagID, err
		}
	}
	return diagID, nil
}

// pollStmtRequests reads the pending rows from system.statement_diagnostics_requests and
// updates r.mu.requests accordingly.
func (r *Registry) pollStmtRequests(ctx context.Context) error {
	var rows []tree.Datums

	// Loop until we run the query without straddling an epoch increment.
	for {
		r.mu.Lock()
		epoch := r.mu.epoch
		r.mu.Unlock()

		it, err := r.db.Executor().QueryIteratorEx(ctx, "stmt-diag-poll", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			`SELECT id, statement_fingerprint, min_execution_latency, expires_at,
				sampling_probability, plan_gist, anti_plan_gist, redacted, username
				FROM system.statement_diagnostics_requests
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

		r.mu.Lock()
		// If the epoch changed it means that a request was added to the registry
		// manually while the query was running. In that case, if we were to process
		// the query results normally, we might remove that manually-added request.
		if r.mu.epoch != epoch {
			r.mu.Unlock()
			continue
		}
		break
	}
	defer r.mu.Unlock()

	now := timeutil.Now()
	var ids intsets.Fast
	for _, row := range rows {
		id := RequestID(*row[0].(*tree.DInt))
		stmtFingerprint := string(*row[1].(*tree.DString))
		var minExecutionLatency time.Duration
		var expiresAt time.Time
		var samplingProbability float64
		var planGist, username string
		var antiPlanGist, redacted bool

		if minExecLatency, ok := row[2].(*tree.DInterval); ok {
			minExecutionLatency = time.Duration(minExecLatency.Nanos())
		}
		if e, ok := row[3].(*tree.DTimestampTZ); ok {
			expiresAt = e.Time
		}
		if prob, ok := row[4].(*tree.DFloat); ok {
			samplingProbability = float64(*prob)
			if samplingProbability < 0 || samplingProbability > 1 {
				log.Dev.Warningf(ctx, "malformed sampling probability for request %d: %f (expected in range [0, 1]), resetting to 1.0",
					id, samplingProbability)
				samplingProbability = 1.0
			}
		}
		if gist, ok := row[5].(*tree.DString); ok {
			planGist = string(*gist)
		}
		if antiGist, ok := row[6].(*tree.DBool); ok {
			antiPlanGist = bool(*antiGist)
		}
		if b, ok := row[7].(*tree.DBool); ok {
			redacted = bool(*b)
		}
		if u, ok := row[8].(*tree.DString); ok {
			username = string(*u)
		}

		ids.Add(int(id))
		r.addRequestInternalLocked(
			ctx, id, stmtFingerprint, planGist, antiPlanGist,
			samplingProbability, minExecutionLatency, expiresAt,
			redacted, username,
		)
	}

	// Remove all other requests.
	for id, req := range r.mu.requestFingerprints {
		if !ids.Contains(int(id)) || req.isExpired(now) {
			delete(r.mu.requestFingerprints, id)
		}
	}
	return nil
}

// StartPolling starts a background task that polls for new statement and
// transaction requests and updates the corresponding registries.
func StartPolling(ctx context.Context, tr *TxnRegistry, sr *Registry, stopper *stop.Stopper) {
	// The registry has the same lifetime as the server, so the cancellation
	// function can be ignored and it'll be called by the stopper.
	ctx, _ = stopper.WithCancelOnQuiesce(ctx) // nolint:quiesce

	// Since background diagnostics collection is not under user
	// control, exclude it from cost accounting and control.
	ctx = multitenant.WithTenantCostControlExemption(ctx)

	// NB: The only error that should occur here would be if the server were
	// shutting down so let's swallow it.
	_ = stopper.RunAsyncTask(ctx, "stmt-txn-diag-poll", func(ctx context.Context) {
		var (
			timer               timeutil.Timer
			lastPoll            time.Time
			deadline            time.Time
			pollIntervalChanged = make(chan struct{}, 1)
			maybeResetTimer     = func() {
				if interval := pollingInterval.Get(&sr.st.SV); interval == 0 {
					// Setting the interval to zero stops the polling.
					timer.Stop()
				} else {
					newDeadline := lastPoll.Add(interval)
					if deadline.IsZero() || !deadline.Equal(newDeadline) {
						deadline = newDeadline
						timer.Reset(timeutil.Until(deadline))
					}
				}
			}
			poll = func() {
				if err := tr.pollTxnRequests(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Ops.Warningf(ctx, "error polling for transaction diagnostics requests: %s", err)
				}
				if err := sr.pollStmtRequests(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Ops.Warningf(ctx, "error polling for statement diagnostics requests: %s", err)
				}
				lastPoll = timeutil.Now()
			}
		)

		pollingInterval.SetOnChange(&sr.st.SV, func(ctx context.Context) {
			select {
			case pollIntervalChanged <- struct{}{}:
			default:
			}
		})
		for {
			maybeResetTimer()
			select {
			case <-pollIntervalChanged:
				continue // go back around and maybe reset the timer
			case <-timer.C:
			case <-ctx.Done():
				return
			}
			poll()
		}
	})
}
