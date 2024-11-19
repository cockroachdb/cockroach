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
	"sql.stmt_diagnostics.poll_interval",
	"rate at which the stmtdiagnostics.Registry polls for requests, set to zero to disable",
	10*time.Second,
	settings.NonNegativeDuration,
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
var collectUntilExpiration = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stmt_diagnostics.collect_continuously.enabled",
	"collect diagnostic bundles continuously until request expiration (to be "+
		"used with care, only has an effect if the diagnostic request has an "+
		"expiration and a sampling probability set)",
	false)

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
}

// IsRedacted returns whether this diagnostic request is for a redacted bundle.
func (r *Request) IsRedacted() bool {
	return r.redacted
}

func (r *Request) isExpired(now time.Time) bool {
	return !r.expiresAt.IsZero() && r.expiresAt.Before(now)
}

func (r *Request) isConditional() bool {
	return r.minExecutionLatency != 0
}

// continueCollecting returns true if we want to continue collecting bundles for
// this request.
func (r *Request) continueCollecting(st *cluster.Settings) bool {
	return collectUntilExpiration.Get(&st.SV) && // continuous collection must be enabled
		r.samplingProbability != 0 && !r.expiresAt.IsZero() && // conditions for continuous collection must be set
		!r.isExpired(timeutil.Now()) // the request must not have expired yet
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

// Start will start the polling loop for the Registry.
func (r *Registry) Start(ctx context.Context, stopper *stop.Stopper) {
	ctx, _ = stopper.WithCancelOnQuiesce(ctx)

	// Since background statement diagnostics collection is not under user
	// control, exclude it from cost accounting and control.
	ctx = multitenant.WithTenantCostControlExemption(ctx)

	// NB: The only error that should occur here would be if the server were
	// shutting down so let's swallow it.
	_ = stopper.RunAsyncTask(ctx, "stmt-diag-poll", r.poll)
}

func (r *Registry) poll(ctx context.Context) {
	var (
		timer               timeutil.Timer
		lastPoll            time.Time
		deadline            time.Time
		pollIntervalChanged = make(chan struct{}, 1)
		maybeResetTimer     = func() {
			if interval := pollingInterval.Get(&r.st.SV); interval == 0 {
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
			if err := r.pollRequests(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Warningf(ctx, "error polling for statement diagnostics requests: %s", err)
			}
			lastPoll = timeutil.Now()
		}
	)
	pollingInterval.SetOnChange(&r.st.SV, func(ctx context.Context) {
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
			timer.Read = true
		case <-ctx.Done():
			return
		}
		poll()
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
) error {
	_, err := r.insertRequestInternal(
		ctx, stmtFingerprint, planGist, antiPlanGist, samplingProbability,
		minExecutionLatency, expiresAfter, redacted,
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
		qargs := make([]interface{}, 2, 8)
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
		r.addRequestInternalLocked(ctx, reqID, stmtFingerprint, planGist, antiPlanGist, samplingProbability, minExecutionLatency, expiresAt, redacted)
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
		if requestID != 0 {
			row, err := txn.QueryRowEx(ctx, "stmt-diag-check-completed", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"SELECT count(1) FROM system.statement_diagnostics_requests WHERE id = $1 AND completed = false",
				requestID)
			if err != nil {
				return err
			}
			if row == nil {
				return errors.New("failed to check completed statement diagnostics")
			}
			cnt := int(*row[0].(*tree.DInt))
			if cnt == 0 {
				// Someone else already marked the request as completed. We've traced for nothing.
				// This can only happen once per node, per request since we're going to
				// remove the request from the registry.
				return nil
			}
		}

		// Generate the values that will be inserted.
		errorVal := tree.DNull
		if collectionErr != nil {
			errorVal = tree.NewDString(collectionErr.Error())
		}

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
				"statement diagnostics bundle",
				tree.NewDBytes(tree.DBytes(chunk)),
			)
			if err != nil {
				return err
			}
			if row == nil {
				return errors.New("failed to check statement bundle chunk")
			}
			chunkID := row[0].(*tree.DInt)
			if err := bundleChunksVal.Append(chunkID); err != nil {
				return err
			}
		}

		collectionTime := timeutil.Now()

		// Insert the collection metadata into system.statement_diagnostics.
		row, err := txn.QueryRowEx(
			ctx, "stmt-diag-insert", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			"INSERT INTO system.statement_diagnostics "+
				"(statement_fingerprint, statement, collected_at, bundle_chunks, error) "+
				"VALUES ($1, $2, $3, $4, $5) RETURNING id",
			stmtFingerprint, stmt, collectionTime, bundleChunksVal, errorVal,
		)
		if err != nil {
			return err
		}
		if row == nil {
			return errors.New("failed to insert statement diagnostics")
		}
		diagID = CollectedInstanceID(*row[0].(*tree.DInt))

		if requestID != 0 {
			// Link the request from system.statement_diagnostics_request to the
			// diagnostic ID we just collected, marking it as completed if we're
			// able.
			shouldMarkCompleted := true
			if collectUntilExpiration.Get(&r.st.SV) {
				// Two other conditions need to hold true for us to continue
				// capturing future traces, i.e. not mark this request as
				// completed.
				// - Requests need to be of the sampling sort (also implies
				//   there's a latency threshold) -- a crude measure to prevent
				//   against unbounded collection;
				// - Requests need to have an expiration set -- same reason as
				// above.
				if req.samplingProbability > 0 && !req.expiresAt.IsZero() {
					shouldMarkCompleted = false
				}
			}
			_, err := txn.ExecEx(ctx, "stmt-diag-mark-completed", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"UPDATE system.statement_diagnostics_requests "+
					"SET completed = $1, statement_diagnostics_id = $2 WHERE id = $3",
				shouldMarkCompleted, diagID, requestID)
			if err != nil {
				return err
			}
		} else {
			// Insert a completed request into system.statement_diagnostics_request.
			// This is necessary because the UI uses this table to discover completed
			// diagnostics.
			_, err := txn.ExecEx(ctx, "stmt-diag-add-completed", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				"INSERT INTO system.statement_diagnostics_requests"+
					" (completed, statement_fingerprint, statement_diagnostics_id, requested_at)"+
					" VALUES (true, $1, $2, $3)",
				stmtFingerprint, diagID, collectionTime)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return diagID, nil
}

// pollRequests reads the pending rows from system.statement_diagnostics_requests and
// updates r.mu.requests accordingly.
func (r *Registry) pollRequests(ctx context.Context) error {
	var rows []tree.Datums

	// Loop until we run the query without straddling an epoch increment.
	for {
		r.mu.Lock()
		epoch := r.mu.epoch
		r.mu.Unlock()

		it, err := r.db.Executor().QueryIteratorEx(ctx, "stmt-diag-poll", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			`SELECT id, statement_fingerprint, min_execution_latency, expires_at, sampling_probability, plan_gist, anti_plan_gist, redacted
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
		var planGist string
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
				log.Warningf(ctx, "malformed sampling probability for request %d: %f (expected in range [0, 1]), resetting to 1.0",
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
		ids.Add(int(id))
		r.addRequestInternalLocked(ctx, id, stmtFingerprint, planGist, antiPlanGist, samplingProbability, minExecutionLatency, expiresAt, redacted)
	}

	// Remove all other requests.
	for id, req := range r.mu.requestFingerprints {
		if !ids.Contains(int(id)) || req.isExpired(now) {
			delete(r.mu.requestFingerprints, id)
		}
	}
	return nil
}
