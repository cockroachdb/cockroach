// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stmtdiagnostics

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var stmtDiagnosticsPollingInterval = settings.RegisterDurationSetting(
	"sql.stmt_diagnostics.poll_interval",
	"rate at which the stmtdiagnostics.Registry polls for requests, set to zero to disable",
	10*time.Second)

// Registry maintains a view on the statement fingerprints
// on which data is to be collected (i.e. system.statement_diagnostics_requests)
// and provides utilities for checking a query against this list and satisfying
// the requests.
type Registry struct {
	mu struct {
		// NOTE: This lock can't be held while the registry runs any statements
		// internally; it'd deadlock.
		syncutil.Mutex
		// requests waiting for the right query to come along.
		requestFingerprints map[requestID]string
		// ids of requests that this node is in the process of servicing.
		ongoing map[requestID]struct{}

		// epoch is observed before reading system.statement_diagnostics_requests, and then
		// checked again before loading the tables contents. If the value changed in
		// between, then the table contents might be stale.
		epoch int
	}
	st     *cluster.Settings
	ie     sqlutil.InternalExecutor
	db     *kv.DB
	gossip gossip.DeprecatedGossip

	// gossipUpdateChan is used to notify the polling loop that a diagnostics
	// request has been added. The gossip callback will not block sending on this
	// channel.
	gossipUpdateChan chan requestID
}

// NewRegistry constructs a new Registry.
func NewRegistry(
	ie sqlutil.InternalExecutor, db *kv.DB, gw gossip.DeprecatedGossip, st *cluster.Settings,
) *Registry {
	r := &Registry{
		ie:               ie,
		db:               db,
		gossip:           gw,
		gossipUpdateChan: make(chan requestID, 1),
		st:               st,
	}
	// Some tests pass a nil gossip, and gossip is not available on SQL tenant
	// servers.
	g, ok := gw.Optional(47893)
	if ok && g != nil {
		g.RegisterCallback(gossip.KeyGossipStatementDiagnosticsRequest, r.gossipNotification)
	}
	return r
}

// Start will start the polling loop for the Registry.
func (r *Registry) Start(ctx context.Context, stopper *stop.Stopper) {
	ctx, _ = stopper.WithCancelOnQuiesce(ctx)
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
			if interval := stmtDiagnosticsPollingInterval.Get(&r.st.SV); interval <= 0 {
				// Setting the interval to a non-positive value stops the polling.
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
	stmtDiagnosticsPollingInterval.SetOnChange(&r.st.SV, func() {
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
		case reqID := <-r.gossipUpdateChan:
			if r.findRequest(reqID) {
				continue // request already exists, don't do anything
			}
			// Poll the data.
		case <-timer.C:
			timer.Read = true
		case <-ctx.Done():
			return
		}
		poll()
	}
}

// requestID is the ID of a diagnostics request, corresponding to the id
// column in statement_diagnostics_requests.
// A zero ID is invalid.
type requestID int

// ID is the ID of an instance of collected diagnostics, corresponding
// to the id column in statement_diagnostics.
type stmtID int

// addRequestInternalLocked adds a request to r.mu.requests. If the request is
// already present, the call is a noop.
func (r *Registry) addRequestInternalLocked(
	ctx context.Context, id requestID, queryFingerprint string,
) {
	if r.findRequestLocked(id) {
		// Request already exists.
		return
	}
	if r.mu.requestFingerprints == nil {
		r.mu.requestFingerprints = make(map[requestID]string)
	}
	r.mu.requestFingerprints[id] = queryFingerprint
}

func (r *Registry) findRequest(requestID requestID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.findRequestLocked(requestID)
}

func (r *Registry) findRequestLocked(requestID requestID) bool {
	_, ok := r.mu.requestFingerprints[requestID]
	if ok {
		return true
	}
	_, ok = r.mu.ongoing[requestID]
	return ok
}

// InsertRequest is part of the StmtDiagnosticsRequester interface.
func (r *Registry) InsertRequest(ctx context.Context, fprint string) error {
	_, err := r.insertRequestInternal(ctx, fprint)
	return err
}

func (r *Registry) insertRequestInternal(ctx context.Context, fprint string) (requestID, error) {
	g, err := r.gossip.OptionalErr(48274)
	if err != nil {
		return 0, err
	}

	var reqID requestID
	err = r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Check if there's already a pending request for this fingerprint.
		row, err := r.ie.QueryRowEx(ctx, "stmt-diag-check-pending", txn,
			sqlbase.InternalExecutorSessionDataOverride{
				User: security.RootUser,
			},
			"SELECT count(1) FROM system.statement_diagnostics_requests "+
				"WHERE completed = false AND statement_fingerprint = $1",
			fprint)
		if err != nil {
			return err
		}
		count := int(*row[0].(*tree.DInt))
		if count != 0 {
			return errors.New("a pending request for the requested fingerprint already exists")
		}

		row, err = r.ie.QueryRowEx(ctx, "stmt-diag-insert-request", txn,
			sqlbase.InternalExecutorSessionDataOverride{
				User: security.RootUser,
			},
			"INSERT INTO system.statement_diagnostics_requests (statement_fingerprint, requested_at) "+
				"VALUES ($1, $2) RETURNING id",
			fprint, timeutil.Now())
		if err != nil {
			return err
		}
		reqID = requestID(*row[0].(*tree.DInt))
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Manually insert the request in the (local) registry. This lets this node
	// pick up the request quickly if the right query comes around, without
	// waiting for the poller.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.epoch++
	r.addRequestInternalLocked(ctx, reqID, fprint)

	// Notify all the other nodes that they have to poll.
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(reqID))
	if err := g.AddInfo(gossip.KeyGossipStatementDiagnosticsRequest, buf, 0 /* ttl */); err != nil {
		log.Warningf(ctx, "error notifying of diagnostics request: %s", err)
	}

	return reqID, nil
}

func (r *Registry) removeOngoing(requestID requestID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Remove the request from r.mu.ongoing.
	delete(r.mu.ongoing, requestID)
}

// ShouldCollectDiagnostics checks whether any data should be collected for the
// given query, which is the case if the registry has a request for this
// statement's fingerprint; in this case ShouldCollectDiagnostics will not
// return true again on this note for the same diagnostics request.
//
// If data is to be collected, Finish() must always be called on the returned
// stmtDiagnosticsHelper once the data was collected.
func (r *Registry) ShouldCollectDiagnostics(
	ctx context.Context, ast tree.Statement,
) (bool, func(ctx context.Context, traceJSON tree.Datum, bundle []byte, collectionErr error)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Return quickly if we have no requests to trace.
	if len(r.mu.requestFingerprints) == 0 {
		return false, nil
	}

	fingerprint := tree.AsStringWithFlags(ast, tree.FmtHideConstants)
	var reqID requestID
	for id, f := range r.mu.requestFingerprints {
		if f == fingerprint {
			reqID = id
			break
		}
	}
	if reqID == 0 {
		return false, nil
	}

	// Remove the request.
	delete(r.mu.requestFingerprints, reqID)
	if r.mu.ongoing == nil {
		r.mu.ongoing = make(map[requestID]struct{})
	}

	r.mu.ongoing[reqID] = struct{}{}
	return true, makeStmtDiagnosticsHelper(r, fingerprint, tree.AsString(ast), reqID).Finish
}

type stmtDiagnosticsHelper struct {
	r            *Registry
	fingerprint  string
	statementStr string
	requestID    requestID
}

func makeStmtDiagnosticsHelper(
	r *Registry, fingerprint string, statementStr string, requestID requestID,
) *stmtDiagnosticsHelper {
	return &stmtDiagnosticsHelper{
		r:            r,
		fingerprint:  fingerprint,
		statementStr: statementStr,
		requestID:    requestID,
	}
}

// Finish reports the trace and creates the support bundle, and inserts them in
// the system tables.
func (h *stmtDiagnosticsHelper) Finish(
	ctx context.Context, traceJSON tree.Datum, bundle []byte, collectionErr error,
) {
	defer h.r.removeOngoing(h.requestID)

	_, err := h.r.insertStatementDiagnostics(
		ctx,
		h.requestID,
		h.fingerprint,
		h.statementStr,
		traceJSON,
		bundle,
		collectionErr,
	)
	if err != nil {
		log.Warningf(ctx, "failed to report statement diagnostics: %s", err)
	}
}

// InsertStatementDiagnostics inserts a trace into system.statement_diagnostics.
//
// traceJSON is either DNull (when collectionErr should not be nil) or a *DJSON.
func (r *Registry) InsertStatementDiagnostics(
	ctx context.Context, stmtFingerprint string, stmt string, traceJSON tree.Datum, bundle []byte,
) (int64, error) {
	id, err := r.insertStatementDiagnostics(ctx, 0, /* requestID */
		stmtFingerprint, stmt, traceJSON, bundle, nil /* collectionErr */)
	return int64(id), err
}

// insertStatementDiagnostics inserts a trace into system.statement_diagnostics.
//
// traceJSON is either DNull (when collectionErr should not be nil) or a *DJSON.
//
// It also marks the request as completed in
// system.statement_diagnostics_requests. If requestID is zero, a new entry is
// inserted.
//
// collectionErr should be any error generated during the serialization of the
// collected trace.
func (r *Registry) insertStatementDiagnostics(
	ctx context.Context,
	requestID requestID,
	stmtFingerprint string,
	stmt string,
	traceJSON tree.Datum,
	bundle []byte,
	collectionErr error,
) (stmtID, error) {
	var diagID stmtID
	err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if requestID != 0 {
			row, err := r.ie.QueryRowEx(ctx, "stmt-diag-check-completed", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"SELECT count(1) FROM system.statement_diagnostics_requests WHERE id = $1 AND completed = false",
				requestID)
			if err != nil {
				return err
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

		bundleChunksVal := tree.DNull
		if len(bundle) != 0 {
			// Insert the bundle into system.statement_bundle_chunks.
			// TODO(radu): split in chunks.
			row, err := r.ie.QueryRowEx(
				ctx, "stmt-bundle-chunks-insert", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"INSERT INTO system.statement_bundle_chunks(description, data) VALUES ($1, $2) RETURNING id",
				"statement diagnostics bundle",
				tree.NewDBytes(tree.DBytes(bundle)),
			)
			if err != nil {
				return err
			}
			chunkID := row[0].(*tree.DInt)

			array := tree.NewDArray(types.Int)
			if err := array.Append(chunkID); err != nil {
				return err
			}
			bundleChunksVal = array
		}

		collectionTime := timeutil.Now()

		// Insert the trace into system.statement_diagnostics.
		row, err := r.ie.QueryRowEx(
			ctx, "stmt-diag-insert", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"INSERT INTO system.statement_diagnostics "+
				"(statement_fingerprint, statement, collected_at, trace, bundle_chunks, error) "+
				"VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
			stmtFingerprint, stmt, collectionTime, traceJSON, bundleChunksVal, errorVal,
		)
		if err != nil {
			return err
		}
		diagID = stmtID(*row[0].(*tree.DInt))

		if requestID != 0 {
			// Mark the request from system.statement_diagnostics_request as completed.
			_, err := r.ie.ExecEx(ctx, "stmt-diag-mark-completed", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"UPDATE system.statement_diagnostics_requests "+
					"SET completed = true, statement_diagnostics_id = $1 WHERE id = $2",
				diagID, requestID)
			if err != nil {
				return err
			}
		} else {
			// Insert a completed request into system.statement_diagnostics_request.
			// This is necessary because the UI uses this table to discover completed
			// diagnostics.
			_, err := r.ie.ExecEx(ctx, "stmt-diag-add-completed", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
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

		var err error
		rows, err = r.ie.QueryEx(ctx, "stmt-diag-poll", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{
				User: security.RootUser,
			},
			"SELECT id, statement_fingerprint FROM system.statement_diagnostics_requests "+
				"WHERE completed = false")
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

	var ids util.FastIntSet
	for _, row := range rows {
		id := requestID(*row[0].(*tree.DInt))
		fprint := string(*row[1].(*tree.DString))

		ids.Add(int(id))
		r.addRequestInternalLocked(ctx, id, fprint)
	}

	// Remove all other requests.
	for id := range r.mu.requestFingerprints {
		if !ids.Contains(int(id)) {
			delete(r.mu.requestFingerprints, id)
		}
	}
	return nil
}

// gossipNotification is called in response to a gossip update informing us that
// we need to poll.
func (r *Registry) gossipNotification(s string, value roachpb.Value) {
	if s != gossip.KeyGossipStatementDiagnosticsRequest {
		// We don't expect any other notifications. Perhaps in a future version we
		// added other keys with the same prefix.
		return
	}
	select {
	case r.gossipUpdateChan <- requestID(binary.LittleEndian.Uint64(value.RawBytes)):
	default:
		// Don't pile up on these requests and don't block gossip.
	}
}
