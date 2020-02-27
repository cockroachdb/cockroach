// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/gogo/protobuf/jsonpb"
)

// StmtDiagnosticsRequester is the interface into stmtDiagnosticsRequestRegistry
// used by AdminUI endpoints.
type StmtDiagnosticsRequester interface {
	// InsertRequest adds an entry to system.statement_diagnostics_requests for
	// tracing a query with the given fingerprint. Once this returns, calling
	// shouldCollectDiagnostics() on the current node will return true for the given
	// fingerprint.
	//
	// It returns the ID of the new row.
	InsertRequest(ctx context.Context, fprint string) (int, error)
}

// stmtDiagnosticsRequestRegistry maintains a view on the statement fingerprints
// on which data is to be collected (i.e. system.statement_diagnostics_requests)
// and provides utilities for checking a query against this list and satisfying
// the requests.
type stmtDiagnosticsRequestRegistry struct {
	mu struct {
		// NOTE: This lock can't be held while the registry runs any statements
		// internally; it'd deadlock.
		syncutil.Mutex
		// requests waiting for the right query to come along.
		requests []stmtDiagRequest
		// ids of requests that this node is in the process of servicing.
		ongoing []int

		// epoch is observed before reading system.statement_diagnostics_requests, and then
		// checked again before loading the tables contents. If the value changed in
		// between, then the table contents might be stale.
		epoch int
	}
	ie     *InternalExecutor
	db     *client.DB
	gossip *gossip.Gossip
	nodeID roachpb.NodeID
}

func newStmtDiagnosticsRequestRegistry(
	ie *InternalExecutor, db *client.DB, g *gossip.Gossip, nodeID roachpb.NodeID,
) *stmtDiagnosticsRequestRegistry {
	r := &stmtDiagnosticsRequestRegistry{
		ie:     ie,
		db:     db,
		gossip: g,
		nodeID: nodeID,
	}
	// Some tests pass a nil gossip.
	if g != nil {
		g.RegisterCallback(gossip.KeyGossipStatementDiagnosticsRequest, r.gossipNotification)
	}
	return r
}

type stmtDiagRequest struct {
	id               int
	queryFingerprint string
}

// addRequestInternalLocked adds a request to r.mu.requests. If the request is
// already present, the call is a noop.
func (r *stmtDiagnosticsRequestRegistry) addRequestInternalLocked(
	ctx context.Context, req stmtDiagRequest,
) {
	if r.findRequestLocked(req.id) {
		// Request already exists.
		return
	}
	r.mu.requests = append(r.mu.requests, req)
}

func (r *stmtDiagnosticsRequestRegistry) findRequestLocked(requestID int) bool {
	for _, er := range r.mu.requests {
		if er.id == requestID {
			return true
		}
	}
	for _, id := range r.mu.ongoing {
		if id == requestID {
			return true
		}
	}
	return false
}

// InsertRequest is part of the StmtDiagnosticsRequester interface.
func (r *stmtDiagnosticsRequestRegistry) InsertRequest(
	ctx context.Context, fprint string,
) (int, error) {
	var requestID int
	err := r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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
		requestID = int(*row[0].(*tree.DInt))
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
	req := stmtDiagRequest{
		id:               requestID,
		queryFingerprint: fprint,
	}
	r.addRequestInternalLocked(ctx, req)

	// Notify all the other nodes that they have to poll.
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(requestID))
	if err := r.gossip.AddInfo(gossip.KeyGossipStatementDiagnosticsRequest, buf, 0 /* ttl */); err != nil {
		log.Warningf(ctx, "error notifying of diagnostics request: %s", err)
	}

	return requestID, nil
}

// removeExtraLocked removes all the requests not in ids.
func (r *stmtDiagnosticsRequestRegistry) removeExtraLocked(ids []int) {
	valid := func(req stmtDiagRequest) bool {
		for _, id := range ids {
			if req.id == id {
				return true
			}
		}
		return false
	}

	// Compact the valid requests in the beginning of r.mu.requests.
	i := 0 // index of valid requests
	for _, req := range r.mu.requests {
		if valid(req) {
			r.mu.requests[i] = req
			i++
		}
	}
	r.mu.requests = r.mu.requests[:i]
}

// shouldCollectDiagnostics checks whether any data should be collected for the
// given query. If data is to be collected, the returned function needs to be
// called once the data was collected.
//
// Once shouldCollectDiagnostics returns true, it will not return true again on
// this node for the same diagnostics request.
func (r *stmtDiagnosticsRequestRegistry) shouldCollectDiagnostics(
	ctx context.Context, ast tree.Statement,
) (bool, func(ctx context.Context, trace tracing.Recording)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Return quickly if we have no requests to trace.
	if len(r.mu.requests) == 0 {
		return false, nil
	}

	fprint := tree.AsStringWithFlags(ast, tree.FmtHideConstants)

	var req stmtDiagRequest
	idx := -1
	for i, er := range r.mu.requests {
		if er.queryFingerprint == fprint {
			req = er
			idx = i
			break
		}
	}
	if idx == -1 {
		return false, nil
	}

	// Remove the request.
	l := len(r.mu.requests)
	r.mu.requests[idx] = r.mu.requests[l-1]
	r.mu.requests = r.mu.requests[:l-1]

	r.mu.ongoing = append(r.mu.ongoing, req.id)

	return true, func(ctx context.Context, trace tracing.Recording) {
		defer func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			// Remove the request from r.mu.ongoing.
			for i, id := range r.mu.ongoing {
				if id == req.id {
					l := len(r.mu.ongoing)
					r.mu.ongoing[i] = r.mu.ongoing[l-1]
					r.mu.ongoing = r.mu.ongoing[:l-1]
				}
			}
		}()

		if err := r.insertDiagnostics(ctx, req, tree.AsString(ast), trace); err != nil {
			log.Warningf(ctx, "failed to insert trace: %s", err)
		}
	}
}

// insertDiagnostics inserts a trace into system.statement_diagnostics and marks
// the corresponding request as completed in
// system.statement_diagnostics_requests.
func (r *stmtDiagnosticsRequestRegistry) insertDiagnostics(
	ctx context.Context, req stmtDiagRequest, stmt string, trace tracing.Recording,
) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		{
			row, err := r.ie.QueryRowEx(ctx, "stmt-diag-check-completed", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"SELECT count(1) FROM system.statement_diagnostics_requests WHERE id = $1 AND completed = false",
				req.id)
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

		var traceID int
		if json, err := traceToJSON(trace); err != nil {
			row, err := r.ie.QueryRowEx(ctx, "stmt-diag-insert-trace", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"INSERT INTO system.statement_diagnostics "+
					"(statement_fingerprint, statement, collected_at, error) "+
					"VALUES ($1, $2, $3, $4) RETURNING id",
				req.queryFingerprint, stmt, timeutil.Now(), err.Error())
			if err != nil {
				return err
			}
			traceID = int(*row[0].(*tree.DInt))
		} else {
			// Insert the trace into system.statement_diagnostics.
			row, err := r.ie.QueryRowEx(ctx, "stmt-diag-insert-trace", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"INSERT INTO system.statement_diagnostics "+
					"(statement_fingerprint, statement, collected_at, trace) "+
					"VALUES ($1, $2, $3, $4) RETURNING id",
				req.queryFingerprint, stmt, timeutil.Now(), json)
			if err != nil {
				return err
			}
			traceID = int(*row[0].(*tree.DInt))
		}

		// Mark the request from system.statement_diagnostics_request as completed.
		_, err := r.ie.ExecEx(ctx, "stmt-diag-mark-completed", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPDATE system.statement_diagnostics_requests "+
				"SET completed = true, statement_diagnostics_id = $1 WHERE id = $2",
			traceID, req.id)
		return err
	})
}

// pollRequests reads the pending rows from system.statement_diagnostics_requests and
// updates r.mu.requests accordingly.
func (r *stmtDiagnosticsRequestRegistry) pollRequests(ctx context.Context) error {
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

	ids := make([]int, 0, len(rows))
	for _, row := range rows {
		id := int(*row[0].(*tree.DInt))
		ids = append(ids, id)
		fprint := string(*row[1].(*tree.DString))

		req := stmtDiagRequest{
			id:               id,
			queryFingerprint: fprint,
		}
		r.addRequestInternalLocked(ctx, req)
		r.removeExtraLocked(ids)
	}
	return nil
}

// gossipNotification is called in response to a gossip update informing us that
// we need to poll.
func (r *stmtDiagnosticsRequestRegistry) gossipNotification(s string, value roachpb.Value) {
	if s != gossip.KeyGossipStatementDiagnosticsRequest {
		// We don't expect any other notifications. Perhaps in a future version we
		// added other keys with the same prefix.
		return
	}
	requestID := int(binary.LittleEndian.Uint64(value.RawBytes))
	r.mu.Lock()
	if r.findRequestLocked(requestID) {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	if err := r.pollRequests(context.TODO()); err != nil {
		log.Warningf(context.TODO(), "failed to poll for diagnostics requests: %s", err)
	}
}

func normalizeSpan(s tracing.RecordedSpan, trace tracing.Recording) tracing.NormalizedSpan {
	var n tracing.NormalizedSpan
	n.Operation = s.Operation
	n.StartTime = s.StartTime
	n.Duration = s.Duration
	n.Tags = s.Tags
	n.Logs = s.Logs

	for _, ss := range trace {
		if ss.ParentSpanID != s.SpanID {
			continue
		}
		n.Children = append(n.Children, normalizeSpan(ss, trace))
	}
	return n
}

// traceToJSON converts a trace to a JSON format suitable for the
// system.statement_diagnostics.trace column.
//
// traceToJSON assumes that the first span in the recording contains all the
// other spans.
func traceToJSON(trace tracing.Recording) (string, error) {
	root := normalizeSpan(trace[0], trace)
	marshaller := jsonpb.Marshaler{
		Indent: "  ",
	}
	return marshaller.MarshalToString(&root)
}
