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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/gogo/protobuf/jsonpb"
)

// stmtInfoRequestRegistry maintains a view on the statement fingerprints on
// which data is to be collected (i.e. system.statement_info_requests) and
// provides utilities for checking a query against this list and satisfying the
// requests.
type stmtInfoRequestRegistry struct {
	mu struct {
		// NOTE: This lock can't be held while the registry runs any statements
		// internally; it'd deadlock.
		syncutil.Mutex
		// requests waiting for the right query to come along.
		requests []stmtInfoRequest
		// ids of requests that this node is in the process of servicing.
		ongoing []int

		// epoch is observed before reading system.statement_info_requests, and then
		// checked again before loading the tables contents. If the value changed in
		// between, then the table contents might be stale.
		epoch int
	}
	ie *InternalExecutor
	db *client.DB
}

func newStmtInfoRequestRegistry(ie *InternalExecutor, db *client.DB) *stmtInfoRequestRegistry {
	return &stmtInfoRequestRegistry{
		ie: ie,
		db: db,
	}
}

type stmtInfoRequestType int

const (
	requestUnknown stmtInfoRequestType = iota
	// requestTrace means that a trace for the query is to be produced in
	// system.statement_info.
	requestTrace
)

// String() turns the request type into the system.statement_info_requests.type
// value.
func (t stmtInfoRequestType) String() string {
	switch t {
	case requestTrace:
		return "trace"
	default:
		panic("unreachable")
	}
}

// stmtInfoRequestTypeFromString is the inverse of stmtInfoRequestType().
func stmtInfoRequestTypeFromString(s string) (bool, stmtInfoRequestType) {
	switch s {
	case "trace":
		return true, requestTrace
	default:
		return false, requestUnknown
	}
}

type stmtInfoRequest struct {
	id               int
	typ              stmtInfoRequestType
	queryFingerprint string
}

// addRequestInternalLocked adds a request to r.mu.requests. If the request is
// already present, the call is a noop.
func (r *stmtInfoRequestRegistry) addRequestInternalLocked(
	ctx context.Context, req stmtInfoRequest,
) {
	for _, er := range r.mu.requests {
		if er.id == req.id {
			return
		}
	}
	for _, id := range r.mu.ongoing {
		if id == req.id {
			return
		}
	}
	r.mu.requests = append(r.mu.requests, req)
}

// insertRequest adds an entry to system.statement_info_requests for tracing a
// query with the given fingerprint. Once this returns, calling shouldCollectInfo() on the current node
// will return true for the given fingerprint.
//
// It returns the ID of the new row.
func (r *stmtInfoRequestRegistry) insertRequest(
	ctx context.Context, typ stmtInfoRequestType, fprint string,
) (int, error) {
	row, err := r.ie.QueryRowEx(ctx, "statement-info-insert-request", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{
			User: security.RootUser,
		},
		"INSERT INTO system.statement_info_requests (type, statement_fingerprint, requested_at) "+
			"VALUES ($1, $2, $3) RETURNING ID",
		typ.String(), fprint, timeutil.Now())
	if err != nil {
		return 0, err
	}
	id := int(*row[0].(*tree.DInt))

	// Manually insert the request in the (local) registry. This lets this node
	// pick up the request quickly if the right query comes around, without
	// waiting for the poller.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.epoch++
	req := stmtInfoRequest{
		id:               id,
		typ:              typ,
		queryFingerprint: fprint,
	}
	r.addRequestInternalLocked(ctx, req)

	return id, nil
}

// removeExtraLocked removes all the requests not in ids.
func (r *stmtInfoRequestRegistry) removeExtraLocked(ids []int) {
	valid := func(req stmtInfoRequest) bool {
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

// shouldCollectInfo checks whether any information should be collected for the
// given query. If information is to be collected, the returned function needs
// to be called with the collected data.
//
// TODO(andrei): make this generic for different types of info requests.
func (r *stmtInfoRequestRegistry) shouldCollectInfo(
	ctx context.Context, ast tree.Statement,
) (bool, func(ctx context.Context, trace tracing.Recording)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Return quickly if we have no requests to trace.
	if len(r.mu.requests) == 0 {
		return false, nil
	}

	stmt := tree.AsString(ast)
	fprint := tree.AsStringWithFlags(ast, tree.FmtHideConstants)

	var req stmtInfoRequest
	idx := -1
	for i, er := range r.mu.requests {
		if er.queryFingerprint == fprint {
			req = er
			idx = i
			// TODO(andrei): What should we do if there's multiple requests for this
			// fingerprint?
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

		if err := r.insertTrace(ctx, req, stmt, trace); err != nil {
			log.Warning(ctx, "failed to insert trace: %s", err)
		}
	}
}

// insertTrace inserts a trace into system.statement_info and marks the
// corresponding request as completed in system.statement_info_requests.
func (r *stmtInfoRequestRegistry) insertTrace(
	ctx context.Context, req stmtInfoRequest, stmt string, trace tracing.Recording,
) error {
	return r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		{
			row, err := r.ie.QueryRowEx(ctx, "statement-info-check-completed", txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"SELECT count(1) FROM system.statement_info_requests WHERE id = $1 AND completed = false",
				req.id)
			if err != nil {
				return err
			}
			cnt := int(*row[0].(*tree.DInt))
			if cnt == 0 {
				// Someone else already marked the request as completed. We've traced for nothing.
				return nil
			}
		}

		var traceID int
		if json, err := traceToJSON(trace); err != nil {
			row, err := r.ie.QueryRowEx(ctx, "statement-info-insert-trace", nil, /* txn */
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"INSERT INTO system.statement_info "+
					"(statement_fingerprint, statement, collected_at, error) "+
					"VALUES ($1, $2, $3, $4) RETURNING ID",
				req.queryFingerprint, stmt, timeutil.Now(), err.Error())
			if err != nil {
				return err
			}
			traceID = int(*row[0].(*tree.DInt))
		} else {
			// Insert the trace into system.statement_info.
			row, err := r.ie.QueryRowEx(ctx, "statement-info-insert-trace", nil, /* txn */
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				"INSERT INTO system.statement_info "+
					"(statement_fingerprint, statement, collected_at, trace) "+
					"VALUES ($1, $2, $3, $4) RETURNING ID",
				req.queryFingerprint, stmt, timeutil.Now(), json)
			if err != nil {
				return err
			}
			traceID = int(*row[0].(*tree.DInt))
		}

		// Mark the request from system.statement_info_request as completed.
		_, err := r.ie.ExecEx(ctx, "statement-info-mark-completed", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPDATE system.statement_info_requests "+
				"SET completed = true, statement_info_id = $1 WHERE id = $2",
			traceID, req.id)
		return err
	})
}

// pollRequests reads the pending rows from system.statement_info_requests and
// updates r.mu.requests accordingly.
func (r *stmtInfoRequestRegistry) pollRequests(ctx context.Context) error {
	var rows []tree.Datums
	// Loop until we run the query without straddling an epoch increment.
	for {
		r.mu.Lock()
		epoch := r.mu.epoch
		r.mu.Unlock()

		var err error
		rows, err = r.ie.QueryEx(ctx, "statement-info-poll", nil, /* txn */
			sqlbase.InternalExecutorSessionDataOverride{
				User: security.RootUser,
			},
			"SELECT ID, type, statement_fingerprint FROM system.statement_info_requests "+
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
		ok, typ := stmtInfoRequestTypeFromString(string(*row[1].(*tree.DString)))
		if !ok {
			// Unrecognized request type. We must have added new requests types in the
			// next version. Good job team.
			continue
		}
		fprint := string(*row[2].(*tree.DString))

		req := stmtInfoRequest{
			id:               id,
			typ:              typ,
			queryFingerprint: fprint,
		}
		r.addRequestInternalLocked(ctx, req)
		r.removeExtraLocked(ids)
	}
	return nil
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
// system.statement_info.trace column.
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
