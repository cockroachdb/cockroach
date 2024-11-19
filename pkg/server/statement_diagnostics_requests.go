// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type stmtDiagnosticsRequest struct {
	ID                   int
	StatementFingerprint string
	// Empty plan gist indicates that any plan will do.
	PlanGist string
	// If true and PlanGist is not empty, then any plan not matching the gist
	// will do.
	AntiPlanGist           bool
	Completed              bool
	StatementDiagnosticsID int
	RequestedAt            time.Time
	// Zero value indicates that we're sampling every execution.
	SamplingProbability float64
	// Zero value indicates that there is no minimum latency set on the request.
	MinExecutionLatency time.Duration
	// Zero value indicates that the request never expires.
	ExpiresAt time.Time
	// Indicates whether redacted bundle is requested.
	Redacted bool
}

type stmtDiagnostics struct {
	ID                   int
	StatementFingerprint string
	CollectedAt          time.Time
}

func (request *stmtDiagnosticsRequest) toProto() serverpb.StatementDiagnosticsReport {
	resp := serverpb.StatementDiagnosticsReport{
		Id:                     int64(request.ID),
		Completed:              request.Completed,
		StatementFingerprint:   request.StatementFingerprint,
		StatementDiagnosticsId: int64(request.StatementDiagnosticsID),
		RequestedAt:            request.RequestedAt,
		MinExecutionLatency:    request.MinExecutionLatency,
		ExpiresAt:              request.ExpiresAt,
	}
	return resp
}

func (diagnostics *stmtDiagnostics) toProto() serverpb.StatementDiagnostics {
	resp := serverpb.StatementDiagnostics{
		Id:                   int64(diagnostics.ID),
		StatementFingerprint: diagnostics.StatementFingerprint,
		CollectedAt:          diagnostics.CollectedAt,
	}
	return resp
}

// CreateStatementDiagnosticsReport creates a statement diagnostics
// request in the `system.statement_diagnostics_requests` table
// to trace the next query matching the provided fingerprint.
func (s *statusServer) CreateStatementDiagnosticsReport(
	ctx context.Context, req *serverpb.CreateStatementDiagnosticsReportRequest,
) (*serverpb.CreateStatementDiagnosticsReportResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.CreateStatementDiagnosticsReportResponse{
		Report: &serverpb.StatementDiagnosticsReport{},
	}

	err := s.stmtDiagnosticsRequester.InsertRequest(
		ctx,
		req.StatementFingerprint,
		req.PlanGist,
		req.AntiPlanGist,
		req.SamplingProbability,
		req.MinExecutionLatency,
		req.ExpiresAfter,
		req.Redacted,
	)
	if err != nil {
		return nil, err
	}

	response.Report.StatementFingerprint = req.StatementFingerprint
	return response, nil
}

// CancelStatementDiagnosticsReport cancels the statement diagnostics request by
// updating the corresponding row from the system.statement_diagnostics_requests
// table to be expired.
func (s *statusServer) CancelStatementDiagnosticsReport(
	ctx context.Context, req *serverpb.CancelStatementDiagnosticsReportRequest,
) (*serverpb.CancelStatementDiagnosticsReportResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	var response serverpb.CancelStatementDiagnosticsReportResponse
	err := s.stmtDiagnosticsRequester.CancelRequest(ctx, req.RequestID)
	if err != nil {
		response.Canceled = false
		response.Error = err.Error()
	} else {
		response.Canceled = true
	}
	return &response, nil
}

// StatementDiagnosticsRequests retrieves all statement diagnostics
// requests in the `system.statement_diagnostics_requests` table that
// have either completed or have not yet expired.
func (s *statusServer) StatementDiagnosticsRequests(
	ctx context.Context, _ *serverpb.StatementDiagnosticsReportsRequest,
) (*serverpb.StatementDiagnosticsReportsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	// TODO(davidh): Add pagination to this request.
	it, err := s.internalExecutor.QueryIteratorEx(ctx, "stmt-diag-get-all", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
			id,
			statement_fingerprint,
			completed,
			statement_diagnostics_id,
			requested_at,
			min_execution_latency,
			expires_at,
			sampling_probability,
			plan_gist,
			anti_plan_gist,
			redacted
		FROM
			system.statement_diagnostics_requests`)
	if err != nil {
		return nil, err
	}

	var requests []stmtDiagnosticsRequest
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		id := int(*row[0].(*tree.DInt))
		statementFingerprint := string(*row[1].(*tree.DString))
		completed := bool(*row[2].(*tree.DBool))
		req := stmtDiagnosticsRequest{
			ID:                   id,
			StatementFingerprint: statementFingerprint,
			Completed:            completed,
		}
		if row[3] != tree.DNull {
			sdi := int(*row[3].(*tree.DInt))
			req.StatementDiagnosticsID = sdi
		}
		if requestedAt, ok := row[4].(*tree.DTimestampTZ); ok {
			req.RequestedAt = requestedAt.Time
		}
		if samplingProbability, ok := row[7].(*tree.DFloat); ok {
			req.SamplingProbability = float64(*samplingProbability)
		}
		if minExecutionLatency, ok := row[5].(*tree.DInterval); ok {
			req.MinExecutionLatency = time.Duration(minExecutionLatency.Duration.Nanos())
		}
		if expiresAt, ok := row[6].(*tree.DTimestampTZ); ok {
			req.ExpiresAt = expiresAt.Time
			// Don't return already expired requests.
			if !completed && req.ExpiresAt.Before(timeutil.Now()) {
				continue
			}
		}
		if planGist, ok := row[8].(*tree.DString); ok {
			req.PlanGist = string(*planGist)
		}
		if antiGist, ok := row[9].(*tree.DBool); ok {
			req.AntiPlanGist = bool(*antiGist)
		}
		if redacted, ok := row[10].(*tree.DBool); ok {
			req.Redacted = bool(*redacted)
		}

		requests = append(requests, req)
	}
	if err != nil {
		return nil, err
	}

	response := &serverpb.StatementDiagnosticsReportsResponse{
		Reports: make([]serverpb.StatementDiagnosticsReport, len(requests)),
	}

	for i, request := range requests {
		response.Reports[i] = request.toProto()
	}
	return response, nil
}

// StatementDiagnostics retrieves a statement diagnostics instance
// identified by the given ID. These are in the
// `system.statement_diagnostics` table.
//
// This is generated once the trace is completed on a request created
// by the CreateStatementDiagnosticsRequest call and is linked to
// the original request with its ID.
func (s *statusServer) StatementDiagnostics(
	ctx context.Context, req *serverpb.StatementDiagnosticsRequest,
) (*serverpb.StatementDiagnosticsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	var err error
	row, err := s.internalExecutor.QueryRowEx(ctx, "stmt-diag-get-one", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
			id,
			statement_fingerprint,
			collected_at
		FROM
			system.statement_diagnostics
		WHERE
			id = $1`, req.StatementDiagnosticsId)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Newf(
			"requested a statement diagnostic (%d) that does not exist",
			req.StatementDiagnosticsId,
		)
	}

	diagnostics := stmtDiagnostics{
		ID: int(req.StatementDiagnosticsId),
	}

	if statementFingerprint, ok := row[1].(*tree.DString); ok {
		diagnostics.StatementFingerprint = statementFingerprint.String()
	}

	if collectedAt, ok := row[2].(*tree.DTimestampTZ); ok {
		diagnostics.CollectedAt = collectedAt.Time
	}

	diagnosticsProto := diagnostics.toProto()
	response := &serverpb.StatementDiagnosticsResponse{
		Diagnostics: &diagnosticsProto,
	}

	return response, nil
}
