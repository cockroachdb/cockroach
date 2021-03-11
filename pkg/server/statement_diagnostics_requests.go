// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

type stmtDiagnosticsRequest struct {
	ID                     int
	StatementFingerprint   string
	Completed              bool
	StatementDiagnosticsID int
	RequestedAt            time.Time
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

// CreateStatementDiagnosticsRequest creates a statement diagnostics
// request in the `system.statement_diagnostics_requests` table
// to trace the next query matching the provided fingerprint.
func (s *statusServer) CreateStatementDiagnosticsReport(
	ctx context.Context, req *serverpb.CreateStatementDiagnosticsReportRequest,
) (*serverpb.CreateStatementDiagnosticsReportResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.CreateStatementDiagnosticsReportResponse{
		Report: &serverpb.StatementDiagnosticsReport{},
	}

	err := s.stmtDiagnosticsRequester.InsertRequest(ctx, req.StatementFingerprint)
	if err != nil {
		return nil, err
	}

	response.Report.StatementFingerprint = req.StatementFingerprint
	return response, nil
}

// StatementDiagnosticsRequests retrieves all of the statement
// diagnostics requests in the `system.statement_diagnostics_requests` table.
func (s *statusServer) StatementDiagnosticsRequests(
	ctx context.Context, req *serverpb.StatementDiagnosticsReportsRequest,
) (*serverpb.StatementDiagnosticsReportsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	var err error

	// TODO(davidh): Add pagination to this request.
	it, err := s.internalExecutor.QueryIteratorEx(ctx, "stmt-diag-get-all", nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.RootUserName(),
		},
		`SELECT
			id,
			statement_fingerprint,
			completed,
			statement_diagnostics_id,
			requested_at
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
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	var err error
	row, err := s.internalExecutor.QueryRowEx(ctx, "stmt-diag-get-one", nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User: security.RootUserName(),
		},
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
