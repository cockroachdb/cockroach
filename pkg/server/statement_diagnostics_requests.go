// Copyright 2014 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// CreateStatementDiagnosticsRequest creates a statement diagnostics
// request to trace the next query matching the provided fingerprint
func (s *statusServer) CreateStatementDiagnosticsRequest(
	ctx context.Context,
	req *serverpb.CreateStatementDiagnosticsReportRequest,
) (*serverpb.CreateStatementDiagnosticsReportResponse, error) {
	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.CreateStatementDiagnosticsReportResponse{
		Report: &serverpb.StatementDiagnosticsReport{},
	}

	_, err := s.stmtDiagnosticsRequester.InsertRequest(ctx, req.StatementFingerprint)
	if err != nil {
		return nil, err
	}

	response.Report.StatementFingerprint = req.StatementFingerprint
	return response, nil
}

// StatementDiagnosticsRequests retrieves all of the statement
// diagnostics requests in the system table
func (s *statusServer) StatementDiagnosticsRequests(
	ctx context.Context,
	req *serverpb.StatementDiagnosticsReportsRequest,
) (*serverpb.StatementDiagnosticsReportsResponse, error) {
	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	requests, err := s.stmtDiagnosticsRequester.GetAllRequests(ctx)
	if err != nil {
		return nil, err
	}

	response := &serverpb.StatementDiagnosticsReportsResponse{
		Reports: make([]serverpb.StatementDiagnosticsReport, len(requests)),
	}

	for i, request := range requests {
		response.Reports[i] = request.ToProto()
	}
	return response, nil
}

// StatementDiagnostics retrieves a statement diagnostics instance
// identified by the given ID.
//
// This is generated once the trace is completed on a request created
// by the CreateStatementDiagnosticsRequest call and is linked to
// the original request with its ID.
func (s *statusServer) StatementDiagnostics(
	ctx context.Context,
	req *serverpb.StatementDiagnosticsRequest,
) (*serverpb.StatementDiagnosticsResponse, error) {
	if _, err := s.admin.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	diagnostics, err := s.stmtDiagnosticsRequester.GetStatementDiagnostics(
		ctx, int(req.StatementDiagnosticsId),
	)
	if err != nil {
		return nil, err
	}

	diagnosticsProto := diagnostics.ToProto()
	response := &serverpb.StatementDiagnosticsResponse{
		Diagnostics: &diagnosticsProto,
	}

	return response, nil
}
