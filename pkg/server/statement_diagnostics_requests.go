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

func (s *statusServer) CreateStatementDiagnosticsRequest(
	ctx context.Context,
	req *serverpb.CreateStatementDiagnosticsRequestRequest,
) (*serverpb.CreateStatementDiagnosticsRequestResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.CreateStatementDiagnosticsRequestResponse{
		Request: &serverpb.StatementDiagnosticsRequest{},
	}

	_, err := s.stmtDiagnosticsRequester.InsertRequest(ctx, req.StatementFingerprint)
	if err != nil {
		return nil, err
	}

	response.Request.StatementFingerprint = req.StatementFingerprint
	return response, nil
}

func (s *statusServer) StatementDiagnosticsRequests(
	ctx context.Context,
	req *serverpb.StatementDiagnosticsRequestsRequest,
) (*serverpb.StatementDiagnosticsRequestsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	requests, err := s.stmtDiagnosticsRequester.GetAllRequests(ctx)
	if err != nil {
		return nil, err
	}

	response := &serverpb.StatementDiagnosticsRequestsResponse{
		Requests: make([]serverpb.StatementDiagnosticsRequest, len(requests)),
	}

	for i, request := range requests {
		response.Requests[i] = request.ToProto()
	}
	return response, nil
}

