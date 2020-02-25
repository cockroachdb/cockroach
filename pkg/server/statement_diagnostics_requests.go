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
	context.Context,
	*serverpb.CreateStatementDiagnosticsRequestRequest,
) (*serverpb.CreateStatementDiagnosticsRequestResponse, error) {
	panic("implement me")
}

func (s *statusServer) StatementDiagnosticsRequests(
	context.Context,
	*serverpb.StatementDiagnosticsRequestsRequest,
) (*serverpb.StatementDiagnosticsRequestsResponse, error) {
	panic("implement me")
}
