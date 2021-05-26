// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverpb

import (
	context "context"

	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
)

// SQLStatusServer is a smaller version of the serverpb.StatusInterface which
// includes only the methods used by the SQL subsystem.
type SQLStatusServer interface {
	ListSessions(context.Context, *ListSessionsRequest) (*ListSessionsResponse, error)
	ListLocalSessions(context.Context, *ListSessionsRequest) (*ListSessionsResponse, error)
	CancelQuery(context.Context, *CancelQueryRequest) (*CancelQueryResponse, error)
	CancelSession(context.Context, *CancelSessionRequest) (*CancelSessionResponse, error)
	ListContentionEvents(context.Context, *ListContentionEventsRequest) (*ListContentionEventsResponse, error)
	ListLocalContentionEvents(context.Context, *ListContentionEventsRequest) (*ListContentionEventsResponse, error)
	ResetSQLStats(context.Context, *ResetSQLStatsRequest) (*ResetSQLStatsResponse, error)
	Statements(context.Context, *StatementsRequest) (*StatementsResponse, error)
	ListDistSQLFlows(context.Context, *ListDistSQLFlowsRequest) (*ListDistSQLFlowsResponse, error)
	ListLocalDistSQLFlows(context.Context, *ListDistSQLFlowsRequest) (*ListDistSQLFlowsResponse, error)
}

// OptionalNodesStatusServer is a StatusServer that is only optionally present
// inside the SQL subsystem. In practice, it is present on the system tenant,
// and not present on "regular" tenants.
type OptionalNodesStatusServer struct {
	w errorutil.TenantSQLDeprecatedWrapper // stores serverpb.StatusServer
}

// MakeOptionalNodesStatusServer initializes and returns an
// OptionalNodesStatusServer. The provided server will be returned via
// OptionalNodesStatusServer() if and only if it is not nil.
func MakeOptionalNodesStatusServer(s NodesStatusServer) OptionalNodesStatusServer {
	return OptionalNodesStatusServer{
		// Return the status server from OptionalSQLStatusServer() only if one was provided.
		// We don't have any calls to .Deprecated().
		w: errorutil.MakeTenantSQLDeprecatedWrapper(s, s != nil /* exposed */),
	}
}

// NodesStatusServer is the subset of the serverpb.StatusInterface that is used
// by the SQL subsystem but is unavailable to tenants.
type NodesStatusServer interface {
	Nodes(context.Context, *NodesRequest) (*NodesResponse, error)
}

// OptionalNodesStatusServer returns the wrapped NodesStatusServer, if it is
// available. If it is not, an error referring to the optionally supplied issues
// is returned.
func (s *OptionalNodesStatusServer) OptionalNodesStatusServer(
	issue int,
) (NodesStatusServer, error) {
	v, err := s.w.OptionalErr(issue)
	if err != nil {
		return nil, err
	}
	return v.(NodesStatusServer), nil
}
