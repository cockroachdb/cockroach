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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
)

// SQLStatusServer is a smaller version of the serverpb.StatusServer which
// includes only the methods used by the SQL subsystem.
type SQLStatusServer interface {
	ListSessions(context.Context, *ListSessionsRequest) (*ListSessionsResponse, error)
	ListLocalSessions(context.Context, *ListSessionsRequest) (*ListSessionsResponse, error)
	CancelQuery(context.Context, *CancelQueryRequest) (*CancelQueryResponse, error)
	CancelQueryByKey(context.Context, *CancelQueryByKeyRequest) (*CancelQueryByKeyResponse, error)
	CancelSession(context.Context, *CancelSessionRequest) (*CancelSessionResponse, error)
	ListContentionEvents(context.Context, *ListContentionEventsRequest) (*ListContentionEventsResponse, error)
	ListLocalContentionEvents(context.Context, *ListContentionEventsRequest) (*ListContentionEventsResponse, error)
	ResetSQLStats(context.Context, *ResetSQLStatsRequest) (*ResetSQLStatsResponse, error)
	CombinedStatementStats(context.Context, *CombinedStatementsStatsRequest) (*StatementsResponse, error)
	Statements(context.Context, *StatementsRequest) (*StatementsResponse, error)
	StatementDetails(context.Context, *StatementDetailsRequest) (*StatementDetailsResponse, error)
	ListDistSQLFlows(context.Context, *ListDistSQLFlowsRequest) (*ListDistSQLFlowsResponse, error)
	ListLocalDistSQLFlows(context.Context, *ListDistSQLFlowsRequest) (*ListDistSQLFlowsResponse, error)
	Profile(context.Context, *ProfileRequest) (*JSONResponse, error)
	IndexUsageStatistics(context.Context, *IndexUsageStatisticsRequest) (*IndexUsageStatisticsResponse, error)
	ResetIndexUsageStats(context.Context, *ResetIndexUsageStatsRequest) (*ResetIndexUsageStatsResponse, error)
	TableIndexStats(context.Context, *TableIndexStatsRequest) (*TableIndexStatsResponse, error)
	UserSQLRoles(context.Context, *UserSQLRolesRequest) (*UserSQLRolesResponse, error)
	TxnIDResolution(context.Context, *TxnIDResolutionRequest) (*TxnIDResolutionResponse, error)
	TransactionContentionEvents(context.Context, *TransactionContentionEventsRequest) (*TransactionContentionEventsResponse, error)
	NodesList(context.Context, *NodesListRequest) (*NodesListResponse, error)
	ListExecutionInsights(context.Context, *ListExecutionInsightsRequest) (*ListExecutionInsightsResponse, error)
	LogFilesList(context.Context, *LogFilesListRequest) (*LogFilesListResponse, error)
	LogFile(context.Context, *LogFileRequest) (*LogEntriesResponse, error)
	Logs(context.Context, *LogsRequest) (*LogEntriesResponse, error)
	NodesUI(context.Context, *NodesRequest) (*NodesResponseExternal, error)
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

// NodesStatusServer is an endpoint that allows the SQL subsystem
// to observe node descriptors.
// It is unavailable to tenants.
type NodesStatusServer interface {
	ListNodesInternal(context.Context, *NodesRequest) (*NodesResponse, error)
}

// TenantStatusServer is the subset of the serverpb.StatusServer that is
// used by tenants to query for debug information, such as tenant-specific
// range reports.
//
// It is available for all tenants.
type TenantStatusServer interface {
	TenantRanges(context.Context, *TenantRangesRequest) (*TenantRangesResponse, error)
	Regions(context.Context, *RegionsRequest) (*RegionsResponse, error)
	HotRangesV2(context.Context, *HotRangesRequest) (*HotRangesResponseV2, error)

	// SpanStats is used to access MVCC stats from KV
	SpanStats(context.Context, *roachpb.SpanStatsRequest) (*roachpb.SpanStatsResponse, error)
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
