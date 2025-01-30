// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
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
	RequestJobProfilerExecutionDetails(context.Context, *RequestJobProfilerExecutionDetailsRequest) (*RequestJobProfilerExecutionDetailsResponse, error)
	TenantServiceStatus(context.Context, *TenantServiceStatusRequest) (*TenantServiceStatusResponse, error)
	UpdateTableMetadataCache(context.Context, *UpdateTableMetadataCacheRequest) (*UpdateTableMetadataCacheResponse, error)
	GetUpdateTableMetadataCacheSignal() chan struct{}
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
	Ranges(context.Context, *RangesRequest) (*RangesResponse, error)
	TenantRanges(context.Context, *TenantRangesRequest) (*TenantRangesResponse, error)
	Regions(context.Context, *RegionsRequest) (*RegionsResponse, error)
	HotRangesV2(context.Context, *HotRangesRequest) (*HotRangesResponseV2, error)

	// SpanStats is used to access MVCC stats from KV
	SpanStats(context.Context, *roachpb.SpanStatsRequest) (*roachpb.SpanStatsResponse, error)
	Nodes(context.Context, *NodesRequest) (*NodesResponse, error)
	// TODO(adityamaru): DownloadSpan has the side effect of telling the engine to
	// download remote files. A method that mutates state should not be on the
	// status server and so in the long run we should move it.
	DownloadSpan(ctx context.Context, request *DownloadSpanRequest) (*DownloadSpanResponse, error)
	NetworkConnectivity(context.Context, *NetworkConnectivityRequest) (*NetworkConnectivityResponse, error)
	Gossip(context.Context, *GossipRequest) (*gossip.InfoStatus, error)
}

// OptionalNodesStatusServer returns the wrapped NodesStatusServer, if it is
// available. If it is not, an error referring to the optionally supplied issues
// is returned.
func (s *OptionalNodesStatusServer) OptionalNodesStatusServer() (NodesStatusServer, error) {
	v, err := s.w.OptionalErr(errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
	if err != nil {
		return nil, err
	}
	return v.(NodesStatusServer), nil
}
