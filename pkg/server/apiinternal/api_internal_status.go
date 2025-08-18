// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/gorilla/mux"
)

// registerStatusRoutes sets up all the status REST endpoints.
func (r *apiInternalServer) registerStatusRoutes() {
	routes := []route{
		// Node information
		{"/_status/nodes", r.nodes, GET},
		{"/_status/nodes/{node_id}", r.node, GET},
		{"/_status/nodes_ui", r.nodesUI, GET},
		{"/_status/nodes_ui/{node_id}", r.nodeUI, GET},

		// Node details and diagnostics
		{"/_status/details/{node_id}", r.details, GET},
		{"/_status/certificates/{node_id}", r.certificates, GET},
		{"/_status/gossip/{node_id}", r.gossip, GET},
		{"/_status/metrics/{node_id}", r.metrics, GET},
		{"/_status/diagnostics/{node_id}", r.diagnostics, GET},
		{"/_status/stores/{node_id}", r.stores, GET},

		// Node profiling and debugging
		{"/_status/profile/{node_id}", r.profile, GET},
		{"/_status/stacks/{node_id}", r.stacks, GET},
		{"/_status/files/{node_id}", r.getFiles, GET},

		// Node logs
		{"/_status/logfiles/{node_id}", r.logFilesList, GET},
		{"/_status/logfiles/{node_id}/{file}", r.logFile, GET},
		{"/_status/logs/{node_id}", r.logs, GET},

		// Range information
		{"/_status/ranges/{node_id}", r.ranges, GET},
		{"/_status/range/{range_id}", r.rangeStatus, GET},
		{"/_status/tenant_ranges", r.tenantRanges, GET},
		{"/_status/v2/hotranges", r.hotRangesV2, POST},
		{"/_status/problemranges", r.problemRanges, GET},

		{"/_status/allocator/node/{node_id}", r.allocator, GET},
		{"/_status/allocator/range/{range_id}", r.allocatorRange, GET},

		{"/_status/enginestats/{node_id}", r.engineStats, GET},
		{"/_status/downloadspans", r.downloadSpan, GET},

		// Statement statistics
		{"/_status/statements", r.statements, GET},
		{"/_status/combinedstmts", r.combinedStatementStats, GET},
		{"/_status/stmtdetails/{fingerprint_id}", r.statementDetails, GET},
		{"/_status/resetsqlstats", r.resetSQLStats, POST},

		// Statement diagnostics
		{"/_status/stmtdiagreports", r.statementDiagnosticsRequests, GET},
		{"/_status/stmtdiagreports", r.createStatementDiagnosticsReport, POST},
		{"/_status/stmtdiagreports/cancel", r.cancelStatementDiagnosticsReport, POST},
		{"/_status/stmtdiag/{statement_diagnostics_id}", r.statementDiagnostics, GET},

		// Index statistics
		{"/_status/indexusagestatistics", r.indexUsageStatistics, GET},
		{"/_status/resetindexusagestats", r.resetIndexUsageStats, POST},
		{"/_status/databases/{database}/tables/{table}/indexstats", r.tableIndexStats, GET},

		// SQL sessions and queries
		{"/_status/sessions", r.listSessions, GET},
		{"/_status/local_sessions", r.listLocalSessions, GET},
		{"/_status/cancel_query/{node_id}", r.cancelQuery, POST},
		{"/_status/cancel_session/{node_id}", r.cancelSession, POST},

		// SQL roles and permissions
		{"/_status/sqlroles", r.userSQLRoles, GET},

		// Contention events
		{"/_status/contention_events", r.listContentionEvents, GET},
		{"/_status/local_contention_events", r.listLocalContentionEvents, GET},
		{"/_status/transactioncontentionevents", r.transactionContentionEvents, GET},

		{"/_status/distsql_flows", r.listDistSQLFlows, GET},
		{"/_status/local_distsql_flows", r.listLocalDistSQLFlows, GET},

		// Job management
		{"/_status/job_registry/{node_id}", r.jobRegistryStatus, GET},
		{"/_status/job/{job_id}", r.jobStatus, GET},

		// Job profiler
		{"/_status/request_job_profiler_execution_details/{job_id}", r.requestJobProfilerExecutionDetails, GET},
		{"/_status/job_profiler_execution_details/{job_id}", r.getJobProfilerExecutionDetails, GET},
		{"/_status/list_job_profiler_execution_details/{job_id}", r.listJobProfilerExecutionDetails, GET},

		{"/_status/raft", r.raftDebug, GET},
		{"/_status/tenant_service_status", r.tenantServiceStatus, GET},
		{"/_status/connectivity", r.networkConnectivity, GET},
		{"/_status/throttling", r.getThrottlingMetadata, GET},
		{"/_status/span", r.spanStats, POST},
		{"/_status/critical_nodes", r.criticalNodes, POST},
		{"/_status/keyvissamples", r.keyVisSamples, POST},
	}

	// Register all routes with error handling wrapper
	for _, route := range routes {
		r.mux.HandleFunc(route.path, wrapHandler(route.handler)).Methods(string(route.method))
	}
}

// nodes handles GET /_status/nodes
func (r *apiInternalServer) nodes(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Nodes, &serverpb.NodesRequest{})
}

// ranges handles GET /_status/ranges/{node_id}
func (r *apiInternalServer) ranges(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Ranges, &serverpb.RangesRequest{NodeId: extractNodeID(req)})
}

// node handles GET /_status/nodes/{node_id}
func (r *apiInternalServer) node(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Node, &serverpb.NodeRequest{NodeId: extractNodeID(req)})
}

// details handles GET /_status/details/{node_id}
func (r *apiInternalServer) details(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Details, &serverpb.DetailsRequest{NodeId: extractNodeID(req)})
}

// certificates handles GET /_status/certificates/{node_id}
func (r *apiInternalServer) certificates(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Certificates, &serverpb.CertificatesRequest{NodeId: extractNodeID(req)})
}

// gossip handles GET /_status/gossip/{node_id}
func (r *apiInternalServer) gossip(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Gossip, &serverpb.GossipRequest{NodeId: extractNodeID(req)})
}

// metrics handles GET /_status/metrics/{node_id}
func (r *apiInternalServer) metrics(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Metrics, &serverpb.MetricsRequest{NodeId: extractNodeID(req)})
}

// nodesUI handles GET /_status/nodes_ui
func (r *apiInternalServer) nodesUI(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.NodesUI, &serverpb.NodesRequest{})
}

// nodeUI handles GET /_status/nodes_ui/{node_id}
func (r *apiInternalServer) nodeUI(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.NodeUI, &serverpb.NodeRequest{NodeId: extractNodeID(req)})
}

// raftDebug handles GET /_status/raft
func (r *apiInternalServer) raftDebug(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.RaftDebug, &serverpb.RaftDebugRequest{})
}

// tenantServiceStatus handles GET /_status/tenant_service_status
func (r *apiInternalServer) tenantServiceStatus(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.TenantServiceStatus, &serverpb.TenantServiceStatusRequest{})
}

// tenantRanges handles GET /_status/tenant_ranges
func (r *apiInternalServer) tenantRanges(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.TenantRanges, &serverpb.TenantRangesRequest{})
}

// engineStats handles GET /_status/enginestats/{node_id}
func (r *apiInternalServer) engineStats(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.EngineStats, &serverpb.EngineStatsRequest{NodeId: extractNodeID(req)})
}

// allocator handles GET /_status/allocator/node/{node_id}
func (r *apiInternalServer) allocator(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Allocator, &serverpb.AllocatorRequest{NodeId: extractNodeID(req)})
}

// allocatorRange handles GET /_status/allocator/range/{range_id}
func (r *apiInternalServer) allocatorRange(w http.ResponseWriter, req *http.Request) error {
	rangeID, err := getInt64Var(req, "range_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.status.AllocatorRange, &serverpb.AllocatorRangeRequest{RangeId: rangeID})
}

// profile handles GET /_status/profile/{node_id}
func (r *apiInternalServer) profile(w http.ResponseWriter, req *http.Request) error {
	reqMessage := &serverpb.ProfileRequest{NodeId: extractNodeID(req)}
	return executeRPC(w, req, r.status.Profile, reqMessage)
}

// getFiles handles GET /_status/files/{node_id}
func (r *apiInternalServer) getFiles(w http.ResponseWriter, req *http.Request) error {
	reqMessage := &serverpb.GetFilesRequest{NodeId: extractNodeID(req)}
	return executeRPC(w, req, r.status.GetFiles, reqMessage)
}

// logFilesList handles GET /_status/logfiles/{node_id}
func (r *apiInternalServer) logFilesList(w http.ResponseWriter, req *http.Request) error {
	reqMessage := &serverpb.LogFilesListRequest{NodeId: extractNodeID(req)}
	return executeRPC(w, req, r.status.LogFilesList, reqMessage)
}

// logFile handles GET /_status/logfiles/{node_id}/{file}
func (r *apiInternalServer) logFile(w http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	nodeID := extractNodeID(req) // Use existing helper function
	file := vars["file"]
	return executeRPC(w, req, r.status.LogFile, &serverpb.LogFileRequest{NodeId: nodeID, File: file})
}

// logs handles GET /_status/logs/{node_id}
func (r *apiInternalServer) logs(w http.ResponseWriter, req *http.Request) error {
	reqMessage := &serverpb.LogsRequest{NodeId: extractNodeID(req)}
	return executeRPC(w, req, r.status.Logs, reqMessage)
}

// problemRanges handles GET /_status/problemranges
func (r *apiInternalServer) problemRanges(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.ProblemRanges, &serverpb.ProblemRangesRequest{})
}

// downloadSpan handles GET /_status/downloadspans
func (r *apiInternalServer) downloadSpan(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.DownloadSpan, &serverpb.DownloadSpanRequest{})
}

// hotRangesV2 handles POST /_status/v2/hotranges
func (r *apiInternalServer) hotRangesV2(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.HotRangesV2, &serverpb.HotRangesRequest{})
}

// keyVisSamples handles POST /_status/keyvissamples
func (r *apiInternalServer) keyVisSamples(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.KeyVisSamples, &serverpb.KeyVisSamplesRequest{})
}

// rangeStatus handles GET /_status/range/{range_id}
func (r *apiInternalServer) rangeStatus(w http.ResponseWriter, req *http.Request) error {
	rangeID, err := getInt64Var(req, "range_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.status.Range, &serverpb.RangeRequest{RangeId: rangeID})
}

// diagnostics handles GET /_status/diagnostics/{node_id}
func (r *apiInternalServer) diagnostics(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Diagnostics, &serverpb.DiagnosticsRequest{NodeId: extractNodeID(req)})
}

// stores handles GET /_status/stores/{node_id}
func (r *apiInternalServer) stores(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Stores, &serverpb.StoresRequest{NodeId: extractNodeID(req)})
}

// statements handles GET /_status/statements
func (r *apiInternalServer) statements(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Statements, &serverpb.StatementsRequest{})
}

// combinedStatementStats handles GET /_status/combinedstmts
func (r *apiInternalServer) combinedStatementStats(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.CombinedStatementStats, &serverpb.CombinedStatementsStatsRequest{})
}

// statementDetails handles GET /_status/stmtdetails/{fingerprint_id}
func (r *apiInternalServer) statementDetails(w http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	fingerprintID := vars["fingerprint_id"]
	reqMessage := &serverpb.StatementDetailsRequest{FingerprintId: fingerprintID}
	return executeRPC(w, req, r.status.StatementDetails, reqMessage)
}

// createStatementDiagnosticsReport handles POST /_status/stmtdiagreports
func (r *apiInternalServer) createStatementDiagnosticsReport(
	w http.ResponseWriter, req *http.Request,
) error {
	return executeRPC(w, req, r.status.CreateStatementDiagnosticsReport,
		&serverpb.CreateStatementDiagnosticsReportRequest{})
}

// cancelStatementDiagnosticsReport handles POST /_status/stmtdiagreports/cancel
func (r *apiInternalServer) cancelStatementDiagnosticsReport(
	w http.ResponseWriter, req *http.Request,
) error {
	return executeRPC(w, req, r.status.CancelStatementDiagnosticsReport,
		&serverpb.CancelStatementDiagnosticsReportRequest{})
}

// statementDiagnosticsRequests handles GET /_status/stmtdiagreports
func (r *apiInternalServer) statementDiagnosticsRequests(
	w http.ResponseWriter, req *http.Request,
) error {
	return executeRPC(w, req, r.status.StatementDiagnosticsRequests,
		&serverpb.StatementDiagnosticsReportsRequest{})
}

// statementDiagnostics handles GET /_status/stmtdiag/{statement_diagnostics_id}
func (r *apiInternalServer) statementDiagnostics(w http.ResponseWriter, req *http.Request) error {
	statementDiagnosticsID, err := getInt64Var(req, "statement_diagnostics_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.status.StatementDiagnostics,
		&serverpb.StatementDiagnosticsRequest{StatementDiagnosticsId: statementDiagnosticsID})
}

// jobRegistryStatus handles GET /_status/job_registry/{node_id}
func (r *apiInternalServer) jobRegistryStatus(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.JobRegistryStatus, &serverpb.JobRegistryStatusRequest{NodeId: extractNodeID(req)})
}

// jobStatus handles GET /_status/job/{job_id}
func (r *apiInternalServer) jobStatus(w http.ResponseWriter, req *http.Request) error {
	jobID, err := getInt64Var(req, "job_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.status.JobStatus, &serverpb.JobStatusRequest{JobId: jobID})
}

// resetSQLStats handles POST /_status/resetsqlstats
func (r *apiInternalServer) resetSQLStats(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.ResetSQLStats, &serverpb.ResetSQLStatsRequest{})
}

// indexUsageStatistics handles GET /_status/indexusagestatistics
func (r *apiInternalServer) indexUsageStatistics(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.IndexUsageStatistics, &serverpb.IndexUsageStatisticsRequest{})
}

// resetIndexUsageStats handles POST /_status/resetindexusagestats
func (r *apiInternalServer) resetIndexUsageStats(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.ResetIndexUsageStats, &serverpb.ResetIndexUsageStatsRequest{})
}

// tableIndexStats handles GET /_status/databases/{database}/tables/{table}/indexstats
func (r *apiInternalServer) tableIndexStats(w http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	database := vars["database"]
	table := vars["table"]
	return executeRPC(w, req, r.status.TableIndexStats,
		&serverpb.TableIndexStatsRequest{Database: database, Table: table})
}

// userSQLRoles handles GET /_status/sqlroles
func (r *apiInternalServer) userSQLRoles(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.UserSQLRoles, &serverpb.UserSQLRolesRequest{})
}

// listSessions handles GET /_status/sessions
func (r *apiInternalServer) listSessions(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.ListSessions, &serverpb.ListSessionsRequest{})
}

// listLocalSessions handles GET /_status/local_sessions
func (r *apiInternalServer) listLocalSessions(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.ListLocalSessions, &serverpb.ListSessionsRequest{})
}

// cancelQuery handles POST /_status/cancel_query/{node_id}
func (r *apiInternalServer) cancelQuery(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.CancelQuery, &serverpb.CancelQueryRequest{NodeId: extractNodeID(req)})
}

// listContentionEvents handles GET /_status/contention_events
func (r *apiInternalServer) listContentionEvents(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.ListContentionEvents, &serverpb.ListContentionEventsRequest{})
}

// listLocalContentionEvents handles GET /_status/local_contention_events
func (r *apiInternalServer) listLocalContentionEvents(
	w http.ResponseWriter, req *http.Request,
) error {
	return executeRPC(w, req, r.status.ListLocalContentionEvents, &serverpb.ListContentionEventsRequest{})
}

// listDistSQLFlows handles GET /_status/distsql_flows
func (r *apiInternalServer) listDistSQLFlows(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.ListDistSQLFlows, &serverpb.ListDistSQLFlowsRequest{})
}

// listLocalDistSQLFlows handles GET /_status/local_distsql_flows
func (r *apiInternalServer) listLocalDistSQLFlows(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.ListLocalDistSQLFlows, &serverpb.ListDistSQLFlowsRequest{})
}

// cancelSession handles POST /_status/cancel_session/{node_id}
func (r *apiInternalServer) cancelSession(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.CancelSession, &serverpb.CancelSessionRequest{NodeId: extractNodeID(req)})
}

// spanStats handles POST /_status/span
func (r *apiInternalServer) spanStats(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.SpanStats, &roachpb.SpanStatsRequest{})
}

// criticalNodes handles POST /_status/critical_nodes
func (r *apiInternalServer) criticalNodes(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.CriticalNodes, &serverpb.CriticalNodesRequest{})
}

// stacks handles GET /_status/stacks/{node_id}
func (r *apiInternalServer) stacks(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.Stacks, &serverpb.StacksRequest{NodeId: extractNodeID(req)})
}

// transactionContentionEvents handles GET /_status/transactioncontentionevents
func (r *apiInternalServer) transactionContentionEvents(
	w http.ResponseWriter, req *http.Request,
) error {
	return executeRPC(w, req, r.status.TransactionContentionEvents,
		&serverpb.TransactionContentionEventsRequest{})
}

// networkConnectivity handles GET /_status/connectivity
func (r *apiInternalServer) networkConnectivity(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.NetworkConnectivity, &serverpb.NetworkConnectivityRequest{})
}

// requestJobProfilerExecutionDetails handles GET /_status/request_job_profiler_execution_details/{job_id}
func (r *apiInternalServer) requestJobProfilerExecutionDetails(
	w http.ResponseWriter, req *http.Request,
) error {
	jobID, err := getInt64Var(req, "job_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.status.RequestJobProfilerExecutionDetails,
		&serverpb.RequestJobProfilerExecutionDetailsRequest{JobId: jobID})
}

// getJobProfilerExecutionDetails handles GET /_status/job_profiler_execution_details/{job_id}
func (r *apiInternalServer) getJobProfilerExecutionDetails(
	w http.ResponseWriter, req *http.Request,
) error {
	jobID, err := getInt64Var(req, "job_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.status.GetJobProfilerExecutionDetails,
		&serverpb.GetJobProfilerExecutionDetailRequest{JobId: jobID})
}

// listJobProfilerExecutionDetails handles GET /_status/list_job_profiler_execution_details/{job_id}
func (r *apiInternalServer) listJobProfilerExecutionDetails(
	w http.ResponseWriter, req *http.Request,
) error {
	jobID, err := getInt64Var(req, "job_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.status.ListJobProfilerExecutionDetails,
		&serverpb.ListJobProfilerExecutionDetailsRequest{JobId: jobID})
}

// getThrottlingMetadata handles GET /_status/throttling
func (r *apiInternalServer) getThrottlingMetadata(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.status.GetThrottlingMetadata, &serverpb.GetThrottlingMetadataRequest{})
}
