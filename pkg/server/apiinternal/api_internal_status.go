// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

// registerStatusRoutes sets up all the status REST endpoints.
func (r *apiInternalServer) registerStatusRoutes() {
	routes := []route{
		// Node information
		{GET, "/_status/nodes", createHandler(r.status.Nodes)},
		{GET, "/_status/nodes/{node_id}", createHandler(r.status.Node)},
		{GET, "/_status/nodes_ui", createHandler(r.status.NodesUI)},
		{GET, "/_status/nodes_ui/{node_id}", createHandler(r.status.NodeUI)},

		// Node details and diagnostics
		{GET, "/_status/details/{node_id}", createHandler(r.status.Details)},
		{GET, "/_status/certificates/{node_id}", createHandler(r.status.Certificates)},
		{GET, "/_status/gossip/{node_id}", createHandler(r.status.Gossip)},
		{GET, "/_status/metrics/{node_id}", createHandler(r.status.Metrics)},
		{GET, "/_status/diagnostics/{node_id}", createHandler(r.status.Diagnostics)},
		{GET, "/_status/stores/{node_id}", createHandler(r.status.Stores)},

		// Node profiling and debugging
		{GET, "/_status/profile/{node_id}", createHandler(r.status.Profile)},
		{GET, "/_status/stacks/{node_id}", createHandler(r.status.Stacks)},
		{GET, "/_status/files/{node_id}", createHandler(r.status.GetFiles)},

		// Node logs
		{GET, "/_status/logfiles/{node_id}", createHandler(r.status.LogFilesList)},
		{GET, "/_status/logfiles/{node_id}/{file}", createHandler(r.status.LogFile)},
		{GET, "/_status/logs/{node_id}", createHandler(r.status.Logs)},

		// Range information
		{GET, "/_status/ranges/{node_id}", createHandler(r.status.Ranges)},
		{GET, "/_status/range/{range_id}", createHandler(r.status.Range)},
		{GET, "/_status/tenant_ranges", createHandler(r.status.TenantRanges)},
		{POST, "/_status/v2/hotranges", createHandler(r.status.HotRangesV2)},
		{GET, "/_status/problemranges", createHandler(r.status.ProblemRanges)},

		{GET, "/_status/allocator/node/{node_id}", createHandler(r.status.Allocator)},
		{GET, "/_status/allocator/range/{range_id}", createHandler(r.status.AllocatorRange)},

		{GET, "/_status/enginestats/{node_id}", createHandler(r.status.EngineStats)},
		{GET, "/_status/downloadspans", createHandler(r.status.DownloadSpan)},

		// Statement statistics
		{GET, "/_status/statements", createHandler(r.status.Statements)},
		{GET, "/_status/combinedstmts", createHandler(r.status.CombinedStatementStats)},
		{GET, "/_status/stmtdetails/{fingerprint_id}", createHandler(r.status.StatementDetails)},
		{POST, "/_status/resetsqlstats", createHandler(r.status.ResetSQLStats)},

		// Statement diagnostics
		{GET, "/_status/stmtdiagreports", createHandler(r.status.StatementDiagnosticsRequests)},
		{POST, "/_status/stmtdiagreports", createHandler(r.status.CreateStatementDiagnosticsReport)},
		{POST, "/_status/stmtdiagreports/cancel", createHandler(r.status.CancelStatementDiagnosticsReport)},
		{GET, "/_status/stmtdiag/{statement_diagnostics_id}", createHandler(r.status.StatementDiagnostics)},

		// Index statistics
		{GET, "/_status/indexusagestatistics", createHandler(r.status.IndexUsageStatistics)},
		{POST, "/_status/resetindexusagestats", createHandler(r.status.ResetIndexUsageStats)},
		{GET, "/_status/databases/{database}/tables/{table}/indexstats", createHandler(r.status.TableIndexStats)},

		// SQL sessions and queries
		{GET, "/_status/sessions", createHandler(r.status.ListSessions)},
		{GET, "/_status/local_sessions", createHandler(r.status.ListLocalSessions)},
		{POST, "/_status/cancel_query/{node_id}", createHandler(r.status.CancelQuery)},
		{POST, "/_status/cancel_session/{node_id}", createHandler(r.status.CancelSession)},

		// SQL roles and permissions
		{GET, "/_status/sqlroles", createHandler(r.status.UserSQLRoles)},

		// Contention events
		{GET, "/_status/contention_events", createHandler(r.status.ListContentionEvents)},
		{GET, "/_status/local_contention_events", createHandler(r.status.ListLocalContentionEvents)},
		{GET, "/_status/transactioncontentionevents", createHandler(r.status.TransactionContentionEvents)},

		{GET, "/_status/distsql_flows", createHandler(r.status.ListDistSQLFlows)},
		{GET, "/_status/local_distsql_flows", createHandler(r.status.ListLocalDistSQLFlows)},

		// Job management
		{GET, "/_status/job_registry/{node_id}", createHandler(r.status.JobRegistryStatus)},
		{GET, "/_status/job/{job_id}", createHandler(r.status.JobStatus)},

		// Job profiler
		{GET, "/_status/request_job_profiler_execution_details/{job_id}", createHandler(r.status.RequestJobProfilerExecutionDetails)},
		{GET, "/_status/job_profiler_execution_details/{job_id}", createHandler(r.status.GetJobProfilerExecutionDetails)},
		{GET, "/_status/list_job_profiler_execution_details/{job_id}", createHandler(r.status.ListJobProfilerExecutionDetails)},

		{GET, "/_status/raft", createHandler(r.status.RaftDebug)},
		{GET, "/_status/tenant_service_status", createHandler(r.status.TenantServiceStatus)},
		{GET, "/_status/connectivity", createHandler(r.status.NetworkConnectivity)},
		{GET, "/_status/throttling", createHandler(r.status.GetThrottlingMetadata)},
		{POST, "/_status/span", createHandler(r.status.SpanStats)},
		{POST, "/_status/critical_nodes", createHandler(r.status.CriticalNodes)},
		{POST, "/_status/keyvissamples", createHandler(r.status.KeyVisSamples)},
	}

	// Register all routes
	for _, route := range routes {
		r.mux.HandleFunc(route.path, route.handler).Methods(string(route.method))
	}
}
