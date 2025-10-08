// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

// registerAdminRoutes sets up all admin REST endpoints.
func (r *apiInternalServer) registerAdminRoutes() {
	routes := []route{
		// User and database metadata.
		{GET, "/_admin/v1/users", createHandler(r.admin.Users)},
		{GET, "/_admin/v1/databases", createHandler(r.admin.Databases)},
		{GET, "/_admin/v1/databases/{database}", createHandler(r.admin.DatabaseDetails)},
		{GET, "/_admin/v1/databases/{database}/tables/{table}", createHandler(r.admin.TableDetails)},
		{GET, "/_admin/v1/databases/{database}/tables/{table}/stats", createHandler(r.admin.TableStats)},
		{GET, "/_admin/v1/nontablestats", createHandler(r.admin.NonTableStats)},
		{GET, "/_admin/v1/events", createHandler(r.admin.Events)},

		// Admin UI persisted state.
		{POST, "/_admin/v1/uidata", createHandler(r.admin.SetUIData)},
		{GET, "/_admin/v1/uidata", createHandler(r.admin.GetUIData)},

		// Cluster-level information.
		{GET, "/_admin/v1/cluster", createHandler(r.admin.Cluster)},
		{GET, "/_admin/v1/settings", createHandler(r.admin.Settings)},
		{GET, "/_admin/v1/health", createHandler(r.admin.Health)},
		{GET, "/health", createHandler(r.admin.Health)},
		{GET, "/_admin/v1/liveness", createHandler(r.admin.Liveness)},

		// Job and node management.
		{GET, "/_admin/v1/jobs", createHandler(r.admin.Jobs)},
		{GET, "/_admin/v1/jobs/{job_id}", createHandler(r.admin.Job)},
		{GET, "/_admin/v1/locations", createHandler(r.admin.Locations)},
		{GET, "/_admin/v1/queryplan", createHandler(r.admin.QueryPlan)},

		// Range and replication utilities.
		{GET, "/_admin/v1/rangelog", createHandler(r.admin.RangeLog)},
		{GET, "/_admin/v1/rangelog/{range_id}", createHandler(r.admin.RangeLog)},
		{GET, "/_admin/v1/data_distribution", createHandler(r.admin.DataDistribution)},
		{GET, "/_admin/v1/metricmetadata", createHandler(r.admin.AllMetricMetadata)},
		{GET, "/_admin/v1/chartcatalog", createHandler(r.admin.ChartCatalog)},
		{POST, "/_admin/v1/enqueue_range", createHandler(r.admin.EnqueueRange)},

		// Tracing utilities.
		{GET, "/_admin/v1/trace_snapshots", createHandler(r.admin.ListTracingSnapshots)},
		{POST, "/_admin/v1/trace_snapshots", createHandler(r.admin.TakeTracingSnapshot)},
		{GET, "/_admin/v1/trace_snapshots/{snapshot_id}", createHandler(r.admin.GetTracingSnapshot)},
		{POST, "/_admin/v1/traces", createHandler(r.admin.GetTrace)},
		{POST, "/_admin/v1/settracerecordingtype", createHandler(r.admin.SetTraceRecordingType)},

		// Multi-tenant metadata.
		{GET, "/_admin/v1/tenants", createHandler(r.admin.ListTenants)},
	}

	for _, route := range routes {
		r.mux.HandleFunc(route.path, route.handler).Methods(string(route.method))
	}
}
