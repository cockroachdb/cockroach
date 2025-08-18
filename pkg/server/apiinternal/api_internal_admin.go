// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/gorilla/mux"
)

// registerAdminRoutes sets up all the admin REST endpoints.
func (r *apiInternalServer) registerAdminRoutes() {
	routes := []route{
		{"/_admin/v1/users", r.users, GET},

		{"/_admin/v1/databases", r.databases, GET},
		{"/_admin/v1/databases/{database}", r.databaseDetails, GET},
		{"/_admin/v1/databases/{database}/tables/{table}", r.tableDetails, GET},
		{"/_admin/v1/databases/{database}/tables/{table}/stats", r.tableStats, GET},

		{"/_admin/v1/nontablestats", r.nonTableStats, GET},
		{"/_admin/v1/metricmetadata", r.allMetricMetadata, GET},
		{"/_admin/v1/chartcatalog", r.chartCatalog, GET},

		{"/_admin/v1/events", r.events, GET},
		{"/_admin/v1/uidata", r.getUIData, GET},
		{"/_admin/v1/uidata", r.setUIData, POST},

		{"/_admin/v1/cluster", r.cluster, GET},
		{"/_admin/v1/settings", r.settings, GET},
		{"/_admin/v1/health", r.health, GET},
		{"/_admin/v1/liveness", r.liveness, GET},

		{"/_admin/v1/jobs", r.jobs, GET},
		{"/_admin/v1/jobs/{job_id}", r.job, GET},

		{"/_admin/v1/locations", r.locations, GET},
		{"/_admin/v1/data_distribution", r.dataDistribution, GET},

		{"/_admin/v1/queryplan", r.queryPlan, GET},

		{"/_admin/v1/rangelog", r.rangeLog, GET},
		{"/_admin/v1/rangelog/{range_id}", r.rangeLogWithID, GET},
		{"/_admin/v1/enqueue_range", r.enqueueRange, POST},

		// Tracing and diagnostics
		{"/_admin/v1/trace_snapshots", r.listTracingSnapshots, GET},
		{"/_admin/v1/trace_snapshots", r.takeTracingSnapshot, POST},
		{"/_admin/v1/trace_snapshots/{snapshot_id}", r.getTracingSnapshot, GET},
		{"/_admin/v1/traces", r.getTrace, POST},
		{"/_admin/v1/settracerecordingtype", r.setTraceRecordingType, POST},
		{"/_admin/v1/stmtbundle/{id}", r.getStatementBundle, GET},

		{"/_admin/v1/tenants", r.listTenants, GET},
	}

	// Register all routes with error handling wrapper
	for _, route := range routes {
		r.mux.HandleFunc(route.path, wrapHandler(route.handler)).Methods(string(route.method))
	}
}

// getStatementBundle handles GET /_admin/v1/stmtbundle/{id}
// This endpoint requires special handling as it streams binary data directly.
func (r *apiInternalServer) getStatementBundle(w http.ResponseWriter, req *http.Request) error {
	id, err := getInt64Var(req, "id")
	if err != nil {
		return err
	}
	r.getStatementBundleServer.GetStatementBundle(req, id, w)
	return nil
}

// users handles GET /_admin/users
func (r *apiInternalServer) users(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Users, &serverpb.UsersRequest{})
}

// databases handles GET /_admin/databases
func (r *apiInternalServer) databases(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Databases, &serverpb.DatabasesRequest{})
}

// databaseDetails handles GET /_admin/databases/{database}
func (r *apiInternalServer) databaseDetails(w http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	database := vars["database"]
	return executeRPC(w, req, r.admin.DatabaseDetails, &serverpb.DatabaseDetailsRequest{Database: database})
}

// tableDetails handles GET /_admin/databases/{database}/tables/{table}
func (r *apiInternalServer) tableDetails(w http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	database := vars["database"]
	table := vars["table"]
	return executeRPC(w, req, r.admin.TableDetails, &serverpb.TableDetailsRequest{Database: database, Table: table})
}

// tableStats handles GET /_admin/databases/{database}/tables/{table}/stats
func (r *apiInternalServer) tableStats(w http.ResponseWriter, req *http.Request) error {
	vars := mux.Vars(req)
	database := vars["database"]
	table := vars["table"]
	return executeRPC(w, req, r.admin.TableStats, &serverpb.TableStatsRequest{Database: database, Table: table})
}

// nonTableStats handles GET /_admin/nontablestats
func (r *apiInternalServer) nonTableStats(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.NonTableStats, &serverpb.NonTableStatsRequest{})
}

// allMetricMetadata handles GET /_admin/metricmetadata
func (r *apiInternalServer) allMetricMetadata(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.AllMetricMetadata, &serverpb.MetricMetadataRequest{})
}

// chartCatalog handles GET /_admin/chartcatalog
func (r *apiInternalServer) chartCatalog(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.ChartCatalog, &serverpb.ChartCatalogRequest{})
}

// events handles GET /_admin/events
func (r *apiInternalServer) events(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Events, &serverpb.EventsRequest{})
}

// getUIData handles GET /_admin/uidata
func (r *apiInternalServer) getUIData(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.GetUIData, &serverpb.GetUIDataRequest{})
}

// setUIData handles POST /_admin/uidata
func (r *apiInternalServer) setUIData(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.SetUIData, &serverpb.SetUIDataRequest{})
}

// cluster handles GET /_admin/cluster
func (r *apiInternalServer) cluster(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Cluster, &serverpb.ClusterRequest{})
}

// settings handles GET /_admin/settings
func (r *apiInternalServer) settings(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Settings, &serverpb.SettingsRequest{})
}

// health handles GET /_admin/health
func (r *apiInternalServer) health(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Health, &serverpb.HealthRequest{})
}

// liveness handles GET /_admin/liveness
func (r *apiInternalServer) liveness(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Liveness, &serverpb.LivenessRequest{})
}

// jobs handles GET /_admin/jobs
func (r *apiInternalServer) jobs(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Jobs, &serverpb.JobsRequest{})
}

// job handles GET /_admin/jobs/{job_id}
func (r *apiInternalServer) job(w http.ResponseWriter, req *http.Request) error {
	jobID, err := getInt64Var(req, "job_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.admin.Job, &serverpb.JobRequest{JobId: jobID})
}

// locations handles GET /_admin/locations
func (r *apiInternalServer) locations(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.Locations, &serverpb.LocationsRequest{})
}

// queryPlan handles GET /_admin/queryplan
func (r *apiInternalServer) queryPlan(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.QueryPlan, &serverpb.QueryPlanRequest{})
}

// rangeLog handles GET /_admin/rangelog
func (r *apiInternalServer) rangeLog(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.RangeLog, &serverpb.RangeLogRequest{})
}

// rangeLogWithID handles GET /_admin/rangelog/{range_id}
func (r *apiInternalServer) rangeLogWithID(w http.ResponseWriter, req *http.Request) error {
	rangeID, err := getInt64Var(req, "range_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.admin.RangeLog, &serverpb.RangeLogRequest{RangeId: rangeID})
}

// dataDistribution handles GET /_admin/data_distribution
func (r *apiInternalServer) dataDistribution(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.DataDistribution, &serverpb.DataDistributionRequest{})
}

// enqueueRange handles POST /_admin/enqueue_range
func (r *apiInternalServer) enqueueRange(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.EnqueueRange, &serverpb.EnqueueRangeRequest{})
}

// listTracingSnapshots handles GET /_admin/trace_snapshots
func (r *apiInternalServer) listTracingSnapshots(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.ListTracingSnapshots, &serverpb.ListTracingSnapshotsRequest{})
}

// takeTracingSnapshot handles POST /_admin/trace_snapshots
func (r *apiInternalServer) takeTracingSnapshot(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.TakeTracingSnapshot, &serverpb.TakeTracingSnapshotRequest{})
}

// getTracingSnapshot handles GET /_admin/trace_snapshots/{snapshot_id}
func (r *apiInternalServer) getTracingSnapshot(w http.ResponseWriter, req *http.Request) error {
	snapshotID, err := getInt64Var(req, "snapshot_id")
	if err != nil {
		return err
	}
	return executeRPC(w, req, r.admin.GetTracingSnapshot, &serverpb.GetTracingSnapshotRequest{SnapshotId: snapshotID})
}

// getTrace handles POST /_admin/traces
func (r *apiInternalServer) getTrace(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.GetTrace, &serverpb.GetTraceRequest{})
}

// setTraceRecordingType handles POST /_admin/settracerecordingtype
func (r *apiInternalServer) setTraceRecordingType(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.SetTraceRecordingType, &serverpb.SetTraceRecordingTypeRequest{})
}

// listTenants handles GET /_admin/tenants
func (r *apiInternalServer) listTenants(w http.ResponseWriter, req *http.Request) error {
	return executeRPC(w, req, r.admin.ListTenants, &serverpb.ListTenantsRequest{})
}
