// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// CockroachDB v2 API
//
// API for querying information about CockroachDB health, nodes, ranges,
// sessions, and other meta entities.
//
//	Schemes: http, https
//	Host: localhost
//	BasePath: /api/v2/
//	Version: 2.0.0
//	License: Business Source License
//
//	Produces:
//	- application/json
//
//	SecurityDefinitions:
//	  api_session:
//	     type: apiKey
//	     name: X-Cockroach-API-Session
//	     description: Handle to logged-in REST session. Use `/login/` to
//	       log in and get a session.
//	     in: header
package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/redact"
	"github.com/gorilla/mux"
)

// Path variables.
const (
	dbIdPathVar    = "database_id"
	tableIdPathVar = "table_id"
)

type ApiV2System interface {
	health(w http.ResponseWriter, r *http.Request)
	listNodes(w http.ResponseWriter, r *http.Request)
	listNodeRanges(w http.ResponseWriter, r *http.Request)
}

type apiV2ServerOpts struct {
	admin            serverpb.AdminServer
	status           serverpb.StatusServer
	promRuleExporter *metric.PrometheusRuleExporter
	sqlServer        *SQLServer
	db               *kv.DB
}

// apiV2Server implements version 2 API endpoints, under apiconstants.APIV2Path. The
// implementation of some endpoints is delegated to sub-servers (eg. auth
// endpoints like `/login` and `/logout` are passed onto authServer), while
// others are implemented directly by apiV2Server.
//
// To register a new API endpoint, add it to the route definitions in
// registerRoutes().
type apiV2Server struct {
	admin            *adminServer
	authServer       authserver.ServerV2
	status           *statusServer
	promRuleExporter *metric.PrometheusRuleExporter
	mux              *mux.Router
	sqlServer        *SQLServer
	db               *kv.DB
}

var _ ApiV2System = &apiV2Server{}
var _ http.Handler = &apiV2Server{}

type apiV2SystemServer struct {
	*apiV2Server

	systemAdmin  *systemAdminServer
	systemStatus *systemStatusServer
}

var _ ApiV2System = &apiV2SystemServer{}
var _ http.Handler = &apiV2Server{}

// newAPIV2Server returns a new apiV2Server.
func newAPIV2Server(ctx context.Context, opts *apiV2ServerOpts) http.Handler {
	authServer := authserver.NewV2Server(ctx, opts.sqlServer, opts.sqlServer.cfg.Config, apiconstants.APIV2Path)
	innerMux := mux.NewRouter()
	allowAnonymous := opts.sqlServer.cfg.Insecure
	authMux := authserver.NewV2Mux(authServer, innerMux, allowAnonymous)
	outerMux := mux.NewRouter()
	serverMetrics := NewServerHttpMetrics(opts.sqlServer.MetricsRegistry(), opts.sqlServer.execCfg.Settings)
	serverMetrics.registerMetricsMiddleware(outerMux)

	systemAdmin, saOk := opts.admin.(*systemAdminServer)
	systemStatus, ssOk := opts.status.(*systemStatusServer)
	if saOk && ssOk {
		inner := &apiV2Server{
			admin:            systemAdmin.adminServer,
			authServer:       authServer,
			status:           systemStatus.statusServer,
			mux:              outerMux,
			promRuleExporter: opts.promRuleExporter,
			sqlServer:        opts.sqlServer,
			db:               opts.db,
		}
		a := &apiV2SystemServer{
			apiV2Server:  inner,
			systemAdmin:  systemAdmin,
			systemStatus: systemStatus,
		}
		registerRoutes(innerMux, authMux, inner, a)
		return a
	} else {
		a := &apiV2Server{
			admin:            opts.admin.(*adminServer),
			authServer:       authServer,
			status:           opts.status.(*statusServer),
			mux:              outerMux,
			promRuleExporter: opts.promRuleExporter,
			sqlServer:        opts.sqlServer,
			db:               opts.db,
		}
		registerRoutes(innerMux, authMux, a, a)
		return a
	}
}

// registerRoutes registers endpoints under the current API server.
func registerRoutes(
	innerMux *mux.Router, authMux http.Handler, a *apiV2Server, systemRoutes ApiV2System,
) {
	// Add any new API endpoint definitions here, even if a sub-server handles
	// them. Arguments:
	//
	// - `url` is the path string that, if matched by the user request, is
	//    routed to this endpoint. Pattern-matching handled by gorilla.Mux; see
	//    https://github.com/gorilla/mux#matching-routes for supported patterns.
	// - `handler` is the http.HandlerFunc to be called if this endpoint url
	//    matches.
	// - `requiresAuth` is a bool that denotes whether this endpoint requires
	//    authentication. If the user isn't authenticated, an HTTP 401 error is
	//    returned. If the user is authenticated, the http.Request's context
	//    contains the current user's username.
	// - `role` and `option` are used to determine if the current user is
	//    authorized to access this endpoint. If the user is not at least of type
	//    `role`, or does not have the roleoption `option`, an HTTP 403 forbidden
	//    error is returned.
	routeDefinitions := []struct {
		url           string
		handler       http.HandlerFunc
		requiresAuth  bool
		role          authserver.APIRole
		tenantEnabled bool
	}{
		// Pass through auth-related endpoints to the auth server.
		{"login/", a.authServer.ServeHTTP, false /* requiresAuth */, authserver.RegularRole, false},
		{"logout/", a.authServer.ServeHTTP, false /* requiresAuth */, authserver.RegularRole, false},

		// Directly register other endpoints in the api server.
		{"sessions/", a.listSessions, true /* requiresAuth */, authserver.ViewClusterMetadataRole, false},
		{"nodes/", systemRoutes.listNodes, true, authserver.ViewClusterMetadataRole, false},
		// Any endpoint returning range information requires an admin user. This is because range start/end keys
		// are sensitive info.
		{"nodes/{node_id}/ranges/", systemRoutes.listNodeRanges, true, authserver.ViewClusterMetadataRole, false},
		{"ranges/hot/", a.listHotRanges, true, authserver.ViewClusterMetadataRole, false},
		{"ranges/{range_id:[0-9]+}/", a.listRange, true, authserver.ViewClusterMetadataRole, false},
		{"health/", systemRoutes.health, false, authserver.RegularRole, false},
		{"users/", a.listUsers, true, authserver.RegularRole, false},
		{"events/", a.listEvents, true, authserver.ViewClusterMetadataRole, false},
		{"databases/", a.listDatabases, true, authserver.RegularRole, false},
		{"databases/{database_name:[\\w.]+}/", a.databaseDetails, true, authserver.RegularRole, false},
		{"databases/{database_name:[\\w.]+}/grants/", a.databaseGrants, true, authserver.RegularRole, false},
		{"databases/{database_name:[\\w.]+}/tables/", a.databaseTables, true, authserver.RegularRole, false},
		{"databases/{database_name:[\\w.]+}/tables/{table_name:[\\w.]+}/", a.tableDetails, true, authserver.RegularRole, false},
		{"rules/", a.listRules, false, authserver.RegularRole, true},

		{"sql/", a.execSQL, true, authserver.RegularRole, true},
		{"database_metadata/", a.GetDbMetadata, true, authserver.RegularRole, true},
		{"database_metadata/{database_id:[0-9]+}/", a.GetDbMetadataWithDetails, true, authserver.RegularRole, true},
		{"table_metadata/", a.GetTableMetadata, true, authserver.RegularRole, true},
		{"table_metadata/{table_id:[0-9]+}/", a.GetTableMetadataWithDetails, true, authserver.RegularRole, true},
		{"table_metadata/updatejob/", a.TableMetadataJob, true, authserver.RegularRole, true},
		{fmt.Sprintf("grants/databases/{%s:[0-9]+}/", dbIdPathVar), a.getDatabaseGrants, true, authserver.RegularRole, true},
		{fmt.Sprintf("grants/tables/{%s:[0-9]+}/", tableIdPathVar), a.getTableGrants, true, authserver.RegularRole, true},
	}

	// For all routes requiring authentication, have the outer mux (a.mux)
	// send requests through to the authMux, and also register the relevant route
	// in innerMux. Routes not requiring login can directly be handled in a.mux.
	for _, route := range routeDefinitions {
		var handler http.Handler
		handler = &callCountDecorator{
			counter: telemetry.GetCounter(fmt.Sprintf("api.v2.%s", route.url)),
			inner:   route.handler,
		}
		if !route.tenantEnabled && !a.sqlServer.execCfg.Codec.ForSystemTenant() {
			a.mux.Handle(apiconstants.APIV2Path+route.url, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "Not Available on Tenants", http.StatusNotImplemented)
			}))
		}

		// Tell the authz server how to connect to SQL.
		authzAccessorFactory := func(ctx context.Context, opName redact.SafeString) (sql.AuthorizationAccessor, func()) {
			txn := a.db.NewTxn(ctx, string(opName))
			p, cleanup := sql.NewInternalPlanner(
				opName,
				txn,
				username.NodeUserName(),
				&sql.MemoryMetrics{},
				a.sqlServer.execCfg,
				sql.NewInternalSessionData(ctx, a.sqlServer.execCfg.Settings, opName),
			)
			return p.(sql.AuthorizationAccessor), cleanup
		}

		if route.requiresAuth {
			a.mux.Handle(apiconstants.APIV2Path+route.url, authMux)
			if route.role != authserver.RegularRole {
				handler = authserver.NewRoleAuthzMux(authzAccessorFactory, route.role, handler)
			}
			innerMux.Handle(apiconstants.APIV2Path+route.url, handler)
		} else {
			a.mux.Handle(apiconstants.APIV2Path+route.url, handler)
		}
	}
}

// ServeHTTP implements the http.Handler interface.
func (a *apiV2Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	a.mux.ServeHTTP(w, req)
}

type callCountDecorator struct {
	counter telemetry.Counter
	inner   http.Handler
}

func (c *callCountDecorator) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	telemetry.Inc(c.counter)
	c.inner.ServeHTTP(w, req)
}

// Response for listSessions.
type listSessionsResponse struct {
	serverpb.ListSessionsResponse

	// The continuation token, for use in the next paginated call in the `start`
	// parameter.
	Next string `json:"next,omitempty"`
}

// # List sessions
//
// List all sessions on this cluster. If a username is provided, only
// sessions from that user are returned.
//
// Client must be logged-in as a user with admin privileges.
//
// ---
// parameters:
//   - name: username
//     type: string
//     in: query
//     description: Username of user to return sessions for; if unspecified,
//     sessions from all users are returned.
//     required: false
//   - name: exclude_closed_sessions
//     type: bool
//     in: query
//     description: Boolean to exclude closed sessions; if unspecified, defaults
//     to false and closed sessions are included in the response.
//     required: false
//   - name: limit
//     type: integer
//     in: query
//     description: Maximum number of results to return in this call.
//     required: false
//   - name: start
//     type: string
//     in: query
//     description: Continuation token for results after a past limited run.
//     required: false
//
// produces:
// - application/json
// security:
// - api_session: []
// responses:
//
//	"200":
//	  description: List sessions response.
//	  schema:
//	    "$ref": "#/definitions/listSessionsResp"
func (a *apiV2Server) listSessions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, start := getRPCPaginationValues(r)
	reqUsername := r.URL.Query().Get("username")
	reqExcludeClosed := r.URL.Query().Get("exclude_closed_sessions") == "true"
	req := &serverpb.ListSessionsRequest{Username: reqUsername, ExcludeClosedSessions: reqExcludeClosed}
	response := &listSessionsResponse{}
	outgoingCtx := authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)

	responseProto, pagState, err := a.status.listSessionsHelper(outgoingCtx, req, limit, start)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	var nextBytes []byte
	if nextBytes, err = pagState.MarshalText(); err != nil {
		err := serverpb.ListSessionsError{Message: err.Error()}
		response.Errors = append(response.Errors, err)
	} else {
		response.Next = string(nextBytes)
	}
	response.ListSessionsResponse = *responseProto
	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, response)
}

// # Check node health
//
// Helper endpoint to check for node health. If `ready` is true, it also checks
// if this node is fully operational and ready to accept SQL connections.
// Otherwise, this endpoint always returns a successful response (if the API
// server is up, of course).
//
// ---
// parameters:
//   - name: ready
//     type: boolean
//     in: query
//     description: If true, check whether this node is ready to accept SQL
//     connections. If false, this endpoint always returns success, unless
//     the API server itself is down.
//     required: false
//
// produces:
// - application/json
// responses:
//
//	"200":
//	  description: Indicates healthy node.
//	"500":
//	  description: Indicates unhealthy node.
func (a *apiV2SystemServer) health(w http.ResponseWriter, r *http.Request) {
	healthInternal(w, r, a.systemAdmin.checkReadinessForHealthCheck)
}

func healthInternal(
	w http.ResponseWriter, r *http.Request, checkReadinessForHealthCheck func(context.Context) error,
) {
	ready := false
	readyStr := r.URL.Query().Get("ready")
	if len(readyStr) > 0 {
		var err error
		ready, err = strconv.ParseBool(readyStr)
		if err != nil {
			http.Error(w, "invalid ready value", http.StatusBadRequest)
			return
		}
	}
	ctx := r.Context()
	resp := &serverpb.HealthResponse{}
	// If Ready is not set, the client doesn't want to know whether this node is
	// ready to receive client traffic.
	if !ready {
		apiutil.WriteJSONResponse(ctx, w, 200, resp)
		return
	}

	if err := checkReadinessForHealthCheck(ctx); err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	apiutil.WriteJSONResponse(ctx, w, 200, resp)
}

func (a *apiV2Server) health(w http.ResponseWriter, r *http.Request) {
	healthInternal(w, r, a.admin.checkReadinessForHealthCheck)
}

// # Get metric recording and alerting rule templates
//
// Endpoint to export recommended metric recording and alerting rules.
// These rules are intended to be used as a guideline for aggregating
// and using metrics for alerting purposes. All rules are in the
// YAML format compatible with Prometheus recording and alerting
// rules. All recording rules are grouped under 'rules/recording'
// label while alerting rules are grouped under 'rules/alerts'.
//
// ---
// produces:
// - text/plain
// responses:
//
//	"200":
//	  description: Recording and Alert Rules
//	  schema:
//	    "$ref": "#/definitions/PrometheusRuleGroup"
func (a *apiV2Server) listRules(w http.ResponseWriter, r *http.Request) {
	a.promRuleExporter.ScrapeRegistry(r.Context())
	response, err := a.promRuleExporter.PrintAsYAML()
	if err != nil {
		srverrors.APIV2InternalError(r.Context(), err, w)
		return
	}
	w.Header().Set(httputil.ContentTypeHeader, httputil.PlaintextContentType)
	_, _ = w.Write(response)
}
