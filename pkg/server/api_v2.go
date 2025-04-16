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
	"math"
	"net/http"
	"sort"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	restartSafetyCheck(w http.ResponseWriter, r *http.Request)
	listNodes(w http.ResponseWriter, r *http.Request)
	listNodeRanges(w http.ResponseWriter, r *http.Request)
	planDrain(w http.ResponseWriter, r *http.Request)
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
var _ http.Handler = &apiV2SystemServer{}

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
		{"drain/plan/", systemRoutes.planDrain, true, authserver.ViewClusterMetadataRole, false},
		{"health/restart_safety/", systemRoutes.restartSafetyCheck, false, authserver.RegularRole, false},
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

func (a *apiV2Server) planDrain(w http.ResponseWriter, r *http.Request) {
	apiutil.WriteJSONResponse(r.Context(), w, http.StatusNotImplemented, nil)
}

func (a *apiV2SystemServer) planDrain(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = authserver.ForwardHTTPAuthInfoToRPCCalls(ctx, r)

	// get the nodes we've already handled
	nodeStrs, ok := r.URL.Query()["doneNodes"]
	doneNodes := make(map[roachpb.NodeID]struct{})
	if ok && len(nodeStrs) > 0 {
		for _, nodeStr := range nodeStrs {
			nID, err := strconv.Atoi(nodeStr)
			if err != nil {
				apiutil.WriteJSONResponse(ctx, w, http.StatusBadRequest, fmt.Errorf("invalid node ID: %s", nodeStr))
				return
			}

			doneNodes[roachpb.NodeID(nID)] = struct{}{}
		}
	}

	// parse the capacityTarget
	capacityTarget := 0.0
	capTargetStr := r.URL.Query().Get("capacityTarget")
	if capTargetStr != "" {
		var err error
		capacityTarget, err = strconv.ParseFloat(capTargetStr, 64)
		if err != nil {
			apiutil.WriteJSONResponse(ctx, w, http.StatusBadRequest, fmt.Errorf("invalid capacityTarget: %s", capTargetStr))
			return
		}
	}

	batch, moreBatches, err := planRollingRestart(ctx, doneNodes, capacityTarget, a.systemStatus)
	if err != nil {
		apiutil.WriteJSONResponse(ctx, w, http.StatusInternalServerError, err)
		return
	}

	resp := &serverpb.RestartPlanBatch{
		Batch:       batch,
		MoreBatches: moreBatches,
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, resp)
}

// planRollingRestart is an iterator over nodes. It produces nodes in batches. To be included in the next batch, a
// node must have healthy ranges, and none of its ranges can overlap any other node in the batch. Due to changes in
// range allocation, or outages that impact range health, each node in the batch should be checked for drain safety
// before being drained.
//
// In a large cluster with several localities, it's likely that the batch will consist of nodes in one locality, due
// to the range overlap constraints. This is not guaranteed, but the goal here is to enable safe parallel draining and
// restart operations on a running cluster, within capacity constraints.
//
// `doneNodes` is the set of nodes that should not be considered, presumably because they've already been restarted.
// `capacityTarget` impacts the size of the batch. We assume that the cluster needs this much capacity to continue
// without disruption, and limit the size of the batch to `totalNodes * (1.0 - capacityTarget)`. capacityTarget must be
// less than 1.0. Targets less than 0.66 are treated as 0.8, since taking down more than a third of the cluster is
// definitely a bad idea. That said, the minimum batch size limit is 1.
//
// The returned batch may be empty, or could be less than the batch size limit, if range health restricts it.
// It's assumed that calls to this method will be well-separated in time. Don't poll in a tight loop; wait long enough
// for cluster conditions to change before checking again.
//
// Since an empty batch does not indicate the end of iteration, there's a separate moreBatches return value.
func planRollingRestart(
	ctx context.Context,
	doneNodes map[roachpb.NodeID]struct{},
	capacityTarget float64,
	systemStatus *systemStatusServer,
) (batch []*roachpb.NodeDescriptor, moreBatches bool, err error) {
	if capacityTarget >= 1.0 {
		return nil, false, fmt.Errorf("capacity target must be less than 1.0")
	}
	if capacityTarget < 0.66 {
		capacityTarget = 0.8
	}

	totalNodes := systemStatus.storePool.ClusterNodeCount()
	nodesPerRound := int(math.Floor(float64(totalNodes) * (1.0 - capacityTarget)))
	if nodesPerRound < 1 {
		nodesPerRound = 1
	}

	nodeVitality := systemStatus.nodeLiveness.ScanNodeVitalityFromCache()

	// track a set of ranges that are problematic, either because they're under-replicated, or about to be.
	problemRanges := make(map[roachpb.RangeID]struct{})
	// start with all live-and-not-draining nodes as candidates
	candidateNodes := make(map[roachpb.NodeID]struct{})
	for nID, nv := range nodeVitality {
		if nv.IsLive(livenesspb.AdminHealthCheck) {
			// node is available and not draining
			candidateNodes[nID] = struct{}{}
		} else if nv.IsDecommissioned() || nv.IsDecommissioning() {
			// remove decommissioned nodes from done nodes, as we don't count them among the totalNodes
			delete(doneNodes, nID)

			if totalNodes == len(doneNodes) {
				// Apparently the last candidate for the rolling restart got decommissioned instead
				return nil, false, nil
			}
		} else if nv.IsDraining() {
			// count draining nodes against the disruption budget
			nodesPerRound -= 1
			if nodesPerRound < 1 {
				// We're already using our disruption budget to drain nodes
				return nil, true, nil
			}

			// If the node is already draining, consider its ranges to be problemRanges
			rr, err := systemStatus.Ranges(ctx, &serverpb.RangesRequest{
				NodeId: strconv.Itoa(int(nID)),
			})
			if err != nil {
				return nil, false, err
			}

			for _, r := range rr.Ranges {
				problemRanges[r.State.Desc.RangeID] = struct{}{}
			}
		}
	}

	candidateToRanges := make(map[roachpb.NodeID][]roachpb.RangeID)
	for nID := range candidateNodes {
		rr, err := systemStatus.Ranges(ctx, &serverpb.RangesRequest{
			NodeId: strconv.Itoa(int(nID)),
		})
		if err != nil {
			return nil, false, err
		}

		var rangeIDs []roachpb.RangeID
		// check the ranges on the node for problems
		for _, r := range rr.Ranges {
			rangeID := r.State.Desc.RangeID
			rangeIDs = append(rangeIDs, rangeID)
			_, marked := problemRanges[rangeID]

			if marked {
				delete(candidateNodes, nID)
				continue
			}

			if r.Problems.Underreplicated || r.Problems.Unavailable {
				problemRanges[rangeID] = struct{}{}
				delete(candidateNodes, nID)
			}
		}

		// at this point, if nID is still a candidate, it has no ranges that are under-replicated, or already draining
		_, isDone := doneNodes[nID]
		if _, ok := candidateNodes[nID]; ok && !isDone {
			candidateToRanges[nID] = rangeIDs
		}
	}

	// Fetch the node metadata for all the surviving candidates
	candidateNodeDescs := make([]roachpb.NodeDescriptor, 0, len(candidateToRanges))
	for nID := range candidateToRanges {
		nodeStr := strconv.Itoa(int(nID))
		node, err := systemStatus.Node(ctx, &serverpb.NodeRequest{
			NodeId: nodeStr,
			Redact: false,
		})
		if err != nil {
			return nil, false, err
		}

		candidateNodeDescs = append(candidateNodeDescs, node.Desc)
	}

	// Sort candidates by node locality, so that we tend to succeed at finding large, disjoint groups of nodes
	sort.Slice(candidateNodeDescs, func(i, j int) bool {
		// compare by locality, then nodeID
		l := candidateNodeDescs[i].Locality.Tiers
		r := candidateNodeDescs[j].Locality.Tiers

		for k, lt := range l {
			if k < len(r) {
				// less if key is less
				if lt.Key < r[k].Key {
					return true
				} else if lt.Key > r[k].Key {
					return false
				}

				// less if value is less
				if lt.Value < r[k].Value {
					return true
				} else if lt.Value > r[k].Value {
					return false
				}
			} else {
				// greater if r is shorter and everything matches up to here
				return false
			}
		}

		if len(l) < len(r) {
			// less if l is shorter and the kv pairs match
			return true
		}

		// nodeID as a tie breaker
		return candidateNodeDescs[i].NodeID < candidateNodeDescs[j].NodeID
	})

	// go through the candidates, and return the first ones that are disjoint.
	toRestart := make([]*roachpb.NodeDescriptor, 0, nodesPerRound)
	for _, node := range candidateNodeDescs {
		nID := node.NodeID
		rangeIDs := candidateToRanges[nID]

		stillOk := true
		for _, rID := range rangeIDs {
			if _, ok := problemRanges[rID]; ok {
				stillOk = false
				break
			}
		}
		if stillOk {
			// node doesn't share ranges with any nodes already in the toRestart list
			// add it to the list, then mark its ranges as problems so subsequent list elements will also be disjoint with it
			toRestart = append(toRestart, &node)

			for _, rID := range rangeIDs {
				problemRanges[rID] = struct{}{}
			}
		}

		if len(toRestart) >= nodesPerRound {
			break
		}
	}

	moreBatches = len(toRestart)+len(doneNodes) < totalNodes

	return toRestart, moreBatches, nil
}

func (a *apiV2Server) restartSafetyCheck(w http.ResponseWriter, r *http.Request) {
	apiutil.WriteJSONResponse(r.Context(), w, http.StatusNotImplemented, nil)
}

// # Quorum Guardrails
//
// Endpoint to expose node criticality information. A 200 response indicates that terminating the node in
// question won't cause any ranges to become unavailable, at the time the response was prepared. Users may
// use this check as a precondition for advancing a rolling restart process.
func (a *apiV2SystemServer) restartSafetyCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	nodeInQuestion := a.systemStatus.node.Descriptor.NodeID

	res, err := checkRestartSafe(ctx, nodeInQuestion, a.systemStatus)
	if err != nil {
		http.Error(w, "Error checking store status", http.StatusInternalServerError)
		return
	}

	if res.IsCritical {
		apiutil.WriteJSONResponse(ctx, w, http.StatusServiceUnavailable, res)
		return
	}
	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, res)
}

func checkRestartSafe(
	ctx context.Context, nodeInQuestion roachpb.NodeID, systemStatus *systemStatusServer,
) (*serverpb.DrainCheckResponse, error) {
	res := &serverpb.DrainCheckResponse{
		IsCritical: false,
		NodeID:     int32(nodeInQuestion),
	}

	isLiveMap := systemStatus.nodeLiveness.ScanNodeVitalityFromCache()
	err := systemStatus.stores.VisitStores(func(store *kvserver.Store) error {
		if int32(store.NodeID()) != res.NodeID {
			return nil
		}

		// for each store on the nodeInQuestion
		now := store.Clock().NowAsClockTimestamp()
		nodeCount := systemStatus.storePool.ClusterNodeCount()
		store.VisitReplicas(func(replica *kvserver.Replica) bool {
			// check that its replicas are non-critical
			metrics := replica.Metrics(ctx, now, isLiveMap, nodeCount)
			id := replica.ID()

			statusString := ""
			switch {
			case metrics.Unavailable:
				statusString = "Unavailable"
			case metrics.Underreplicated:
				statusString = "Underreplicated"
			case metrics.Leader:
				statusString = "IsRaftLeader"
			case !store.IsDraining():
				statusString = "StoreNotDraining"
			default:
				return true
			}
			status := serverpb.ReplicaStatus{
				StoreID:   int32(store.StoreID()),
				RangeID:   int32(id.RangeID),
				ReplicaID: int32(id.ReplicaID),
				Status:    statusString,
			}

			res.Replicas = append(res.Replicas, &status)
			res.IsCritical = true

			return true
		})
		return nil
	})
	return res, err
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
