// Copyright 2021 The Cockroach Authors.
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
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/gorilla/mux"
)

const (
	apiV2Path       = "/api/v2/"
	apiV2AuthHeader = "X-Cockroach-API-Session"
)

func writeJSONResponse(ctx context.Context, w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	res, err := json.Marshal(payload)
	if err != nil {
		apiV2InternalError(ctx, err, w)
	}
	_, _ = w.Write(res)
}

// apiV2Server implements version 2 API endpoints, under apiV2Path. The
// implementation of some endpoints is delegated to sub-servers (eg. auth
// endpoints like `/login` and `/logout` are passed onto authServer), while
// others are implemented directly by apiV2Server.
//
// To register a new API endpoint, add it to the route definitions in
// registerRoutes().
type apiV2Server struct {
	admin      *adminServer
	authServer *authenticationV2Server
	status     *statusServer
	mux        *mux.Router
}

// newAPIV2Server returns a new apiV2Server.
func newAPIV2Server(ctx context.Context, s *Server) *apiV2Server {
	authServer := newAuthenticationV2Server(ctx, s, apiV2Path)
	innerMux := mux.NewRouter()

	authMux := newAuthenticationV2Mux(authServer, innerMux)
	outerMux := mux.NewRouter()
	a := &apiV2Server{
		admin:      s.admin,
		authServer: authServer,
		status:     s.status,
		mux:        outerMux,
	}
	a.registerRoutes(innerMux, authMux)
	return a
}

// registerRoutes registers endpoints under the current API server.
func (a *apiV2Server) registerRoutes(innerMux *mux.Router, authMux http.Handler) {
	var noOption roleoption.Option

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
		url          string
		handler      http.HandlerFunc
		requiresAuth bool
		role         apiRole
		option       roleoption.Option
	}{
		// Pass through auth-related endpoints to the auth server.
		{"login/", a.authServer.ServeHTTP, false /* requiresAuth */, regularRole, noOption},
		{"logout/", a.authServer.ServeHTTP, false /* requiresAuth */, regularRole, noOption},

		// Directly register other endpoints in the api server.
		{"sessions/", a.listSessions, true /* requiresAuth */, adminRole, noOption},
		{"nodes/", a.listNodes, true, adminRole, noOption},
		{"nodes/{node_id}/ranges/", a.listNodeRanges, true, regularRole, noOption},
		{"ranges/hot/", a.listHotRanges, true, regularRole, noOption},
		{"ranges/{range_id:[0-9]+}/", a.listRange, true, regularRole, noOption},
		{"health/", a.health, false, regularRole, noOption},
	}

	// For all routes requiring authentication, have the outer mux (a.mux)
	// send requests through to the authMux, and also register the relevant route
	// in innerMux. Routes not requiring login can directly be handled in a.mux.
	for _, route := range routeDefinitions {
		var handler http.Handler
		handler = &callCountDecorator{
			counter: telemetry.GetCounter(fmt.Sprintf("api.v2.%s", route.url)),
			inner:   http.Handler(route.handler),
		}
		if route.requiresAuth {
			a.mux.Handle(apiV2Path+route.url, authMux)
			if route.role != regularRole {
				handler = &roleAuthorizationMux{
					ie:     a.admin.ie,
					role:   route.role,
					option: route.option,
					inner:  handler,
				}
			}
			innerMux.Handle(apiV2Path+route.url, handler)
		} else {
			a.mux.Handle(apiV2Path+route.url, handler)
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

type listSessionsResponse struct {
	serverpb.ListSessionsResponse

	Next string `json:"next,omitempty"`
}

func (a *apiV2Server) listSessions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, start := getRPCPaginationValues(r)
	reqUsername := r.URL.Query().Get("username")
	req := &serverpb.ListSessionsRequest{Username: reqUsername}
	response := &listSessionsResponse{}
	outgoingCtx := apiToOutgoingGatewayCtx(ctx, r)

	responseProto, pagState, err := a.status.listSessionsHelper(outgoingCtx, req, limit, start)
	if err != nil {
		apiV2InternalError(ctx, err, w)
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
	writeJSONResponse(ctx, w, http.StatusOK, response)
}

type nodeStatus struct {
	// Fields that are a subset of NodeDescriptor.
	NodeID          roachpb.NodeID      `json:"node_id"`
	Address         util.UnresolvedAddr `json:"address"`
	Attrs           roachpb.Attributes  `json:"attrs"`
	Locality        roachpb.Locality    `json:"locality"`
	ServerVersion   roachpb.Version     `json:"ServerVersion"`
	BuildTag        string              `json:"build_tag"`
	StartedAt       int64               `json:"started_at"`
	ClusterName     string              `json:"cluster_name"`
	SQLAddress      util.UnresolvedAddr `json:"sql_address"`


	// Other fields that are a subset of roachpb.NodeStatus.
	Metrics           map[string]float64 `json:"metrics,omitempty"`
	TotalSystemMemory int64              `json:"total_system_memory,omitempty"`
	NumCpus           int32              `json:"num_cpus,omitempty"`
	UpdatedAt         int64              `json:"updated_at,omitempty"`

	// Retrieved from the liveness status map.
	LivenessStatus livenesspb.NodeLivenessStatus `json:"liveness_status"`
}

type nodesResponse struct {
	Nodes []nodeStatus `json:"nodes"`
	Next int `json:"next,omitempty"`
}

func (a *apiV2Server) listNodes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset := getSimplePaginationValues(r)
	ctx = apiToOutgoingGatewayCtx(ctx, r)

	nodes, next, err := a.status.nodesHelper(ctx, limit, offset)
	if err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}
	var resp nodesResponse
	resp.Next = next
	for _, n := range nodes.Nodes {
		var cur nodeStatus
		cur.NodeID = n.Desc.NodeID
		cur.Address = n.Desc.Address
		cur.Attrs = n.Desc.Attrs
		cur.Locality = n.Desc.Locality
		cur.ServerVersion = n.Desc.ServerVersion
		cur.BuildTag = n.Desc.BuildTag
		cur.StartedAt = n.Desc.StartedAt
		cur.ClusterName = n.Desc.ClusterName
		cur.SQLAddress = n.Desc.SQLAddress

		cur.Metrics = n.Metrics
		cur.TotalSystemMemory = n.TotalSystemMemory
		cur.NumCpus = n.NumCpus
		cur.UpdatedAt = n.UpdatedAt
		cur.LivenessStatus = nodes.LivenessByNodeID[n.Desc.NodeID]

		resp.Nodes = append(resp.Nodes, cur)
	}
	writeJSONResponse(ctx, w, 200, resp)
}

func parseRangeIDs(input string, w http.ResponseWriter) (ranges []roachpb.RangeID, ok bool) {
	if len(input) == 0 {
		return nil, true
	}
	for _, reqRange := range strings.Split(input, ",") {
		rangeID, err := strconv.ParseInt(reqRange, 10, 64)
		if err != nil {
			http.Error(w, "invalid range ID", http.StatusBadRequest)
			return nil, false
		}

		ranges = append(ranges, roachpb.RangeID(rangeID))
	}
	return ranges, true
}

type nodeRangeResponse struct {
	RangeInfo rangeInfo `json:"range_info"`
	Error     string    `json:"error,omitempty"`
}

type rangeResponse struct {
	Responses map[roachpb.NodeID]nodeRangeResponse `json:"responses_by_node_id"`
}

func (a *apiV2Server) listRange(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = apiToOutgoingGatewayCtx(ctx, r)
	vars := mux.Vars(r)
	rangeID, err := strconv.ParseInt(vars["range_id"], 10, 64)
	if err != nil {
		http.Error(w, "invalid range ID", http.StatusBadRequest)
		return
	}

	response := &rangeResponse{
		Responses: make(map[roachpb.NodeID]nodeRangeResponse),
	}

	rangesRequest := &serverpb.RangesRequest{
		RangeIDs: []roachpb.RangeID{roachpb.RangeID(rangeID)},
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := a.status.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.Ranges(ctx, rangesRequest)
	}
	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		rangesResp := resp.(*serverpb.RangesResponse)
		// Age the MVCCStats to a consistent current timestamp. An age that is
		// not up to date is less useful.
		if len(rangesResp.Ranges) == 0 {
			return
		}
		var ri rangeInfo
		ri.init(rangesResp.Ranges[0])
		response.Responses[nodeID] = nodeRangeResponse{RangeInfo: ri}
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.Responses[nodeID] = nodeRangeResponse{
			Error: err.Error(),
		}
	}

	if err := a.status.iterateNodes(
		ctx, fmt.Sprintf("details about range %d", rangeID), dialFn, nodeFn, responseFn, errorFn,
	); err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}
	writeJSONResponse(ctx, w, 200, response)
}

// rangeDescriptorInfo contains a subset of fields from roachpb.RangeDescriptor
// that are safe to be returned from APIs.
type rangeDescriptorInfo struct {
	RangeID  roachpb.RangeID `json:"range_id"`
	StartKey roachpb.RKey    `json:"start_key,omitempty"`
	EndKey   roachpb.RKey    `json:"end_key,omitempty"`

	// Set for HotRanges.
	StoreID roachpb.StoreID    `json:"store_id"`
}

func (r *rangeDescriptorInfo) init(rd *roachpb.RangeDescriptor) {
	if rd == nil {
		*r = rangeDescriptorInfo{}
		return
	}
	*r = rangeDescriptorInfo{
		RangeID:  rd.RangeID,
		StartKey: rd.StartKey,
		EndKey:   rd.EndKey,
	}
}

type rangeInfo struct {
	Desc rangeDescriptorInfo `json:"desc"`

	// Subset of fields copied from serverpb.RangeInfo
	Span          serverpb.PrettySpan         `json:"span"`
	SourceNodeID  roachpb.NodeID              `json:"source_node_id,omitempty"`
	SourceStoreID roachpb.StoreID             `json:"source_store_id,omitempty"`
	ErrorMessage  string                      `json:"error_message,omitempty"`
	LeaseHistory  []roachpb.Lease             `json:"lease_history"`
	Problems      serverpb.RangeProblems      `json:"problems"`
	Stats         serverpb.RangeStatistics    `json:"stats"`
	Quiescent     bool                        `json:"quiescent,omitempty"`
	Ticking       bool                        `json:"ticking,omitempty"`
}

func (ri *rangeInfo) init(r serverpb.RangeInfo) {
	*ri = rangeInfo{
		Span:          r.Span,
		SourceNodeID:  r.SourceNodeID,
		SourceStoreID: r.SourceStoreID,
		ErrorMessage:  r.ErrorMessage,
		LeaseHistory:  r.LeaseHistory,
		Problems:      r.Problems,
		Stats:         r.Stats,
		Quiescent:     r.Quiescent,
		Ticking:       r.Ticking,
	}
	ri.Desc.init(r.State.Desc)
}

type nodeRangesResponse struct {
	Ranges []rangeInfo `json:"ranges"`
	Next   int         `json:"next,omitempty"`
}

func (a *apiV2Server) listNodeRanges(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = apiToOutgoingGatewayCtx(ctx, r)
	vars := mux.Vars(r)
	nodeIDStr := vars["node_id"]
	if nodeIDStr != "local" {
		nodeID, err := strconv.ParseInt(nodeIDStr, 10, 32)
		if err != nil || nodeID <= 0 {
			http.Error(w, "invalid node ID", http.StatusBadRequest)
			return
		}
	}

	ranges, ok := parseRangeIDs(r.URL.Query().Get("ranges"), w)
	if !ok {
		return
	}
	req := &serverpb.RangesRequest{
		NodeId:   nodeIDStr,
		RangeIDs: ranges,
	}
	limit, offset := getSimplePaginationValues(r)
	statusResp, next, err := a.status.rangesHelper(ctx, req, limit, offset)
	if err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}
	resp := nodeRangesResponse{
		Ranges: make([]rangeInfo, 0, len(statusResp.Ranges)),
		Next:   next,
	}
	for _, r := range statusResp.Ranges {
		var ri rangeInfo
		ri.init(r)
		resp.Ranges = append(resp.Ranges, ri)
	}
	writeJSONResponse(ctx, w, 200, resp)
}

type responseError struct {
	ErrorMessage string         `json:"error_message"`
	NodeID       roachpb.NodeID `json:"node_id,omitempty"`
}

type hotRangesResponse struct {
	RangesByNodeID map[roachpb.NodeID][]rangeDescriptorInfo `json:"ranges_by_node_id"`
	Errors         []responseError                          `json:"response_error,omitempty"`
	Next           string                                   `json:"next,omitempty"`
}

func (a *apiV2Server) listHotRanges(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx = apiToOutgoingGatewayCtx(ctx, r)
	nodeIDStr := r.URL.Query().Get("node_id")
	limit, start := getRPCPaginationValues(r)

	response := &hotRangesResponse{
		RangesByNodeID: make(map[roachpb.NodeID][]rangeDescriptorInfo),
	}
	var requestedNodes []roachpb.NodeID
	if len(nodeIDStr) > 0 {
		requestedNodeID, _, err := a.status.parseNodeID(nodeIDStr)
		if err != nil {
			http.Error(w, "invalid node ID", http.StatusBadRequest)
			return
		}
		requestedNodes = []roachpb.NodeID{requestedNodeID}
	}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := a.status.dialNode(ctx, nodeID)
		return client, err
	}
	remoteRequest := serverpb.HotRangesRequest{NodeID: "local"}
	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		resp, err := status.HotRanges(ctx, &remoteRequest)
		if err != nil || resp == nil {
			return nil, err
		}
		rangeDescriptorInfos := make([]rangeDescriptorInfo, 0)
		for _, store := range resp.HotRangesByNodeID[nodeID].Stores {
			for _, hotRange := range store.HotRanges {
				var r rangeDescriptorInfo
				r.init(&hotRange.Desc)
				r.StoreID = store.StoreID
				rangeDescriptorInfos = append(rangeDescriptorInfos, r)
			}
		}
		sort.Slice(rangeDescriptorInfos, func(i, j int) bool {
			if rangeDescriptorInfos[i].StoreID == rangeDescriptorInfos[j].StoreID {
				return rangeDescriptorInfos[i].RangeID < rangeDescriptorInfos[j].RangeID
			}
			return rangeDescriptorInfos[i].StoreID < rangeDescriptorInfos[j].StoreID
		})
		return rangeDescriptorInfos, nil
	}
	responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
		if hotRangesResp, ok := resp.([]rangeDescriptorInfo); ok {
			response.RangesByNodeID[nodeID] = hotRangesResp
		}
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		response.Errors = append(response.Errors, responseError{
			ErrorMessage: err.Error(),
			NodeID:       nodeID,
		})
	}

	next, err := a.status.paginatedIterateNodes(
		ctx, "hot ranges", limit, start, requestedNodes, dialFn,
		nodeFn, responseFn, errorFn)

	if err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}
	var nextBytes []byte
	if nextBytes, err = next.MarshalText(); err != nil {
		response.Errors = append(response.Errors, responseError{ErrorMessage: err.Error()})
	} else {
		response.Next = string(nextBytes)
	}
	writeJSONResponse(ctx, w, 200, response)
}

func (a *apiV2Server) health(w http.ResponseWriter, r *http.Request) {
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
		writeJSONResponse(ctx, w, 200, resp)
		return
	}

	if err := a.admin.checkReadinessForHealthCheck(ctx); err != nil {
		apiV2InternalError(ctx, err, w)
		return
	}
	writeJSONResponse(ctx, w, 200, resp)
}
