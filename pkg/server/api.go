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
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/gorilla/mux"
)

const (
	apiV2Path       = "/api/v2/"
	apiV2AuthHeader = "X-Cockroach-API-Session"
)

func writeJsonResponse(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	res, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	if _, err := w.Write(res); err != nil {
		panic(err)
	}
}

// apiV2Server implements endpoints under apiV2Path.
type apiV2Server struct {
	admin      *adminServer
	authServer *authenticationV2Server
	status     *statusServer
	mux        *mux.Router
}

func newApiServer(ctx context.Context, s *Server) *apiV2Server {
	authServer := newAuthenticationV2Server(ctx, s, apiV2Path)
	innerMux := mux.NewRouter()

	authMux := newAuthenticationV2Mux(authServer, innerMux)
	outerMux := mux.NewRouter()
	a :=  &apiV2Server{
		admin:      s.admin,
		authServer: authServer,
		status:     s.status,
		mux:        outerMux,
	}
	a.registerRoutes(innerMux, authMux)
	return a
}

func (a *apiV2Server) registerRoutes(innerMux *mux.Router, authMux http.Handler) {
	routeDefinitions := []struct{
		endpoint      string
		handler       http.HandlerFunc
		requiresAuth  bool
		role          apiRole
	}{
		// Pass through auth-related endpoints to the auth server.
		{"login/", a.authServer.ServeHTTP, false /* requiresAuth */, regularRole},
		{"logout/", a.authServer.ServeHTTP, false /* requiresAuth */, regularRole},

		// Directly register other endpoints in the api server.
		{"sessions/", a.listSessions, true /* requiresAuth */ , adminRole},
		{"hotranges/", a.hotRanges, true /* requiresAuth */ , adminRole},
		{"ranges/{range_id}/", a.rangeHandler, true /* requiresAuth */ , adminRole},
		{"nodes/", a.nodes, true /* requiresAuth */ , regularRole},
	}

	// For all routes requiring authentication, have the outer mux (a.mux)
	// send requests through to the authMux, and also register the relevant route
	// in innerMux. Routes not requiring login can directly be handled in a.mux.
	for _, route := range routeDefinitions {
		if route.requiresAuth {
			a.mux.Handle(apiV2Path + route.endpoint, authMux)
			handler := http.Handler(route.handler)
			if route.role != regularRole {
				handler = &roleAuthorizationMux{
					ie:    a.admin.ie,
					role:  route.role,
					inner: route.handler,
				}
			}
			innerMux.Handle(apiV2Path + route.endpoint, handler)
		} else {
			a.mux.HandleFunc(apiV2Path + route.endpoint, route.handler)
		}
	}
}

type listSessionsResponse struct {
	serverpb.ListSessionsResponse

	Next string `json:"next"`
}

func (a *apiV2Server) listSessions(w http.ResponseWriter, r *http.Request) {
	limit, start := getRPCPaginationValues(r)
	req := &serverpb.ListSessionsRequest{Username: r.Context().Value(webSessionUserKey{}).(string)}
	response := &listSessionsResponse{}

	responseProto, pagState, err := a.status.listSessionsHelper(r.Context(), req, limit, start)
	var nextBytes []byte
	if nextBytes, err = pagState.MarshalText(); err != nil {
		err := serverpb.ListSessionsError{Message: err.Error()}
		response.Errors = append(response.Errors, err)
	} else {
		response.Next = string(nextBytes)
	}
	response.ListSessionsResponse = *responseProto
	writeJsonResponse(w, http.StatusOK, response)
}

type rangeResponse struct {
	serverpb.RangeResponse

	Next string `json:"next"`
}

func (a *apiV2Server) rangeHandler(w http.ResponseWriter, r *http.Request) {
	limit, start := getRPCPaginationValues(r)
	var err error
	var rangeID int64
	vars := mux.Vars(r)
	if rangeID, err = strconv.ParseInt(vars["range_id"], 10, 64); err != nil {
		http.Error(w, "invalid range id", http.StatusBadRequest)
		return
	}

	req := &serverpb.RangeRequest{RangeId: rangeID}
	response := &rangeResponse{}
	responseProto, next, err := a.status.rangeHelper(r.Context(), req, limit, start)
	if err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	}
	response.RangeResponse = *responseProto
	if nextBytes, err := next.MarshalText(); err == nil {
		response.Next = string(nextBytes)
	}
	writeJsonResponse(w, http.StatusOK, response)
}

type hotRangesResponse struct {
	serverpb.HotRangesResponse

	Next string `json:"next"`
}

func (a *apiV2Server) hotRanges(w http.ResponseWriter, r *http.Request) {
	limit, start := getRPCPaginationValues(r)
	req := &serverpb.HotRangesRequest{NodeID: r.URL.Query().Get("node_id")}
	response := &hotRangesResponse{}

	responseProto, next, err := a.status.hotRangesHelper(r.Context(), req, limit, start)
	if err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	}
	response.HotRangesResponse = *responseProto
	if nextBytes, err := next.MarshalText(); err == nil {
		response.Next = string(nextBytes)
	}
	writeJsonResponse(w, http.StatusOK, response)
}

type nodesResponse struct {
	serverpb.NodesResponse

	Next int `json:"next"`
}

func (a *apiV2Server) nodes(w http.ResponseWriter, r *http.Request) {
	limit, offset := getSimplePaginationValues(r)
	req := &serverpb.NodesRequest{}
	response := &nodesResponse{}

	responseProto, next, err := a.status.nodesHelper(r.Context(), req, limit, offset)
	if err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	}
	response.NodesResponse = *responseProto
	response.Next = next
	writeJsonResponse(w, http.StatusOK, response)
}

func (a *apiV2Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

