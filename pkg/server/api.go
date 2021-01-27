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

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/gorilla/mux"
)

const (
	apiV2Path       = "/api/v2/"
	apiV2AuthHeader = "X-Cockroach-API-Session"
)

// Telemetry counters for API endpoints.
var listSessionsCounter = telemetry.GetCounterOnce("api.v2.list-sessions")

func writeJsonResponse(ctx context.Context, w http.ResponseWriter, code int, payload interface{}) {
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

// newApiV2Server returns a new apiV2Server.
func newApiV2Server(ctx context.Context, s *Server) *apiV2Server {
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

// registerRoutes registers endpoints under the current API server.
func (a *apiV2Server) registerRoutes(innerMux *mux.Router, authMux http.Handler) {
	var noOption roleoption.Option

	// Add any new API endpoint definitions here, even if a sub-server handles
	// them. Arguments:
	//
	// - `endpoint` is the path string that, if matched by the user request, is
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
	routeDefinitions := []struct{
		endpoint      string
		handler       http.HandlerFunc
		requiresAuth  bool
		role          apiRole
		option        roleoption.Option
	}{
		// Pass through auth-related endpoints to the auth server.
		{"login/", a.authServer.ServeHTTP, false /* requiresAuth */, regularRole, noOption},
		{"logout/", a.authServer.ServeHTTP, false /* requiresAuth */, regularRole, noOption},

		// Directly register other endpoints in the api server.
		{"sessions/", a.listSessions, true /* requiresAuth */ , adminRole, noOption},
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
					ie:     a.admin.ie,
					role:   route.role,
					option: route.option,
					inner:  route.handler,
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
	ctx := r.Context()
	telemetry.Inc(listSessionsCounter)
	limit, start := getRPCPaginationValues(r)
	req := &serverpb.ListSessionsRequest{Username: ctx.Value(webSessionUserKey{}).(string)}
	response := &listSessionsResponse{}

	responseProto, pagState, err := a.status.listSessionsHelper(ctx, req, limit, start)
	var nextBytes []byte
	if nextBytes, err = pagState.MarshalText(); err != nil {
		err := serverpb.ListSessionsError{Message: err.Error()}
		response.Errors = append(response.Errors, err)
	} else {
		response.Next = string(nextBytes)
	}
	response.ListSessionsResponse = *responseProto
	writeJsonResponse(ctx, w, http.StatusOK, response)
}
