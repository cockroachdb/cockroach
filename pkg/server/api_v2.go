// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:generate swagger generate spec -w . -o ../../docs/generated/swagger/spec.json --scan-models

// CockroachDB v2 API
//
// API for querying information about CockroachDB health, nodes, ranges,
// sessions, and other meta entities.
//
//     Schemes: http, https
//     Host: localhost
//     BasePath: /api/v2/
//     Version: 2.0.0
//     License: Business Source License
//
//     Produces:
//     - application/json
//
//     SecurityDefinitions:
//       api_session:
//          type: apiKey
//          name: X-Cockroach-API-Session
//          description: Handle to logged-in REST session. Use `/login/` to
//            log in and get a session.
//          in: header
//
// swagger:meta
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
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

// Returns a SQL username from the request context of a route requiring login.
// Only use in routes that require login (requiresAuth = true in its route
// definition).
func getSQLUsername(ctx context.Context) security.SQLUsername {
	return security.MakeSQLUsernameFromPreNormalizedString(ctx.Value(webSessionUserKey{}).(string))
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
		// Any endpoint returning range information requires an admin user. This is because range start/end keys
		// are sensitive info.
		{"nodes/{node_id}/ranges/", a.listNodeRanges, true, adminRole, noOption},
		{"ranges/hot/", a.listHotRanges, true, adminRole, noOption},
		{"ranges/{range_id:[0-9]+}/", a.listRange, true, adminRole, noOption},
		{"health/", a.health, false, regularRole, noOption},
		{"users/", a.listUsers, true, regularRole, noOption},
		{"events/", a.listEvents, true, adminRole, noOption},
		{"databases/", a.listDatabases, true, regularRole, noOption},
		{"databases/{database_name:[\\w.]+}/", a.databaseDetails, true, regularRole, noOption},
		{"databases/{database_name:[\\w.]+}/grants/", a.databaseGrants, true, regularRole, noOption},
		{"databases/{database_name:[\\w.]+}/tables/", a.databaseTables, true, regularRole, noOption},
		{"databases/{database_name:[\\w.]+}/tables/{table_name:[\\w.]+}/", a.tableDetails, true, regularRole, noOption},
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

// Response for listSessions.
//
// swagger:model listSessionsResp
type listSessionsResponse struct {
	serverpb.ListSessionsResponse

	// The continuation token, for use in the next paginated call in the `start`
	// parameter.
	Next string `json:"next,omitempty"`
}

// swagger:operation GET /sessions/ listSessions
//
// List sessions
//
// List all sessions on this cluster. If a username is provided, only
// sessions from that user are returned.
//
// Client must be logged-in as a user with admin privileges.
//
// ---
// parameters:
// - name: username
//   type: string
//   in: query
//   description: Username of user to return sessions for; if unspecified,
//     sessions from all users are returned.
//   required: false
// - name: limit
//   type: integer
//   in: query
//   description: Maximum number of results to return in this call.
//   required: false
// - name: start
//   type: string
//   in: query
//   description: Continuation token for results after a past limited run.
//   required: false
// produces:
// - application/json
// security:
// - api_session: []
// responses:
//   "200":
//     description: List sessions response.
//     schema:
//       "$ref": "#/definitions/listSessionsResp"
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

// swagger:operation GET /health/ health
//
// Check node health
//
// Helper endpoint to check for node health. If `ready` is true, it also checks
// if this node is fully operational and ready to accept SQL connections.
// Otherwise, this endpoint always returns a successful response (if the API
// server is up, of course).
//
// ---
// parameters:
// - name: ready
//   type: boolean
//   in: query
//   description: If true, check whether this node is ready to accept SQL
//     connections. If false, this endpoint always returns success, unless
//     the API server itself is down.
//   required: false
// produces:
// - application/json
// responses:
//   "200":
//     description: Indicates healthy node.
//   "500":
//     description: Indicates unhealthy node.
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
