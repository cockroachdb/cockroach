// Copyright 2020 The Cockroach Authors.
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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// simplePaginate takes in an input slice, and returns a sub-slice of the next
// `limit` elements starting at `offset`. The second returned value is the
// next offset that can be used to return the next "limit" results, or
// len(result) if there are no more results.
func simplePaginate(input interface{}, limit, offset int) (result interface{}, next int) {
	val := reflect.ValueOf(input)
	if limit <= 0 || val.Kind() != reflect.Slice {
		return val.Interface(), offset
	}
	startIdx := offset
	endIdx := offset + limit
	if startIdx > val.Len() {
		startIdx = val.Len()
	}
	if endIdx > val.Len() {
		endIdx = val.Len()
	}
	return val.Slice(startIdx, endIdx).Interface(), endIdx
}

// paginationState represents the current state of pagination through the result
// set of an RPC-based endpoint. Meant for use with
// statusServer.paginatedIterateNodes, which implements most of the pagination
// logic.
type paginationState struct {
	nodesQueried    []roachpb.NodeID
	inProgress      roachpb.NodeID
	inProgressIndex int
	nodesToQuery    []roachpb.NodeID
}

// mergeNodeIDs merges sortedNodeIDs with all node IDs in the paginationState;
// adding any nodes to the end of p.nodesToQuery that don't already exist in p.
// sortedNodeIDs must be a sorted slice of all currently-live nodes.
func (p *paginationState) mergeNodeIDs(sortedNodeIDs []roachpb.NodeID) {
	allNodeIDs := make([]roachpb.NodeID, 0, len(p.nodesQueried) + 1 + len(p.nodesToQuery))
	allNodeIDs = append(allNodeIDs, p.nodesQueried...)
	if p.inProgress != 0 {
		allNodeIDs = append(allNodeIDs, p.inProgress)
	}
	allNodeIDs = append(allNodeIDs, p.nodesToQuery...)
	sort.Slice(allNodeIDs, func(i, j int) bool {
		return allNodeIDs[i] < allNodeIDs[j]
	})
	j := 0
	for i := range sortedNodeIDs {
		for j < len(allNodeIDs) && allNodeIDs[j] < sortedNodeIDs[i] {
			j++
		}
		if j >= len(allNodeIDs) || allNodeIDs[j] != sortedNodeIDs[i] {
			p.nodesToQuery = append(p.nodesToQuery, sortedNodeIDs[i])
		}
	}
	if p.inProgress == 0 && len(p.nodesToQuery) > 0 {
		p.inProgress = p.nodesToQuery[0]
		p.inProgressIndex = 0
		p.nodesToQuery = p.nodesToQuery[1:]
	}
}

func (p *paginationState) UnmarshalText(text []byte) error {
	decoder := base64.NewDecoder(base64.URLEncoding, bytes.NewReader(text))
	var decodedText []byte
	var err error
	if decodedText, err = ioutil.ReadAll(decoder); err != nil {
		return err
	}
	parts := strings.Split(string(decodedText), "|")
	if len(parts) != 4 {
		return errors.New("invalid pagination state")
	}
	parseNodeIDSlice := func(str string) ([]roachpb.NodeID, error) {
		parts := strings.Split(str, ",")
		res := make([]roachpb.NodeID, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if len(part) == 0 {
				continue
			}
			val, err := strconv.Atoi(part)
			if err != nil {
				return nil, errors.Wrap(err, "invalid pagination state")
			}
			res = append(res, roachpb.NodeID(val))
		}
		return res, nil
	}
	p.nodesQueried, err = parseNodeIDSlice(parts[0])
	if err != nil {
		return err
	}
	var inProgressInt int
	inProgressInt, err = strconv.Atoi(parts[1])
	if err != nil {
		return errors.Wrap(err, "invalid pagination state")
	}
	p.inProgress = roachpb.NodeID(inProgressInt)
	p.inProgressIndex, err = strconv.Atoi(parts[2])
	if err != nil {
		return errors.Wrap(err, "invalid pagination state")
	}
	p.nodesToQuery, err = parseNodeIDSlice(parts[3])
	if err != nil {
		return err
	}
	return nil
}

func (p *paginationState) MarshalText() (text []byte, err error) {
	var builder, builder2 bytes.Buffer
	for _, nid := range p.nodesQueried {
		fmt.Fprintf(&builder, "%d,", nid)
	}
	fmt.Fprintf(&builder, "|%d|%d|", p.inProgress, p.inProgressIndex)
	for _, nid := range p.nodesToQuery {
		fmt.Fprintf(&builder, "%d,", nid)
	}
	encoder := base64.NewEncoder(base64.URLEncoding, &builder2)
	if _, err = encoder.Write(builder.Bytes()); err != nil {
		return nil, err
	}
	if err = encoder.Close(); err != nil {
		return nil, err
	}
	return builder2.Bytes(), nil
}

func getRPCPaginationValues(r *http.Request) (limit int, start paginationState) {
	var err error
	if limit, err = strconv.Atoi(r.URL.Query().Get("limit")); err != nil || limit <= 0 {
		return 0, paginationState{}
	}
	if err = start.UnmarshalText([]byte(r.URL.Query().Get("start"))); err != nil {
		return limit, paginationState{}
	}
	return limit, start
}

func getSimplePaginationValues(r *http.Request) (limit, offset int) {
	var err error
	if limit, err = strconv.Atoi(r.URL.Query().Get("limit")); err != nil || limit <= 0 {
		return 0, 0
	}
	if offset, err = strconv.Atoi(r.URL.Query().Get("offset")); err != nil || offset <= 0 {
		return limit, 0
	}
	return limit, offset
}

type authenticationV2Server struct {
	ctx        context.Context
	sqlServer  *sqlServer
	authServer *authenticationServer
	mux        *http.ServeMux
	basePath   string
}

func newAuthenticationV2Server(ctx context.Context, s *Server, basePath string) *authenticationV2Server {
	simpleMux := http.NewServeMux()

	authServer := &authenticationV2Server{
		sqlServer:  s.sqlServer,
		authServer: newAuthenticationServer(s),
		mux:        simpleMux,
		ctx:        ctx,
		basePath:   basePath,
	}

	authServer.registerRoutes()
	return authServer
}

func (a *authenticationV2Server) registerRoutes() {
	a.bindEndpoint("login/", a.login)
	a.bindEndpoint("logout/", a.logout)
}

func (a *authenticationV2Server) bindEndpoint(endpoint string, handler http.HandlerFunc) {
	a.mux.HandleFunc(a.basePath+endpoint, handler)
}

// createSessionFor creates a login session for the given user.
//
// The caller is responsible to ensure the username has been normalized already.
func (a *authenticationV2Server) createSessionFor(
	ctx context.Context, username security.SQLUsername,
) (string, error) {
	// Create a new database session, generating an ID and secret key.
	id, secret, err := a.authServer.newAuthSession(ctx, username)
	if err != nil {
		return "", apiInternalError(ctx, err)
	}

	// Generate and set a session for the response. Because HTTP cookies
	// must be strings, the cookie value (a marshaled protobuf) is encoded in
	// base64. We just piggyback on the v1 API SessionCookie here, however
	// this won't be set as an HTTP cookie on the client side.
	cookieValue := &serverpb.SessionCookie{
		ID:     id,
		Secret: secret,
	}
	cookieValueBytes, err := protoutil.Marshal(cookieValue)
	if err != nil {
		return "", errors.Wrap(err, "session cookie could not be encoded")
	}
	value := base64.StdEncoding.EncodeToString(cookieValueBytes)
	return value, nil
}

type loginResponse struct {
	Session string `json:"session"`
}

func (a *authenticationV2Server) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "not found", http.StatusNotFound)
	}
	if err := r.ParseForm(); err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	}
	if r.Form.Get("username") == "" {
		http.Error(w, "username not specified", http.StatusBadRequest)
		return
	}

	// In CockroachDB SQL, unlike in PostgreSQL, usernames are
	// case-insensitive. Therefore we need to normalize the username
	// here, so that the normalized username is retained in the session
	// table: the APIs extract the username from the session table
	// without further normalization.
	username, _ := security.MakeSQLUsernameFromUserInput(r.Form.Get("username"), security.UsernameValidation)

	// Verify the provided username/password pair.
	verified, expired, err := a.authServer.verifyPassword(a.ctx, username, r.Form.Get("password"))
	if err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	}
	if expired {
		http.Error(w, "the password has expired", http.StatusUnauthorized)
		return
	}
	if !verified {
		http.Error(w, "the provided credentials did not match any account on the server", http.StatusUnauthorized)
		return
	}

	session, err := a.createSessionFor(a.ctx, username)
	if err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	}

	writeJsonResponse(w, http.StatusOK, &loginResponse{Session: session})
}

type logoutResponse struct {
	LoggedOut bool `json:"logged_out"`
}

func (a *authenticationV2Server) logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "not found", http.StatusNotFound)
	}
	session := r.Header.Get(apiV2AuthHeader)
	if session == "" {
		http.Error(w, "invalid or unspecified session", http.StatusBadRequest)
		return
	}
	var sessionCookie serverpb.SessionCookie
	decoded, err := base64.StdEncoding.DecodeString(session)
	if err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	}
	if err := protoutil.Unmarshal(decoded, &sessionCookie); err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	}

	// Revoke the session.
	if n, err := a.sqlServer.internalExecutor.ExecEx(
		a.ctx,
		"revoke-auth-session",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		`UPDATE system.web_sessions SET "revokedAt" = now() WHERE id = $1`,
		sessionCookie.ID,
	); err != nil {
		apiV2InternalError(r.Context(), err, w)
		return
	} else if n == 0 {
		err := status.Errorf(
			codes.InvalidArgument,
			"session with id %d nonexistent", sessionCookie.ID)
		log.Infof(a.ctx, "%v", err)
		http.Error(w, "invalid session", http.StatusBadRequest)
		return
	}

	writeJsonResponse(w, http.StatusOK, &logoutResponse{LoggedOut: true})
}

func (a *authenticationV2Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

// authenticationV2Mux provides authentication checks for an arbitrary inner
// http.Handler. If the session cookie is not set, an HTTP 401 error is returned
// and the request isn't routed through to the inner handler. On success, the
// username is set on the request context for use in the inner handler.
type authenticationV2Mux struct {
	s     *authenticationV2Server
	inner http.Handler
}

func newAuthenticationV2Mux(s *authenticationV2Server, inner http.Handler) *authenticationV2Mux {
	return &authenticationV2Mux{
		s:     s,
		inner: inner,
	}
}

// getSession decodes the cookie from the request, looks up the corresponding session, and
// returns the logged in user name. If there's an error, it returns an error value and
// also sends the error over http using w.
func (a *authenticationV2Mux) getSession(
	w http.ResponseWriter, req *http.Request,
) (string, *serverpb.SessionCookie, error) {
	// Validate the returned cookie.
	rawSession := req.Header.Get(apiV2AuthHeader)
	if len(rawSession) == 0 {
		err := errors.New("invalid session header")
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return "", nil, err
	}
	sessionCookie := &serverpb.SessionCookie{}
	decoded, err := base64.StdEncoding.DecodeString(rawSession)
	if err != nil {
		err := errors.New("invalid session header")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return "", nil, err
	}
	if err := protoutil.Unmarshal(decoded, sessionCookie); err != nil {
		err := errors.New("invalid session header")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return "", nil, err
	}

	valid, username, err := a.s.authServer.verifySession(req.Context(), sessionCookie)
	if err != nil {
		apiV2InternalError(req.Context(), err, w)
		return "", nil, err
	}
	if !valid {
		err := errors.New("the provided authentication session could not be validated")
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return "", nil, err
	}

	return username, sessionCookie, nil
}

func (am *authenticationV2Mux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	username, cookie, err := am.getSession(w, req)
	if err == nil {
		ctx := req.Context()
		ctx = context.WithValue(ctx, webSessionUserKey{}, username)
		ctx = context.WithValue(ctx, webSessionIDKey{}, cookie.ID)
		req = req.WithContext(ctx)
	} else {
		// getSession writes an error to w if err != nil.
		return
	}
	am.inner.ServeHTTP(w, req)
}

// requireAdminWrapper is a simpler wrapper around an
type requireAdminWrapper struct {
	a     *adminPrivilegeChecker
	inner http.Handler
}

func (r *requireAdminWrapper) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// The username is set in authenticationV2Mux, and must correspond with a
	// logged-in user.
	username := security.MakeSQLUsernameFromPreNormalizedString(
		req.Context().Value(webSessionUserKey{}).(string))
	if admin, err := r.a.hasAdminRole(req.Context(), username); err != nil || !admin {
		if err != nil {
			apiV2InternalError(req.Context(), err, w)
		} else {
			http.Error(w, "this operation requires admin privilege", http.StatusForbidden)
		}
		return
	}
	r.inner.ServeHTTP(w, req)
}

// apiV2Server implements endpoints under apiV2Path.
type apiV2Server struct {
	admin  *adminServer
	status *statusServer
	mux    *mux.Router
}

func newApiServer(ctx context.Context, s *Server) *apiV2Server {
	authServer := newAuthenticationV2Server(ctx, s, apiV2Path)
	innerMux := mux.NewRouter()

	authMux := newAuthenticationV2Mux(authServer, innerMux)
	outerMux := mux.NewRouter()
	a :=  &apiV2Server{
		admin:  s.admin,
		status: s.status,
		mux:    outerMux,
	}
	a.registerRoutes(innerMux, authMux)
	a.mux.Handle(apiV2Path + "login/", authServer)
	a.mux.Handle(apiV2Path + "logout/", authServer)
	return a
}

func (a *apiV2Server) registerRoutes(innerMux *mux.Router, authMux http.Handler) {
	routeDefinitions := []struct{
		endpoint      string
		handler       http.HandlerFunc
		requiresAuth  bool
		requiresAdmin bool
	}{
		{"sessions/", a.listSessions, true /* requiresAuth */ , true /* requiresAdmin */},
		{"hotranges/", a.hotRanges, true /* requiresAuth */ , true /* requiresAdmin */},
		{"ranges/{range_id}/", a.rangeHandler, true /* requiresAuth */ , true /* requiresAdmin */},
		{"nodes/", a.nodes, true /* requiresAuth */ , false /* requiresAdmin */},
	}

	// For all routes requiring authentication, have the outer mux (a.mux)
	// send requests through to the authMux, and also register the relevant route
	// in innerMux. Routes not requiring login can directly be handled in a.mux.
	for _, route := range routeDefinitions {
		if route.requiresAuth {
			a.mux.Handle(apiV2Path + route.endpoint, authMux)
			handler := http.Handler(route.handler)
			if route.requiresAdmin {
				handler = &requireAdminWrapper{
					a:     a.admin.adminPrivilegeChecker,
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

