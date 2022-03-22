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
	"encoding/base64"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// authenticationV2Server is a sub-server under apiV2Server that handles
// authentication-related endpoints, such as login and logout. The actual
// verification of sessions for regular endpoints happens in authenticationV2Mux,
// not here.
type authenticationV2Server struct {
	ctx        context.Context
	sqlServer  *SQLServer
	authServer *authenticationServer
	mux        *http.ServeMux
	basePath   string
}

// newAuthenticationV2Server creates a new authenticationV2Server for the given
// outer Server, and base path.
func newAuthenticationV2Server(
	ctx context.Context, s *Server, basePath string,
) *authenticationV2Server {
	simpleMux := http.NewServeMux()

	authServer := &authenticationV2Server{
		sqlServer:  s.sqlServer,
		authServer: newAuthenticationServer(s.cfg.Config, s.sqlServer),
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

// swagger:model loginResponse
type loginResponse struct {
	// Session string for a valid API session. Specify this in header for any API
	// requests that require authentication.
	Session string `json:"session"`
}

// swagger:operation POST /login/ login
//
// API Login
//
// Creates an API session for use with API endpoints that require
// authentication.
//
// ---
// parameters:
// - name: credentials
//   schema:
//     type: object
//     properties:
//       username:
//         type: string
//       password:
//         type: string
//     required:
//       - username
//       - password
//   in: body
//   description: Credentials for login
//   required: true
// produces:
// - application/json
// - text/plain
// consumes:
// - application/x-www-form-urlencoded
// responses:
//   "200":
//     description: Login response.
//     schema:
//       "$ref": "#/definitions/loginResponse"
//   "400":
//     description: Bad request, if required parameters absent.
//     type: string
//   "401":
//     description: Unauthorized, if credentials don't match.
//     type: string
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
	verified, expired, err := a.authServer.verifyPasswordDBConsole(a.ctx, username, r.Form.Get("password"))
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

	writeJSONResponse(r.Context(), w, http.StatusOK, &loginResponse{Session: session})
}

// swagger:model logoutResponse
type logoutResponse struct {
	// Indicates whether logout was successful.
	LoggedOut bool `json:"logged_out"`
}

// swagger:operation POST /logout/ logout
//
// API Logout
//
// Logs out on a previously-created API session.
//
// ---
// produces:
// - application/json
// - text/plain
// security:
// - api_session: []
// responses:
//   "200":
//     description: Logout response.
//     schema:
//       "$ref": "#/definitions/logoutResponse"
//   "400":
//     description: Bad request, if API session not present in headers, or
//       invalid session.
//     type: string
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

	writeJSONResponse(r.Context(), w, http.StatusOK, &logoutResponse{LoggedOut: true})
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

func (a *authenticationV2Mux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	username, cookie, err := a.getSession(w, req)
	if err == nil {
		// Valid session found. Set the username in the request context, so
		// child http.Handlers can access it.
		ctx := req.Context()
		ctx = context.WithValue(ctx, webSessionUserKey{}, username)
		ctx = context.WithValue(ctx, webSessionIDKey{}, cookie.ID)
		req = req.WithContext(ctx)
	} else {
		// getSession writes an error to w if err != nil.
		return
	}
	a.inner.ServeHTTP(w, req)
}

type apiRole int

const (
	regularRole apiRole = iota
	adminRole
	superUserRole
)

// roleAuthorizationMux enforces a role (eg. type of user, role option)
// for an arbitrary inner mux. Meant to be used under authenticationV2Mux. If
// the logged-in user is not at least of `role` type, and doesn't have
// the `option` roleoption, an HTTP 403 forbidden error is returned. Otherwise,
// the request is passed onto the inner http.Handler.
type roleAuthorizationMux struct {
	ie     *sql.InternalExecutor
	role   apiRole
	option roleoption.Option
	inner  http.Handler
}

func (r *roleAuthorizationMux) getRoleForUser(
	ctx context.Context, user security.SQLUsername,
) (apiRole, error) {
	if user.IsRootUser() {
		// Shortcut.
		return superUserRole, nil
	}
	row, err := r.ie.QueryRowEx(
		ctx, "check-is-admin", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: user},
		"SELECT crdb_internal.is_admin()")
	if err != nil {
		return regularRole, err
	}
	if row == nil {
		return regularRole, errors.AssertionFailedf("hasAdminRole: expected 1 row, got 0")
	}
	if len(row) != 1 {
		return regularRole, errors.AssertionFailedf("hasAdminRole: expected 1 column, got %d", len(row))
	}
	dbDatum, ok := tree.AsDBool(row[0])
	if !ok {
		return regularRole, errors.AssertionFailedf("hasAdminRole: expected bool, got %T", row[0])
	}
	if dbDatum {
		return adminRole, nil
	}
	return regularRole, nil
}

func (r *roleAuthorizationMux) hasRoleOption(
	ctx context.Context, user security.SQLUsername, roleOption roleoption.Option,
) (bool, error) {
	if user.IsRootUser() {
		// Shortcut.
		return true, nil
	}
	row, err := r.ie.QueryRowEx(
		ctx, "check-role-option", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: user},
		"SELECT crdb_internal.has_role_option($1)", roleOption.String())
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, errors.AssertionFailedf("hasRoleOption: expected 1 row, got 0")
	}
	if len(row) != 1 {
		return false, errors.AssertionFailedf("hasRoleOption: expected 1 column, got %d", len(row))
	}
	dbDatum, ok := tree.AsDBool(row[0])
	if !ok {
		return false, errors.AssertionFailedf("hasRoleOption: expected bool, got %T", row[0])
	}
	return bool(dbDatum), nil
}

func (r *roleAuthorizationMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// The username is set in authenticationV2Mux, and must correspond with a
	// logged-in user.
	username := security.MakeSQLUsernameFromPreNormalizedString(
		req.Context().Value(webSessionUserKey{}).(string))
	if role, err := r.getRoleForUser(req.Context(), username); err != nil || role < r.role {
		if err != nil {
			apiV2InternalError(req.Context(), err, w)
		} else {
			http.Error(w, "user not allowed to access this endpoint", http.StatusForbidden)
		}
		return
	}
	if r.option > 0 {
		ok, err := r.hasRoleOption(req.Context(), username, r.option)
		if err != nil {
			apiV2InternalError(req.Context(), err, w)
			return
		} else if !ok {
			http.Error(w, "user not allowed to access this endpoint", http.StatusForbidden)
			return
		}
	}
	r.inner.ServeHTTP(w, req)
}

// apiToOutgoingGatewayCtx converts an HTTP API (v1 or v2) context, to one that
// can issue outgoing RPC requests under the same logged-in user.
func apiToOutgoingGatewayCtx(ctx context.Context, r *http.Request) context.Context {
	return metadata.NewOutgoingContext(ctx, forwardAuthenticationMetadata(ctx, r))
}
