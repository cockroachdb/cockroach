// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package authserver

import (
	"context"
	"encoding/base64"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	APIV2AuthHeader = "X-Cockroach-API-Session"
)

// authenticationV2Server is a sub-server under apiV2Server that handles
// authentication-related endpoints, such as login and logout. The actual
// verification of sessions for regular endpoints happens in authenticationV2Mux,
// not here.
type authenticationV2Server struct {
	sqlServer  SQLServerInterface
	authServer *authenticationServer
	mux        *http.ServeMux
	basePath   string
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
	ctx context.Context, userName username.SQLUsername,
) (string, error) {
	// Create a new database session, generating an ID and secret key.
	id, secret, err := a.authServer.NewAuthSession(ctx, userName)
	if err != nil {
		return "", srverrors.APIInternalError(ctx, err)
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
	// Session string for a valid API session. Specify this in header for any API
	// requests that require authentication.
	Session string `json:"session"`
}

// # API Login
//
// Creates an API session for use with API endpoints that require
// authentication.
//
// ---
// parameters:
//   - name: credentials
//     schema:
//     type: object
//     properties:
//     username:
//     type: string
//     password:
//     type: string
//     required:
//   - username
//   - password
//     in: body
//     description: Credentials for login
//     required: true
//
// produces:
// - application/json
// - text/plain
// consumes:
// - application/x-www-form-urlencoded
// responses:
//
//	"200":
//	  description: Login response.
//	  schema:
//	    "$ref": "#/definitions/loginResponse"
//	"400":
//	  description: Bad request, if required parameters absent.
//	  type: string
//	"401":
//	  description: Unauthorized, if credentials don't match.
//	  type: string
func (a *authenticationV2Server) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "not found", http.StatusNotFound)
	}
	ctx := r.Context()
	if err := r.ParseForm(); err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
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
	username, _ := username.MakeSQLUsernameFromUserInput(r.Form.Get("username"), username.PurposeValidation)

	// Verify the user and check if DB console session could be started.
	verified, pwRetrieveFn, err := a.authServer.VerifyUserSessionDBConsole(ctx, username)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	if !verified {
		http.Error(w, "the provided credentials did not match any account on the server", http.StatusUnauthorized)
		return
	}

	// Verify the provided username/password pair.
	verified, expired, err := a.authServer.VerifyPasswordDBConsole(ctx, username, r.Form.Get("password"), pwRetrieveFn)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
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

	session, err := a.createSessionFor(ctx, username)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, &loginResponse{Session: session})
}

type logoutResponse struct {
	// Indicates whether logout was successful.
	LoggedOut bool `json:"logged_out"`
}

// # API Logout
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
//
//	"200":
//	  description: Logout response.
//	  schema:
//	    "$ref": "#/definitions/logoutResponse"
//	"400":
//	  description: Bad request, if API session not present in headers, or
//	    invalid session.
//	  type: string
func (a *authenticationV2Server) logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "not found", http.StatusNotFound)
	}
	session := r.Header.Get(APIV2AuthHeader)
	if session == "" {
		http.Error(w, "invalid or unspecified session", http.StatusBadRequest)
		return
	}
	var sessionCookie serverpb.SessionCookie
	decoded, err := base64.StdEncoding.DecodeString(session)
	ctx := r.Context()
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	if err := protoutil.Unmarshal(decoded, &sessionCookie); err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}

	// Revoke the session.
	if n, err := a.sqlServer.InternalExecutor().ExecEx(
		ctx,
		"revoke-auth-session",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.web_sessions SET "revokedAt" = now() WHERE id = $1`,
		sessionCookie.ID,
	); err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	} else if n == 0 {
		err := status.Errorf(
			codes.InvalidArgument,
			"session with id %d nonexistent", sessionCookie.ID)
		log.Infof(ctx, "%v", err)
		http.Error(w, "invalid session", http.StatusBadRequest)
		return
	}

	apiutil.WriteJSONResponse(ctx, w, http.StatusOK, &logoutResponse{LoggedOut: true})
}

func (a *authenticationV2Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mux.ServeHTTP(w, r)
}

// authenticationV2Mux provides authentication checks for an arbitrary inner
// http.Handler. If the session cookie is not set, an HTTP 401 error is returned
// and the request isn't routed through to the inner handler. On success, the
// username is set on the request context for use in the inner handler.
type authenticationV2Mux struct {
	s              *authenticationV2Server
	inner          http.Handler
	allowAnonymous bool
}

// APIV2UseCookieBasedAuth is a magic value of the auth header that
// tells us to look for the session in the cookie. This can be used by
// frontend code to maintain cookie-based auth while interacting with
// the API.
const APIV2UseCookieBasedAuth = "cookie"

// getSession decodes the cookie from the request, looks up the corresponding
// session, and returns the logged-in username. The session can be looked up
// either from a session cookie as used in the non-v2 API server, or via the
// session header. In order for us to use the cookie as the session source, the
// header `"X-Cockroach-API-Session"` must be set to `"cookie"` (This is to
// guard against CSRF attacks in the browser since it forces the caller to use
// javascript to set the header). If there's an error, it returns an error value
// and also sends the error over http using w.
func (a *authenticationV2Mux) getSession(
	w http.ResponseWriter, req *http.Request,
) (string, *serverpb.SessionCookie, int, error) {
	ctx := req.Context()
	// Validate the returned session header or cookie.
	rawSession := req.Header.Get(APIV2AuthHeader)
	if len(rawSession) == 0 {
		err := errors.New("invalid session header")
		return "", nil, http.StatusUnauthorized, err
	}

	cookie := &serverpb.SessionCookie{}
	var err error
	if rawSession == APIV2UseCookieBasedAuth {
		st := a.s.sqlServer.ExecutorConfig().Settings
		cookie, err = FindAndDecodeSessionCookie(req.Context(), st, req.Cookies())
	} else {
		decoded, err := base64.StdEncoding.DecodeString(rawSession)
		if err != nil {
			log.Warningf(ctx, "attempted to decode session but failed: %v", err)
			return "", nil, http.StatusBadRequest, err
		}
		err = protoutil.Unmarshal(decoded, cookie)
		if err != nil {
			log.Warningf(ctx, "attempted to unmarshal session but failed: %v", err)
		}
	}
	if err != nil {
		err := errors.New("invalid session header")
		return "", nil, http.StatusBadRequest, err
	}
	valid, username, err := a.s.authServer.VerifySession(req.Context(), cookie)
	if err != nil {
		srverrors.APIV2InternalError(req.Context(), err, w)
		return "", nil, http.StatusInternalServerError, err
	}
	if !valid {
		err := errors.New("the provided authentication session could not be validated")
		return "", nil, http.StatusUnauthorized, err
	}

	return username, cookie, http.StatusOK, nil
}

func (a *authenticationV2Mux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	u, cookie, errStatus, err := a.getSession(w, req)
	if err != nil && !a.allowAnonymous {
		// getSession writes an error to w if err != nil.
		http.Error(w, err.Error(), errStatus)
		return
	}
	if a.allowAnonymous {
		u = username.RootUser
	}
	// Valid session found, or insecure. Set the username in the request context,
	// so child http.Handlers can access it.
	var sessionID int64
	if cookie != nil {
		sessionID = cookie.ID
	}
	req = req.WithContext(ContextWithHTTPAuthInfo(req.Context(), u, sessionID))
	a.inner.ServeHTTP(w, req)
}

// APIRole is an enum representing the authorization level
// needed for an APIv2 endpoint.
type APIRole int

const (
	// RegularRole is the default role for an APIv2 endpoint.
	RegularRole APIRole = iota
	// ViewClusterMetadataRole is the role for an APIv2 endpoint that requires
	// VIEWCLUSTERMETADATA privileges.
	ViewClusterMetadataRole
)

type authzAccessorFactory func(ctx context.Context, opName redact.SafeString) (_ sql.AuthorizationAccessor, cleanup func())

// roleAuthorizationMux enforces a role (eg. type of user) for an arbitrary
// inner mux. Meant to be used under authenticationV2Mux. If the logged-in user
// is not at least of `role` type, an HTTP 403 forbidden error is returned.
// Otherwise, the request is passed onto the inner http.Handler.
type roleAuthorizationMux struct {
	authzAccessorFactory authzAccessorFactory
	role                 APIRole
	inner                http.Handler
}

func (r *roleAuthorizationMux) getRoleForUser(
	ctx context.Context, user username.SQLUsername,
) (APIRole, error) {
	if user.IsRootUser() {
		// Shortcut.
		return ViewClusterMetadataRole, nil
	}

	authzAccessor, cleanup := r.authzAccessorFactory(ctx, "check-privilege")
	defer cleanup()

	hasPriv, err := authzAccessor.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA, user)
	if err != nil {
		return RegularRole, err
	} else if hasPriv {
		return ViewClusterMetadataRole, nil
	} else {
		return RegularRole, nil
	}
}

func (r *roleAuthorizationMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// The username is set in authenticationV2Mux, and must correspond with a
	// logged-in user.
	username := UserFromHTTPAuthInfoContext(req.Context())
	if role, err := r.getRoleForUser(req.Context(), username); err != nil || role < r.role {
		if err != nil {
			srverrors.APIV2InternalError(req.Context(), err, w)
		} else {
			http.Error(w, "user not allowed to access this endpoint", http.StatusForbidden)
		}
		return
	}
	r.inner.ServeHTTP(w, req)
}
