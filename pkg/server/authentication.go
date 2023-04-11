// Copyright 2017 The Cockroach Authors.
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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// authPrefix is the prefix for RESTful endpoints used to provide
	// authentication methods.
	loginPath  = "/login"
	logoutPath = "/logout"
	// secretLength is the number of random bytes generated for session secrets.
	secretLength = 16
	// SessionCookieName is the name of the cookie used for HTTP auth.
	SessionCookieName = "session"
	// DemoLoginPath is the demo shell auto-login URL.
	DemoLoginPath = "/demologin"
)

type noOIDCConfigured struct{}

var _ ui.OIDCUI = &noOIDCConfigured{}

func (c *noOIDCConfigured) GetOIDCConf() ui.OIDCUIConf {
	return ui.OIDCUIConf{
		Enabled: false,
	}
}

// OIDC is an interface that an OIDC-based authentication module should implement to integrate with
// the rest of the node's functionality
type OIDC interface {
	ui.OIDCUI
}

// ConfigureOIDC is a hook for the `oidcccl` library to add OIDC login support. It's called during
// server startup to initialize a client for OIDC support.
var ConfigureOIDC = func(
	ctx context.Context,
	st *cluster.Settings,
	locality roachpb.Locality,
	handleHTTP func(pattern string, handler http.Handler),
	userLoginFromSSO func(ctx context.Context, username string) (*http.Cookie, error),
	ambientCtx log.AmbientContext,
	cluster uuid.UUID,
) (OIDC, error) {
	return &noOIDCConfigured{}, nil
}

var webSessionTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"server.web_session_timeout",
	"the duration that a newly created web session will be valid",
	7*24*time.Hour,
	settings.NonNegativeDuration,
).WithPublic()

type authenticationServer struct {
	cfg       *base.Config
	sqlServer *SQLServer
}

// newAuthenticationServer allocates and returns a new REST server for
// authentication APIs.
func newAuthenticationServer(cfg *base.Config, s *SQLServer) *authenticationServer {
	return &authenticationServer{
		cfg:       cfg,
		sqlServer: s,
	}
}

// RegisterService registers the GRPC service.
func (s *authenticationServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterLogInServer(g, s)
	serverpb.RegisterLogOutServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *authenticationServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	if err := serverpb.RegisterLogInHandler(ctx, mux, conn); err != nil {
		return err
	}
	return serverpb.RegisterLogOutHandler(ctx, mux, conn)
}

// UserLogin verifies an incoming request by a user to create an web
// authentication session. It checks the provided credentials against the
// system.users table, and if successful creates a new authentication session.
// The session's ID and secret are returned to the caller as an HTTP cookie,
// added via a "Set-Cookie" header.
func (s *authenticationServer) UserLogin(
	ctx context.Context, req *serverpb.UserLoginRequest,
) (*serverpb.UserLoginResponse, error) {
	if req.Username == "" {
		return nil, status.Errorf(
			codes.Unauthenticated,
			"no username was provided",
		)
	}

	// In CockroachDB SQL, unlike in PostgreSQL, usernames are
	// case-insensitive. Therefore we need to normalize the username
	// here, so that the normalized username is retained in the session
	// table: the APIs extract the username from the session table
	// without further normalization.
	username, _ := username.MakeSQLUsernameFromUserInput(req.Username, username.PurposeValidation)

	// Verify the provided username/password pair.
	verified, expired, err := s.verifyPasswordDBConsole(ctx, username, req.Password)
	if err != nil {
		return nil, apiInternalError(ctx, err)
	}
	if expired {
		return nil, status.Errorf(
			codes.Unauthenticated,
			"the password for %s has expired",
			username,
		)
	}
	if !verified {
		return nil, errWebAuthenticationFailure
	}

	cookie, err := s.createSessionFor(ctx, username)
	if err != nil {
		return nil, apiInternalError(ctx, err)
	}

	// Set the cookie header on the outgoing response.
	if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", cookie.String())); err != nil {
		return nil, apiInternalError(ctx, err)
	}

	return &serverpb.UserLoginResponse{}, nil
}

// demoLogin is the same as UserLogin but using the GET method.
// It is only available for demo and test clusters.
func (s *authenticationServer) demoLogin(w http.ResponseWriter, req *http.Request) {
	ctx := context.Background()
	ctx = logtags.AddTag(ctx, "client", log.SafeOperational(req.RemoteAddr))
	ctx = logtags.AddTag(ctx, "demologin", nil)

	fail := func(err error) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(fmt.Sprintf("invalid request: %v", err)))
	}

	if err := req.ParseForm(); err != nil {
		fail(err)
		return
	}

	var userInput, password string
	if len(req.Form["username"]) != 1 {
		fail(errors.New("username not passed right"))
		return
	}
	if len(req.Form["password"]) != 1 {
		fail(errors.New("password not passed right"))
		return
	}
	userInput = req.Form["username"][0]
	password = req.Form["password"][0]

	// In CockroachDB SQL, unlike in PostgreSQL, usernames are
	// case-insensitive. Therefore we need to normalize the username
	// here, so that the normalized username is retained in the session
	// table: the APIs extract the username from the session table
	// without further normalization.
	username, _ := username.MakeSQLUsernameFromUserInput(userInput, username.PurposeValidation)
	// Verify the provided username/password pair.
	verified, expired, err := s.verifyPasswordDBConsole(ctx, username, password)
	if err != nil {
		fail(err)
		return
	}
	if expired {
		fail(errors.New("password expired"))
		return
	}
	if !verified {
		fail(errors.New("password invalid"))
		return
	}

	cookie, err := s.createSessionFor(ctx, username)
	if err != nil {
		fail(err)
		return
	}

	w.Header()["Set-Cookie"] = []string{cookie.String()}
	w.Header()["Location"] = []string{"/"}
	w.WriteHeader(http.StatusTemporaryRedirect)
	_, _ = w.Write([]byte("you can use the UI now"))
}

var errWebAuthenticationFailure = status.Errorf(
	codes.Unauthenticated,
	"the provided credentials did not match any account on the server",
)

// UserLoginFromSSO checks for the existence of a given username and if it exists,
// creates a session for the username in the `web_sessions` table.
// The session's ID and secret are returned to the caller as an HTTP cookie,
// added via a "Set-Cookie" header.
func (s *authenticationServer) UserLoginFromSSO(
	ctx context.Context, reqUsername string,
) (*http.Cookie, error) {
	// In CockroachDB SQL, unlike in PostgreSQL, usernames are
	// case-insensitive. Therefore we need to normalize the username
	// here, so that the normalized username is retained in the session
	// table: the APIs extract the username from the session table
	// without further normalization.
	username, _ := username.MakeSQLUsernameFromUserInput(reqUsername, username.PurposeValidation)

	exists, _, _, canLoginDBConsole, _, _, _, err := sql.GetUserSessionInitInfo(
		ctx,
		s.sqlServer.execCfg,
		username,
		"", /* databaseName */
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed creating session for username")
	}
	if !exists || !canLoginDBConsole {
		return nil, errWebAuthenticationFailure
	}

	return s.createSessionFor(ctx, username)
}

// createSessionFor creates a login cookie for the given user.
//
// The caller is responsible to ensure the username has been normalized already.
func (s *authenticationServer) createSessionFor(
	ctx context.Context, userName username.SQLUsername,
) (*http.Cookie, error) {
	// Create a new database session, generating an ID and secret key.
	id, secret, err := s.newAuthSession(ctx, userName)
	if err != nil {
		return nil, apiInternalError(ctx, err)
	}

	// Generate and set a session cookie for the response. Because HTTP cookies
	// must be strings, the cookie value (a marshaled protobuf) is encoded in
	// base64.
	cookieValue := &serverpb.SessionCookie{
		ID:     id,
		Secret: secret,
	}
	return EncodeSessionCookie(cookieValue, !s.cfg.DisableTLSForHTTP)
}

// UserLogout allows a user to terminate their currently active session.
func (s *authenticationServer) UserLogout(
	ctx context.Context, req *serverpb.UserLogoutRequest,
) (*serverpb.UserLogoutResponse, error) {
	md, ok := grpcutil.FastFromIncomingContext(ctx)
	if !ok {
		return nil, apiInternalError(ctx, fmt.Errorf("couldn't get incoming context"))
	}
	sessionIDs := md.Get(webSessionIDKeyStr)
	if len(sessionIDs) != 1 {
		return nil, apiInternalError(ctx, fmt.Errorf("couldn't get incoming context"))
	}

	sessionID, err := strconv.Atoi(sessionIDs[0])
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"invalid session id: %d", sessionID)
	}

	// Revoke the session.
	if n, err := s.sqlServer.internalExecutor.ExecEx(
		ctx,
		"revoke-auth-session",
		nil, /* txn */
		sessiondata.RootUserSessionDataOverride,
		`UPDATE system.web_sessions SET "revokedAt" = now() WHERE id = $1`,
		sessionID,
	); err != nil {
		return nil, apiInternalError(ctx, err)
	} else if n == 0 {
		err := status.Errorf(
			codes.InvalidArgument,
			"session with id %d nonexistent", sessionID)
		log.Infof(ctx, "%v", err)
		return nil, err
	}

	// Send back a header which will cause the browser to destroy the cookie.
	// See https://tools.ietf.org/search/rfc6265, page 7.
	cookie := makeCookieWithValue("", false /* forHTTPSOnly */)
	cookie.MaxAge = -1

	// Set the cookie header on the outgoing response.
	if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", cookie.String())); err != nil {
		return nil, apiInternalError(ctx, err)
	}

	return &serverpb.UserLogoutResponse{}, nil
}

// verifySession verifies the existence and validity of the session claimed by
// the supplied SessionCookie. Returns three parameters: a boolean indicating if
// the session was valid, the username associated with the session (if
// validated), and an error for any internal errors which prevented validation.
func (s *authenticationServer) verifySession(
	ctx context.Context, cookie *serverpb.SessionCookie,
) (bool, string, error) {
	// Look up session in database and verify hashed secret value.
	const sessionQuery = `
SELECT "hashedSecret", "username", "expiresAt", "revokedAt"
FROM system.web_sessions
WHERE id = $1`

	var (
		hashedSecret []byte
		userName     string
		expiresAt    time.Time
		isRevoked    bool
	)

	row, err := s.sqlServer.internalExecutor.QueryRowEx(
		ctx,
		"lookup-auth-session",
		nil, /* txn */
		sessiondata.RootUserSessionDataOverride,
		sessionQuery, cookie.ID)
	if row == nil || err != nil {
		return false, "", err
	}

	if row.Len() != 4 ||
		row[0].ResolvedType().Family() != types.BytesFamily ||
		row[1].ResolvedType().Family() != types.StringFamily ||
		row[2].ResolvedType().Family() != types.TimestampFamily {
		return false, "", errors.Errorf("values returned from auth session lookup do not match expectation")
	}

	// Extract datum values.
	hashedSecret = []byte(*row[0].(*tree.DBytes))
	userName = string(*row[1].(*tree.DString))
	expiresAt = row[2].(*tree.DTimestamp).Time
	isRevoked = row[3].ResolvedType().Family() != types.UnknownFamily

	if isRevoked {
		return false, "", nil
	}

	if now := s.sqlServer.execCfg.Clock.PhysicalTime(); !now.Before(expiresAt) {
		return false, "", nil
	}

	hasher := sha256.New()
	_, _ = hasher.Write(cookie.Secret)
	hashedCookieSecret := hasher.Sum(nil)
	if !bytes.Equal(hashedSecret, hashedCookieSecret) {
		return false, "", nil
	}

	return true, userName, nil
}

// verifyPasswordDBConsole verifies the passed username/password pair against the
// system.users table. The returned boolean indicates whether or not the
// verification succeeded; an error is returned if the validation process could
// not be completed.
//
// This function should *not* be used to validate logins into the SQL
// shell since it checks a separate authentication scheme.
//
// The caller is responsible for ensuring that the username is normalized.
// (CockroachDB has case-insensitive usernames, unlike PostgreSQL.)
func (s *authenticationServer) verifyPasswordDBConsole(
	ctx context.Context, userName username.SQLUsername, passwordStr string,
) (valid bool, expired bool, err error) {
	exists, _, _, canLoginDBConsole, _, _, pwRetrieveFn, err := sql.GetUserSessionInitInfo(
		ctx,
		s.sqlServer.execCfg,
		userName,
		"", /* databaseName */
	)
	if err != nil {
		return false, false, err
	}
	if !exists || !canLoginDBConsole {
		return false, false, nil
	}
	expired, hashedPassword, err := pwRetrieveFn(ctx)
	if err != nil {
		return false, false, err
	}

	if expired {
		return false, true, nil
	}

	ok, err := password.CompareHashAndCleartextPassword(
		ctx, hashedPassword, passwordStr, security.GetExpensiveHashComputeSem(ctx),
	)
	if ok && err == nil {
		// Password authentication succeeded using cleartext.  If the
		// stored hash was encoded using crdb-bcrypt, we might want to
		// upgrade it to SCRAM instead.
		//
		// This auto-conversion is a CockroachDB-specific feature, which
		// pushes clusters upgraded from a previous version into using
		// SCRAM-SHA-256.
		sql.MaybeConvertStoredPasswordHash(ctx,
			s.sqlServer.execCfg,
			userName,
			passwordStr, hashedPassword)
	}
	return ok, false, err
}

// CreateAuthSecret creates a secret, hash pair to populate a session auth token.
func CreateAuthSecret() (secret, hashedSecret []byte, err error) {
	secret = make([]byte, secretLength)
	if _, err := rand.Read(secret); err != nil {
		return nil, nil, err
	}

	hasher := sha256.New()
	_, _ = hasher.Write(secret)
	hashedSecret = hasher.Sum(nil)
	return secret, hashedSecret, nil
}

// newAuthSession attempts to create a new authentication session for the given
// user. If successful, returns the ID and secret value for the new session.
//
// The caller is responsible to ensure the username has been normalized already.
func (s *authenticationServer) newAuthSession(
	ctx context.Context, userName username.SQLUsername,
) (int64, []byte, error) {
	webSessionsTableHasUserIDCol := s.sqlServer.execCfg.Settings.Version.IsActive(ctx,
		clusterversion.V23_1WebSessionsTableHasUserIDColumn)

	secret, hashedSecret, err := CreateAuthSecret()
	if err != nil {
		return 0, nil, err
	}

	expiration := s.sqlServer.execCfg.Clock.PhysicalTime().Add(webSessionTimeout.Get(&s.sqlServer.execCfg.Settings.SV))

	insertSessionStmt := `
INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt")
VALUES($1, $2, $3)
RETURNING id
`
	if webSessionsTableHasUserIDCol {
		insertSessionStmt = `
INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt", user_id)
VALUES($1, $2, $3, (SELECT user_id FROM system.users WHERE username = $2))
RETURNING id
`
	}
	var id int64

	row, err := s.sqlServer.internalExecutor.QueryRowEx(
		ctx,
		"create-auth-session",
		nil, /* txn */
		sessiondata.RootUserSessionDataOverride,
		insertSessionStmt,
		hashedSecret,
		userName.Normalized(),
		expiration,
	)
	if err != nil {
		return 0, nil, err
	}
	if row.Len() != 1 || row[0].ResolvedType().Family() != types.IntFamily {
		return 0, nil, errors.Errorf(
			"expected create auth session statement to return exactly one integer, returned %v",
			row,
		)
	}

	// Extract integer value from single datum.
	id = int64(*row[0].(*tree.DInt))

	return id, secret, nil
}

// authenticationMux implements http.Handler, and is used to provide session
// authentication for an arbitrary "inner" handler.
type authenticationMux struct {
	server *authenticationServer
	inner  http.Handler

	// allowAnonymous, if true, indicates that the authentication mux should
	// call its inner HTTP handler even if the request doesn't have a valid
	// session. If there is a valid session, the mux calls its inner handler
	// with a context containing the username and session ID.
	//
	// If allowAnonymous is false, the mux returns an error if there is no
	// valid session.
	allowAnonymous bool
}

func newAuthenticationMuxAllowAnonymous(
	s *authenticationServer, inner http.Handler,
) *authenticationMux {
	return &authenticationMux{
		server:         s,
		inner:          inner,
		allowAnonymous: true,
	}
}

func newAuthenticationMux(s *authenticationServer, inner http.Handler) *authenticationMux {
	return &authenticationMux{
		server:         s,
		inner:          inner,
		allowAnonymous: false,
	}
}

type webSessionUserKey struct{}
type webSessionIDKey struct{}

const webSessionUserKeyStr = "websessionuser"
const webSessionIDKeyStr = "websessionid"

func (am *authenticationMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	username, cookie, err := am.getSession(w, req)
	if err == nil {
		req = req.WithContext(
			contextWithHTTPAuthInfo(req.Context(), username, cookie.ID))
	} else if !am.allowAnonymous {
		if log.V(1) {
			log.Infof(req.Context(), "web session error: %v", err)
		}
		http.Error(w, "a valid authentication cookie is required", http.StatusUnauthorized)
		return
	}
	am.inner.ServeHTTP(w, req)
}

// EncodeSessionCookie encodes a SessionCookie proto into an http.Cookie.
// The flag forHTTPSOnly, if set, produces the "Secure" flag on the
// resulting HTTP cookie, which means the cookie should only be
// transmitted over HTTPS channels. Note that a cookie without
// the "Secure" flag can be transmitted over either HTTP or HTTPS channels.
func EncodeSessionCookie(
	sessionCookie *serverpb.SessionCookie, forHTTPSOnly bool,
) (*http.Cookie, error) {
	cookieValueBytes, err := protoutil.Marshal(sessionCookie)
	if err != nil {
		return nil, errors.Wrap(err, "session cookie could not be encoded")
	}
	value := base64.StdEncoding.EncodeToString(cookieValueBytes)
	return makeCookieWithValue(value, forHTTPSOnly), nil
}

func makeCookieWithValue(value string, forHTTPSOnly bool) *http.Cookie {
	return &http.Cookie{
		Name:     SessionCookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   forHTTPSOnly,
	}
}

// getSession decodes the cookie from the request, looks up the corresponding session, and
// returns the logged in user name. If there's an error, it returns an error value and the
// HTTP error code.
func (am *authenticationMux) getSession(
	w http.ResponseWriter, req *http.Request,
) (string, *serverpb.SessionCookie, error) {
	st := am.server.sqlServer.cfg.Settings
	cookie, err := findAndDecodeSessionCookie(req.Context(), st, req.Cookies())
	if err != nil {
		return "", nil, err
	}

	valid, username, err := am.server.verifySession(req.Context(), cookie)
	if err != nil {
		err := apiInternalError(req.Context(), err)
		return "", nil, err
	}
	if !valid {
		err := errors.New("the provided authentication session could not be validated")
		return "", nil, err
	}

	return username, cookie, nil
}

func decodeSessionCookie(encodedCookie *http.Cookie) (*serverpb.SessionCookie, error) {
	// Cookie value should be a base64 encoded protobuf.
	cookieBytes, err := base64.StdEncoding.DecodeString(encodedCookie.Value)
	if err != nil {
		return nil, errors.Wrap(err, "session cookie could not be decoded")
	}
	var sessionCookieValue serverpb.SessionCookie
	if err := protoutil.Unmarshal(cookieBytes, &sessionCookieValue); err != nil {
		return nil, errors.Wrap(err, "session cookie could not be unmarshaled")
	}
	return &sessionCookieValue, nil
}

// authenticationHeaderMatcher is a GRPC header matcher function, which provides
// a conversion from GRPC headers to HTTP headers. This function is needed to
// attach the "set-cookie" header to the response; by default, Grpc-Gateway
// adds a prefix to all GRPC headers before adding them to the response.
func authenticationHeaderMatcher(key string) (string, bool) {
	// GRPC converts all headers to lower case.
	if key == "set-cookie" {
		return key, true
	}
	// This is the default behavior of GRPC Gateway when matching headers -
	// it adds a constant prefix to the HTTP header so that by default they
	// do not conflict with any HTTP headers that might be used by the
	// browser.
	// TODO(mrtracy): A function "DefaultOutgoingHeaderMatcher" should
	// likely be added to GRPC Gateway so that the logic does not have to be
	// duplicated here.
	return fmt.Sprintf("%s%s", gwruntime.MetadataHeaderPrefix, key), true
}

// contextWithHTTPAuthInfo embeds the HTTP authentication details into
// a go context. Meant for use with userFromHTTPAuthInfoContext().
func contextWithHTTPAuthInfo(
	ctx context.Context, username string, sessionID int64,
) context.Context {
	ctx = context.WithValue(ctx, webSessionUserKey{}, username)
	if sessionID != 0 {
		ctx = context.WithValue(ctx, webSessionIDKey{}, sessionID)
	}
	return ctx
}

// userFromHTTPAuthInfoContext returns a SQL username from the request
// context of a HTTP route requiring login. Only use in routes that require
// login (e.g. requiresAuth = true in the API v2 route definition).
//
// Do not use this function in _RPC_ API handlers. These access their
// SQL identity via the RPC incoming context. See
// userFromIncomingRPCContext().
func userFromHTTPAuthInfoContext(ctx context.Context) username.SQLUsername {
	return username.MakeSQLUsernameFromPreNormalizedString(ctx.Value(webSessionUserKey{}).(string))
}

// maybeUserFromHTTPAuthInfoContext is like userFromHTTPAuthInfoContext but
// it returns a boolean false if there is no user in the context.
func maybeUserFromHTTPAuthInfoContext(ctx context.Context) (username.SQLUsername, bool) {
	if u := ctx.Value(webSessionUserKey{}); u != nil {
		return username.MakeSQLUsernameFromPreNormalizedString(u.(string)), true
	}
	return username.SQLUsername{}, false
}

// translateHTTPAuthInfoToGRPCMetadata translates the context.Value
// that results from HTTP authentication into gRPC metadata suitable
// for use by RPC API handlers.
func translateHTTPAuthInfoToGRPCMetadata(ctx context.Context, _ *http.Request) metadata.MD {
	md := metadata.MD{}
	if user := ctx.Value(webSessionUserKey{}); user != nil {
		md.Set(webSessionUserKeyStr, user.(string))
	}
	if sessionID := ctx.Value(webSessionIDKey{}); sessionID != nil {
		md.Set(webSessionIDKeyStr, fmt.Sprintf("%v", sessionID))
	}
	return md
}

// forwardSQLIdentityThroughRPCCalls forwards the SQL identity of the
// original request (as populated by translateHTTPAuthInfoToGRPCMetadata in
// grpc-gateway) so it remains available to the remote node handling
// the request.
func forwardSQLIdentityThroughRPCCalls(ctx context.Context) context.Context {
	if md, ok := grpcutil.FastFromIncomingContext(ctx); ok {
		if u, ok := md[webSessionUserKeyStr]; ok {
			return metadata.NewOutgoingContext(ctx, metadata.MD{webSessionUserKeyStr: u})
		}
	}
	return ctx
}

// forwardHTTPAuthInfoToRPCCalls converts an HTTP API (v1 or v2) context, to one that
// can issue outgoing RPC requests under the same logged-in user.
func forwardHTTPAuthInfoToRPCCalls(ctx context.Context, r *http.Request) context.Context {
	md := translateHTTPAuthInfoToGRPCMetadata(ctx, r)
	return metadata.NewOutgoingContext(ctx, md)
}

// userFromIncomingRPCContext is to be used in RPC API handlers. It
// assumes the SQL identity was populated in the context implicitly by
// gRPC via translateHTTPAuthInfoToGRPCMetadata(), or explicitly via
// forwardHTTPAuthInfoToRPCCalls() or
// forwardSQLIdentityThroughRPCCalls().
//
// Do not use this function in _HTTP_ API handlers. Those access their
// SQL identity via a special context key. See
// userFromHTTPAuthInfoContext().
func userFromIncomingRPCContext(ctx context.Context) (res username.SQLUsername, err error) {
	md, ok := grpcutil.FastFromIncomingContext(ctx)
	if !ok {
		return username.RootUserName(), nil
	}
	usernames, ok := md[webSessionUserKeyStr]
	if !ok {
		// If the incoming context has metadata but no attached web session user,
		// it's a gRPC / internal SQL connection which has root on the cluster.
		// This assumption is a historical hiccup, and would be best described
		// as a bug. See: https://github.com/cockroachdb/cockroach/issues/45018
		return username.RootUserName(), nil
	}
	if len(usernames) != 1 {
		log.Warningf(ctx, "context's incoming metadata contains unexpected number of usernames: %+v ", md)
		return res, fmt.Errorf(
			"context's incoming metadata contains unexpected number of usernames: %+v ", md)
	}
	// At this point the user is already logged in, so we can assume
	// the username has been normalized already.
	username := username.MakeSQLUsernameFromPreNormalizedString(usernames[0])
	return username, nil
}

// sessionCookieValue defines the data needed to construct the
// aggregate session cookie in the order provided.
type sessionCookieValue struct {
	// The name of the tenant.
	name string
	// The value of set-cookie.
	setCookie string
}

// createAggregatedSessionCookieValue is used for multi-tenant login.
// It takes a slice of sessionCookieValue and converts it to a single
// string which is the aggregated session. Currently the format of the
// aggregated session is: `session,tenant_name,session2,tenant_name2` etc.
func createAggregatedSessionCookieValue(sessionCookieValue []sessionCookieValue) string {
	var sessionsStr string
	for _, val := range sessionCookieValue {
		sessionCookieSlice := strings.Split(strings.ReplaceAll(val.setCookie, "session=", ""), ";")
		sessionsStr += sessionCookieSlice[0] + "," + val.name + ","
	}
	if len(sessionsStr) > 0 {
		sessionsStr = sessionsStr[:len(sessionsStr)-1]
	}
	return sessionsStr
}

// findAndDecodeSessionCookie looks for multitenant-session and session cookies
// in the cookies slice. If they are found the value will need to be processed if
// it is a multitenant-session cookie (see findSessionCookieValueForTenant for details)
// and then decoded. If there is an error in decoding or processing, the function
// will return an error.
func findAndDecodeSessionCookie(
	ctx context.Context, st *cluster.Settings, cookies []*http.Cookie,
) (*serverpb.SessionCookie, error) {
	found := false
	var sessionCookie *serverpb.SessionCookie
	tenantSelectCookieVal := findTenantSelectCookieValue(cookies)
	for _, cookie := range cookies {
		if cookie.Name != SessionCookieName {
			continue
		}
		found = true
		mtSessionVal, err := findSessionCookieValueForTenant(
			st,
			cookie,
			tenantSelectCookieVal)
		if err != nil {
			return sessionCookie, apiInternalError(ctx, err)
		}
		if mtSessionVal != "" {
			cookie.Value = mtSessionVal
		}
		sessionCookie, err = decodeSessionCookie(cookie)
		if err != nil {
			// Multiple cookies with the same name may be included in the
			// header. We continue searching even if we find a matching
			// name with an invalid value.
			log.Infof(ctx, "found a matching cookie that failed decoding: %v", err)
			found = false
			continue
		}
		break
	}
	if !found {
		return nil, http.ErrNoCookie
	}
	return sessionCookie, nil
}

// findSessionCookieValueForTenant finds the encoded session in the provided
// aggregated session cookie value established in multi-tenant clusters that's
// associated with the provided tenant name. If an empty tenant name is provided,
// we default to the defaultTenantSelect cluster setting value.
//
// If the method cannot find a match between the tenant name and session, or
// if the provided session cookie is nil, it will return an empty string.
//
// e.g. tenant name is "system" and session cookie's value is
// "abcd1234,system,efgh5678,app" the output will be "abcd1234".
//
// In the case of legacy session cookies, where tenant names are not encoded
// into the cookie value, we assume that the session belongs to defaultTenantSelect.
// Note that these legacy session cookies only contained a single session string
// as the cookie's value.
func findSessionCookieValueForTenant(
	st *cluster.Settings, sessionCookie *http.Cookie, tenantName string,
) (string, error) {
	if sessionCookie == nil {
		return "", nil
	}
	if mtSessionStr := sessionCookie.Value; sessionCookie.Value != "" {
		sessionSlice := strings.Split(mtSessionStr, ",")
		if len(sessionSlice) == 1 {
			// If no separator was found in the cookie value, this is likely
			// a cookie from a previous CRDB version where the cookie value
			// contained a single session string without any tenant names encoded.
			// To maintain backwards compatibility, assume this session belongs
			// to the default tenant. In this case, the entire cookie value is
			// the session string.
			return mtSessionStr, nil
		}
		if tenantName == "" {
			tenantName = defaultTenantSelect.Get(&st.SV)
		}
		var encodedSession string
		for idx, val := range sessionSlice {
			if val == tenantName && idx > 0 {
				encodedSession = sessionSlice[idx-1]
			}
		}
		if encodedSession == "" {
			return "", errors.Newf("unable to find session cookie value that matches tenant %q", tenantName)
		}
		return encodedSession, nil
	}
	return "", nil
}

// findTenantSelectCookieValue iterates through all request cookies in order
// to find the value of the tenant select cookie. If the tenant select cookie
// is not found, it returns the empty string.
func findTenantSelectCookieValue(cookies []*http.Cookie) string {
	for _, c := range cookies {
		if c.Name == TenantSelectCookieName {
			return c.Value
		}
	}
	return ""
}
