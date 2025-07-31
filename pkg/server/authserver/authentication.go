// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package authserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/settings/rulebasedscanner"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
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
	// LoginPath is the URL path to the login handler.
	LoginPath = "/login"

	// LogoutPath is the URL path to the logout handler.
	LogoutPath = "/logout"

	// secretLength is the number of random bytes generated for session secrets.
	secretLength = 16

	// DemoLoginPath is the demo shell auto-login URL.
	DemoLoginPath = "/demologin"

	// AuthorizationHeader is the 'Authorization' header in the HTTP request.
	AuthorizationHeader = "Authorization"

	// bearerType denotes Bearer token based request authentication.
	// In this case, the Authorization header is set to "Bearer <token>".
	bearerType = "Bearer"

	// UsernameHeader is the HTTP request header to hold the SQL username. This is used to identify a
	// specific user identity making the request, when there is a possibility to match against
	// multiple identities, for example, a JWT could be matched to multiple principals/identities.
	UsernameHeader = "X-Cockroach-User"
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

// WebSessionTimeout is the cluster setting for web session TTL.
var WebSessionTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"server.web_session_timeout",
	"the duration that a newly created web session will be valid",
	7*24*time.Hour,
	settings.NonNegativeDuration,
	settings.WithName("server.web_session.timeout"),
	settings.WithPublic)

// jwtVerifier is a duplicate of the singleton global pgwire object which gets
// initialized from VerifyJWT method whenever a JWT auth attempt for accessing
// DB console APIs happens. It depends on jwtauthccl module to be imported
// properly to override its default ConfigureJWTAuth constructor.
var jwtVerifier = struct {
	sync.Once
	j pgwire.JWTVerifier
}{}

type authenticationServer struct {
	cfg       *base.Config
	sqlServer SQLServerInterface
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

// ldapManager is a duplicate of singleton global pgwire object which gets
// initialized from UserLogin method whenever an LDAP auth attempt happens. It
// depends on ldapccl module to be imported properly to override its default
// ConfigureLDAPAuth constructor.
var ldapManager = struct {
	sync.Once
	m pgwire.LDAPManager
}{}

// UserLogin is part of the Server interface.
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
	// Verify the user and check if DB console session could be started.
	verified, pwRetrieveFn, err := s.VerifyUserSessionDBConsole(ctx, username)
	if err != nil {
		return nil, srverrors.APIInternalError(ctx, err)
	}
	if !verified {
		return nil, errWebAuthenticationFailure
	}

	ldapAuthSuccess := false
	originIP := s.lookupIncomingRequestOriginIP(ctx)
	hbaConf, identMap := s.sqlServer.PGServer().GetAuthenticationConfiguration()
	authMethod, hbaEntry, err := s.lookupAuthenticationMethodUsingRules(hba.ConnHostSSL, hbaConf, username, originIP)
	if err != nil {
		if log.V(1) {
			log.Infof(ctx, "invalid retrieval of HBA entry: error: %v", err)
		}
	} else if authMethod.String() == "ldap" {
		if log.V(1) {
			log.Infof(ctx, "retrieved LDAP HBA entry successfully: authMethod: %s, hbaEntry: %v", authMethod.String(), hbaEntry)
		}
		execCfg := s.sqlServer.ExecutorConfig()
		ldapManager.Do(func() {
			if ldapManager.m == nil {
				ldapManager.m = pgwire.ConfigureLDAPAuth(ctx, execCfg.AmbientCtx, execCfg.Settings, execCfg.NodeInfo.LogicalClusterID())
			}
		})
		ldapUserDN, detailedErrors, authError := ldapManager.m.FetchLDAPUserDN(ctx, execCfg.Settings, username, hbaEntry, identMap)
		if authError != nil {
			if log.V(1) {
				log.Infof(ctx, "ldap search response error: ldapUserDN %v, authError %v, detailedErrors %v", ldapUserDN, authError, detailedErrors)
			}
		} else {
			detailedErrors, authError = ldapManager.m.ValidateLDAPLogin(ctx, execCfg.Settings, ldapUserDN, username, req.Password, hbaEntry, identMap)
			if authError != nil {
				if log.V(1) {
					log.Infof(ctx, "ldap bind response error: ldapUserDN %v, authError %v, detailedErrors %v", ldapUserDN, authError, detailedErrors)
				}
			} else {
				ldapAuthSuccess = true
			}
		}
	}

	if !ldapAuthSuccess {
		// Verify the provided username/password pair.
		verified, expired, err := s.VerifyPasswordDBConsole(ctx, username, req.Password, pwRetrieveFn)
		if err != nil {
			return nil, srverrors.APIInternalError(ctx, err)
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
	}

	cookie, err := s.createSessionFor(ctx, username)
	if err != nil {
		return nil, srverrors.APIInternalError(ctx, err)
	}

	// Set the cookie header on the outgoing response.
	if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", cookie.String())); err != nil {
		return nil, srverrors.APIInternalError(ctx, err)
	}

	return &serverpb.UserLoginResponse{}, nil
}

// DemoLogin is the same as UserLogin but using the GET method.
// It is only available for 'cockroach demo' and test clusters.
func (s *authenticationServer) DemoLogin(w http.ResponseWriter, req *http.Request) {
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
	// Verify the user and check if DB console session could be started.
	verified, pwRetrieveFn, err := s.VerifyUserSessionDBConsole(ctx, username)
	if err != nil {
		fail(err)
		return
	}
	if !verified {
		fail(errors.New("password invalid"))
		return
	}

	// Verify the provided username/password pair.
	verified, expired, err := s.VerifyPasswordDBConsole(ctx, username, password, pwRetrieveFn)
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

// UserLoginFromSSO is part of the Server interface.
func (s *authenticationServer) UserLoginFromSSO(
	ctx context.Context, reqUsername string,
) (*http.Cookie, error) {
	// In CockroachDB SQL, unlike in PostgreSQL, usernames are
	// case-insensitive. Therefore we need to normalize the username
	// here, so that the normalized username is retained in the session
	// table: the APIs extract the username from the session table
	// without further normalization.
	username, _ := username.MakeSQLUsernameFromUserInput(reqUsername, username.PurposeValidation)

	exists, _, canLoginDBConsole, _, _, _, _, _, err := sql.GetUserSessionInitInfo(
		ctx,
		s.sqlServer.ExecutorConfig(),
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
	id, secret, err := s.NewAuthSession(ctx, userName)
	if err != nil {
		return nil, srverrors.APIInternalError(ctx, err)
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

// UserLogout is part of the Server interface.
func (s *authenticationServer) UserLogout(
	ctx context.Context, req *serverpb.UserLogoutRequest,
) (*serverpb.UserLogoutResponse, error) {
	md, ok := grpcutil.FastFromIncomingContext(ctx)
	if !ok {
		return nil, srverrors.APIInternalError(ctx, fmt.Errorf("couldn't get incoming context"))
	}
	sessionIDs := md.Get(webSessionIDKeyStr)
	if len(sessionIDs) != 1 {
		return nil, srverrors.APIInternalError(ctx, fmt.Errorf("couldn't get incoming context"))
	}

	sessionID, err := strconv.Atoi(sessionIDs[0])
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"invalid session id: %d", sessionID)
	}

	// Revoke the session.
	if n, err := s.sqlServer.InternalExecutor().ExecEx(
		ctx,
		"revoke-auth-session",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`UPDATE system.web_sessions SET "revokedAt" = now() WHERE id = $1`,
		sessionID,
	); err != nil {
		return nil, srverrors.APIInternalError(ctx, err)
	} else if n == 0 {
		err := status.Errorf(
			codes.InvalidArgument,
			"session with id %d nonexistent", sessionID)
		log.Infof(ctx, "%v", err)
		return nil, err
	}

	// Send back a header which will cause the browser to destroy the cookie.
	// See https://tools.ietf.org/search/rfc6265, page 7.
	cookie := CreateSessionCookie("", false /* forHTTPSOnly */)
	cookie.MaxAge = -1

	// Set the cookie header on the outgoing response.
	if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", cookie.String())); err != nil {
		return nil, srverrors.APIInternalError(ctx, err)
	}

	return &serverpb.UserLogoutResponse{}, nil
}

// VerifySession is part of the Server interface.
func (s *authenticationServer) VerifySession(
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

	row, err := s.sqlServer.InternalExecutor().QueryRowEx(
		ctx,
		"lookup-auth-session",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
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

	if now := s.sqlServer.ExecutorConfig().Clock.PhysicalTime(); !now.Before(expiresAt) {
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

// VerifyUserSessionDBConsole is part of the Server interface. It retrieves user
// session details required for proceeding with DB console login.
// (CockroachDB has case-insensitive usernames, unlike PostgreSQL.)
func (s *authenticationServer) VerifyUserSessionDBConsole(
	ctx context.Context, userName username.SQLUsername,
) (
	verified bool,
	pwRetrieveFn func(ctx context.Context) (expired bool, hashedPassword password.PasswordHash, err error),
	err error,
) {
	exists, _, canLoginDBConsole, _, _, _, _, pwRetrieveFn, err := sql.GetUserSessionInitInfo(
		ctx,
		s.sqlServer.ExecutorConfig(),
		userName,
		"", /* databaseName */
	)
	if err != nil {
		return false, nil, err
	}
	if !exists || !canLoginDBConsole {
		return false, nil, nil
	}
	return true, pwRetrieveFn, nil
}

// VerifyPasswordDBConsole is part of the Server interface.
// (CockroachDB has case-insensitive usernames, unlike PostgreSQL.)
func (s *authenticationServer) VerifyPasswordDBConsole(
	ctx context.Context,
	userName username.SQLUsername,
	passwordStr string,
	pwRetrieveFn func(ctx context.Context) (expired bool, hashedPassword password.PasswordHash, err error),
) (valid bool, expired bool, err error) {
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
			s.sqlServer.ExecutorConfig(),
			userName,
			passwordStr, hashedPassword)
	}
	return ok, false, err
}

// VerifyJWT is part of the Server interface.
func (s *authenticationServer) VerifyJWT(
	ctx context.Context, jwtStr, usernameOptional string,
) (valid bool, userName string, err error) {
	execCfg := s.sqlServer.ExecutorConfig()
	jwtVerifier.Do(func() {
		if jwtVerifier.j == nil {
			jwtVerifier.j = pgwire.ConfigureJWTAuth(
				ctx,
				execCfg.AmbientCtx,
				execCfg.Settings,
				execCfg.NodeInfo.LogicalClusterID(),
			)
		}
	})

	// Retrieve the matching user identity within the JWT.
	_, identMap := s.sqlServer.PGServer().GetAuthenticationConfiguration()
	inputUser, _ := username.MakeSQLUsernameFromUserInput(usernameOptional, username.PurposeValidation)
	retrievedUser, err := jwtVerifier.j.RetrieveIdentity(
		ctx,
		inputUser,
		[]byte(jwtStr),
		identMap,
	)
	if err != nil {
		return false, "", err
	}

	// Validate the user identity for access to DB console APIs.
	verified, _, err := s.VerifyUserSessionDBConsole(ctx, retrievedUser)
	if err != nil {
		return false, "", err
	}
	if !verified {
		return false, "", errors.Errorf("access denied for user %v", retrievedUser)
	}

	// Verify the JWT against the user identity. The configured cluster settings
	// for Cluster SSO via JWT are honored.
	//
	// TODO: RetrieveIdentity is again called within ValidateJWTLogin. While we do
	// not anticipate major performance degradation due to this, we could move
	// RetrieveIdentity outside of ValidateJWTLogin and expect callers (e.g. cluster SSO flow)
	// to always call these methods in the order: RetrieveIdentity, ValidateJWTLogin.
	if _, err = jwtVerifier.j.ValidateJWTLogin(
		ctx,
		execCfg.Settings,
		retrievedUser,
		[]byte(jwtStr),
		identMap,
	); err != nil {
		return false, "", err
	}

	return true, retrievedUser.Normalized(), nil
}

// lookupIncomingRequestOriginIP retrieves the client IP address from the
// context metadata, parsing the `x-forwarded-for` tag.
//
// TODO(souravcrl): update the implementation to handle load balancer added
// x-forwarded-for metadata tag. e.g.
// X-Forwarded-For: <client-ip>,<load-balancer-ip>
// X-Forwarded-For: <supplied-value>,<client-ip>,<load-balancer-ip>
// X-forwarded-for might also be missing and the implementation should consider
// X-real-ip tag for client IP address. Reference managed-service util
// `GetPublicPeerIP` which supports more IP formats and metadata tags.
func (s *authenticationServer) lookupIncomingRequestOriginIP(ctx context.Context) net.IP {
	const xForwardedFor = "x-forwarded-for"
	const localhost = "127.0.0.1"
	clientIP := localhost
	if reqMetadata, ok := metadata.FromIncomingContext(ctx); ok {
		if xForwardedFor, ok := reqMetadata[xForwardedFor]; ok && len(xForwardedFor) == 1 {
			clientIP = xForwardedFor[0]
		}
	}
	return net.ParseIP(clientIP)
}

func (s *authenticationServer) lookupAuthenticationMethodUsingRules(
	connType hba.ConnType, auth *hba.Conf, user username.SQLUsername, originIP net.IP,
) (authMethod rulebasedscanner.String, entry *hba.Entry, err error) {
	// Look up the method.
	for i := range auth.Entries {
		entry = &auth.Entries[i]
		var connMatch bool
		connMatch, err = entry.ConnMatches(connType, originIP)
		if err != nil {
			// TODO(souravcrl): Determine if an error should be reported
			// upon unknown address formats.
			// See: https://github.com/cockroachdb/cockroach/issues/43716
			return
		}
		if !connMatch {
			// The address does not match.
			continue
		}
		if !entry.UserMatches(user) {
			// The user does not match.
			continue
		}

		return entry.Method, entry, nil
	}

	// No match.
	err = errors.Errorf("no hba_conf entry for host %q, user %q", originIP, user)
	return rulebasedscanner.String{}, nil, err
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

// NewAuthSession attempts to create a new authentication session for
// the given user. If successful, returns the ID and secret value for
// the new session.
//
// The caller is responsible to ensure the username has been
// normalized already.
func (s *authenticationServer) NewAuthSession(
	ctx context.Context, userName username.SQLUsername,
) (int64, []byte, error) {
	st := s.sqlServer.ExecutorConfig().Settings

	secret, hashedSecret, err := CreateAuthSecret()
	if err != nil {
		return 0, nil, err
	}

	expiration := s.sqlServer.ExecutorConfig().Clock.PhysicalTime().Add(WebSessionTimeout.Get(&st.SV))

	insertSessionStmt := `
INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt", user_id)
VALUES($1, $2, $3, (SELECT user_id FROM system.users WHERE username = $2))
RETURNING id
`
	var id int64

	row, err := s.sqlServer.InternalExecutor().QueryRowEx(
		ctx,
		"create-auth-session",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
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

// ServeHTTP implements the http.Handler interface.
func (am *authenticationMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Call into the inner HTTP handler if either of the following holds true:
	// 1. a valid session cookie is provided
	// 2. a valid JWT is provided
	// 3. allowAnonymous is set to true

	// Validate session cookie in the request, if present.
	userName, cookie, werr := am.getSession(w, req)
	if werr == nil {
		req = req.WithContext(ContextWithHTTPAuthInfo(req.Context(), userName, cookie.ID))
	}

	// If the cookie is absent, fallback to JWT based authentication.
	jerr := errors.New("default JWT error")
	if errors.Is(werr, http.ErrNoCookie) {
		userName, jerr = am.verifyJWT(req)
		if jerr == nil {
			// JWTs are inherently stateless and do not require any session
			// info to be persisted, so set the session ID to 0 for now.
			// TODO(pritesh-lahoti): Evaluate if we need to persist the
			// session for observability or auditing purposes.
			req = req.WithContext(ContextWithHTTPAuthInfo(req.Context(), userName, 0))
		}
	}

	if !am.allowAnonymous && (werr != nil && jerr != nil) {
		if log.V(1) {
			log.Infof(req.Context(), "session error: %v; jwt error: %v", werr, jerr)
		}
		http.Error(w, "a valid authentication cookie or JWT is required", http.StatusUnauthorized)
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
	return CreateSessionCookie(value, forHTTPSOnly), nil
}

// getSession decodes the cookie from the request, looks up the corresponding session, and
// returns the logged in user name. If there's an error, it returns an error value and the
// HTTP error code.
func (am *authenticationMux) getSession(
	w http.ResponseWriter, req *http.Request,
) (string, *serverpb.SessionCookie, error) {
	st := am.server.sqlServer.ExecutorConfig().Settings
	cookie, err := FindAndDecodeSessionCookie(req.Context(), st, req.Cookies())
	if err != nil {
		return "", nil, err
	}

	valid, username, err := am.server.VerifySession(req.Context(), cookie)
	if err != nil {
		err := srverrors.APIInternalError(req.Context(), err)
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

// AuthenticationHeaderMatcher is a GRPC header matcher function, which provides
// a conversion from GRPC headers to HTTP headers. This function is needed to
// attach the "set-cookie" header to the response; by default, Grpc-Gateway
// adds a prefix to all GRPC headers before adding them to the response.
func AuthenticationHeaderMatcher(key string) (string, bool) {
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

// verifyJWT retrieves the JWT from the Authorization request header,
// verifies its signature and returns the associated username.
func (am *authenticationMux) verifyJWT(req *http.Request) (string, error) {
	authHeaderVal := req.Header.Get(AuthorizationHeader)
	authHeaderParts := strings.Split(strings.TrimSpace(authHeaderVal), " ")
	if len(authHeaderParts) != 2 || !strings.EqualFold(authHeaderParts[0], bearerType) {
		return "", errors.New("could not retrieve JWT from the request header")
	}

	jwtString := authHeaderParts[1]
	inputUsername := req.Header.Get(UsernameHeader)
	valid, retrievedUsername, err := am.server.VerifyJWT(req.Context(), jwtString, inputUsername)
	if err != nil {
		err := srverrors.APIInternalError(req.Context(), err)
		return "", err
	}

	if !valid {
		err := errors.New("the provided JWT could not be verified")
		return "", err
	}

	return retrievedUsername, nil
}
