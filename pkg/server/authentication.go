// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"time"

	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

const (
	// authPrefix is the prefix for RESTful endpoints used to provide
	// authentication methods.
	authPrefix = "/_auth/v1/"
	// secretLength is the number of random bytes generated for session secrets.
	secretLength      = 16
	sessionCookieName = "session"
)

var webSessionTimeout = settings.RegisterNonNegativeDurationSetting(
	"server.web_session_timeout",
	"the duration that a newly created web session will be valid",
	7*24*time.Hour,
)

type authenticationServer struct {
	server     *Server
	executor   *sql.InternalExecutor
	memMetrics *sql.MemoryMetrics
}

// newAuthenticationServer allocates and returns a new REST server for
// authentication APIs.
func newAuthenticationServer(s *Server, ie *sql.InternalExecutor) *authenticationServer {
	return &authenticationServer{
		server:     s,
		executor:   ie,
		memMetrics: &s.adminMemMetrics,
	}
}

// RegisterService registers the GRPC service.
func (s *authenticationServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterAuthenticationServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *authenticationServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	return serverpb.RegisterAuthenticationHandler(ctx, mux, conn)
}

// UserLogin verifies an incoming request by a user to create an web
// authentication session. It checks the provided credentials against the
// system.users table, and if successful creates a new authentication session.
// The session's ID and secret are returned to the caller as an HTTP cookie,
// added via a "Set-Cookie" header.
func (s *authenticationServer) UserLogin(
	ctx context.Context, req *serverpb.UserLoginRequest,
) (*serverpb.UserLoginResponse, error) {
	username := req.Username
	if username == "" {
		return nil, status.Errorf(
			codes.Unauthenticated,
			"no username was provided",
		)
	}

	// Root user does not have a password, simply disallow this.
	if username == security.RootUser {
		return nil, status.Errorf(
			codes.Unauthenticated,
			"user %s must use certificate authentication instead of password authentication",
			security.RootUser,
		)
	}

	// Verify the provided username/password pair.
	verified, err := s.verifyPassword(ctx, username, req.Password)
	if err != nil {
		return nil, apiInternalError(ctx, err)
	}
	if !verified {
		return nil, status.Errorf(
			codes.Unauthenticated,
			"the provided username and password did not match any credentials on the server",
		)
	}

	// Create a new database session, generating an ID and secret key.
	id, secret, err := s.newAuthSession(ctx, username)
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
	cookie, err := encodeSessionCookie(cookieValue)
	if err != nil {
		return nil, apiInternalError(ctx, err)
	}

	// Set the cookie header on the outgoing response.
	if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", cookie.String())); err != nil {
		return nil, apiInternalError(ctx, err)
	}

	return &serverpb.UserLoginResponse{}, nil
}

// UserLogout allows a user to terminate their currently active session.
func (s *authenticationServer) UserLogout(
	ctx context.Context, req *serverpb.UserLogoutRequest,
) (*serverpb.UserLogoutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "Logout method has not yet been implemented.")
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
FROM system.public.web_sessions
WHERE id = $1`

	var (
		sessionFound bool
		hashedSecret []byte
		username     string
		expiresAt    time.Time
		isRevoked    bool
	)

	if err := s.server.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		datum, err := s.executor.QueryRowInTransaction(
			ctx,
			"lookup-auth-session",
			txn,
			sessionQuery,
			cookie.ID,
		)
		if err != nil {
			return err
		}

		if datum.Len() == 0 {
			return nil
		}
		if datum.Len() != 4 ||
			datum[0].ResolvedType() != types.Bytes ||
			datum[1].ResolvedType() != types.String ||
			datum[2].ResolvedType() != types.Timestamp {
			return errors.Errorf("values returned from auth session lookup do not match expectation")
		}

		// Extract datum values.
		sessionFound = true
		hashedSecret = []byte(*datum[0].(*tree.DBytes))
		username = string(*datum[1].(*tree.DString))
		expiresAt = datum[2].(*tree.DTimestamp).Time
		isRevoked = datum[3].ResolvedType() != types.Null
		return nil
	}); err != nil {
		return false, "", err
	}

	if !sessionFound {
		return false, "", nil
	}

	if isRevoked {
		return false, "", nil
	}

	if now := s.server.clock.PhysicalTime(); !now.Before(expiresAt) {
		return false, "", nil
	}

	hasher := sha256.New()
	hashedCookieSecret := hasher.Sum(cookie.Secret)
	if !bytes.Equal(hashedSecret, hashedCookieSecret) {
		return false, "", nil
	}

	return true, username, nil
}

// verifyPassword verifies the passed username/password pair against the
// system.users table. The returned boolean indicates whether or not the
// verification succeeded; an error is returned if the validation process could
// not be completed.
func (s *authenticationServer) verifyPassword(
	ctx context.Context, username string, password string,
) (bool, error) {
	exists, hashedPassword, err := sql.GetUserHashedPassword(
		ctx, s.server.sqlExecutor, s.memMetrics, username,
	)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	return (security.CompareHashAndPassword(hashedPassword, password) == nil), nil
}

// newAuthSession attempts to create a new authentication session for the given
// user. If successful, returns the ID and secret value for the new session.
func (s *authenticationServer) newAuthSession(
	ctx context.Context, username string,
) (int64, []byte, error) {
	secret := make([]byte, secretLength)
	if _, err := rand.Read(secret); err != nil {
		return 0, nil, err
	}

	hasher := sha256.New()
	hashedSecret := hasher.Sum(secret)
	expiration := s.server.clock.PhysicalTime().Add(webSessionTimeout.Get(&s.server.st.SV))

	insertSessionStmt := `
INSERT INTO system.public.web_sessions ("hashedSecret", username, "expiresAt")
VALUES($1, $2, $3)
RETURNING id
`
	var id int64

	if err := s.server.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		datum, err := s.executor.QueryRowInTransaction(
			ctx,
			"create-auth-session",
			txn,
			insertSessionStmt,
			hashedSecret,
			username,
			expiration,
		)
		if err != nil {
			return err
		}
		if datum.Len() != 1 || datum[0].ResolvedType() != types.Int {
			return errors.Errorf(
				"expected create auth session statement to return exactly one integer, returned %v",
				datum,
			)
		}

		// Extract integer value from single datum.
		id = int64(*datum[0].(*tree.DInt))
		return nil
	}); err != nil {
		return 0, nil, err
	}

	return id, secret, nil
}

// authenticationMux implements http.Handler, and is used to provide session
// authentication for an arbitrary "inner" handler.
type authenticationMux struct {
	server *authenticationServer
	inner  http.Handler
}

func newAuthenticationMux(s *authenticationServer, inner http.Handler) *authenticationMux {
	return &authenticationMux{
		server: s,
		inner:  inner,
	}
}

func (am *authenticationMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Validate the returned cookie.
	rawCookie, err := req.Cookie(sessionCookieName)
	if err != nil {
		err = errors.Wrap(err, "a valid authentication cookie is required")
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	cookie, err := decodeSessionCookie(rawCookie)
	if err != nil {
		err = errors.Wrap(err, "a valid authentication cookie is required")
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	valid, _, err := am.server.verifySession(req.Context(), cookie)
	if err != nil {
		http.Error(w, apiInternalError(req.Context(), err).Error(), http.StatusInternalServerError)
		return
	}
	if !valid {
		http.Error(w, "The provided authentication session could not be validated.", http.StatusUnauthorized)
		return
	}

	// TODO(mrtracy): At this point, we should set the session ID and username
	// on the request context. However, GRPC Gateway does not correctly use the
	// request context, and even if it did we are not providing any
	// authorization for API methods (only authentication).
	am.inner.ServeHTTP(w, req)
}

func encodeSessionCookie(sessionCookie *serverpb.SessionCookie) (*http.Cookie, error) {
	cookieValueBytes, err := protoutil.Marshal(sessionCookie)
	if err != nil {
		return nil, errors.Wrap(err, "session cookie could not be encoded")
	}

	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    base64.StdEncoding.EncodeToString(cookieValueBytes),
		HttpOnly: true,
		Secure:   true,
	}, nil
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
