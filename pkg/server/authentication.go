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
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

const (
	// authPrefix is the prefix for RESTful endpoints used to provide
	// authentication methods.
	authPrefix = "/_auth/v1/"
	// secretLength is the number of random bytes generated for session secrets.
	secretLength      = 16
	sessionCookieName = "session"
)

type authenticationServer struct {
	server     *Server
	executor   sql.InternalExecutor
	memMetrics *sql.MemoryMetrics
}

// newAuthenticationServer allocates and returns a new REST server for
// authentication APIs.
func newAuthenticationServer(s *Server) *authenticationServer {
	return &authenticationServer{
		server: s,
		executor: sql.InternalExecutor{
			LeaseManager: s.leaseMgr,
		},
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
		return nil, grpc.Errorf(
			codes.Unauthenticated,
			"no username was provided",
		)
	}

	// Root user does not have a password, simply disallow this.
	if username == security.RootUser {
		return nil, grpc.Errorf(
			codes.Unauthenticated,
			"user %s must use certificate authentication instead of password authentication",
			security.RootUser,
		)
	}

	// Verify the provided username/password pair.
	verified, err := s.verifyPassword(ctx, username, req.Password)
	if err != nil {
		return nil, apiError(ctx, err)
	}
	if !verified {
		return nil, grpc.Errorf(
			codes.Unauthenticated,
			"the provided username and password did not match any credentials on the server",
		)
	}

	// Create a new database session, generating an ID and secret key.
	id, secret, err := s.newAuthSession(ctx, username)
	if err != nil {
		return nil, apiError(ctx, err)
	}

	// Generate and set a session cookie for the response. Because HTTP cookies
	// must be strings, the cookie value (a marshaled protobuf) is encoded in
	// base64.
	cookieValue := &serverpb.SessionCookie{
		Id:     id,
		Secret: secret,
	}
	cookieValueBytes, err := cookieValue.Marshal()
	if err != nil {
		return nil, apiError(ctx, err)
	}

	cookie := http.Cookie{
		Name:     sessionCookieName,
		Value:    base64.StdEncoding.EncodeToString(cookieValueBytes),
		HttpOnly: true,
		Secure:   true,
	}

	// Set the cookie header on the outgoing response.
	if err := grpc.SetHeader(ctx, metadata.Pairs("set-cookie", cookie.String())); err != nil {
		return nil, apiError(ctx, err)
	}

	return &serverpb.UserLoginResponse{}, nil
}

// UserLogout allows a user to terminate their currently active session.
func (s *authenticationServer) UserLogout(
	ctx context.Context, req *serverpb.UserLogoutRequest,
) (*serverpb.UserLogoutResponse, error) {
	return nil, grpc.Errorf(codes.Unimplemented, "Logout method has not yet been implemented.")
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
	expiration := s.server.clock.PhysicalTime().Add(s.server.st.WebSessionTimeout.Get())

	insertSessionStmt := `
INSERT INTO system.web_sessions ("hashedSecret", username, "expiresAt")
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
		if datum.Len() != 1 || datum[0].ResolvedType() != parser.TypeInt {
			return fmt.Errorf(
				"expected create auth session statement to return exactly one integer, returned %v",
				datum,
			)
		}

		// Extract integer value from single datum.
		id = int64(*datum[0].(*parser.DInt))
		return nil
	}); err != nil {
		return 0, nil, err
	}

	return id, secret, nil
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
