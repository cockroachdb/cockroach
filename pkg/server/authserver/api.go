// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package authserver

import (
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

type Server interface {
	RegisterService(*grpc.Server)
	RegisterGateway(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error

	// UserLogin verifies an incoming request by a user to create an web
	// authentication session. It checks the provided credentials against
	// the system.users table, and if successful creates a new
	// authentication session. The session's ID and secret are returned to
	// the caller as an HTTP cookie, added via a "Set-Cookie" header.
	UserLogin(ctx context.Context, req *serverpb.UserLoginRequest) (*serverpb.UserLoginResponse, error)

	// UserLoginFromSSO checks for the existence of a given username and
	// if it exists, creates a session for the username in the
	// `web_sessions` table. The session's ID and secret are returned to
	// the caller as an HTTP cookie, added via a "Set-Cookie" header.
	UserLoginFromSSO(ctx context.Context, reqUsername string) (*http.Cookie, error)

	// UserLogout allows a user to terminate their currently active session.
	UserLogout(ctx context.Context, req *serverpb.UserLogoutRequest) (*serverpb.UserLogoutResponse, error)

	// DemoLogin is the same as UserLogin but using the GET method.
	// It is only available for 'cockroach demo' and test clusters.
	DemoLogin(w http.ResponseWriter, req *http.Request)

	// NewAuthSession attempts to create a new authentication session for
	// the given user. If successful, returns the ID and secret value for
	// the new session.
	//
	// The caller is responsible to ensure the username has been
	// normalized already.
	//
	// This is a low level API and is only exported for use in tests.
	// Regular flows should use the login endpoints intead.
	NewAuthSession(ctx context.Context, userName username.SQLUsername) (int64, []byte, error)

	// VerifySession verifies the existence and validity of the session
	// claimed by the supplied SessionCookie. Returns three parameters: a
	// boolean indicating if the session was valid, the username
	// associated with the session (if validated), and an error for any
	// internal errors which prevented validation.
	//
	// This is a low level API and is only exported for use in tests.
	VerifySession(
		ctx context.Context, cookie *serverpb.SessionCookie,
	) (bool, string, error)

	// VerifyPasswordDBConsole verifies the passed username/password
	// pair against the system.users table. The returned boolean indicates
	// whether or not the verification succeeded; an error is returned if
	// the validation process could not be completed.
	//
	// This is a low level API and is only exported for use in tests.
	// Regular flows should use the login endpoints intead.
	//
	// This function should *not* be used to validate logins into the SQL
	// shell since it checks a separate authentication scheme.
	//
	// The caller is responsible for ensuring that the username is
	// normalized. (CockroachDB has case-insensitive usernames, unlike
	// PostgreSQL.)
	VerifyPasswordDBConsole(
		ctx context.Context, userName username.SQLUsername, passwordStr string,
	) (valid bool, expired bool, err error)
}

type SQLServerInterface interface {
	ExecutorConfig() *sql.ExecutorConfig
	InternalExecutor() isql.Executor
}

type AuthMux interface {
	http.Handler
}

func NewServer(cfg *base.Config, sqlServer SQLServerInterface) Server {
	return &authenticationServer{
		cfg:       cfg,
		sqlServer: sqlServer,
	}
}

func NewMux(s Server, inner http.Handler, allowAnonymous bool) AuthMux {
	return &authenticationMux{
		server:         s.(*authenticationServer),
		inner:          inner,
		allowAnonymous: allowAnonymous,
	}
}
