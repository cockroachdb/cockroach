// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpcauth

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/bufbuild/buf/private/pkg/rpc"
)

const (
	// authenticationHeader is the standard OAuth header used for authenticating
	// a user. Ignore the misnomer.
	authenticationHeader = "Authorization"
	// authenticationTokenPrefix is the standard OAuth token prefix.
	// We use it for familiarity.
	authenticationTokenPrefix = "Bearer "
)

type authContextKey struct{}

// User describes an authenticated user.
type User struct {
	Subject string
}

// GetUser gets the currently authenticated user, if
// there is one. It returns nil, false, if there is no
// authenticated user.
func GetUser(ctx context.Context) (*User, bool) {
	userValue := ctx.Value(authContextKey{})
	if userValue == nil {
		return nil, false
	}
	// This is the only package where we can set this context key, so
	// this is guaranteed to be of this type if it exists.
	return userValue.(*User), true
}

// WithToken adds the token to the context via a header.
func WithToken(ctx context.Context, token string) context.Context {
	if token != "" {
		return rpc.WithOutgoingHeader(ctx, authenticationHeader, authenticationTokenPrefix+token)
	}
	return ctx
}

// GetTokenFromHeader gets the current authentication token, if
// there is one.
func GetTokenFromHeader(ctx context.Context) (string, error) {
	authHeader := rpc.GetIncomingHeader(ctx, authenticationHeader)
	if authHeader == "" {
		return "", errors.New("no auth header provided")
	}
	return getTokenFromString(authHeader)
}

// GetTokenFromHTTPHeaders gets the current authentication token from
// the HTTP headers, if there is one.
func GetTokenFromHTTPHeaders(headers http.Header) (string, error) {
	authHeader := headers.Get(authenticationHeader)
	if authHeader == "" {
		return "", errors.New("no auth header provided")
	}
	return getTokenFromString(authHeader)
}

func getTokenFromString(value string) (string, error) {
	if !strings.HasPrefix(value, authenticationTokenPrefix) {
		return "", errors.New("invalid header format")
	}
	token := strings.TrimPrefix(value, authenticationTokenPrefix)
	if token == "" {
		return "", errors.New("invalid header format")
	}
	return token, nil
}

// Authenticator defines the interface used to authenticate a token.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (*User, error)
}

// AuthenticatorFunc is a function that implements Authenticator
type AuthenticatorFunc func(context.Context, string) (*User, error)

// Authenticate implements Authenticator.
func (f AuthenticatorFunc) Authenticate(ctx context.Context, token string) (*User, error) {
	return f(ctx, token)
}

// NewServerInterceptor returns a new ServerInterceptor.
func NewServerInterceptor(authenticator Authenticator) rpc.ServerInterceptor {
	return rpc.ServerInterceptorFunc(
		func(
			ctx context.Context,
			request interface{},
			serverInfo *rpc.ServerInfo,
			serverHandler rpc.ServerHandler,
		) (interface{}, error) {
			user, ok := authenticate(ctx, authenticator)
			if ok {
				ctx = context.WithValue(ctx, authContextKey{}, user)
			}
			return serverHandler.Handle(ctx, request)
		},
	)
}

func authenticate(ctx context.Context, authenticator Authenticator) (*User, bool) {
	token, err := GetTokenFromHeader(ctx)
	if err != nil {
		return nil, false
	}
	user, err := authenticator.Authenticate(ctx, token)
	if err != nil {
		return nil, false
	}
	return user, true
}
