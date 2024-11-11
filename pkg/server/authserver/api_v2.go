// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package authserver

import (
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/base"
)

type ServerV2 interface {
	http.Handler
}

type AuthV2Mux interface {
	http.Handler
}

type RoleAuthzMux interface {
	http.Handler
}

// NewV2Server creates a new ServerV2 for the given outer Server,
// and base path.
func NewV2Server(
	ctx context.Context, s SQLServerInterface, cfg *base.Config, basePath string,
) ServerV2 {
	simpleMux := http.NewServeMux()

	innerServer := NewServer(cfg, s).(*authenticationServer)
	authServer := &authenticationV2Server{
		sqlServer:  s,
		authServer: innerServer,
		mux:        simpleMux,
		basePath:   basePath,
	}

	authServer.registerRoutes()
	return authServer
}

// NewV2Mux creates a new AuthV2Mux for the given ServerV2.
func NewV2Mux(s ServerV2, inner http.Handler, allowAnonymous bool) AuthV2Mux {
	as := s.(*authenticationV2Server)
	return &authenticationV2Mux{
		s:              as,
		inner:          inner,
		allowAnonymous: allowAnonymous,
	}
}

// NewRoleAuthzMux creates a new RoleAuthzMux.
func NewRoleAuthzMux(
	authzAccessorFactory authzAccessorFactory, role APIRole, inner http.Handler,
) RoleAuthzMux {
	return &roleAuthorizationMux{
		authzAccessorFactory: authzAccessorFactory,
		role:                 role,
		inner:                inner,
	}
}
