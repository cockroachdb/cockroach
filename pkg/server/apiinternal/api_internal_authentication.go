// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

import "github.com/cockroachdb/cockroach/pkg/server/authserver"

// registerAuthenticationRoutes sets up the authentication REST endpoints.
// Unlike other routes, these directly use HTTP handlers instead of RPC proxying
// because login/logout deal with HTTP cookies which are HTTP-specific concerns.
func (r *apiInternalServer) registerAuthenticationRoutes() {
	routes := []route{
		{POST, authserver.LoginPath, r.authentication.HTTPLogin},
		{GET, authserver.LogoutPath, r.authentication.HTTPLogout},
	}

	// Register all routes
	for _, route := range routes {
		r.mux.HandleFunc(route.path, route.handler).Methods(string(route.method))
	}
}
