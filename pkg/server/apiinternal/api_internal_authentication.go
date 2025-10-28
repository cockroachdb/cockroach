// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiinternal

// registerAuthenticationRoutes sets up the authentication REST endpoints.
func (r *apiInternalServer) registerAuthenticationRoutes() {
	routes := []route{
		// Login endpoint
		{POST, "/login", createHandler(r.login.UserLogin)},

		// Logout endpoint
		{GET, "/logout", createHandler(r.logout.UserLogout)},
	}

	// Register all routes
	for _, route := range routes {
		r.mux.HandleFunc(route.path, route.handler).Methods(string(route.method))
	}
}
