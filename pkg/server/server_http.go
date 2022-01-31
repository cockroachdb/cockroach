// Copyright 2022 The Cockroach Authors.
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
	"context"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

type httpServer struct {
	cfg Config
	mux http.ServeMux
}

func newHTTPServer(cfg Config) *httpServer {
	return &httpServer{cfg: cfg}
}

const healthPath = "/health"

func (s *httpServer) handleHealth(healthHandler http.Handler) {
	s.mux.Handle(healthPath, healthHandler)
}

func (s *httpServer) setupRoutes(
	ctx context.Context,
	authnServer *authenticationServer,
	adminAuthzCheck *adminPrivilegeChecker,
	handleRequestsUnauthenticated http.Handler,
	handleStatusVarsUnauthenticated http.Handler,
	handleDebugUnauthenticated http.Handler,
	apiServer *apiV2Server,
) error {
	// OIDC Configuration must happen prior to the UI Handler being defined below so that we have
	// the system settings initialized for it to pick up from the oidcAuthenticationServer.
	oidc, err := ConfigureOIDC(
		ctx, s.cfg.Settings, s.cfg.Locality,
		s.mux.Handle, authnServer.UserLoginFromSSO, s.cfg.AmbientCtx, s.cfg.ClusterIDContainer.Get(),
	)
	if err != nil {
		return err
	}

	// Define the http.Handler for UI assets.
	assetHandler := ui.Handler(ui.Config{
		ExperimentalUseLogin: s.cfg.EnableWebSessionAuthentication,
		LoginEnabled:         s.cfg.RequireWebSession(),
		NodeID:               s.cfg.IDContainer,
		OIDC:                 oidc,
		GetUser: func(ctx context.Context) *string {
			if u, ok := ctx.Value(webSessionUserKey{}).(string); ok {
				return &u
			}
			return nil
		},
	})

	// The authentication mux used here is created in "allow anonymous" mode so that the UI
	// assets are served up whether or not there is a session. If there is a session, the mux
	// adds it to the context, and it is templated into index.html so that the UI can show
	// the username of the currently-logged-in user.
	authenticatedUIHandler := newAuthenticationMuxAllowAnonymous(
		authnServer, assetHandler)
	s.mux.Handle("/", authenticatedUIHandler)

	// Add HTTP authentication to the gRPC-gateway endpoints used by the UI,
	// if not disabled by configuration.
	var authenticatedHandler http.Handler = handleRequestsUnauthenticated
	if s.cfg.RequireWebSession() {
		authenticatedHandler = newAuthenticationMux(authnServer, authenticatedHandler)
	}

	// Login and logout paths.
	// The /login endpoint is, by definition, available pre-authentication.
	s.mux.Handle(loginPath, handleRequestsUnauthenticated)
	s.mux.Handle(logoutPath, authenticatedHandler)
	// The login path for 'cockroach demo', if we're currently running
	// that.
	if s.cfg.EnableDemoLoginEndpoint {
		s.mux.Handle(DemoLoginPath, http.HandlerFunc(authnServer.demoLogin))
	}

	// Admin/Status servers. These are used by the UI via RPC-over-HTTP.
	s.mux.Handle(statusPrefix, authenticatedHandler)
	s.mux.Handle(adminPrefix, authenticatedHandler)

	// The timeseries endpoint, used to produce graphs.
	s.mux.Handle(ts.URLPrefix, authenticatedHandler)

	// Exempt the 2nd health check endpoint from authentication.
	// (This simply mirrors /health and exists for backward compatibility.)
	s.mux.Handle(adminHealth, handleRequestsUnauthenticated)
	// The /_status/vars endpoint is not authenticated either. Useful for monitoring.
	s.mux.Handle(statusVars, handleStatusVarsUnauthenticated)

	// The new "v2" HTTP API tree.
	s.mux.Handle(apiV2Path, apiServer)

	// Register debugging endpoints.
	handleDebugAuthenticated := handleDebugUnauthenticated
	if s.cfg.RequireWebSession() {
		// Mandate both authentication and admin authorization.
		handleDebugAuthenticated = makeAdminAuthzCheckHandler(adminAuthzCheck, handleDebugAuthenticated)
		handleDebugAuthenticated = newAuthenticationMux(authnServer, handleDebugAuthenticated)
	}
	s.mux.Handle(debug.Endpoint, handleDebugAuthenticated)

	log.Event(ctx, "added http endpoints")

	return nil
}

func makeAdminAuthzCheckHandler(
	adminAuthzCheck *adminPrivilegeChecker, handler http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Retrieve the username embedded in the grpc metadata, if any.
		// This will be provided by the authenticationMux.
		md := forwardAuthenticationMetadata(req.Context(), req)
		authCtx := metadata.NewIncomingContext(req.Context(), md)
		// Check the privileges of the requester.
		_, err := adminAuthzCheck.requireAdminUser(authCtx)
		if errors.Is(err, errRequiresAdmin) {
			http.Error(w, "admin privilege required", http.StatusUnauthorized)
			return
		} else if err != nil {
			log.Ops.Infof(authCtx, "web session error: %s", err)
			http.Error(w, "error checking authentication", http.StatusInternalServerError)
			return
		}
		// Forward the request to the inner handler.
		handler.ServeHTTP(w, req)
	})
}

// baseHandler is the top-level HTTP handler for all HTTP traffic, before
// authentication and authorization.
func (s *httpServer) baseHandler(w http.ResponseWriter, r *http.Request) {
	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	ae := r.Header.Get(httputil.AcceptEncodingHeader)
	switch {
	case strings.Contains(ae, httputil.GzipEncoding):
		w.Header().Set(httputil.ContentEncodingHeader, httputil.GzipEncoding)
		gzw := newGzipResponseWriter(w)
		defer func() {
			// Certain requests must not have a body, yet closing the gzip writer will
			// attempt to write the gzip header. Avoid logging a warning in this case.
			// This is notably triggered by:
			//
			// curl -H 'Accept-Encoding: gzip' \
			// 	    -H 'If-Modified-Since: Thu, 29 Mar 2018 22:36:32 GMT' \
			//      -v http://localhost:8080/favicon.ico > /dev/null
			//
			// which results in a 304 Not Modified.
			if err := gzw.Close(); err != nil && !errors.Is(err, http.ErrBodyNotAllowed) {
				ctx := s.cfg.AmbientCtx.AnnotateCtx(r.Context())
				log.Ops.Warningf(ctx, "error closing gzip response writer: %v", err)
			}
		}()
		w = gzw
	}

	// This is our base handler.
	// Intercept all panics, log them, and return an internal server error as a response.
	defer func() {
		if p := recover(); p != nil {
			// Note: use of a background context here so we can log even with the absence of a client.
			// Assumes appropriate timeouts are used.
			logcrash.ReportPanic(context.Background(), &s.cfg.Settings.SV, p, 1 /* depth */)
			http.Error(w, errAPIInternalErrorString, http.StatusInternalServerError)
		}
	}()

	s.mux.ServeHTTP(w, r)
}
