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
	"crypto/tls"
	"net/http"
	"strings"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

type httpServer struct {
	cfg   BaseConfig
	mux   http.ServeMux
	proxy *nodeProxy
}

func newHTTPServer(
	cfg BaseConfig,
	rpcContext *rpc.Context,
	parseNodeID ParseNodeIDFn,
	getNodeIDHTTPAddress GetNodeIDHTTPAddressFn,
) *httpServer {
	return &httpServer{
		cfg: cfg,
		proxy: &nodeProxy{
			scheme:               cfg.HTTPRequestScheme(),
			parseNodeID:          parseNodeID,
			getNodeIDHTTPAddress: getNodeIDHTTPAddress,
			rpcContext:           rpcContext,
		},
	}
}

const healthPath = "/health"

func (s *httpServer) handleHealth(healthHandler http.Handler) {
	s.mux.Handle(healthPath, healthHandler)
}

func (s *httpServer) setupRoutes(
	ctx context.Context,
	authnServer *authenticationServer,
	adminAuthzCheck *adminPrivilegeChecker,
	metricSource metricMarshaler,
	runtimeStatSampler *status.RuntimeStatSampler,
	handleRequestsUnauthenticated http.Handler,
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
	s.mux.Handle(statusVars, http.HandlerFunc(varsHandler{metricSource, s.cfg.Settings}.handleVars))
	// Same for /_status/load.
	s.mux.Handle(loadStatusVars, http.HandlerFunc(makeStatusLoadHandler(ctx, runtimeStatSampler)))

	if apiServer != nil {
		// The new "v2" HTTP API tree.
		s.mux.Handle(apiV2Path, apiServer)
	}

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

// start starts the network listener for the HTTP interface
// and also starts accepting incoming HTTP connections.
func (s *httpServer) start(
	ctx, workersCtx context.Context,
	connManager netutil.Server,
	uiTLSConfig *tls.Config,
	stopper *stop.Stopper,
) error {
	httpLn, err := ListenAndUpdateAddrs(ctx, &s.cfg.HTTPAddr, &s.cfg.HTTPAdvertiseAddr, "http")
	if err != nil {
		return err
	}
	log.Eventf(ctx, "listening on http port %s", s.cfg.HTTPAddr)

	// The HTTP listener shutdown worker, which closes everything under
	// the HTTP port when the stopper indicates we are shutting down.
	waitQuiesce := func(ctx context.Context) {
		// NB: we can't do this as a Closer because (*Server).ServeWith is
		// running in a worker and usually sits on accept() which unblocks
		// only when the listener closes. In other words, the listener needs
		// to close when quiescing starts to allow that worker to shut down.
		<-stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Ops.Fatalf(ctx, "%v", err)
		}
	}
	if err := stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
		waitQuiesce(workersCtx)
		return err
	}

	if uiTLSConfig != nil {
		httpMux := cmux.New(httpLn)
		clearL := httpMux.Match(cmux.HTTP1())
		tlsL := httpMux.Match(cmux.Any())

		// Dispatch incoming requests to either clearL or tlsL.
		if err := stopper.RunAsyncTask(workersCtx, "serve-ui", func(context.Context) {
			netutil.FatalIfUnexpected(httpMux.Serve())
		}); err != nil {
			return err
		}

		// Serve the plain HTTP (non-TLS) connection over clearL.
		// This produces a HTTP redirect to the `https` URL for the path /,
		// handles the request normally (via s.baseHandler) for the path /health,
		// and produces 404 for anything else.
		if err := stopper.RunAsyncTask(workersCtx, "serve-health", func(context.Context) {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
			})
			mux.Handle(healthPath, http.HandlerFunc(s.baseHandler))

			plainRedirectServer := netutil.MakeServer(stopper, uiTLSConfig, mux)

			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		}); err != nil {
			return err
		}

		httpLn = tls.NewListener(tlsL, uiTLSConfig)
	}

	// Serve the HTTP endpoint. This will be the original httpLn
	// listening on --http-addr without TLS if uiTLSConfig was
	// nil, or overridden above if uiTLSConfig was not nil to come from
	// the TLS negotiation over the HTTP port.
	return stopper.RunAsyncTask(workersCtx, "server-http", func(context.Context) {
		netutil.FatalIfUnexpected(connManager.Serve(httpLn))
	})
}

// gzipHandler intercepts HTTP Requests and will gzip the response if
// the request contains the `Accept-Encoding: gzip` header. Requests
// are then delegated to the server mux.
func (s *httpServer) gzipHandler(w http.ResponseWriter, r *http.Request) {
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
	s.mux.ServeHTTP(w, r)
}

// baseHandler is the top-level HTTP handler for all HTTP traffic, before
// authentication and authorization.
//
// The server handles the node-to-node proxying first, and then runs the
// gzip middleware, then delegates to the mux for handling the request.
func (s *httpServer) baseHandler(w http.ResponseWriter, r *http.Request) {
	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

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

	s.proxy.nodeProxyHandler(w, r, s.gzipHandler)
}
