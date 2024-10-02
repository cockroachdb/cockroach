// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/NYTimes/gziphandler"
	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/inspectz"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/privchecker"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

type httpServer struct {
	cfg BaseConfig
	mux http.ServeMux
	// gzMux is an HTTP handler that gzip-compresses mux.
	gzMux http.Handler
	proxy *nodeProxy
}

func newHTTPServer(
	cfg BaseConfig,
	rpcContext *rpc.Context,
	parseNodeID ParseNodeIDFn,
	getNodeIDHTTPAddress GetNodeIDHTTPAddressFn,
) *httpServer {
	server := &httpServer{
		cfg: cfg,
		proxy: &nodeProxy{
			scheme:               cfg.HTTPRequestScheme(),
			parseNodeID:          parseNodeID,
			getNodeIDHTTPAddress: getNodeIDHTTPAddress,
			rpcContext:           rpcContext,
		},
	}
	server.gzMux = gziphandler.GzipHandler(http.HandlerFunc(server.mux.ServeHTTP))
	return server
}

// HSTSEnabled is a boolean that enables HSTS headers on the HTTP
// server. These instruct a valid user agent to use HTTPS *only*
// for all future connections to this host.
var HSTSEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.hsts.enabled",
	"if true, HSTS headers will be sent along with all HTTP "+
		"requests. The headers will contain a max-age setting of one "+
		"year. Browsers honoring the header will always use HTTPS to "+
		"access the DB Console. Ensure that TLS is correctly configured "+
		"prior to enabling.",
	false,
	settings.WithPublic)

const hstsHeaderKey = "Strict-Transport-Security"

// hstsHeaderValue contains the static HSTS header value we return when
// HSTS is enabled via the clusters setting above. It sets the expiry
// of 1 year that the browser should remember to only use HTTPS for
// this site.
const hstsHeaderValue = "max-age=31536000"

const healthPath = "/health"

func (s *httpServer) handleHealth(healthHandler http.Handler) {
	s.mux.Handle(healthPath, healthHandler)
}

const virtualClustersPath = "/virtual_clusters"

// virtualClustersResp is the response returned by the virtual clusters
// endpoint that contains a list of active tenant sessions in the
// current session cookie. This allows the DB Console to populate a
// dropdown allowing the user to select a cluster.
type virtualClustersResp struct {
	// VirtualClusters is a list of virtual cluster names.
	VirtualClusters []string `json:"virtual_clusters"`
}

// virtualClustersHandler parses the session cookie from the request
// and returns a JSON body with the list of cluster names contained
// within the session. If the session does not contain any cluster
// names, it returns an empty list. If the cookie is missing, it
// returns 200 OK with an empty request body.
var virtualClustersHandler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	sessionCookie, err := req.Cookie(authserver.SessionCookieName)
	if err != nil {
		if errors.Is(err, http.ErrNoCookie) {
			w.Header().Add("Content-Type", "application/json")
			if _, err := w.Write([]byte(`{"virtual_clusters":[]}`)); err != nil {
				log.Errorf(req.Context(), "unable to write virtual clusters response: %s", err.Error())
			}
			return
		}
		http.Error(w, "unable to decode session cookie", http.StatusInternalServerError)
		return
	}
	sessionsAndClusters := strings.Split(sessionCookie.Value, ",")
	resp := &virtualClustersResp{
		VirtualClusters: make([]string, 0),
	}
	for i, c := range sessionsAndClusters {
		if i%2 == 1 {
			resp.VirtualClusters = append(resp.VirtualClusters, c)
		}
	}

	respBytes, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "unable to marshal virtual clusterse JSON", http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	if _, err := w.Write(respBytes); err != nil {
		log.Errorf(req.Context(), "unable to write virtual clusters response: %s", err.Error())
	}
})

func (s *httpServer) setupRoutes(
	ctx context.Context,
	authnServer authserver.Server,
	adminAuthzCheck privchecker.CheckerForRPCHandlers,
	metricSource metricMarshaler,
	runtimeStatSampler *status.RuntimeStatSampler,
	handleRequestsUnauthenticated http.Handler,
	handleDebugUnauthenticated http.Handler,
	handleInspectzUnauthenticated http.Handler,
	apiServer http.Handler,
	flags serverpb.FeatureFlags,
) error {
	// OIDC Configuration must happen prior to the UI Handler being defined below so that we have
	// the system settings initialized for it to pick up from the oidcAuthenticationServer.
	oidc, err := authserver.ConfigureOIDC(
		ctx, s.cfg.Settings, s.cfg.Locality,
		s.mux.Handle, authnServer.UserLoginFromSSO, s.cfg.AmbientCtx, s.cfg.ClusterIDContainer.Get(),
	)
	if err != nil {
		return err
	}

	// Define the http.Handler for UI assets.
	assetHandler := ui.Handler(ui.Config{
		Insecure: s.cfg.InsecureWebAccess(),
		NodeID:   s.cfg.IDContainer,
		OIDC:     oidc,
		GetUser: func(ctx context.Context) *string {
			if user, ok := authserver.MaybeUserFromHTTPAuthInfoContext(ctx); ok {
				ustring := user.Normalized()
				return &ustring
			}
			return nil
		},
		Flags:    flags,
		Settings: s.cfg.Settings,
	})

	// The authentication mux used here is created in "allow anonymous" mode so that the UI
	// assets are served up whether or not there is a session. If there is a session, the mux
	// adds it to the context, and it is templated into index.html so that the UI can show
	// the username of the currently-logged-in user.
	authenticatedUIHandler := authserver.NewMux(
		authnServer, assetHandler, true /* allowAnonymous */)
	s.mux.Handle("/", authenticatedUIHandler)

	// Add HTTP authentication to the gRPC-gateway endpoints used by the UI,
	// if not disabled by configuration.
	var authenticatedHandler = handleRequestsUnauthenticated
	if !s.cfg.InsecureWebAccess() {
		authenticatedHandler = authserver.NewMux(authnServer, authenticatedHandler, false /* allowAnonymous */)
	}

	// Login and logout paths.
	// The /login endpoint is, by definition, available pre-authentication.
	s.mux.Handle(authserver.LoginPath, handleRequestsUnauthenticated)
	s.mux.Handle(authserver.LogoutPath, authenticatedHandler)
	s.mux.Handle(virtualClustersPath, virtualClustersHandler)
	// The login path for 'cockroach demo', if we're currently running
	// that.
	if s.cfg.EnableDemoLoginEndpoint {
		s.mux.Handle(authserver.DemoLoginPath, http.HandlerFunc(authnServer.DemoLogin))
	}

	// Admin/Status servers. These are used by the UI via RPC-over-HTTP.
	s.mux.Handle(apiconstants.StatusPrefix, authenticatedHandler)
	s.mux.Handle(apiconstants.AdminPrefix, authenticatedHandler)

	// The timeseries endpoint, used to produce graphs.
	s.mux.Handle(ts.URLPrefix, authenticatedHandler)

	// Exempt the 2nd health check endpoint from authentication.
	// (This simply mirrors /health and exists for backward compatibility.)
	s.mux.Handle(apiconstants.AdminHealth, handleRequestsUnauthenticated)
	// The /_status/vars endpoint is not authenticated either. Useful for monitoring.
	s.mux.Handle(apiconstants.StatusVars, http.HandlerFunc(varsHandler{metricSource, s.cfg.Settings}.handleVars))
	// Same for /_status/load.
	le, err := newLoadEndpoint(runtimeStatSampler, metricSource)
	if err != nil {
		return err
	}
	s.mux.Handle(apiconstants.LoadStatusVars, le)

	if apiServer != nil {
		// The new "v2" HTTP API tree.
		s.mux.Handle(apiconstants.APIV2Path, apiServer)
	}

	// Register debugging endpoints.
	handleDebugAuthenticated := handleDebugUnauthenticated
	handleInspectzAuthenticated := handleInspectzUnauthenticated
	if !s.cfg.InsecureWebAccess() {
		// Mandate both authentication and admin authorization.
		handleDebugAuthenticated = makeAdminAuthzCheckHandler(adminAuthzCheck, handleDebugAuthenticated)
		handleDebugAuthenticated = authserver.NewMux(authnServer, handleDebugAuthenticated, false /* allowAnonymous */)

		handleInspectzAuthenticated = makeAdminAuthzCheckHandler(adminAuthzCheck, handleInspectzAuthenticated)
		handleInspectzAuthenticated = authserver.NewMux(authnServer, handleInspectzAuthenticated, false /* allowAnonymous */)
	}
	s.mux.Handle(debug.Endpoint, handleDebugAuthenticated)
	s.mux.Handle(inspectz.URLPrefix, handleInspectzAuthenticated)

	log.Event(ctx, "added http endpoints")

	return nil
}

func makeAdminAuthzCheckHandler(
	adminAuthzCheck privchecker.CheckerForRPCHandlers, handler http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Retrieve the username embedded in the grpc metadata, if any.
		// This will be provided by the authenticationMux.
		md := authserver.TranslateHTTPAuthInfoToGRPCMetadata(req.Context(), req)
		authCtx := metadata.NewIncomingContext(req.Context(), md)
		// Check the privileges of the requester.
		err := adminAuthzCheck.RequireViewDebugPermission(authCtx)
		if err != nil {
			http.Error(w, "admin privilege or VIEWDEBUG global privilege required", http.StatusUnauthorized)
			return
		}
		// Forward the request to the inner handler.
		handler.ServeHTTP(w, req)
	})
}

// startHTTPService starts the network listener for the HTTP interface
// and also starts accepting incoming HTTP connections.
func startHTTPService(
	ctx, workersCtx context.Context,
	cfg *BaseConfig,
	uiTLSConfig *tls.Config,
	stopper *stop.Stopper,
	handler http.HandlerFunc,
) error {
	httpLn, err := ListenAndUpdateAddrs(ctx, &cfg.HTTPAddr, &cfg.HTTPAdvertiseAddr, "http", cfg.AcceptProxyProtocolHeaders)
	if err != nil {
		return err
	}
	log.Eventf(ctx, "listening on http port %s", cfg.HTTPAddr)

	// The HTTP listener shutdown worker, which closes everything under
	// the HTTP port when the stopper indicates we are shutting down.
	waitQuiesce := func(ctx context.Context) {
		// NB: we can't do this as a Closer because (*Server).ServeWith is
		// running in a worker and usually sits on accept() which unblocks
		// only when the listener closes. In other words, the listener needs
		// to close when quiescing starts to allow that worker to shut down.
		<-stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Ops.Warningf(ctx, "%v", err)
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
				if HSTSEnabled.Get(&cfg.Settings.SV) {
					w.Header().Set(hstsHeaderKey, hstsHeaderValue)
				}
				http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
			})
			mux.Handle(healthPath, handler)

			plainRedirectServer := netutil.MakeHTTPServer(workersCtx, stopper, nil /* tlsConfig */, mux)

			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		}); err != nil {
			return err
		}

		httpLn = tls.NewListener(tlsL, uiTLSConfig)
	}

	// The connManager is responsible for tearing down the net.Conn
	// objects when the stopper tells us to shut down.
	connManager := netutil.MakeHTTPServer(workersCtx, stopper, uiTLSConfig, handler)

	// Serve the HTTP endpoint. This will be the original httpLn
	// listening on --http-addr without TLS if uiTLSConfig was
	// nil, or overridden above if uiTLSConfig was not nil to come from
	// the TLS negotiation over the HTTP port.
	return stopper.RunAsyncTask(workersCtx, "server-http", func(context.Context) {
		netutil.FatalIfUnexpected(connManager.Serve(httpLn))
	})
}

// baseHandler is the top-level HTTP handler for all HTTP traffic, before
// authentication and authorization.
//
// The server handles the node-to-node proxying first, and then runs the
// gzip middleware, then delegates to the mux for handling the request.
func (s *httpServer) baseHandler(w http.ResponseWriter, r *http.Request) {
	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	if HSTSEnabled.Get(&s.cfg.Settings.SV) {
		w.Header().Set("Strict-Transport-Security", hstsHeaderValue)
	}

	// This is our base handler.
	// Intercept all panics, log them, and return an internal server error as a response.
	// There is an exception made for the `http.ErrAbortHandler` which the
	// `net/http` library suppresses stacktrace printing on. Since this defer
	// runs before the `http.Server` panic catcher, it should also ignore that
	// specific panic.
	defer func() {
		if p := recover(); p != nil && p != http.ErrAbortHandler {
			// Note: use of a background context here so we can log even with the absence of a client.
			// Assumes appropriate timeouts are used.
			logcrash.ReportPanic(context.Background(), &s.cfg.Settings.SV, p, 1 /* depth */)
			http.Error(w, srverrors.ErrAPIInternalErrorString, http.StatusInternalServerError)
		}
	}()

	s.proxy.nodeProxyHandler(w, r, s.gzMux.ServeHTTP)
}
