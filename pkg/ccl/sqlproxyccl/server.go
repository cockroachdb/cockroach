// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlproxyccl

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/prometheus/common/expfmt"
)

// Variable to be swapped in tests.
var awaitNoConnectionsInterval = time.Minute

// maxErrorLogLimiterCacheSize defines the maximum cache size for the error log
// limiter. We set it to 1M to align with the cache size of the connection
// throttler (see pkg/ccl/sqlproxyccl/throttler). Based on testing, this
// corresponds to roughly 200-300MB of memory usage when the cache is fully
// utilized. However, in practice, since the limiter is only applied to
// high-frequency errors, the cache will typically store only a small number of
// entries (e.g., around 2-3MB for 10K entries).
const maxErrorLogLimiterCacheSize = 1e6

// errorLogLimiterDuration indicates how frequent an error should be logged
// for a given (ip, tenant ID) pair.
const errorLogLimiterDuration = 5 * time.Minute

// Server is a TCP server that proxies SQL connections to a configurable
// backend. It may also run an HTTP server to expose a health check and
// prometheus metrics.
type Server struct {
	Stopper         *stop.Stopper
	handler         *proxyHandler
	mux             *http.ServeMux
	metrics         *metrics
	metricsRegistry *metric.Registry

	prometheusExporter metric.PrometheusExporter

	// errorLogLimiter is used to rate-limit the frequency at which *common*
	// errors are logged. It ensures that repeated error messages for the same
	// connection tag are logged at most once every 5 minutes (see
	// errorLogLimiterDuration).
	mu struct {
		syncutil.Mutex
		errorLogLimiter *cache.UnorderedCache // map[connTag]*log.EveryN
	}
}

// connTag represents the key for errorLogLimiter.
type connTag struct {
	// ip is the IP address of the client.
	ip string
	// tenantID is the ID of the tenant database the client is connecting to.
	tenantID string
}

// NewServer constructs a new proxy server and provisions metrics and health
// checks as well.
func NewServer(ctx context.Context, stopper *stop.Stopper, options ProxyOptions) (*Server, error) {
	registry := metric.NewRegistry()

	proxyMetrics := makeProxyMetrics()
	registry.AddMetricStruct(&proxyMetrics)

	handler, err := newProxyHandler(ctx, stopper, registry, &proxyMetrics, options)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()

	s := &Server{
		Stopper:            stopper,
		handler:            handler,
		mux:                mux,
		metrics:            &proxyMetrics,
		metricsRegistry:    registry,
		prometheusExporter: metric.MakePrometheusExporter(),
	}

	// Configure the error log limiter cache.
	cacheConfig := cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(size int, key, value interface{}) bool {
			return size > maxErrorLogLimiterCacheSize
		},
	}
	s.mu.errorLogLimiter = cache.NewUnorderedCache(cacheConfig)

	// /_status/{healthz,vars} matches CRDB's healthcheck and metrics
	// endpoints.
	mux.HandleFunc("/_status/vars/", s.handleVars)
	mux.HandleFunc("/_status/healthz/", s.handleHealth)
	mux.HandleFunc("/_status/cancel/", s.handleCancel)

	// Taken from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return s, nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// TODO(chrisseto): Ideally, this health check should actually check the
	// proxy's health in some fashion. How to actually check the health of a
	// proxy remains to be seen.
	// It has been noted that an overloaded CRDB server may fail to respond to
	// a simple HTTP request, such as this one, within a short time frame
	// (~5 seconds). Therefore, this health check provides a non-zero amount of
	// value as it indicates if the service is _massively_ overloaded or not.
	// In Kubernetes, a failed liveness check will result in the container
	// being terminated and replaced.
	// Its reasonable to assume that many, if not most, of the connections
	// being served by this proxy are unusable, if this check can not be
	// handled.  An operator may adjust this window by changing the timeout on
	// the check.
	w.WriteHeader(http.StatusOK)
	// Explicitly ignore any errors from writing our body as there's
	// nothing to be done if the write fails.
	_, _ = w.Write([]byte("OK"))
}

func (s *Server) handleVars(w http.ResponseWriter, r *http.Request) {
	contentType := expfmt.Negotiate(r.Header)
	w.Header().Set(httputil.ContentTypeHeader, string(contentType))
	scrape := func(pm *metric.PrometheusExporter) {
		pm.ScrapeRegistry(s.metricsRegistry, true /* includeChildMetrics*/)
	}
	if err := s.prometheusExporter.ScrapeAndPrintAsText(w, contentType, scrape); err != nil {
		log.Errorf(r.Context(), "%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleCancel processes a cancel request that has been forwarded from another
// sqlproxy.
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var retErr error
	defer func() {
		if retErr != nil {
			// Lots of noise from this log indicates that somebody is spamming
			// fake cancel requests.
			log.Warningf(
				r.Context(), "could not handle cancel request from client %s: %v",
				r.RemoteAddr, retErr,
			)
		}
		if f := s.handler.testingKnobs.httpCancelErrHandler; f != nil {
			f(retErr)
		}
	}()
	buf := make([]byte, proxyCancelRequestLen)
	n, err := r.Body.Read(buf)
	// Write the response as soon as we read the data, so we don't reveal if we
	// are processing the request or not.
	// Explicitly ignore any errors from writing the response as there's
	// nothing to be done if the write fails.
	_, _ = w.Write([]byte("OK"))
	if err != nil && err != io.EOF {
		retErr = err
		return
	}
	if n != len(buf) {
		retErr = errors.Errorf("unexpected number of bytes %d", n)
		return
	}
	p := &proxyCancelRequest{}
	if err := p.Decode(buf); err != nil {
		retErr = err
		return
	}
	// This request should never be forwarded, since if it is handled here, it
	// was already forwarded to the correct node.
	retErr = s.handler.handleCancelRequest(p, false /* allowForward */)
}

// ServeHTTP starts the proxy's HTTP server on the given listener.
// The server provides Prometheus metrics at /_status/vars,
// a health check endpoint at /_status/healthz, and pprof debug
// endpoints at /debug/pprof.
func (s *Server) ServeHTTP(ctx context.Context, ln net.Listener) error {
	if s.handler.RequireProxyProtocol {
		ln = &proxyproto.Listener{
			Listener: ln,
			Policy: func(upstream net.Addr) (proxyproto.Policy, error) {
				// There is a possibility where components doing healthchecking
				// (e.g. Kubernetes) do not support the PROXY protocol directly.
				// We use the `USE` policy here (which is also the default) to
				// optionally allow the PROXY protocol to be supported. If a
				// connection doesn't have the proxy headers, it'll just be
				// treated as a regular one.
				return proxyproto.USE, nil
			},
			ValidateHeader: s.handler.testingKnobs.validateProxyHeader,
		}
	}

	srv := http.Server{Handler: s.mux}

	err := s.Stopper.RunAsyncTask(ctx, "sqlproxy-http-cleanup", func(ctx context.Context) {
		<-ctx.Done()

		// Wait up to 15 seconds for the HTTP server to shut itself
		// down. The HTTP service is an auxiliary service for health
		// checking and metrics, which does not need a completely
		// graceful shutdown.
		_ = timeutil.RunWithTimeout(
			context.Background(),
			"http server shutdown",
			15*time.Second,
			func(shutdownCtx context.Context) error {
				// Ignore any errors as this routine will only be called
				// when the server is shutting down.
				_ = srv.Shutdown(shutdownCtx)
				return nil
			},
		)
	})
	if err != nil {
		return err
	}

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

// Serve serves up to two listeners according to the Options given in
// NewServer().
//
// If ln is not nil, a listener is served which does not require
// proxy protocol headers, unless RequireProxyProtocol is true.
//
// If proxyProtocolLn is not nil, a listener is served which requires proxy
// protocol headers.
//
// Incoming client connections are taken through the Postgres handshake and
// relayed to the configured backend server.
func (s *Server) ServeSQL(
	ctx context.Context, ln net.Listener, proxyProtocolLn net.Listener,
) error {
	if ln != nil {
		if s.handler.RequireProxyProtocol {
			ln = s.requireProxyProtocolOnListener(ln)
		}
		log.Infof(ctx, "proxy server listening at %s", ln.Addr())
		if err := s.Stopper.RunAsyncTask(ctx, "listener-serve", func(ctx context.Context) {
			_ = s.serve(ctx, ln, s.handler.RequireProxyProtocol)
		}); err != nil {
			return err
		}
	}
	if proxyProtocolLn != nil {
		proxyProtocolLn = s.requireProxyProtocolOnListener(proxyProtocolLn)
		log.Infof(ctx, "proxy with required proxy headers server listening at %s", proxyProtocolLn.Addr())
		if err := s.Stopper.RunAsyncTask(ctx, "proxy-protocol-listener-serve", func(ctx context.Context) {
			_ = s.serve(ctx, proxyProtocolLn, true /* requireProxyProtocol */)
		}); err != nil {
			return err
		}
	}
	return nil
}

// serve is called by ServeSQL to serve a single listener.
func (s *Server) serve(ctx context.Context, ln net.Listener, requireProxyProtocol bool) error {
	err := s.Stopper.RunAsyncTask(ctx, "listen-quiesce", func(ctx context.Context) {
		<-s.Stopper.ShouldQuiesce()
		if err := ln.Close(); err != nil && !grpcutil.IsClosedConnection(err) {
			log.Fatalf(ctx, "closing proxy listener: %s", err)
		}
	})
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		s.metrics.AcceptedConnCount.Inc(1)

		err = s.Stopper.RunAsyncTask(ctx, "proxy-con-serve", func(ctx context.Context) {
			defer func() { _ = conn.Close() }()

			s.metrics.CurConnCount.Inc(1)
			defer s.metrics.CurConnCount.Dec(1)

			ctx = logtags.AddTag(ctx, "client", log.SafeOperational(conn.RemoteAddr()))

			// Use a map to collect request-specific information at higher
			// layers of the stack. This helps ensure that all relevant
			// information is captured, providing better context for the error
			// logs.
			//
			// We could improve this by creating a custom context.Context object
			// to track all data related to the request (including migration
			// history). For now, this approach is adequate.
			reqTags := make(map[string]interface{})
			ctx = contextWithRequestTags(ctx, reqTags)

			err := s.handler.handle(ctx, conn, requireProxyProtocol)
			if err != nil && !errors.Is(err, context.Canceled) {
				// Ensure that context is tagged with request tags which are
				// populated at higher layers of the stack.
				for key, value := range reqTags {
					ctx = logtags.AddTag(ctx, key, value)
				}

				// log.Infof automatically prints hints (one per line) that are
				// associated with the input error object. This causes
				// unnecessary log spam, especially when proxy hints are meant
				// for the user. We will intentionally create a new error object
				// without the hints just for logging purposes.
				//
				// TODO(jaylim-crl): Ensure that handle does not return user
				// facing errors (i.e. one that contains hints).
				if s.shouldLogError(ctx, err, conn, reqTags) {
					errWithoutHints := errors.Newf("%s", err.Error()) // nolint:errwrap
					log.Infof(ctx, "connection closed: %v", errWithoutHints)
				}
			}
		})
		if err != nil {
			return err
		}
	}
}

func (s *Server) requireProxyProtocolOnListener(ln net.Listener) net.Listener {
	return &proxyproto.Listener{
		Listener: ln,
		Policy: func(upstream net.Addr) (proxyproto.Policy, error) {
			// REQUIRE enforces the connection to send a PROXY header.
			// The connection will be rejected if one was not present.
			return proxyproto.REQUIRE, nil
		},
		ValidateHeader: s.handler.testingKnobs.validateProxyHeader,
	}
}

// AwaitNoConnections returns a channel that is closed once the server has no open connections.
// This is meant to be used after the server has stopped accepting new connections and we are
// waiting to shutdown the server without inturrupting existing connections
//
// If the context is cancelled the channel will never close because we have to end the async task
// to allow the stopper to completely finish
func (s *Server) AwaitNoConnections(ctx context.Context) <-chan struct{} {
	c := make(chan struct{})

	_ = s.Stopper.RunAsyncTask(ctx, "await-no-connections", func(context.Context) {
		for {
			connCount := s.metrics.CurConnCount.Value()
			if connCount == 0 {
				close(c)
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(awaitNoConnectionsInterval):
				continue
			}
		}

	})

	return c
}

// shouldLogError returns true if an error should be logged, or false otherwise.
// The goal is to only throttle high-frequency errors for every (IP, tenantID)
// pair.
func (s *Server) shouldLogError(
	ctx context.Context, err error, conn net.Conn, reqTags map[string]interface{},
) bool {
	// Always log non high-frequency errors.
	if !errors.Is(err, highFreqErrorMarker) {
		return true
	}

	// Request hasn't been populated with a tenant ID yet, so log them.
	tenantID, hasTenantID := reqTags["tenant"]
	if !hasTenantID {
		return true
	}

	// Tenant ID must be a string, or else there has to be a bug in the code.
	// Instead of panicking, we'll skip throttling.
	tenantIDStr, ok := tenantID.(string)
	if !ok {
		log.Errorf(
			ctx,
			"unexpected error: cannot extract tenant ID from request tags; found: %v",
			tenantID,
		)
		return true
	}

	// Extract just the IP from the remote address. We'll log anyway if we get
	// an error. This case cannot happen in practice since conn.RemoteAddr()
	// will always return a valid IP.
	ipAddr, _, err := addr.SplitHostPort(conn.RemoteAddr().String(), "")
	if err != nil {
		log.Errorf(
			ctx,
			"unexpected error: cannot extract remote IP from connection; found: %v",
			conn.RemoteAddr().String(),
		)
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var limiter *log.EveryN
	key := connTag{ip: ipAddr, tenantID: tenantIDStr}
	c, ok := s.mu.errorLogLimiter.Get(key)
	if ok && c != nil {
		limiter = c.(*log.EveryN)
	} else {
		e := log.Every(errorLogLimiterDuration)
		limiter = &e
		s.mu.errorLogLimiter.Add(key, limiter)
	}
	return limiter.ShouldLog()
}

// requestTagsContextKey is the fast value key used to carry the request tags
// map in a context.Context object.
var requestTagsContextKey = ctxutil.RegisterFastValueKey()

// contextWithRequestTags returns a context annotated with the provided request
// tags map. Use requestTagsFromContext(ctx) to retrieve it back.
func contextWithRequestTags(ctx context.Context, reqTags map[string]interface{}) context.Context {
	return ctxutil.WithFastValue(ctx, requestTagsContextKey, reqTags)
}

// requestTagsFromContext retrieves the request tags map stored in the context
// via contextWithRequestTags.
func requestTagsFromContext(ctx context.Context) map[string]interface{} {
	r := ctxutil.FastValue(ctx, requestTagsContextKey)
	if r == nil {
		return nil
	}
	return r.(map[string]interface{})
}
