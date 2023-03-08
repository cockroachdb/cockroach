// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package httpproxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/http/pprof"
	"net/url"
	"os"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ReverseHTTPProxyConfig groups the configuration for ReverseHTTPProxy.
//
// See RunAsync().
type ReverseHTTPProxyConfig struct {
	// HttpAddr represents the address on which the proxy will listen.
	HTTPAddr string
	// TargetURL represents the URL to which requests will be forwarded.
	TargetURL string
	// CACertPath represents the path to a file containing a certificate authority
	// cert. This CA will be the only one trusted to sign CRDB's certificates. If
	// not specified, the system's CAs are trusted.
	CACertPath string
	// UICertPath and UICertKeyPath represent paths to the certificate the proxy
	// will use for autheticating itself to clients. If not specified, the proxy
	// will just speak HTTP. If specified, the proxy will redirect any HTTP
	// requests to HTTPS.
	UICertPath, UICertKeyPath string
}

// ReverseHTTPProxy proxies HTTP requests to a given target (assumed to be
// CRDB).
type ReverseHTTPProxy struct {
	listenAddr string
	proxy      *httputil.ReverseProxy
	certs      certificates
}

// NewReverseHTTPProxy creates a ReverseHTTPProxy.
func NewReverseHTTPProxy(ctx context.Context, cfg ReverseHTTPProxyConfig) *ReverseHTTPProxy {
	certs, err := loadCerts(cfg.UICertPath, cfg.UICertKeyPath, cfg.CACertPath)
	if err != nil {
		log.Fatalf(ctx, "%s", err)
	}

	url, err := url.Parse(cfg.TargetURL)
	if err != nil {
		log.Fatalf(ctx, "Invalid CRDB UI target: %s", cfg.TargetURL)
	}
	if certs.CAPool != nil && url.Scheme != "https" {
		log.Fatalf(ctx, "HTTPS is required for CRDB target when --ca-cert is specified.")
	}
	if url.Path != "" {
		// Supporting a path would require extra code in the proxy to join a
		// particular request's path with it.
		log.Fatalf(ctx, "Specifying a path in --crdb-http-url is not supported.")
	}

	// If the proxy is not configured for HTTP, remember to refuse redirects to
	// HTTPS from Cockroach.
	var HTTPToHTTPSErr error
	if certs.UICert == nil {
		HTTPToHTTPSErr = errors.New(`The CockroachDB cluster is configured to only serve HTTPS, 
but the Observability Service has not been configured for HTTPS.
Set the --ui-cert and --ui-cert-key flags to configure the Observability Service to serve HTTPS,
set the scheme in the --crdb-http-url URL to "https://", and perhaps set --ca-cert
to trust the certificate presented by CockroachDB.`)
	}

	return &ReverseHTTPProxy{
		listenAddr: cfg.HTTPAddr,
		proxy:      newProxy(url, certs.CAPool, HTTPToHTTPSErr),
		certs:      certs,
	}
}

// We define our own copy of this struct that also exists in
// pkg/server/authentication.go because we don't want to import that
// package. This struct is used to define a null OIDC configuration for
// the UI Config. DB Console uses information present here to show
// different login UI when OIDC is present.
type noOIDCConfigured struct{}

var _ ui.OIDCUI = &noOIDCConfigured{}

// GetOIDCConf implements the `ui.OIDCUI` interface with a configuration
// that disables OIDC login options for the UI server.
func (c *noOIDCConfigured) GetOIDCConf() ui.OIDCUIConf {
	return ui.OIDCUIConf{
		Enabled: false,
	}
}

// CRDBProxyPaths is the list of path prefixes that are proxied to the
// underlying CRDB cluster.
var CRDBProxyPaths = []string{"/_admin/", "/_status/", "/ts/", "/api/v2/"}

// Start runs an HTTP proxy server in stopper tasks.
func (p *ReverseHTTPProxy) Start(ctx context.Context, stop *stop.Stopper) {
	listener, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen for incoming HTTP connections on address %s: %s",
			p.listenAddr, err)
		exit.WithCode(exit.UnspecifiedError())
	}

	https := p.certs.UICert != nil
	// Create the HTTP mux. Some requests will be forwarded to p.proxy, others
	// will be served locally.
	mux := http.NewServeMux()
	// TODO(davidh): Ideally, the UI handler should probably be
	// configured in `obsservice` and not hardcoded into `obslib`. This
	// gives lib users a chance to do whatever they want with the UI.
	mux.Handle("/", ui.Handler(ui.Config{
		Insecure: true,
		GetUser: func(ctx context.Context) *string {
			u := "Observability Service"
			return &u
		},
		OIDC: &noOIDCConfigured{},
		Flags: serverpb.FeatureFlags{
			IsObservabilityService: true,
			// TODO(obs-infra): make conditional once obsservice becomes tenant-aware.
			CanViewKvMetricDashboards: true,
		},
	}))
	for _, path := range CRDBProxyPaths {
		mux.Handle(path, p.proxy)
	}
	// This seems to be the minimal set of handlers that we need to register in
	// order to get all the pprof functionality. The pprof.Index handler handles
	// some types of profiles itself.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	var servers []*http.Server

	if !https {
		// The Observability Service is not configured with certs, so it can only
		// serve HTTP.
		s := &http.Server{Handler: mux}
		servers = append(servers, s)
		if err := stop.RunAsyncTask(ctx, "reverse proxy HTTP server", func(ctx context.Context) {
			_ = s.Serve(listener)
		}); err != nil {
			return
		}
	} else {
		// We're configured to serve HTTPS. We'll also listen for HTTP requests, and redirect them
		// to HTTPS.

		// Separate HTTP traffic from HTTPS traffic.
		protocolMux := cmux.New(listener)
		clearL := protocolMux.Match(cmux.HTTP1()) // Note that adding this matcher first gives it priority.
		tlsL := protocolMux.Match(cmux.Any())
		// Redirect HTTP to HTTPS.
		redirectHandler := http.NewServeMux()
		redirectHandler.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// TODO(andrei): Consider dealing with HSTS headers. Probably drop HSTS
			// headers coming from CRDB, and set our own headers.
			http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
		})
		redirectServer := &http.Server{Handler: redirectHandler}
		servers = append(servers, redirectServer)
		if err := stop.RunAsyncTask(ctx, "reverse proxy redirect server", func(ctx context.Context) {
			_ = redirectServer.Serve(clearL)
		}); err != nil {
			return
		}

		// Serve HTTPS traffic by delegating it to the proxy.
		tlsServer := &http.Server{Handler: mux}
		servers = append(servers, tlsServer)
		if err := stop.RunAsyncTask(ctx, "reverse proxy TLS server", func(ctx context.Context) {
			_ = tlsServer.ServeTLS(tlsL, p.certs.UICertPath, p.certs.UICertKeyPath)
		}); err != nil {
			return
		}
		if err := stop.RunAsyncTask(ctx, "reverse proxy protocol muxer", func(ctx context.Context) {
			_ = protocolMux.Serve()
		}); err != nil {
			return
		}
	}

	scheme := "http"
	if https {
		scheme = "https"
	}
	fmt.Printf("Listening for HTTP requests on %s://%s.\n", scheme, p.listenAddr)

	// Wait for the stopper to signal quiescing and shut down everything.
	go func() {
		<-stop.ShouldQuiesce()
		for _, s := range servers {
			_ = s.Shutdown(ctx)
		}
		_ = listener.Close()
	}()
}

// certificates groups together all the certificates relevant to the proxy
// server.
type certificates struct {
	UICertPath, UICertKeyPath string
	UICert                    *tls.Certificate
	CAPool                    *x509.CertPool
}

func loadCerts(uiCert, uiKey, caCert string) (certificates, error) {
	var certs certificates
	certs.UICertPath = uiCert
	certs.UICertKeyPath = uiKey
	if uiCert != "" {
		if uiKey == "" {
			return certificates{}, errors.New("--ui-cert-key needs to be specified if --ui-cert is specified")
		}
		cert, err := tls.LoadX509KeyPair(uiCert, uiKey)
		if err != nil {
			return certificates{}, errors.Wrap(err, "error parsing UI certificate")
		}
		certs.UICert = &cert
	}

	if caCert != "" {
		data, err := os.ReadFile(caCert)
		if err != nil {
			return certificates{}, errors.Wrap(err, "error reading CA cert")
		}
		block, rest := pem.Decode(data)
		if len(rest) != 0 {
			return certificates{}, errors.Newf("More than one certificate present in %s. Not sure how to deal with that.", caCert)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return certificates{}, errors.Wrap(err, "error parsing CA cert")
		}
		certs.CAPool = x509.NewCertPool()
		certs.CAPool.AddCert(cert)
	}
	return certs, nil
}

// atomicURL is a thread-safe URL.
type atomicURL struct {
	mu struct {
		syncutil.Mutex // this could be a RWMutex, but it's not worth it as the critical sections are small
		url            *url.URL
	}
}

func newAtomicURL(url *url.URL) *atomicURL {
	u := &atomicURL{}
	u.mu.url = url
	return u
}

func (u *atomicURL) Get() *url.URL {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.mu.url
}

func (u *atomicURL) ReplaceScheme(newScheme string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.mu.url.Scheme = newScheme
}

// newProxy creates a proxy that can forward requests to a CRDB cluster
// identified by url. If CRDB ever returns a redirect, then the redirect target
// will be used by subsequent requests.
//
// caCerts, if not nil, specifies what CA is trusted to sign CRDB's certs. If
// nil, the system defaults are used.
//
// HTTPToHTTPSErr, if set, will cause the proxy to detect when CRDB performs a
// HTTP to HTTPS redirection and return this error instead of proceeding to talk
// HTTPS to CRDB. The idea is that, if the Obs Service is not prepared to talk
// HTTPS to its clients, but CRDB insists on talking HTTPS to its clients, we'd
// rather return errors and ask people to configure the Obs Service for HTTPS
// than downgrade the security that CRDB insists on.
func newProxy(url *url.URL, caCerts *x509.CertPool, HTTPToHTTPSErr error) *httputil.ReverseProxy {
	atomicTarget := newAtomicURL(url)
	director := func(req *http.Request) {
		target := atomicTarget.Get()
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		targetQuery := target.RawQuery
		if targetQuery == "" || req.URL.RawQuery == "" {
			req.URL.RawQuery = targetQuery + req.URL.RawQuery
		} else {
			req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
		}
		if _, ok := req.Header["User-Agent"]; !ok {
			// explicitly disable User-Agent so it's not set to default value
			req.Header.Set("User-Agent", "")
		}
	}
	modifyResponse := func(r *http.Response) error {
		// We deal with redirects specifically: we detect when CRDB wants us to
		// switch from HTTP to HTTPS and remember that.
		if r.StatusCode != http.StatusTemporaryRedirect &&
			r.StatusCode != http.StatusPermanentRedirect &&
			r.StatusCode != http.StatusMovedPermanently {
			return nil
		}

		// Check if CRDB is asking to switch from HTTP to HTTPS. If it is, switch
		// future requests to use HTTPS.
		redirectTarget := r.Header.Get("Location")
		newURL, err := url.Parse(redirectTarget)
		if err != nil {
			return errors.Wrap(err, "invalid redirection")
		}
		if r.Request.URL.Scheme != newURL.Scheme {
			if r.Request.URL.Scheme == "http" && newURL.Scheme == "https" {
				// If we're not prepared to server HTTPS, error out.
				if HTTPToHTTPSErr != nil {
					return HTTPToHTTPSErr
				}
			}
			// From now on, go to https directly.
			atomicTarget.ReplaceScheme(newURL.Scheme)
			// We'll continue returning this redirect to the client. It will appear to
			// the client as a redirection to the same URL that it already requested;
			// that's fine. On the retry, we'll forward to the updated CRDB url.
		}
		return nil
	}
	proxy := &httputil.ReverseProxy{
		Director:       director,
		ModifyResponse: modifyResponse,
		// Overwrite the default error handler so that we render errors produced by
		// ModifyResponse. The default handler only logs them on the server and
		// doesn't return them to the client.
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte(err.Error()))
		},
	}
	if caCerts != nil {
		// Accept only the specified roots.
		proxy.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caCerts,
			},
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}
	return proxy
}
