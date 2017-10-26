// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"fmt"
	"net/http"
	"strings"

	// Register the net/trace endpoint with http.DefaultServeMux.

	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"

	// This is imported for its side-effect of registering pprof endpoints with
	// the http.DefaultServeMux.
	_ "net/http/pprof"
)

// debugEndpoint is the prefix of golang's standard debug functionality
// for access to exported vars and pprof tools.
const debugEndpoint = "/debug/"

const debugLogSpyEndpoint = "/debug/logspy"

// We use the default http mux for the debug endpoint (as pprof and net/trace
// register to that via import, and go-metrics registers to that via exp.Exp())
var debugServeMux = http.DefaultServeMux

// DebugRemoteMode controls who can access /debug/requests.
type DebugRemoteMode string

const (
	// DebugRemoteOff disallows access to /debug/requests.
	DebugRemoteOff DebugRemoteMode = "off"
	// DebugRemoteLocal allows only host-local access to /debug/requests.
	DebugRemoteLocal DebugRemoteMode = "local"
	// DebugRemoteAny allows all access to /debug/requests.
	DebugRemoteAny DebugRemoteMode = "any"
)

var debugRemote = settings.RegisterValidatedStringSetting(
	"server.remote_debugging.mode",
	"set to enable remote debugging, localhost-only or disable (any, local, off)",
	"local",
	func(s string) error {
		switch DebugRemoteMode(strings.ToLower(s)) {
		case DebugRemoteOff, DebugRemoteLocal, DebugRemoteAny:
			return nil
		default:
			return errors.Errorf("invalid mode: '%s'", s)
		}
	},
)

// authorizedHandler is a middleware http handler that checks that the caller
// is authorized to access the handler.
func authorizedHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if any, _ := authRequest(r); !any {
			http.Error(w, "not allowed (due to the 'server.remote_debugging.mode' setting)",
				http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleDebug passes requests with the debugPathPrefix onto the default
// serve mux, which is preconfigured (by import of net/http/pprof and registration
// of go-metrics) to serve endpoints which access exported variables and pprof tools.
func handleDebug(w http.ResponseWriter, r *http.Request) {
	handler, _ := debugServeMux.Handler(r)
	handler.ServeHTTP(w, r)
}

// traceAuthRequest is the original trace.AuthRequest, populated in init().
var traceAuthRequest func(*http.Request) (bool, bool)

// authRequest restricts access to /debug/*.
func authRequest(r *http.Request) (allow, sensitive bool) {
	allow, sensitive = traceAuthRequest(r)
	switch DebugRemoteMode(strings.ToLower(debugRemote.Get(settings.TODO()))) {
	case DebugRemoteAny:
		allow = true
	case DebugRemoteLocal:
		break
	default:
		allow = false
	}
	return allow, sensitive
}

func init() {
	traceAuthRequest = trace.AuthRequest

	// Tweak the authentication logic for the tracing endpoint. By default it's
	// open for localhost only, but we want it to behave according to our
	// settings.
	//
	// TODO(mberhault): properly secure this once we require client certs.
	trace.AuthRequest = authRequest

	debugServeMux.HandleFunc(debugEndpoint, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != debugEndpoint {
			http.Redirect(w, r, debugEndpoint, http.StatusMovedPermanently)
			return
		}

		// The explicit header is necessary or (at least Chrome) will try to
		// download a gzipped file (Content-type comes back application/x-gzip).
		w.Header().Add("Content-type", "text/html")

		fmt.Fprint(w, `
<html>
  <head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="1; url=/#/debug">
    <script type="text/javascript">
        window.location.href = "/#/debug"
    </script>
    <title>Page Redirection</title>
  </head>
  <body>
    This page has moved.
    If you are not redirected automatically, follow this <a href='/#/debug'>link</a>.
  </body>
</html>
`)
	})

	spy := logSpy{
		setIntercept: log.Intercept,
	}
	debugServeMux.HandleFunc(debugLogSpyEndpoint, spy.handleDebugLogSpy)

	// This registers a superset of the variables exposed through the /debug/vars endpoint
	// onto the /debug/metrics endpoint. It includes all expvars registered globally and
	// all metrics registered on the DefaultRegistry.
	exp.Exp(metrics.DefaultRegistry)
}
