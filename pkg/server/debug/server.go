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

package debug

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"strings"

	// Register the net/trace endpoint with http.DefaultServeMux.

	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
)

var origTraceAuthRequest = trace.AuthRequest

func init() {
	// Disable the net/trace auth handler. We saved it (in origTraceAuthRequest)
	// and will consult it as appropriate.
	trace.AuthRequest = func(r *http.Request) (allowed, sensitive bool) {
		return true, true
	}
}

// RemoteMode controls who can access /debug/requests.
type RemoteMode string

const (
	// RemoteOff disallows access to /debug/requests.
	RemoteOff RemoteMode = "off"
	// RemoteLocal allows only host-local access to /debug/requests.
	RemoteLocal RemoteMode = "local"
	// RemoteAny allows all access to /debug/requests.
	RemoteAny RemoteMode = "any"
)

// Endpoint is the entry point under which the debug tools are housed.
const Endpoint = "/debug/"

var debugRemote = settings.RegisterValidatedStringSetting(
	"server.remote_debugging.mode",
	"set to enable remote debugging, localhost-only or disable (any, local, off)",
	"local",
	func(s string) error {
		switch RemoteMode(strings.ToLower(s)) {
		case RemoteOff, RemoteLocal, RemoteAny:
			return nil
		default:
			return errors.Errorf("invalid mode: '%s'", s)
		}
	},
)

// Server serves the /debug/* family of tools.
type Server struct {
	st  *cluster.Settings
	mux *http.ServeMux
	spy logSpy
}

// NewServer sets up a debug server.
func NewServer(st *cluster.Settings) *Server {
	mux := http.NewServeMux()

	// Install a redirect to the UI's collection of debug tools.
	mux.HandleFunc(Endpoint, handleLanding)

	// Cribbed straight from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Cribbed straight from trace's `init()` method. See:
	// https://github.com/golang/net/blob/master/trace/trace.go
	mux.HandleFunc("/debug/requests", trace.Traces)
	mux.HandleFunc("/debug/events", trace.Events)

	// This registers a superset of the variables exposed through the
	// /debug/vars endpoint onto the /debug/metrics endpoint. It includes all
	// expvars registered globally and all metrics registered on the
	// DefaultRegistry.
	mux.Handle("/debug/metrics", exp.ExpHandler(metrics.DefaultRegistry))
	// Also register /debug/vars (even though /debug/metrics is better).
	mux.Handle("/debug/vars", expvar.Handler())

	// Set up the log spy, a tool that allows inspecting filtered logs at high
	// verbosity.
	spy := logSpy{
		setIntercept: log.Intercept,
	}
	mux.HandleFunc("/debug/logspy", spy.handleDebugLogSpy)

	return &Server{
		st:  st,
		mux: mux,
		spy: spy,
	}
}

// ServeHTTP serves various tools under the /debug endpoint. It restricts access
// according to the `server.remote_debugging.mode` cluster variable.
func (ds *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if authed := ds.authRequest(r); !authed {
		http.Error(w, "not allowed (due to the 'server.remote_debugging.mode' setting)",
			http.StatusForbidden)
		return
	}

	handler, _ := ds.mux.Handler(r)
	handler.ServeHTTP(w, r)
}

// authRequest restricts access to /debug/*.
func (ds *Server) authRequest(r *http.Request) bool {
	allow, _ := origTraceAuthRequest(r)

	switch RemoteMode(strings.ToLower(debugRemote.Get(&ds.st.SV))) {
	case RemoteAny:
		allow = true
	case RemoteLocal:
		// Default behavior of trace.AuthRequest.
		break
	default:
		allow = false
	}
	return allow
}

func handleLanding(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != Endpoint {
		http.Redirect(w, r, Endpoint, http.StatusMovedPermanently)
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
}
