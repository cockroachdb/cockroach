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

// Endpoint is the entry point under which the debug tools are housed.
const Endpoint = "/debug/"

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

type DebugServer struct {
	st  *cluster.Settings
	mux *http.ServeMux
	spy logSpy
}

func NewDebugServer(st *cluster.Settings) *DebugServer {
	mux := http.NewServeMux()

	// Install a redirect to the UI's collection of debug tools.
	mux.HandleFunc(Endpoint, handleLanding) // TODO /debug?

	// Cribbed straight from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

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

	return &DebugServer{
		st:  st,
		mux: mux,
		spy: spy,
	}
}

// ServeHTTP serves various tools under the /debug endpoint. It restricts access
// according to the `server.remote_debugging.mode` cluster variable.
func (ds *DebugServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if any, _ := ds.authRequest(r); !any {
		http.Error(w, "not allowed (due to the 'server.remote_debugging.mode' setting)",
			http.StatusForbidden)
		return
	}

	handler, _ := ds.mux.Handler(r)
	handler.ServeHTTP(w, r)
}

// authRequest restricts access to /debug/*.
func (ds *DebugServer) authRequest(r *http.Request) (allow, sensitive bool) {
	allow, sensitive = trace.AuthRequest(r)
	switch DebugRemoteMode(strings.ToLower(debugRemote.Get(&ds.st.SV))) {
	case DebugRemoteAny:
		allow = true
	case DebugRemoteLocal:
		// Default behavior of trace.AuthRequest.
		break
	default:
		allow = false
	}
	return allow, sensitive
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
