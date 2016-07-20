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
//
// Author: Author: Tobias Schottdorf (tobias@cockroachlabs.com)

package server

import (
	"fmt"
	"net/http"

	// Register the net/trace endpoint with http.DefaultServeMux.
	"golang.org/x/net/trace"

	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"

	// This is imported for its side-effect of registering pprof endpoints with
	// the http.DefaultServeMux.
	_ "net/http/pprof"
)

// debugEndpoint is the prefix of golang's standard debug functionality
// for access to exported vars and pprof tools.
const debugEndpoint = "/debug/"

// We use the default http mux for the debug endpoint (as pprof and net/trace
// register to that via import, and go-metrics registers to that via exp.Exp())
var debugServeMux = http.DefaultServeMux

// handleDebug passes requests with the debugPathPrefix onto the default
// serve mux, which is preconfigured (by import of net/http/pprof and registration
// of go-metrics) to serve endpoints which access exported variables and pprof tools.
func handleDebug(w http.ResponseWriter, r *http.Request) {
	handler, _ := debugServeMux.Handler(r)
	handler.ServeHTTP(w, r)
}

func init() {
	// Tweak the authentication logic for the tracing endpoint. By default it's
	// open for localhost only, but with Docker we want to get there from
	// anywhere. We maintain the default behavior of only allowing access to
	// sensitive logs from localhost.
	//
	// TODO(mberhault): properly secure this once we require client certs.
	origAuthRequest := trace.AuthRequest
	trace.AuthRequest = func(req *http.Request) (bool, bool) {
		_, sensitive := origAuthRequest(req)
		return true, sensitive
	}

	debugServeMux.HandleFunc(debugEndpoint, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != debugEndpoint {
			http.Redirect(w, r, debugEndpoint, http.StatusMovedPermanently)
		}

		// The explicit header is necessary or (at least Chrome) will try to
		// download a gzipped file (Content-type comes back application/x-gzip).
		w.Header().Add("Content-type", "text/html")

		fmt.Fprint(w, `
<html>
<head>
<style>
table tr td {
  vertical-align: top;
}
</style>
<title>Debug endpoints</title>
</head>
<body>
<h1>Debug endpoints</h1>
<table>
<tr>
<td>trace (local node only)</td>
<td><a href="./requests">requests</a>, <a href="./events">events</a></td>
</tr>
<tr>
<td>stopper</td>
<td><a href="./stopper">active tasks</a></td>
</tr>
<tr>
<td>metrics</td>
<td>
<a href="./metrics">variables</a>
<a href="/_status/vars">prometheus</a>
</td>
</tr>
<tr>
<td>node status</td>
<td>
<a href="/_status/gossip/local">gossip</a><br />
<a href="/_status/ranges/local">ranges</a><br />
</td>
</tr>
<tr>
<td>raft</td>
<td><a href="/_status/raft">raft</a></td>
</tr>
<tr>
<td>pprof</td>
<td>
<!-- cribbed from the /debug/pprof endpoint -->
<a href="./pprof/block?debug=1">block</a><br />
<a href="./pprof/goroutine?debug=1">goroutine</a> (<a href="./pprof/goroutine?debug=2">all</a>)<br />
<a href="./pprof/heap?debug=1">heap</a><br />
<a href="./pprof/threadcreate?debug=1">threadcreate</a><br />
</td>
</tr>
</table>
</body></html>
`)
	})

	// This registers a superset of the variables exposed through the /debug/vars endpoint
	// onto the /debug/metrics endpoint. It includes all expvars registered globally and
	// all metrics registered on the DefaultRegistry.
	exp.Exp(metrics.DefaultRegistry)
}
