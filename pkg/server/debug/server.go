// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package debug

import (
	"bytes"
	"context"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug/goroutineui"
	"github.com/cockroachdb/cockroach/pkg/server/debug/pprofui"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pebbletool "github.com/cockroachdb/pebble/tool"
	"github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	"github.com/spf13/cobra"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/metadata"
)

func init() {
	// Disable the net/trace auth handler.
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

// DebugRemote controls which clients are allowed to access certain
// confidential debug pages, such as those served under the /debug/ prefix.
var DebugRemote = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		"server.remote_debugging.mode",
		"set to enable remote debugging, localhost-only or disable (any, local, off)",
		"local",
		func(sv *settings.Values, s string) error {
			switch RemoteMode(strings.ToLower(s)) {
			case RemoteOff, RemoteLocal, RemoteAny:
				return nil
			default:
				return errors.Errorf("invalid mode: '%s'", s)
			}
		},
	)
	s.SetReportable(true)
	s.SetVisibility(settings.Public)
	return s
}()

// Server serves the /debug/* family of tools.
type Server struct {
	st  *cluster.Settings
	mux *http.ServeMux
	spy logSpy
}

// NewServer sets up a debug server.
func NewServer(st *cluster.Settings, hbaConfDebugFn http.HandlerFunc) *Server {
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

	if hbaConfDebugFn != nil {
		// Expose the processed HBA configuration through the debug
		// interface for inspection during troubleshooting.
		mux.HandleFunc("/debug/hba_conf", hbaConfDebugFn)
	}

	// Register the stopper endpoint, which lists all active tasks.
	mux.HandleFunc("/debug/stopper", stop.HandleDebug)

	// Set up the log spy, a tool that allows inspecting filtered logs at high
	// verbosity.
	spy := logSpy{
		setIntercept: log.Intercept,
	}
	mux.HandleFunc("/debug/logspy", spy.handleDebugLogSpy)

	ps := pprofui.NewServer(pprofui.NewMemStorage(1, 0), func(profile string, labels bool, do func()) {
		tBegin := timeutil.Now()

		extra := ""
		if profile == "profile" && labels {
			extra = " (enabling profiler labels)"
			st.SetCPUProfiling(true)
			defer st.SetCPUProfiling(false)
		}
		log.Infof(context.Background(), "pprofui: recording %s%s", profile, extra)

		do()

		log.Infof(context.Background(), "pprofui: recorded %s in %.2fs", profile, timeutil.Since(tBegin).Seconds())
	})
	mux.Handle("/debug/pprof/ui/", http.StripPrefix("/debug/pprof/ui", ps))

	mux.HandleFunc("/debug/pprof/goroutineui/", func(w http.ResponseWriter, req *http.Request) {
		dump := goroutineui.NewDump(timeutil.Now())

		_ = req.ParseForm()
		switch req.Form.Get("sort") {
		case "count":
			dump.SortCountDesc()
		case "wait":
			dump.SortWaitDesc()
		default:
		}
		_ = dump.HTML(w)
	})

	mux.HandleFunc("/debug/threads", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add("Content-type", "text/plain")
		fmt.Fprint(w, storage.ThreadStacks())
	})

	return &Server{
		st:  st,
		mux: mux,
		spy: spy,
	}
}

func analyzeLSM(dir string, writer io.Writer) error {
	manifestName, err := ioutil.ReadFile(path.Join(dir, "CURRENT"))
	if err != nil {
		return err
	}

	manifestPath := path.Join(dir, string(bytes.TrimSpace(manifestName)))

	t := pebbletool.New(pebbletool.Comparers(storage.MVCCComparer))

	// TODO(yevgeniy): Consider exposing LSM tool directly.
	var lsm *cobra.Command
	for _, c := range t.Commands {
		if c.Name() == "lsm" {
			lsm = c
		}
	}
	if lsm == nil {
		return errors.New("no such command")
	}

	lsm.SetOutput(writer)
	lsm.Run(lsm, []string{manifestPath})
	return nil
}

// RegisterEngines setups up debug engine endpoints for the known storage engines.
func (ds *Server) RegisterEngines(specs []base.StoreSpec, engines []storage.Engine) error {
	if len(specs) != len(engines) {
		// TODO(yevgeniy): Consider adding accessors to storage.Engine to get their path.
		return errors.New("number of store specs must match number of engines")
	}
	for i := 0; i < len(specs); i++ {
		if specs[i].InMemory {
			// TODO(yevgeniy): Add plumbing to support LSM visualization for in memory engines.
			continue
		}

		id, err := kvserver.ReadStoreIdent(context.Background(), engines[i])
		if err != nil {
			return err
		}

		dir := specs[i].Path
		ds.mux.HandleFunc(fmt.Sprintf("/debug/lsm/%d", id.StoreID),
			func(w http.ResponseWriter, req *http.Request) {
				if err := analyzeLSM(dir, w); err != nil {
					fmt.Fprintf(w, "error analyzing LSM at %s: %v", dir, err)
				}
			})
	}
	return nil
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
	return authRequest(r.RemoteAddr, ds.st)
}

// authRequest restricts access according to the DebugRemote setting.
func authRequest(remoteAddr string, st *cluster.Settings) bool {
	switch RemoteMode(strings.ToLower(DebugRemote.Get(&st.SV))) {
	case RemoteAny:
		return true
	case RemoteLocal:
		return isLocalhost(remoteAddr)
	default:
		return false
	}
}

// isLocalhost returns true if the remoteAddr represents a client talking to
// us via localhost.
func isLocalhost(remoteAddr string) bool {
	// RemoteAddr is commonly in the form "IP" or "IP:port".
	// If it is in the form "IP:port", split off the port.
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

// GatewayRemoteAllowed returns whether a request that has been passed through
// the grpc gateway should be allowed accessed to privileged debugging
// information. Because this function assumes the presence of a context field
// populated by the grpc gateway, it's not applicable for other uses.
func GatewayRemoteAllowed(ctx context.Context, st *cluster.Settings) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		// This should only happen for direct grpc connections, which are allowed.
		return true
	}
	peerAddr, ok := md["x-forwarded-for"]
	if !ok || len(peerAddr) == 0 {
		// This should only happen for direct grpc connections, which are allowed.
		return true
	}

	return authRequest(peerAddr[0], st)
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
