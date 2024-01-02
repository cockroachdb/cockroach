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
	"context"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/debug/goroutineui"
	"github.com/cockroachdb/cockroach/pkg/server/debug/pprofui"
	"github.com/cockroachdb/cockroach/pkg/server/debug/replay"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	pebbletool "github.com/cockroachdb/pebble/tool"
	"github.com/cockroachdb/pebble/vfs"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	"github.com/spf13/cobra"
	"golang.org/x/net/trace"
)

func init() {
	// Disable the net/trace auth handler.
	trace.AuthRequest = func(r *http.Request) (allowed, sensitive bool) {
		return true, true
	}
}

// Endpoint is the entry point under which the debug tools are housed.
const Endpoint = "/debug/"

// This setting definition still exists so as to not break
// deployment scripts that set it unconditionally.
var _ = settings.RegisterStringSetting(
	settings.ApplicationLevel, "server.remote_debugging.mode", "unused", "local",
	settings.Retired)

// Server serves the /debug/* family of tools.
type Server struct {
	ambientCtx log.AmbientContext
	st         *cluster.Settings
	mux        *http.ServeMux
	spy        logSpy
}

func setupProcessWideRoutes(
	mux *http.ServeMux,
	st *cluster.Settings,
	tenantID roachpb.TenantID,
	authorizer tenantcapabilities.Authorizer,
	vsrv *vmoduleServer,
	profiler pprofui.Profiler,
) {
	authzCheck := func(w http.ResponseWriter, r *http.Request) bool {
		if err := authorizer.HasProcessDebugCapability(r.Context(), tenantID); err != nil {
			http.Error(w, "tenant does not have capability to debug the running process", http.StatusForbidden)
			return false
		}
		return true
	}

	authzFunc := func(origHandler http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if authzCheck(w, r) {
				origHandler(w, r)
			}
		}
	}

	// Cribbed straight from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", authzFunc(pprof.Index))
	mux.HandleFunc("/debug/pprof/cmdline", authzFunc(pprof.Cmdline))
	mux.HandleFunc("/debug/pprof/profile", authzFunc(func(w http.ResponseWriter, r *http.Request) {
		CPUProfileHandler(st, w, r)
	}))
	mux.HandleFunc("/debug/pprof/symbol", authzFunc(pprof.Symbol))
	mux.HandleFunc("/debug/pprof/trace", authzFunc(pprof.Trace))

	// Cribbed straight from trace's `init()` method. See:
	// https://github.com/golang/net/blob/master/trace/trace.go
	mux.HandleFunc("/debug/requests", authzFunc(trace.Traces))
	mux.HandleFunc("/debug/events", authzFunc(trace.Events))

	// This registers a superset of the variables exposed through the
	// /debug/vars endpoint onto the /debug/metrics endpoint. It includes all
	// expvars registered globally and all metrics registered on the
	// DefaultRegistry.
	mux.HandleFunc("/debug/metrics", authzFunc(exp.ExpHandler(metrics.DefaultRegistry).ServeHTTP))
	// Also register /debug/vars (even though /debug/metrics is better).
	mux.HandleFunc("/debug/vars", authzFunc(expvar.Handler().ServeHTTP))

	// Register the stopper endpoint, which lists all active tasks.
	mux.HandleFunc("/debug/stopper", authzFunc(stop.HandleDebug))

	// Set up the vmodule endpoint.
	mux.HandleFunc("/debug/vmodule", authzFunc(vsrv.vmoduleHandleDebug))

	ps := pprofui.NewServer(pprofui.NewMemStorage(pprofui.ProfileConcurrency, pprofui.ProfileExpiry), profiler)
	mux.Handle("/debug/pprof/ui/", authzFunc(func(w http.ResponseWriter, r *http.Request) {
		http.StripPrefix("/debug/pprof/ui", ps).ServeHTTP(w, r)
	}))

	mux.HandleFunc("/debug/pprof/goroutineui/", authzFunc(func(w http.ResponseWriter, req *http.Request) {
		dump := goroutineui.NewDump()

		_ = req.ParseForm()
		switch req.Form.Get("sort") {
		case "count":
			dump.SortCountDesc()
		case "wait":
			dump.SortWaitDesc()
		default:
		}
		_ = dump.HTML(w)
	}))

}

// NewServer sets up a debug server.
func NewServer(
	ambientContext log.AmbientContext,
	st *cluster.Settings,
	hbaConfDebugFn http.HandlerFunc,
	profiler pprofui.Profiler,
	tenantID roachpb.TenantID,
	authorizer tenantcapabilities.Authorizer,
) *Server {
	mux := http.NewServeMux()

	// Install a redirect to the UI's collection of debug tools.
	mux.HandleFunc(Endpoint, handleLanding)

	// Debug routes that retrieve process-wide state.
	vsrv := &vmoduleServer{}
	setupProcessWideRoutes(mux, st, tenantID, authorizer, vsrv, profiler)

	if hbaConfDebugFn != nil {
		// Expose the processed HBA configuration through the debug
		// interface for inspection during troubleshooting.
		mux.HandleFunc("/debug/hba_conf", hbaConfDebugFn)
	}

	// Set up the log spy, a tool that allows inspecting filtered logs at high
	// verbosity. We require the tenant ID from the ambientCtx to set the logSpy
	// tenant filter.
	spy := logSpy{
		vsrv:         vsrv,
		setIntercept: log.InterceptWith,
	}
	serverTenantID := ambientContext.ServerIDs.ServerIdentityString(serverident.IdentifyTenantID)
	if serverTenantID == "" {
		panic("programmer error: cannot instantiate a debug.Server with no tenantID in the ambientCtx")
	}
	parsed, err := strconv.ParseUint(serverTenantID, 10, 64)
	if err != nil {
		panic("programmer error: failed parsing ambientCtx tenantID during debug.Server initialization")
	}
	spy.tenantID = roachpb.MustMakeTenantID(parsed)

	mux.HandleFunc("/debug/logspy", spy.handleDebugLogSpy)

	return &Server{
		ambientCtx: ambientContext,
		st:         st,
		mux:        mux,
		spy:        spy,
	}
}

func analyzeLSM(dir string, writer io.Writer) error {
	db, err := pebble.Peek(dir, vfs.Default)
	if err != nil {
		return err
	}

	t := pebbletool.New(pebbletool.Comparers(storage.EngineComparer))

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
	return lsm.RunE(lsm, []string{db.ManifestFilename})
}

func (ds *Server) RegisterWorkloadCollector(stores *kvserver.Stores) error {
	h := replay.HTTPHandler{Stores: stores}
	ds.mux.HandleFunc("/debug/workload_capture", h.HandleRequest)
	return nil
}

// RegisterEngines setups up debug engine endpoints for the known storage engines.
func (ds *Server) RegisterEngines(specs []base.StoreSpec, engines []storage.Engine) error {
	if len(specs) != len(engines) {
		// TODO(yevgeniy): Consider adding accessors to storage.Engine to get their path.
		return errors.New("number of store specs must match number of engines")
	}

	storeIDs := make([]roachpb.StoreIdent, len(engines))
	for i := range engines {
		id, err := kvstorage.ReadStoreIdent(context.Background(), engines[i])
		if err != nil {
			return err
		}
		storeIDs[i] = id
	}

	ds.mux.HandleFunc("/debug/lsm", func(w http.ResponseWriter, req *http.Request) {
		for i := range engines {
			fmt.Fprintf(w, "Store %d:\n", storeIDs[i].StoreID)
			_, _ = io.WriteString(w, engines[i].GetMetrics().String())
			fmt.Fprintln(w)
		}
	})

	for i := 0; i < len(specs); i++ {
		if specs[i].InMemory {
			// TODO(yevgeniy): Add plumbing to support LSM visualization for in memory engines.
			continue
		}

		dir := specs[i].Path
		ds.mux.HandleFunc(fmt.Sprintf("/debug/lsm-viz/%d", storeIDs[i].StoreID),
			func(w http.ResponseWriter, req *http.Request) {
				if err := analyzeLSM(dir, w); err != nil {
					fmt.Fprintf(w, "error analyzing LSM at %s: %v", dir, err)
				}
			})
	}
	return nil
}

// sidetransportReceiver abstracts *sidetransport.Receiver.
type sidetransportReceiver interface {
	HTML() string
}

// RegisterClosedTimestampSideTransport registers web endpoints for the closed
// timestamp side transport sender and receiver.
func (ds *Server) RegisterClosedTimestampSideTransport(
	sender *sidetransport.Sender, receiver sidetransportReceiver,
) {
	ds.mux.HandleFunc("/debug/closedts-receiver",
		func(w http.ResponseWriter, req *http.Request) {
			w.Header().Add("Content-type", "text/html")
			fmt.Fprint(w, receiver.HTML())
		})
	ds.mux.HandleFunc("/debug/closedts-sender",
		func(w http.ResponseWriter, req *http.Request) {
			w.Header().Add("Content-type", "text/html")
			fmt.Fprint(w, sender.HTML())
		})
}

// ServeHTTP serves various tools under the /debug endpoint.
func (ds *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler, _ := ds.mux.Handler(r)
	handler.ServeHTTP(w, r)
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
