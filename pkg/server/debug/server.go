// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debug

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/util/tracing/goexectrace"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	pebbletool "github.com/cockroachdb/pebble/tool"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
	"github.com/felixge/fgprof"
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

	// flightRecorder, if set, is used to serve /debug/pprof/trace from the
	// flight recorder buffer instead of running a synchronous trace via
	// pprof.Trace. This avoids the Go runtime's single-trace-consumer
	// constraint.
	flightRecorder *goexectrace.SimpleFlightRecorder
}

func setupProcessWideRoutes(
	ds *Server,
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
	mux.HandleFunc("/debug/pprof/trace", authzFunc(func(w http.ResponseWriter, r *http.Request) {
		// When the flight recorder is active, the Go runtime only allows one
		// trace consumer. Stream the flight recorder buffer instead of calling
		// pprof.Trace, which would fail.
		if etw := ds.flightRecorder; etw != nil && etw.Enabled() {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", `attachment; filename="trace"`)
			if _, err := etw.WriteTraceTo(r.Context(), w); err != nil {
				// Response headers are already sent; logging is the only
				// option since http.Error would corrupt the partial response.
				log.Ops.Warningf(r.Context(), "goexectrace: error writing trace to HTTP response: %v", err)
			}
			return
		}
		pprof.Trace(w, r)
	}))

	// On-demand execution trace dump endpoint. Triggers DumpNow on the flight
	// recorder, saving a trace file to disk and returning the filename.
	mux.HandleFunc("/debug/execution-trace/dump", authzFunc(func(w http.ResponseWriter, r *http.Request) {
		etw := ds.flightRecorder
		if etw == nil || !etw.Enabled() {
			http.Error(w, "flight recorder is not enabled; "+
				"set obs.execution_tracer.duration to a positive value", http.StatusServiceUnavailable)
			return
		}
		reason := r.URL.Query().Get("reason")
		if reason == "" {
			reason = "HTTP debug endpoint"
		}
		tag := r.URL.Query().Get("tag")
		filename, err := etw.DumpNow(r.Context(), redact.Sprintf("%s", reason), tag)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if filename == "" {
			http.Error(w, "dump rate limited; try again later", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, filename)
	}))

	// Cribbed straight from trace's `init()` method. See:
	// https://github.com/golang/net/blob/master/trace/trace.go
	mux.HandleFunc("/debug/requests", authzFunc(trace.Traces))

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

	// WARNING: The /debug/pprof/fgprof endpoint provides wall-clock profiling for
	// both On-CPU and Off-CPU time. While it is safe to use in production, note
	// that profiling can introduce performance overhead, especially in
	// applications with a large number of goroutines (>10k). Use this endpoint
	// judiciously and monitor its impact on system performance.
	mux.HandleFunc("/debug/pprof/fgprof", authzFunc(fgprof.Handler().ServeHTTP))
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

	// Set up the log spy, a tool that allows inspecting filtered logs at high
	// verbosity. We require the tenant ID from the ambientCtx to set the logSpy
	// tenant filter.
	vsrv := &vmoduleServer{}
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

	ds := &Server{
		ambientCtx: ambientContext,
		st:         st,
		mux:        mux,
		spy:        spy,
	}

	// Debug routes that retrieve process-wide state.
	setupProcessWideRoutes(ds, mux, st, tenantID, authorizer, vsrv, profiler)

	if hbaConfDebugFn != nil {
		mux.HandleFunc("/debug/hba_conf", hbaConfDebugFn)
	}

	mux.HandleFunc("/debug/logspy", spy.handleDebugLogSpy)

	return ds
}

// RegisterFlightRecorder sets the execution trace flight recorder for the
// debug server, enabling the /debug/pprof/trace and
// /debug/execution-trace/dump endpoints.
func (ds *Server) RegisterFlightRecorder(fr *goexectrace.SimpleFlightRecorder) {
	ds.flightRecorder = fr
}

func analyzeLSM(dir string, writer io.Writer) error {
	db, err := pebble.Peek(dir, vfs.Default)
	if err != nil {
		return err
	}

	t := pebbletool.New(
		pebbletool.Comparers(&storage.EngineComparer),
		pebbletool.KeySchema(storage.DefaultKeySchema),
		pebbletool.KeySchemas(storage.KeySchemas...),
	)

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

// GetLSMStats creates a mapping between store IDs and LSM stats for all of the
// provided storage engines.
func GetLSMStats(engines []kvstorage.Engines) (map[roachpb.StoreID]string, error) {
	stats := make(map[roachpb.StoreID]string, len(engines))
	for _, eng := range engines {
		storeID, err := eng.TODOEngine().GetStoreID()
		if err != nil {
			return nil, err
		}
		stats[roachpb.StoreID(storeID)] = eng.TODOEngine().GetMetrics().String()
	}
	return stats, nil
}

// FormatLSMStats combines LSM stats from multiple stores into a single string.
func FormatLSMStats(stats map[roachpb.StoreID]string) string {
	var sb strings.Builder
	for storeID, stat := range stats {
		sb.WriteString(fmt.Sprintf("Store %d:\n%s\n\n", storeID, stat))
	}
	return sb.String()
}

// RegisterEngines setups up debug engine endpoints for the known storage engines.
func (ds *Server) RegisterEngines(engines []kvstorage.Engines) error {
	ds.mux.HandleFunc("/debug/lsm", func(w http.ResponseWriter, req *http.Request) {
		stats, err := GetLSMStats(engines)
		if err != nil {
			fmt.Fprintf(w, "error retrieving LSM stats: %v", err)
		}
		fmt.Fprint(w, FormatLSMStats(stats))
	})

	for _, eng := range engines {
		dir := eng.TODOEngine().Env().Dir
		if dir == "" {
			// TODO(yevgeniy): Add plumbing to support LSM visualization for in memory engines.
			continue
		}

		storeID, err := eng.TODOEngine().GetStoreID()
		if err != nil {
			return err
		}

		ds.mux.HandleFunc(fmt.Sprintf("/debug/lsm-viz/%d", storeID),
			func(w http.ResponseWriter, req *http.Request) {
				if err := analyzeLSM(dir, w); err != nil {
					fmt.Fprintf(w, "error analyzing LSM at %s: %v", dir, err)
				}
			})
		ds.mux.HandleFunc(fmt.Sprintf("/debug/storage/%d/profiles/separated-value-retrievals", storeID),
			func(w http.ResponseWriter, req *http.Request) {
				dur := 30 * time.Second
				if secsStr := req.Header.Get("seconds"); secsStr != "" {
					secs, err := strconv.ParseInt(secsStr, 10, 64)
					if err != nil {
						http.Error(w, "error parsing seconds", http.StatusBadRequest)
						return
					}
					dur = time.Duration(secs) * time.Second
				}
				ctx := req.Context()
				ctx, cancel := context.WithTimeout(ctx, dur)
				defer cancel()
				profile, err := eng.TODOEngine().ProfileSeparatedValueRetrievals(ctx)
				if err != nil {
					http.Error(w, "error profiling separated value retrievals", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, profile.String())
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
