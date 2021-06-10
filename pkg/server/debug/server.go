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
	"net/http"
	"net/http/pprof"
	"path"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
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

var _ = func() *settings.StringSetting {
	// This setting definition still exists so as to not break
	// deployment scripts that set it unconditionally.
	v := settings.RegisterStringSetting("server.remote_debugging.mode", "unused", "local")
	v.SetRetired()
	return v
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
	mux.HandleFunc("/debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) {
		CPUProfileHandler(st, w, r)
	})
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

	// Set up the vmodule endpoint.
	vsrv := &vmoduleServer{}
	mux.HandleFunc("/debug/vmodule", vsrv.vmoduleHandleDebug)

	// Set up the log spy, a tool that allows inspecting filtered logs at high
	// verbosity.
	spy := logSpy{
		vsrv:         vsrv,
		setIntercept: log.InterceptWith,
	}
	mux.HandleFunc("/debug/logspy", spy.handleDebugLogSpy)

	ps := pprofui.NewServer(pprofui.NewMemStorage(1, 0), func(profile string, labels bool, do func()) {
		ctx := context.Background()
		tBegin := timeutil.Now()

		if profile != "profile" {
			do()
			return
		}

		if err := CPUProfileDo(st, CPUProfileOptions{WithLabels: labels}.Type(), func() error {
			var extra string
			if labels {
				extra = " (enabling profiler labels)"
			}
			log.Infof(context.Background(), "pprofui: recording %s%s", profile, extra)
			do()
			return nil
		}); err != nil {
			// NB: we don't have good error handling here. Could be changed if we find
			// this problematic. In practice, `do()` wraps the pprof handler which will
			// return an error if there's already a profile going on just the same.
			log.Warningf(ctx, "unable to start CPU profile: %s", err)
			return
		}
		log.Infof(ctx, "pprofui: recorded %s in %.2fs", profile, timeutil.Since(tBegin).Seconds())
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

		eng := engines[i]
		ds.mux.HandleFunc(fmt.Sprintf("/debug/lsm/%d", id.StoreID),
			func(w http.ResponseWriter, req *http.Request) {
				_, _ = io.WriteString(w, eng.GetMetrics().String())
			})

		dir := specs[i].Path
		ds.mux.HandleFunc(fmt.Sprintf("/debug/lsm-viz/%d", id.StoreID),
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
