// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package debug

import (
	"context"
	"fmt"
	"net/http"
	rttrace "runtime/trace"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ExecutionTraceHandler serves Go execution traces with embedded CRDB metadata.
// It reimplements net/http/pprof.Trace (based on Go 1.24) to inject a
// runtime/trace.Log event containing node identity and version information
// immediately after starting the trace. If upgrading Go versions, diff this
// function against the upstream net/http/pprof.Trace implementation.
//
// The logMetadata callback is invoked with source="pprof_endpoint" to emit the
// metadata event into the trace buffer.
func ExecutionTraceHandler(
	logMetadata func(ctx context.Context, source string), w http.ResponseWriter, r *http.Request,
) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	sec, err := strconv.ParseFloat(r.FormValue("seconds"), 64)
	if sec <= 0 || err != nil {
		sec = 1
	}

	// Extend the server's write deadline to accommodate the trace duration,
	// matching the behavior of net/http/pprof.Trace.
	if srv, ok := r.Context().Value(http.ServerContextKey).(*http.Server); ok && srv.WriteTimeout > 0 {
		rc := http.NewResponseController(w)
		timeout := srv.WriteTimeout + time.Duration(sec*float64(time.Second))
		_ = rc.SetWriteDeadline(timeutil.Now().Add(timeout))
	}

	// Set Content-Type before starting the trace, since trace.Start will
	// begin writing binary data to w immediately.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="trace"`)
	if err := rttrace.Start(w); err != nil {
		// Clean up the success-path headers before sending the error.
		w.Header().Del("Content-Disposition")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		http.Error(w,
			fmt.Sprintf("Could not enable tracing: %s", err),
			http.StatusInternalServerError)
		return
	}
	defer rttrace.Stop()

	if logMetadata != nil {
		logMetadata(r.Context(), "pprof_endpoint")
	}

	timer := time.NewTimer(time.Duration(sec * float64(time.Second)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.Context().Done():
	}
}
