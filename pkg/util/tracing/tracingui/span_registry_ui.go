// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracingui

import (
	"context"
	// embed is required for go:embed directives
	_ "embed"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

// This file deals with producing the /debug/tracez page, which lists
// a snapshot of the spans in the Tracer's active spans registry.

const dateAndtimeFormat = "2006-01-02 15:04:05.000"
const timeFormat = "15:04:05.000"

// RegisterHTTPHandlers registers the /debug/tracez handlers, and helpers.
func RegisterHTTPHandlers(ambientCtx log.AmbientContext, mux *http.ServeMux, tr *tracing.Tracer) {
	mux.HandleFunc("/debug/tracez",
		func(w http.ResponseWriter, req *http.Request) {
			serveHTTP(ambientCtx.AnnotateCtx(context.Background()), w, req, tr)
		})
	mux.HandleFunc("/debug/show-trace",
		func(w http.ResponseWriter, req *http.Request) {
			serveHTTPTrace(ambientCtx.AnnotateCtx(context.Background()), w, req, tr)
		})
	mux.HandleFunc("/debug/list.js",
		func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
			_, _ = w.Write([]byte(listJS))
		})
}

//go:embed list.js
var listJS string

//go:embed html_template.html
var spansTableTemplateSrc string

type pageData struct {
	SnapshotID   tracing.SnapshotID
	Now          time.Time
	CapturedAt   time.Time
	Err          error
	AllSnapshots []tracing.SnapshotInfo
	SpansList    spansList
}

type spansList struct {
	Spans []tracingpb.RecordedSpan
	// Stacks contains stack traces for the goroutines referenced by the Spans
	// through their GoroutineID field.
	Stacks map[int]string // GoroutineID to stack trace
}

var spansTableTemplate *template.Template

func init() {
	spansTableTemplate = template.Must(template.New("spans-list").Funcs(
		template.FuncMap{
			"formatTime":         formatTime,
			"formatTimeNoMillis": formatTimeNoMillis,
			"since": func(t time.Time, capturedAt time.Time) string {
				return fmt.Sprintf("(%s ago)", formatDuration(capturedAt.Sub(t)))
			},
			"timeRaw": func(t time.Time) int64 { return t.UnixMicro() },
		},
	).Parse(spansTableTemplateSrc))
}

func formatTime(t time.Time) string {
	t = t.UTC()
	if t.Truncate(24*time.Hour) == timeutil.Now().Truncate(24*time.Hour) {
		return t.Format(timeFormat)
	}
	return t.Format(dateAndtimeFormat)
}

func formatTimeNoMillis(t time.Time) string {
	t = t.UTC()
	if t.Truncate(24*time.Hour) == timeutil.Now().Truncate(24*time.Hour) {
		const format = "15:04:05"
		return t.Format(format)
	}
	const format = "2006-01-02 15:04:05"
	return t.Format(format)
}

// formatDuration formats a duration in one of the following formats, depending
// on its magnitude.
// 0.001s
// 1.000s
// 1m01s
// 1h05m01s
// 1d02h05m
func formatDuration(d time.Duration) string {
	d = d.Round(time.Millisecond)
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	d -= s * time.Second
	millis := d / time.Millisecond
	if days != 0 {
		return fmt.Sprintf("%dd%02dh%02dm", days, h, m)
	}
	if h != 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	if m != 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%d.%03ds", s, millis)
}

func serveHTTP(ctx context.Context, w http.ResponseWriter, r *http.Request, tr *tracing.Tracer) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var pageErr error
	if err := r.ParseForm(); err != nil {
		log.Warningf(ctx, "error parsing /tracez form: %s", err.Error())
		pageErr = err
	}

	snapID := r.Form.Get("snap")
	var snapshot tracing.SpansSnapshot
	var snapshotID tracing.SnapshotID
	switch snapID {
	case "new":
		// Capture a new snapshot and return a redirect to that snapshot's ID.
		snapshotID = tr.SaveSnapshot()
		newURL, err := r.URL.Parse(fmt.Sprintf("?snap=%d", snapshotID))
		if err != nil {
			pageErr = err
			break
		}
		http.Redirect(w, r, newURL.String(), http.StatusFound)
		return
	case "":
	default:
		id, err := strconv.Atoi(snapID)
		if err != nil {
			pageErr = errors.Errorf("invalid snapshot ID: %s", snapID)
			break
		}
		snapshotID = tracing.SnapshotID(id)
		snapshot, err = tr.GetSnapshot(snapshotID)
		if err != nil {
			pageErr = err
			break
		}
	}

	// Flatten the recordings.
	spans := make([]tracingpb.RecordedSpan, 0, len(snapshot.Traces)*3)
	for _, r := range snapshot.Traces {
		spans = append(spans, r...)
	}

	// Copy the stack traces and augment the map.
	stacks := make(map[int]string, len(snapshot.Stacks))
	for k, v := range snapshot.Stacks {
		stacks[k] = v
	}
	// Fill in messages for the goroutines for which we don't have a stack trace.
	for _, s := range spans {
		gid := int(s.GoroutineID)
		if _, ok := stacks[gid]; !ok {
			stacks[gid] = "Goroutine not found. Goroutine must have finished since the span was created."
		}
	}

	if pageErr != nil {
		snapshot.Err = pageErr
	}
	err := spansTableTemplate.ExecuteTemplate(w, "spans-list", pageData{
		Now:          timeutil.Now(),
		CapturedAt:   snapshot.CapturedAt,
		SnapshotID:   snapshotID,
		AllSnapshots: tr.GetSnapshots(),
		Err:          snapshot.Err,
		SpansList: spansList{
			Spans:  spans,
			Stacks: stacks,
		},
	})
	if err != nil {
		// We can get a "connection reset by peer" error if the browser requesting
		// the page has gone away.
		if !sysutil.IsErrConnectionReset(err) {
			log.Warningf(ctx, "error executing tracez template: %s", err)
			_, _ = w.Write([]byte(err.Error()))
		}
	}
}

func serveHTTPTrace(
	ctx context.Context, w http.ResponseWriter, r *http.Request, tr *tracing.Tracer,
) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	var err error
	if err = r.ParseForm(); err != nil {
		goto Error
	}

	{
		var traceID tracingpb.TraceID
		tid := r.Form.Get("trace")
		if tid == "" {
			err = errors.Errorf("trace ID missing; use ?trace=<x>")
			goto Error
		}
		var id int
		id, err = strconv.Atoi(tid)
		if err != nil {
			err = errors.Errorf("invalid trace ID: %s", id)
			goto Error
		}
		traceID = tracingpb.TraceID(id)

		var snapshotID tracing.SnapshotID
		var snapshot tracing.SpansSnapshot
		snapID := r.Form.Get("snap")
		if snapID != "" {
			var id int
			id, err = strconv.Atoi(snapID)
			if err != nil {
				err = errors.Errorf("invalid snapshot ID: %s", snapID)
				goto Error
			}
			snapshotID = tracing.SnapshotID(id)
			snapshot, err = tr.GetSnapshot(snapshotID)
			if err != nil {
				goto Error
			}
		} else {
			// If no snapshot is specified, we'll take a new one now and redirect to
			// it.
			snapshotID = tr.SaveSnapshot()
			var newURL *url.URL
			newURL, err = r.URL.Parse(fmt.Sprintf("?trace=%d&snap=%d", traceID, snapshotID))
			if err != nil {
				goto Error
			}
			http.Redirect(w, r, newURL.String(), http.StatusFound)
			return
		}

		for _, r := range snapshot.Traces {
			if r[0].TraceID == traceID {
				w.Write([]byte("<pre>"))
				w.Write([]byte(r.String()))
				w.Write([]byte("\n</pre>"))
				return
			}
		}
		err = errors.Errorf("trace %d not found in snapshot", traceID)
		goto Error
	}

Error:
	w.Write([]byte(err.Error()))
}
