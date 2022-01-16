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
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ui"
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
	fileServer := http.StripPrefix("/debug/assets/", http.FileServer(http.FS(ui.ListJS)))
	mux.HandleFunc("/debug/tracez",
		func(w http.ResponseWriter, req *http.Request) {
			serveHTTP(ambientCtx.AnnotateCtx(context.Background()), w, req, tr)
		})
	mux.HandleFunc("/debug/show-trace",
		func(w http.ResponseWriter, req *http.Request) {
			serveHTTPTrace(ambientCtx.AnnotateCtx(req.Context()), w, req, tr)
		})
	mux.HandleFunc("/debug/assets/list.min.js", fileServer.ServeHTTP)
}

type pageData struct {
	SnapshotID   tracing.SnapshotID
	Now          time.Time
	CapturedAt   time.Time
	Err          error
	AllSnapshots []tracing.SnapshotInfo
	SpansList    spansList
}

type spansList struct {
	Spans []processedSpan
	// Stacks contains stack traces for the goroutines referenced by the Spans
	// through their GoroutineID field.
	Stacks map[int]string // GoroutineID to stack trace
}

var spansTableTemplate *template.Template

var hiddenTags = map[string]struct{}{
	"_unfinished": {},
	"_verbose":    {},
	"_dropped":    {},
	"node":        {},
	"store":       {},
}

func generateTagValue(t processedTag) string {
	val := t.val
	if t.link != "" {
		val = fmt.Sprintf("<button onclick=\"search('%s')\" class='tag-link'>%s</button>", t.link, t.val)
	}
	if t.caption == "" {
		return val
	}
	return fmt.Sprintf("<span title='%s'>%s</span>", t.caption, val)
}

func init() {
	// concatTags takes in a span's tags and stringiefies the ones that pass filter.
	concatTags := func(tags []processedTag, filter func(tag processedTag) bool) string {
		tagsArr := make([]string, 0, len(tags))
		for _, t := range tags {
			icon := ""
			if t.highlight {
				icon = "<img style='width:16px;height:16px;' src='https://icons.iconarchive.com/icons/google/noto-emoji-symbols/256/73028-warning-icon.png'/>"
			}
			keyAnnotation := ""
			if t.inherited {
				keyAnnotation = "<span title='inherited'>(↓)</span>"
			}

			k, v := t.key, t.val
			if !filter(t) {
				continue
			}
			if v != "" {
				tagsArr = append(tagsArr, fmt.Sprintf("%s%s%s:%s", icon, k, keyAnnotation, generateTagValue(t)))
			} else {
				tagsArr = append(tagsArr, k)
			}
		}
		return strings.Join(tagsArr, ", ")
	}

	spansTableTemplate = template.Must(template.New("spans-list").Funcs(
		template.FuncMap{
			"formatTime":         formatTime,
			"formatTimeNoMillis": formatTimeNoMillis,
			"since": func(t time.Time, capturedAt time.Time) string {
				return fmt.Sprintf("(%s ago)", formatDuration(capturedAt.Sub(t)))
			},
			"timeRaw": func(t time.Time) int64 { return t.UnixMicro() },
			"tags": func(sp processedSpan) string {
				return concatTags(sp.Tags, func(t processedTag) bool {
					return !t.hidden
				})
			},
			"hiddenTags": func(sp processedSpan) string {
				return concatTags(sp.Tags, func(t processedTag) bool {
					return t.hidden
				})
			},
		},
	).Parse(ui.SpansTableTemplateSrc))
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

	spansMap := make(map[uint64]*processedSpan)
	childrenMap := make(map[uint64][]*processedSpan)
	processedSpans := make([]processedSpan, len(spans))
	for i, s := range spans {
		p := processSpan(s, snapshot)
		ptr := &processedSpans[i]
		*ptr = p
		spansMap[p.SpanID] = &processedSpans[i]
		if _, ok := childrenMap[p.ParentSpanID]; !ok {
			childrenMap[p.ParentSpanID] = []*processedSpan{&processedSpans[i]}
		} else {
			childrenMap[p.ParentSpanID] = append(childrenMap[p.ParentSpanID], &processedSpans[i])
		}
	}
	// Propagate the highlight tags up.
	for _, s := range processedSpans {
		for _, t := range s.Tags {
			if !t.highlight {
				continue
			}
			propagateInterestingTagUpwards(t, &s, spansMap)
		}
	}
	// Propagate the inherit tags down.
	for _, s := range processedSpans {
		for _, t := range s.Tags {
			if !t.inherit || t.inherited {
				continue
			}
			propagateInheritTagDownwards(t, &s, childrenMap)
		}
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
			Spans:  processedSpans,
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

type processedSpan struct {
	Operation                     string
	TraceID, SpanID, ParentSpanID uint64
	Start                         time.Time
	GoroutineID                   uint64
	Tags                          []processedTag
}

type processedTag struct {
	key, val string
	caption  string
	link     string
	hidden   bool
	// copiedFromChild is set if this tag did not originate on the owner span, but
	// instead was propagated upwards from a child span.
	copiedFromChild bool
	// highlight is set if the tag should be rendered with a little exclamation
	// mark.
	highlight bool
	// inherit is set if this tag should be passed down to children, and
	// recursively.
	inherit bool
	// inherited is set if this tag was passed over from an ancestor.
	inherited bool
}

// propagateInterestingTagUpwards copies tag from sp to all of sp's ancestors.
func propagateInterestingTagUpwards(
	tag processedTag, sp *processedSpan, spans map[uint64]*processedSpan,
) {
	tag.copiedFromChild = true
	parentID := sp.ParentSpanID
	for {
		p, ok := spans[parentID]
		if !ok {
			return
		}
		p.Tags = append(p.Tags, tag)
		parentID = p.ParentSpanID
	}
}

func propagateInheritTagDownwards(
	tag processedTag, sp *processedSpan, children map[uint64][]*processedSpan,
) {
	tag.inherited = true
	tag.hidden = true
	for _, child := range children[sp.SpanID] {
		child.Tags = append(child.Tags, tag)
		propagateInheritTagDownwards(tag, child, children)
	}
}

func processSpan(s tracingpb.RecordedSpan, snap tracing.SpansSnapshot) processedSpan {
	p := processedSpan{
		Operation:    s.Operation,
		TraceID:      uint64(s.TraceID),
		SpanID:       uint64(s.SpanID),
		ParentSpanID: uint64(s.ParentSpanID),
		Start:        s.StartTime,
		GoroutineID:  s.GoroutineID,
	}

	// Sort the tags.
	tagKeys := make([]string, 0, len(s.Tags))
	for k := range s.Tags {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)

	p.Tags = make([]processedTag, len(s.Tags))
	for i, k := range tagKeys {
		p.Tags[i] = processTag(k, s.Tags[k], snap)
	}
	return p
}

func processTag(k, v string, snap tracing.SpansSnapshot) processedTag {
	p := processedTag{
		key: k,
		val: v,
	}
	_, hidden := hiddenTags[k]
	p.hidden = hidden

	switch k {
	case "lock_holder_txn":
		txnID := v
		// Take only the first 8 bytes, to keep the text shorter.
		txnIDShort := v[:8]
		p.val = txnIDShort
		p.highlight = true
		p.link = txnIDShort
		txnState := findTxnState(txnID, snap)
		if !txnState.found {
			p.caption = "blocked on unknown transaction"
		} else if txnState.curQuery != "" {
			p.caption = "blocked on txn currently running query: " + txnState.curQuery
		} else {
			p.caption = "blocked on idle txn"
		}
	case "statement":
		p.inherit = true
	}

	return p
}

type txnState struct {
	found    bool
	curQuery string
}

func findTxnState(txnID string, snap tracing.SpansSnapshot) txnState {
	// Iterate through all the traces and look for a "sql txn" span for the
	// respective transaction.
	for _, t := range snap.Traces {
		for _, s := range t {
			if s.Operation != "sql txn" || s.Tags["txn"] != txnID {
				continue
			}
			// I've found the transaction. Look through its children and find a SQL query.
			// !!! The search here is a bit brutal.
			for _, s2 := range t {
				if s2.Operation == "sql query" {
					return txnState{
						found:    true,
						curQuery: s2.Tags["statement"],
					}
				}
			}
			return txnState{
				found: true,
			}
		}
	}
	return txnState{
		found: false,
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
				err = errors.Errorf("invalid snapshot ID: %d", snapID)
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
				_, err = w.Write([]byte("<pre>" + r.String() + "\n</pre>"))
				if err != nil {
					goto Error
				}
				return
			}
		}
		err = errors.Errorf("trace %d not found in snapshot", traceID)
		goto Error
	}

Error:
	_, _ = w.Write([]byte(err.Error()))
}
