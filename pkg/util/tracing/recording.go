// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	jaegerjson "github.com/jaegertracing/jaeger/model/json"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pmezard/go-difflib/difflib"
)

// RecordingType is the type of recording that a Span might be performing.
type RecordingType int32

const (
	// RecordingOff means that the Span discards all events handed to it.
	// Child spans created from it similarly won't be recording by default.
	RecordingOff RecordingType = iota
	// RecordingVerbose means that the Span is adding events passed in via LogKV
	// and LogData to its recording and that derived spans will do so as well.
	RecordingVerbose

	// TODO(tbg): add RecordingBackground for always-on tracing.
)

type traceLogData struct {
	opentracing.LogRecord
	depth int
	// timeSincePrev represents the duration since the previous log line (previous in the
	// set of log lines that this is part of). This is always computed relative to a log line
	// from the same Span, except for start of Span in which case the duration is computed relative
	// to the last log in the parent occurring before this start. For example:
	// start Span A
	// log 1           // duration relative to "start Span A"
	//   start Span B  // duration relative to "log 1"
	//   log 2  			 // duration relative to "start Span B"
	// log 3  				 // duration relative to "log 1"
	timeSincePrev time.Duration
}

// String formats the given spans for human consumption, showing the
// relationship using nesting and times as both relative to the previous event
// and cumulative.
//
// Child spans are inserted into the parent at the point of the child's
// StartTime; see the diagram on generateSessionTraceVTable() for the ordering
// of messages.
//
// Each log line show the time since the beginning of the trace
// and since the previous log line. Span starts are shown with special "===
// <operation>" lines. For a Span start, the time since the relative log line
// can be negative when the Span start follows a message from the parent that
// was generated after the child Span started (or even after the child
// finished).
//
// TODO(andrei): this should be unified with
// SessionTracing.generateSessionTraceVTable().
func (r Recording) String() string {
	if len(r) == 0 {
		return "<empty recording>"
	}

	var buf strings.Builder
	start := r[0].StartTime
	writeLogs := func(logs []traceLogData) {
		for _, entry := range logs {
			fmt.Fprintf(&buf, "% 10.3fms % 10.3fms%s",
				1000*entry.Timestamp.Sub(start).Seconds(),
				1000*entry.timeSincePrev.Seconds(),
				strings.Repeat("    ", entry.depth+1))
			for i, f := range entry.Fields {
				if i != 0 {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%s:%v", f.Key(), f.Value())
			}
			buf.WriteByte('\n')
		}
	}

	logs := r.visitSpan(r[0], 0 /* depth */)
	writeLogs(logs)

	// Check if there's any orphan spans (spans for which the parent is missing).
	// This shouldn't happen, but we're protecting against incomplete traces. For
	// example, ingesting of remote spans through DistSQL is complex. Orphan spans
	// would not be reflected in the output string at all without this.
	orphans := r.OrphanSpans()
	if len(orphans) > 0 {
		// This shouldn't happen.
		buf.WriteString("orphan spans (trace is missing spans):\n")
		for _, o := range orphans {
			logs := r.visitSpan(o, 0 /* depth */)
			writeLogs(logs)
		}
	}
	return buf.String()
}

// OrphanSpans returns the spans with parents missing from the recording.
func (r Recording) OrphanSpans() []tracingpb.RecordedSpan {
	spanIDs := make(map[uint64]struct{})
	for _, sp := range r {
		spanIDs[sp.SpanID] = struct{}{}
	}

	var orphans []tracingpb.RecordedSpan
	for i, sp := range r {
		if i == 0 {
			// The first Span can be a root Span. Note that any other root Span will
			// be considered an orphan.
			continue
		}
		if _, ok := spanIDs[sp.ParentSpanID]; !ok {
			orphans = append(orphans, sp)
		}
	}
	return orphans
}

// FindLogMessage returns the first log message in the recording that matches
// the given regexp. The bool return value is true if such a message is found.
func (r Recording) FindLogMessage(pattern string) (string, bool) {
	re := regexp.MustCompile(pattern)
	for _, sp := range r {
		for _, l := range sp.Logs {
			msg := l.Msg()
			if re.MatchString(msg) {
				return msg, true
			}
		}
	}
	return "", false
}

// FindSpan returns the Span with the given operation. The bool retval is false
// if the Span is not found.
func (r Recording) FindSpan(operation string) (tracingpb.RecordedSpan, bool) {
	for _, sp := range r {
		if sp.Operation == operation {
			return sp, true
		}
	}
	return tracingpb.RecordedSpan{}, false
}

// visitSpan returns the log messages for sp, and all of sp's children.
//
// All messages from a Span are kept together. Sibling spans are ordered within
// the parent in their start order.
func (r Recording) visitSpan(sp tracingpb.RecordedSpan, depth int) []traceLogData {
	ownLogs := make([]traceLogData, 0, len(sp.Logs)+1)

	conv := func(l opentracing.LogRecord, ref time.Time) traceLogData {
		var timeSincePrev time.Duration
		if ref != (time.Time{}) {
			timeSincePrev = l.Timestamp.Sub(ref)
		}
		return traceLogData{
			LogRecord:     l,
			depth:         depth,
			timeSincePrev: timeSincePrev,
		}
	}

	// Add a log line representing the start of the Span.
	lr := opentracing.LogRecord{
		Timestamp: sp.StartTime,
		Fields:    []otlog.Field{otlog.String("=== operation", sp.Operation)},
	}
	if len(sp.Tags) > 0 {
		tags := make([]string, 0, len(sp.Tags))
		for k := range sp.Tags {
			tags = append(tags, k)
		}
		sort.Strings(tags)
		for _, k := range tags {
			lr.Fields = append(lr.Fields, otlog.String(k, sp.Tags[k]))
		}
	}
	ownLogs = append(ownLogs, conv(
		lr,
		// ref - this entries timeSincePrev will be computed when we merge it into the parent
		time.Time{}))

	for _, l := range sp.Logs {
		lr := opentracing.LogRecord{
			Timestamp: l.Time,
			Fields:    make([]otlog.Field, len(l.Fields)),
		}
		for i, f := range l.Fields {
			lr.Fields[i] = otlog.String(f.Key, f.Value)
		}
		lastLog := ownLogs[len(ownLogs)-1]
		ownLogs = append(ownLogs, conv(lr, lastLog.Timestamp))
	}

	// If the span was verbose then the Structured events would have been
	// stringified and included in the Logs above. If the span was not verbose
	// we should add the Structured events now.
	if !isVerbose(sp) {
		sp.Structured(func(sr *types.Any, t time.Time) {
			lr := opentracing.LogRecord{
				Timestamp: t,
			}
			str, err := MessageToJSONString(sr, true /* emitDefaults */)
			if err != nil {
				return
			}
			lr.Fields = append(lr.Fields, otlog.String("structured", str))
			lastLog := ownLogs[len(ownLogs)-1]
			ownLogs = append(ownLogs, conv(lr, lastLog.Timestamp))
		})
	}

	childSpans := make([][]traceLogData, 0)
	for _, osp := range r {
		if osp.ParentSpanID != sp.SpanID {
			continue
		}
		childSpans = append(childSpans, r.visitSpan(osp, depth+1))
	}

	// Merge ownLogs with childSpans.
	mergedLogs := make([]traceLogData, 0, len(ownLogs))
	timeMax := time.Date(2200, 0, 0, 0, 0, 0, 0, time.UTC)
	i, j := 0, 0
	var lastTimestamp time.Time
	for i < len(ownLogs) || j < len(childSpans) {
		if len(mergedLogs) > 0 {
			lastTimestamp = mergedLogs[len(mergedLogs)-1].Timestamp
		}
		nextLog, nextChild := timeMax, timeMax
		if i < len(ownLogs) {
			nextLog = ownLogs[i].Timestamp
		}
		if j < len(childSpans) {
			nextChild = childSpans[j][0].Timestamp
		}
		if nextLog.After(nextChild) {
			// Fill in timeSincePrev for the first one of the child's entries.
			if lastTimestamp != (time.Time{}) {
				childSpans[j][0].timeSincePrev = childSpans[j][0].Timestamp.Sub(lastTimestamp)
			}
			mergedLogs = append(mergedLogs, childSpans[j]...)
			lastTimestamp = childSpans[j][0].Timestamp
			j++
		} else {
			mergedLogs = append(mergedLogs, ownLogs[i])
			lastTimestamp = ownLogs[i].Timestamp
			i++
		}
	}

	return mergedLogs
}

// ToJaegerJSON returns the trace as a JSON that can be imported into Jaeger for
// visualization.
//
// The format is described here: https://github.com/jaegertracing/jaeger-ui/issues/381#issuecomment-494150826
//
// The statement is passed in so it can be included in the trace.
func (r Recording) ToJaegerJSON(stmt, comment, nodeStr string) (string, error) {
	if len(r) == 0 {
		return "", nil
	}

	cpy := make(Recording, len(r))
	copy(cpy, r)
	r = cpy
	tagsCopy := make(map[string]string)
	for k, v := range r[0].Tags {
		tagsCopy[k] = v
	}
	tagsCopy["statement"] = stmt
	r[0].Tags = tagsCopy

	toJaegerSpanID := func(spanID uint64) jaegerjson.SpanID {
		return jaegerjson.SpanID(strconv.FormatUint(spanID, 10))
	}

	// Each Span in Jaeger belongs to a "process" that generated it. Spans
	// belonging to different colors are colored differently in Jaeger. We're
	// going to map our different nodes to different processes.
	processes := make(map[jaegerjson.ProcessID]jaegerjson.Process)
	// getProcessID figures out what "process" a Span belongs to. It looks for an
	// "node: <node id>" tag. The processes map is populated with an entry for every
	// node present in the trace.
	getProcessID := func(sp tracingpb.RecordedSpan) jaegerjson.ProcessID {
		node := "unknown node"
		for k, v := range sp.Tags {
			if k == "node" {
				node = fmt.Sprintf("node %s", v)
				break
			}
		}
		// If we have passed in an explicit nodeStr then use that as a processID.
		if nodeStr != "" {
			node = nodeStr
		}
		pid := jaegerjson.ProcessID(node)
		if _, ok := processes[pid]; !ok {
			processes[pid] = jaegerjson.Process{
				ServiceName: node,
				Tags:        nil,
			}
		}
		return pid
	}

	var t jaegerjson.Trace
	t.TraceID = jaegerjson.TraceID(strconv.FormatUint(r[0].TraceID, 10))
	t.Processes = processes

	for _, sp := range r {
		var s jaegerjson.Span

		s.TraceID = t.TraceID
		s.Duration = uint64(sp.Duration.Microseconds())
		s.StartTime = uint64(sp.StartTime.UnixNano() / 1000)
		s.SpanID = toJaegerSpanID(sp.SpanID)
		s.OperationName = sp.Operation
		s.ProcessID = getProcessID(sp)

		if sp.ParentSpanID != 0 {
			s.References = []jaegerjson.Reference{{
				RefType: jaegerjson.ChildOf,
				TraceID: s.TraceID,
				SpanID:  toJaegerSpanID(sp.ParentSpanID),
			}}
		}

		for k, v := range sp.Tags {
			s.Tags = append(s.Tags, jaegerjson.KeyValue{
				Key:   k,
				Value: v,
				Type:  "STRING",
			})
		}
		for _, l := range sp.Logs {
			jl := jaegerjson.Log{Timestamp: uint64(l.Time.UnixNano() / 1000)}
			for _, field := range l.Fields {
				jl.Fields = append(jl.Fields, jaegerjson.KeyValue{
					Key:   field.Key,
					Value: field.Value,
					Type:  "STRING",
				})
			}
			s.Logs = append(s.Logs, jl)
		}

		// If the span was verbose then the Structured events would have been
		// stringified and included in the Logs above. If the span was not verbose
		// we should add the Structured events now.
		if !isVerbose(sp) {
			sp.Structured(func(sr *types.Any, t time.Time) {
				jl := jaegerjson.Log{Timestamp: uint64(t.UnixNano() / 1000)}
				jsonStr, err := MessageToJSONString(sr, true /* emitDefaults */)
				if err != nil {
					return
				}
				jl.Fields = append(jl.Fields, jaegerjson.KeyValue{
					Key:   "structured",
					Value: jsonStr,
					Type:  "STRING",
				})
				s.Logs = append(s.Logs, jl)
			})
		}

		t.Spans = append(t.Spans, s)
	}

	data := TraceCollection{
		Data: []jaegerjson.Trace{t},
		// Add a comment that will show-up at the top of the JSON file, is someone opens the file.
		// NOTE: This comment is scarce on newlines because they appear as \n in the
		// generated file doing more harm than good.
		Comment: comment,
	}
	json, err := json.MarshalIndent(data, "" /* prefix */, "\t" /* indent */)
	if err != nil {
		return "", err
	}
	return string(json), nil
}

// TraceCollection is the format accepted by the Jaegar upload feature, as per
// https://github.com/jaegertracing/jaeger-ui/issues/381#issuecomment-494150826
type TraceCollection struct {
	// Comment is a dummy field we use to put instructions on how to load the trace.
	Comment string             `json:"_comment"`
	Data    []jaegerjson.Trace `json:"data"`
}

// isVerbose returns true if the RecordedSpan was started is a verbose mode.
func isVerbose(s tracingpb.RecordedSpan) bool {
	if s.Baggage == nil {
		return false
	}
	_, isVerbose := s.Baggage[verboseTracingBaggageKey]
	return isVerbose
}

// TestingCheckRecordedSpans checks whether a recording looks like an expected
// one represented by a string with one line per expected span and one line per
// expected event (i.e. log message), with a tab-indentation for child spans.
//
//      if err := TestingCheckRecordedSpans(Span.GetRecording(), `
//          span: root
//              event: a
//              span: child
//                  event: [ambient] b
//                  event: c
//      `); err != nil {
//        t.Fatal(err)
//      }
//
// The event lines can (and generally should) omit the file:line part that they
// might contain (depending on the level at which they were logged).
//
// Note: this test function is in this file because it needs to be used by
// both tests in the tracing package and tests outside of it, and the function
// itself depends on tracing.
func TestingCheckRecordedSpans(rec Recording, expected string) error {
	normalize := func(rec string) string {
		// normalize the string form of a recording for ease of comparison.
		//
		// 1. Strip out any leading new lines.
		rec = strings.TrimLeft(rec, "\n")
		// 2. Strip out trailing whitespace.
		rec = strings.TrimRight(rec, "\n\t ")
		// 3. Strip out file:line information from the recordings.
		//
		// 	 Before |  "event: util/log/trace_test.go:111 log"
		// 	 After  |  "event: log"
		re := regexp.MustCompile(`event: .*:[0-9]*`)
		rec = string(re.ReplaceAll([]byte(rec), []byte("event:")))
		// 4. Change all tabs to four spaces.
		rec = strings.ReplaceAll(rec, "\t", "    ")
		// 5. Compute the outermost indentation.
		indent := strings.Repeat(" ", len(rec)-len(strings.TrimLeft(rec, " ")))
		// 6. Outdent each line by that amount.
		var lines []string
		for _, line := range strings.Split(rec, "\n") {
			lines = append(lines, strings.TrimPrefix(line, indent))
		}
		// 7. Stitch everything together.
		return strings.Join(lines, "\n")
	}

	var rows []string
	row := func(depth int, format string, args ...interface{}) {
		rows = append(rows, strings.Repeat("    ", depth)+fmt.Sprintf(format, args...))
	}

	mapping := make(map[uint64]uint64) // spanID -> parentSpanID
	for _, rs := range rec {
		mapping[rs.SpanID] = rs.ParentSpanID
	}
	depth := func(spanID uint64) int {
		// Traverse up the parent links until one is not found.
		curSpanID := spanID
		d := 0
		for {
			var ok bool
			curSpanID, ok = mapping[curSpanID]
			if !ok {
				break
			}
			d++
		}
		return d
	}

	for _, rs := range rec {
		d := depth(rs.SpanID)
		row(d, "span: %s", rs.Operation)
		if len(rs.Tags) > 0 {
			var tags []string
			for k, v := range rs.Tags {
				tags = append(tags, fmt.Sprintf("%s=%v", k, v))
			}
			sort.Strings(tags)
			row(d, "    tags: %s", strings.Join(tags, " "))
		}
		for _, l := range rs.Logs {
			var msg string
			for _, f := range l.Fields {
				msg = msg + fmt.Sprintf("    %s: %v", f.Key, f.Value)
			}
			row(d, "%s", msg)
		}
	}

	exp := normalize(expected)
	got := normalize(strings.Join(rows, "\n"))
	if got != exp {
		diff := difflib.UnifiedDiff{
			A:        difflib.SplitLines(exp),
			FromFile: "exp",
			B:        difflib.SplitLines(got),
			ToFile:   "got",
			Context:  4,
		}
		diffText, _ := difflib.GetUnifiedDiffString(diff)
		return errors.Newf("unexpected diff:\n%s\n\nrecording:\n%s", diffText, rec.String())
	}
	return nil
}

// TestingCheckRecording checks whether a recording looks like the expected
// one. The expected string is allowed to elide timing information, and the
// outer-most indentation level is adjusted for when comparing.
//
//       if err := TestingCheckRecording(sp.GetRecording(), `
//           === operation:root
//           event:root 1
//               === operation:remote child
//               event:remote child 1
//       `); err != nil {
//           t.Fatal(err)
//       }
//
func TestingCheckRecording(rec Recording, expected string) error {
	normalize := func(rec string) string {
		// normalize the string form of a recording for ease of comparison.
		//
		// 1. Strip out any leading new lines.
		rec = strings.TrimLeft(rec, "\n")
		// 2. Strip out trailing space.
		rec = strings.TrimRight(rec, "\n\t ")
		// 3. Strip out all timing information from the recordings.
		//
		// 	 Before |  "0.007ms      0.007ms    event:root 1"
		// 	 After  |  "event:root 1"
		re := regexp.MustCompile(`.*s.*s\s{4}`)
		rec = string(re.ReplaceAll([]byte(rec), nil))
		// 4. Change all tabs to four spaces.
		rec = strings.ReplaceAll(rec, "\t", "    ")
		// 5. Compute the outermost indentation.
		indent := strings.Repeat(" ", len(rec)-len(strings.TrimLeft(rec, " ")))
		// 6. Outdent each line by that amount.
		var lines []string
		for _, line := range strings.Split(rec, "\n") {
			lines = append(lines, strings.TrimPrefix(line, indent))
		}
		// 6. Stitch everything together.
		return strings.Join(lines, "\n")
	}

	exp := normalize(expected)
	got := normalize(rec.String())
	if got != exp {
		diff := difflib.UnifiedDiff{
			A:        difflib.SplitLines(exp),
			FromFile: "exp",
			B:        difflib.SplitLines(got),
			ToFile:   "got",
			Context:  4,
		}
		diffText, _ := difflib.GetUnifiedDiffString(diff)
		return errors.Newf("unexpected diff:\n%s", diffText)
	}
	return nil
}
