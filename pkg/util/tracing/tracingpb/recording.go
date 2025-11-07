// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingpb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	jaegerjson "github.com/jaegertracing/jaeger/model/json"
)

// RecordingType is the type of recording that a Span might be performing.
type RecordingType int32

const (
	// RecordingOff means that the Span discards events passed in.
	RecordingOff RecordingType = iota

	// RecordingStructured means that the Span discards events passed in through
	// Recordf(), but collects events passed in through RecordStructured(), as
	// well as information about child spans (their name, start and stop time).
	RecordingStructured

	// RecordingVerbose means that the Span collects events passed in through
	// Recordf() in its recording.
	RecordingVerbose
)

// ToCarrierValue encodes the RecordingType to be propagated through a carrier.
func (t RecordingType) ToCarrierValue() string {
	switch t {
	case RecordingOff:
		return "n"
	case RecordingStructured:
		return "s"
	case RecordingVerbose:
		return "v"
	default:
		panic(fmt.Sprintf("invalid RecordingType: %d", t))
	}
}

// ToProto converts t to the corresponding proto enum.
func (t RecordingType) ToProto() RecordingMode {
	switch t {
	case RecordingOff:
		return RecordingMode_OFF
	case RecordingStructured:
		return RecordingMode_STRUCTURED
	case RecordingVerbose:
		return RecordingMode_VERBOSE
	default:
		panic(fmt.Sprintf("invalid RecordingType: %d", t))
	}
}

// RecordingTypeFromProto converts from the proto values to the corresponding enum.
func RecordingTypeFromProto(val RecordingMode) RecordingType {
	switch val {
	case RecordingMode_OFF:
		return RecordingOff
	case RecordingMode_STRUCTURED:
		return RecordingStructured
	case RecordingMode_VERBOSE:
		return RecordingVerbose
	default:
		panic(fmt.Sprintf("invalid RecordingType: %d", val))
	}
}

// RecordingTypeFromCarrierValue decodes a recording type carried by a carrier.
func RecordingTypeFromCarrierValue(val string) RecordingType {
	switch val {
	case "v":
		return RecordingVerbose
	case "s":
		return RecordingStructured
	case "n":
		return RecordingOff
	default:
		// Unrecognized.
		return RecordingOff
	}
}

type traceLogData struct {
	logRecord
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

type logRecord struct {
	Timestamp time.Time
	Msg       redact.RedactableString
}

// formatMode controls the level of detail for a printed
// Recording.
type formatMode int

const (
	// formatFull includes timestamps and all metadata.
	formatFull formatMode = iota
	// formatMinimal includes only messages, no timestamps, and strips file:line info.
	// It's primarily intended for using traces as part of deterministic test output.
	formatMinimal
)

// condensePathLinePrefix strips the entire path:line prefix from the input
// string, which is assumed to originate from a trace message. It preserves
// redaction markers and only modifies path:line patterns that are not inside
// redaction markers.
func condensePathLinePrefix(in redact.RedactableString) (out redact.RedactableString) {
	// NB: this won't reliably work with the format seen in formatFull mode. It
	// only works when redactability is enabled on the tracer (otherwise the
	// entire "inner message" is enclosed in redaction markers, and we make no
	// attempt here to handle the case of the additional `event:` prefix):
	//     `event: ‹pkg/foo/bar.go:1234 held breath›` // won't be condensed
	//     `event: pkg/foo/bar.go:1234 held ‹sensitive› breath` // will be condensed
	// This is inconsequential since this message is only called in formatMinimal,
	// in which case the `event:` is absent.
	msg := string(in) // `pkg/foo/bar.go:123 held breath`
	sm, em := string(redact.StartMarker()), string(redact.EndMarker())
	if strings.HasPrefix(msg, sm) && strings.HasSuffix(msg, em) {
		// We're dealing with an "unredactable" trace (in which case everything is
		// redacted). Strip these markers now, and add them back before returning.
		// Otherwise, the heuristic below sees these markers and returns the input
		// unchanged, but we'd like to do better (and also not have the behavior
		// depend so heavily on whether redactability is on).
		msg = msg[len(sm) : len(msg)-len(em)] // multi-byte unicode beware
		defer func() {
			out = redact.RedactableString(sm + msg + em)
		}()
	}
	const substr = ".go:"
	colonIdx := strings.Index(msg, substr)
	if colonIdx == -1 {
		return in
	}
	{
		// Verify that what we're cutting of has nothing to with
		// redaction. This avoids accidentally clobbering a message such
		// as `pkg/foo/‹very-sensitive.go:1234› message`
		pathPart := msg[:colonIdx] // `pkg/foo/bar`
		startMarker := string(redact.StartMarker())
		endMarker := string(redact.EndMarker())
		// Check for redaction markers in the path part.
		if strings.Contains(pathPart, startMarker) || strings.Contains(pathPart, endMarker) {
			return in
		}
	}
	// Find the end of the line number (space or end of string).
	msg = msg[colonIdx+len(substr):]           // 123 held breath
	msg = strings.TrimLeft(msg, "0123456789 ") // held breath
	return redact.RedactableString(msg)
}

func writeEntry(w redact.SafeWriter, entry traceLogData, start time.Time, mode formatMode) {
	if mode == formatFull {
		w.Printf("% 10.3fms % 10.3fms",
			1000*entry.Timestamp.Sub(start).Seconds(),
			1000*entry.timeSincePrev.Seconds())
		for i := 0; i < entry.depth+1; i++ {
			w.SafeString("    ")
		}
	}
	w.Printf("%s\n", entry.Msg)
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
	return redact.Sprint(r).StripMarkers()
}

// SafeFormat implements the redact.SafeFormatter interface.
func (r Recording) SafeFormat(w redact.SafePrinter, _ rune) {
	if len(r) == 0 {
		w.SafeString("<empty recording>")
		return
	}

	start := r[0].StartTime
	r.visitSpanWithWriter(r[0], 0 /* depth */, w, formatFull, start)

	// Check if there's any orphan spans (spans for which the parent is missing).
	// This shouldn't happen, but we're protecting against incomplete traces. For
	// example, ingesting of remote spans through DistSQL is complex. Orphan spans
	// would not be reflected in the output string at all without this.
	orphans := r.OrphanSpans()
	if len(orphans) > 0 {
		// This shouldn't happen.
		w.SafeString("orphan spans (trace is missing spans):\n")
		for _, o := range orphans {
			r.visitSpanWithWriter(o, 0 /* depth */, w, formatFull, start)
		}
	}
}

// OrphanSpans returns the spans with parents missing from the recording.
func (r Recording) OrphanSpans() []RecordedSpan {
	spanIDs := make(map[SpanID]struct{})
	for _, sp := range r {
		spanIDs[sp.SpanID] = struct{}{}
	}

	var orphans []RecordedSpan
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

// SafeFormatMinimal formats the recording using a minimal representation suitable for
// test output. It includes operation lines and log messages, but excludes
// timestamps, goroutine IDs, and file:line information from log messages.
//
// This method is intended for use in tests where trace output needs to be
// compared or included in test expectations.
func (r Recording) SafeFormatMinimal(w redact.SafeWriter) {
	if len(r) == 0 {
		w.SafeString("<empty recording>")
		return
	}

	start := r[0].StartTime
	r.visitSpanWithWriter(r[0], 0 /* depth */, w, formatMinimal, start)

	// Check if there's any orphan spans (spans for which the parent is missing).
	orphans := r.OrphanSpans()
	if len(orphans) > 0 {
		for _, o := range orphans {
			r.visitSpanWithWriter(o, 0 /* depth */, w, formatMinimal, start)
		}
	}
}

// FindLogMessage returns the first log message in the recording that matches
// the given regexp. The bool return value is true if such a message is found.
//
// This method strips the redaction markers from all the log messages, which is
// pretty inefficient.
func (r Recording) FindLogMessage(pattern string) (string, bool) {
	re := regexp.MustCompile(pattern)
	for _, sp := range r {
		for _, l := range sp.Logs {
			msg := l.Msg().StripMarkers()
			if re.MatchString(msg) {
				return msg, true
			}
		}
	}
	return "", false
}

// FindSpan returns the Span with the given operation. The bool retval is false
// if the Span is not found.
func (r Recording) FindSpan(operation string) (RecordedSpan, bool) {
	for _, sp := range r {
		if sp.Operation == operation {
			return sp, true
		}
	}
	return RecordedSpan{}, false
}

// OperationAndMetadata contains information about a tracing span operation and
// its corresponding metadata.
type OperationAndMetadata struct {
	Operation string
	Metadata  OperationMetadata
}

// visitSpan returns the log messages for sp, and all of sp's children.
//
// All messages from a Span are kept together. Sibling spans are ordered within
// the parent in their start order.
//
// visitSpanWithWriter is the generalized version that accepts an optional SafeWriter and format mode.
// If w is nil, it only collects traceLogData without writing. If w is non-nil, it writes to it.
func (r Recording) visitSpanWithWriter(
	sp RecordedSpan, depth int, w redact.SafeWriter, mode formatMode, start time.Time,
) []traceLogData {
	ownLogs := make([]traceLogData, 0, len(sp.Logs)+1)

	conv := func(msg redact.RedactableString, timestamp time.Time, ref time.Time) traceLogData {
		var timeSincePrev time.Duration
		if ref != (time.Time{}) {
			timeSincePrev = timestamp.Sub(ref)
		}
		return traceLogData{
			logRecord: logRecord{
				Timestamp: timestamp,
				Msg:       msg,
			},
			depth:         depth,
			timeSincePrev: timeSincePrev,
		}
	}

	// Build operation line only in full mode.
	if mode == formatFull {
		var sb redact.StringBuilder
		sb.SafeString("=== operation:")
		sb.SafeString(redact.SafeString(sp.Operation))

		if sp.GoroutineID != 0 {
			sb.Printf(" gid:%d", sp.GoroutineID)
		}

		for _, tg := range sp.TagGroups {
			var prefix redact.RedactableString
			if tg.Name != AnonymousTagGroupName {
				prefix = redact.Sprintf("%s-", redact.SafeString(tg.Name))
			}
			for _, tag := range tg.Tags {
				sb.Printf(" %s%s:%s", prefix, redact.SafeString(tag.Key), tag.Value)
			}
		}
		ownLogs = append(ownLogs, conv(sb.RedactableString(), sp.StartTime, time.Time{}))
	}

	// Sort the OperationMetadata of s' children in descending order of duration.
	childrenMetadata := make([]OperationAndMetadata, 0, len(sp.ChildrenMetadata))
	for operation, metadata := range sp.ChildrenMetadata {
		childrenMetadata = append(childrenMetadata,
			OperationAndMetadata{operation, metadata})
	}
	sort.Slice(childrenMetadata, func(i, j int) bool {
		return childrenMetadata[i].Metadata.Duration > childrenMetadata[j].Metadata.Duration
	})
	for _, c := range childrenMetadata {
		var sb redact.StringBuilder
		sb.Printf("[%s: %s]", redact.SafeString(c.Operation), c.Metadata)
		ownLogs = append(ownLogs, conv(sb.RedactableString(), sp.StartTime, time.Time{}))
	}

	for _, l := range sp.Logs {
		var sb redact.StringBuilder
		var ref time.Time
		if mode == formatFull {
			// NB: even an empty span has the `operation` line.
			ref = ownLogs[len(ownLogs)-1].Timestamp
			sb.Printf("event:%s", l.Msg())
		} else {
			// Timestamp will not be printed, but use sensible timestamp
			// anyway just because.
			ref = sp.StartTime
			// Skip "event:" prefix and strip paths in minimal mode.
			sb.Printf("%s", condensePathLinePrefix(l.Msg()))
		}
		ownLogs = append(ownLogs, conv(sb.RedactableString(), l.Time, ref))
	}

	// If the span was verbose at the time when the structured event was recorded,
	// then the Structured events will also have been stringified and included in
	// the Logs above. We conservatively serialize the structured events again
	// here, for the case when the span had not been verbose at the time.
	sp.Structured(func(sr *types.Any, t time.Time) {
		if mode == formatMinimal {
			return
		}
		str, err := MessageToJSONString(sr, false /* emitDefaults */)
		if err != nil {
			return
		}
		var ref time.Time
		if len(ownLogs) > 0 {
			ref = ownLogs[len(ownLogs)-1].Timestamp
		} else {
			ref = sp.StartTime
		}
		var sb redact.StringBuilder
		sb.SafeString("structured:")
		_, _ = sb.WriteString(str)
		ownLogs = append(ownLogs, conv(sb.RedactableString(), t, ref))
	})

	childSpans := make([][]traceLogData, 0)
	for _, osp := range r {
		if osp.ParentSpanID != sp.SpanID {
			continue
		}
		slice := r.visitSpanWithWriter(osp, depth+1, nil, mode, start)
		if len(slice) > 0 {
			childSpans = append(childSpans, slice)
		}
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
		} else if i < len(ownLogs) {
			mergedLogs = append(mergedLogs, ownLogs[i])
			lastTimestamp = ownLogs[i].Timestamp
			i++
		} else {
			// No more logs to merge.
			break
		}
	}

	// Write merged logs using the SafePrinter if provided.
	if w != nil {
		for _, entry := range mergedLogs {
			writeEntry(w, entry, start, mode)
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
func (r Recording) ToJaegerJSON(stmt, comment, nodeStr string, indent bool) (string, error) {
	if len(r) == 0 {
		return "", nil
	}

	cpy := make(Recording, len(r))
	copy(cpy, r)
	r = cpy

	tagGroupsCopy := make([]TagGroup, len(r[0].TagGroups))
	var anonymousTagGroup *TagGroup
	for i, tg := range r[0].TagGroups {
		tagGroupsCopy[i] = tg
		if tagGroupsCopy[i].Name == "" {
			anonymousTagGroup = &tagGroupsCopy[i]
		}
	}
	if anonymousTagGroup == nil {
		tagGroupsCopy = append(tagGroupsCopy, TagGroup{})
		anonymousTagGroup = &tagGroupsCopy[len(tagGroupsCopy)-1]
	}
	anonymousTagGroup.Tags = append(anonymousTagGroup.Tags, Tag{
		Key:   "statement",
		Value: stmt,
	})
	r[0].TagGroups = tagGroupsCopy

	toJaegerSpanID := func(spanID SpanID) jaegerjson.SpanID {
		return jaegerjson.SpanID(strconv.FormatUint(uint64(spanID), 10))
	}

	// Each Span in Jaeger belongs to a "process" that generated it. Spans
	// belonging to different colors are colored differently in Jaeger. We're
	// going to map our different nodes to different processes.
	processes := make(map[jaegerjson.ProcessID]jaegerjson.Process)
	// getProcessID figures out what "process" a Span belongs to. It looks for an
	// "node: <node id>" tag. The processes map is populated with an entry for every
	// node present in the trace.
	getProcessID := func(sp RecordedSpan) jaegerjson.ProcessID {
		node := "unknown node"
		for _, tagGroup := range sp.TagGroups {
			if tagGroup.Name != "" {
				// We know this particular tag is in the anonymous tag group, so only
				// search for that one.
				continue
			}
			for _, tag := range tagGroup.Tags {
				if tag.Key == "node" {
					node = fmt.Sprintf("node %s", tag.Value)
					break
				}
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
	t.TraceID = jaegerjson.TraceID(strconv.FormatUint(uint64(r[0].TraceID), 10))
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

		if sp.GoroutineID != 0 {
			s.Tags = append(s.Tags, jaegerjson.KeyValue{
				Key:   "goroutine",
				Value: sp.GoroutineID,
				Type:  jaegerjson.Int64Type,
			})
		}
		for _, tagGroup := range sp.TagGroups {
			for _, tag := range tagGroup.Tags {
				var prefix string
				if tagGroup.Name != AnonymousTagGroupName {
					prefix = fmt.Sprintf("%s-", tagGroup.Name)
				}
				s.Tags = append(s.Tags, jaegerjson.KeyValue{
					Key:   fmt.Sprintf("%s%s", prefix, tag.Key),
					Value: tag.Value,
					Type:  "STRING",
				})
			}
		}
		for _, l := range sp.Logs {
			jl := jaegerjson.Log{
				Timestamp: uint64(l.Time.UnixNano() / 1000),
				Fields: []jaegerjson.KeyValue{{
					Key:   "event",
					Value: l.Msg(),
					Type:  "STRING",
				}},
			}
			s.Logs = append(s.Logs, jl)
		}

		// If the span was verbose at the time when each structured event was
		// recorded, then the respective events would have been stringified and
		// included in the Logs above. If the span was not verbose at the time, we
		// need to produce a string now. We don't know whether the span was verbose
		// or not at the time each event was recorded, so we make a guess based on
		// whether the span was verbose at the moment when the Recording was
		// produced.
		if !(sp.Verbose || sp.RecordingMode == RecordingMode_VERBOSE) {
			sp.Structured(func(sr *types.Any, t time.Time) {
				jl := jaegerjson.Log{Timestamp: uint64(t.UnixNano() / 1000)}
				jsonStr, err := MessageToJSONString(sr, false /* emitDefaults */)
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
	var out []byte
	var err error
	if indent {
		out, err = json.MarshalIndent(data, "" /* prefix */, "\t" /* indent */)
	} else {
		out, err = json.Marshal(data)
	}
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// TraceCollection is the format accepted by the Jaegar upload feature, as per
// https://github.com/jaegertracing/jaeger-ui/issues/381#issuecomment-494150826
type TraceCollection struct {
	// Comment is a dummy field we use to put instructions on how to load the trace.
	Comment string             `json:"_comment"`
	Data    []jaegerjson.Trace `json:"data"`
}

// MessageToJSONString converts a protocol message into a JSON string. The
// emitDefaults flag dictates whether fields with zero values are rendered or
// not.
//
// TODO(andrei): It'd be nice if this function dealt with redactable vs safe
// fields, like EventPayload.AppendJSONFields does.
func MessageToJSONString(msg protoutil.Message, emitDefaults bool) (string, error) {
	// Convert to json.
	jsonEncoder := jsonpb.Marshaler{EmitDefaults: emitDefaults}
	msgJSON, err := jsonEncoder.MarshalToString(msg)
	if err != nil {
		return "", errors.Newf("error when converting %s to JSON string", proto.MessageName(msg))
	}

	return msgJSON, nil
}
