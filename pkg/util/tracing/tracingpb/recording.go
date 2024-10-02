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
	writeLogs := func(logs []traceLogData) {
		for _, entry := range logs {
			w.Printf("% 10.3fms % 10.3fms",
				1000*entry.Timestamp.Sub(start).Seconds(),
				1000*entry.timeSincePrev.Seconds())
			for i := 0; i < entry.depth+1; i++ {
				w.SafeString("    ")
			}
			w.Printf("%s\n", entry.Msg)
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
		w.SafeString("orphan spans (trace is missing spans):\n")
		for _, o := range orphans {
			logs := r.visitSpan(o, 0 /* depth */)
			writeLogs(logs)
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
func (r Recording) visitSpan(sp RecordedSpan, depth int) []traceLogData {
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

	// Add a log line representing the start of the Span.
	var sb redact.StringBuilder
	sb.SafeString("=== operation:")
	sb.SafeString(redact.SafeString(sp.Operation))

	for _, tg := range sp.TagGroups {
		var prefix redact.RedactableString
		if tg.Name != AnonymousTagGroupName {
			prefix = redact.Sprint("%s-", redact.SafeString(tg.Name))
		}
		for _, tag := range tg.Tags {
			sb.Printf(" %s%s:%s", prefix, redact.SafeString(tag.Key), tag.Value)
		}
	}

	ownLogs = append(ownLogs, conv(
		sb.RedactableString(),
		sp.StartTime,
		// ref - this entries timeSincePrev will be computed when we merge it into the parent
		time.Time{}))

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
		lastLog := ownLogs[len(ownLogs)-1]
		var sb redact.StringBuilder
		sb.Printf("event:%s", l.Msg())
		ownLogs = append(ownLogs, conv(sb.RedactableString(), l.Time, lastLog.Timestamp))
	}

	// If the span was verbose at the time when the structured event was recorded,
	// then the Structured events will also have been stringified and included in
	// the Logs above. We conservatively serialize the structured events again
	// here, for the case when the span had not been verbose at the time.
	sp.Structured(func(sr *types.Any, t time.Time) {
		str, err := MessageToJSONString(sr, false /* emitDefaults */)
		if err != nil {
			return
		}
		lastLog := ownLogs[len(ownLogs)-1]
		var sb redact.StringBuilder
		sb.SafeString("structured:")
		_, _ = sb.WriteString(str)
		ownLogs = append(ownLogs, conv(sb.RedactableString(), t, lastLog.Timestamp))
	})

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
