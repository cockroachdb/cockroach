// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	proto "github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	jaegerjson "github.com/jaegertracing/jaeger/model/json"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"golang.org/x/net/trace"
)

// spanMeta stores Span information that is common to Span and SpanContext.
type spanMeta struct {
	// A probabilistically unique identifier for a [multi-Span] trace.
	TraceID uint64

	// A probabilistically unique identifier for a Span.
	SpanID uint64
}

// SpanContext is information about a Span, used to derive spans
// from a parent in a way that's uniform between local and remote
// parents. For local parents, this generally references their Span
// to unlock features such as sharing recordings with the parent. For
// remote parents, it only contains the TraceID and related metadata.
type SpanContext struct {
	spanMeta

	// Underlying shadow tracer info and context (optional).
	shadowTr  *shadowTracer
	shadowCtx opentracing.SpanContext

	// If set, all spans derived from this context are being recorded.
	recordingType RecordingType
	// span is set if this context corresponds to a local span. If so, pointing
	// back to the span is used for registering child spans with their parent.
	// Children of remote spans act as roots when it comes to recordings - someone
	// is responsible for calling GetRecording() on them and marshaling the
	// recording back to the parent (generally an RPC handler does this).
	span *Span

	// The Span's associated baggage.
	Baggage map[string]string
}

const (
	// TagPrefix is prefixed to all tags that should be output in SHOW TRACE.
	TagPrefix = "cockroach."
	// StatTagPrefix is prefixed to all stats output in Span tags.
	StatTagPrefix = TagPrefix + "stat."
)

// SpanStats are stats that can be added to a Span.
type SpanStats interface {
	proto.Message
	// Stats returns the stats that the object represents as a map from stat name
	// to value to be added to Span tags. The keys will be prefixed with
	// StatTagPrefix.
	Stats() map[string]string
}

var _ opentracing.SpanContext = &SpanContext{}

// ForeachBaggageItem is part of the opentracing.SpanContext interface.
func (sc *SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	for k, v := range sc.Baggage {
		if !handler(k, v) {
			break
		}
	}
}

// RecordingType is the type of recording that a Span might be performing.
type RecordingType int

const (
	// NoRecording means that the Span isn't recording. Child spans created from
	// it similarly won't be recording by default.
	NoRecording RecordingType = iota
	// SnowballRecording means that the Span is recording and that derived
	// spans will be as well, in the same mode (this includes remote spans,
	// i.e. this mode crosses RPC boundaries). Derived spans will maintain
	// their own recording, and this recording will be included in that of
	// any local parent spans.
	SnowballRecording
	// SingleNodeRecording means that the Span is recording and that locally
	// derived spans will as well (i.e. a remote Span typically won't be
	// recording by default, in contrast to SnowballRecording). Similar to
	// SnowballRecording, children have their own recording which is also
	// included in that of their parents.
	SingleNodeRecording
)

type crdbSpan struct {
	spanMeta

	parentSpanID uint64

	operation string
	startTime time.Time

	// logTags are set to the log tags that were available when this Span was
	// created, so that there's no need to eagerly copy all of those log tags into
	// this Span's tags. If the Span's tags are actually requested, these logTags
	// will be copied out at that point.
	// Note that these tags have not gone through the log tag -> Span tag
	// remapping procedure; tagName() needs to be called before exposing each
	// tag's key to a user.
	logTags *logtags.Buffer

	// Atomic flag used to avoid taking the mutex in the hot path.
	recording int32

	mu struct {
		syncutil.Mutex
		// duration is initialized to -1 and set on Finish().
		duration time.Duration

		// recording maintains state once StartRecording() is called.
		recording struct {
			recordingType RecordingType
			recordedLogs  []opentracing.LogRecord
			// children contains the list of child spans started after this Span
			// started recording.
			children []*crdbSpan
			// remoteSpan contains the list of remote child spans manually imported.
			remoteSpans []tracingpb.RecordedSpan
		}

		// tags are only set when recording. These are tags that have been added to
		// this Span, and will be appended to the tags in logTags when someone
		// needs to actually observe the total set of tags that is a part of this
		// Span.
		// TODO(radu): perhaps we want a recording to capture all the tags (even
		// those that were set before recording started)?
		tags opentracing.Tags

		stats SpanStats

		// The Span's associated baggage.
		Baggage map[string]string
	}
}

func (s *crdbSpan) isRecording() bool {
	return s != nil && atomic.LoadInt32(&s.recording) != 0
}

type otSpan struct {
	// TODO(tbg): see if we can lose the shadowTr here and rely on shadowSpan.Tracer().
	// Probably not - but worth checking.
	// TODO(tbg): consider renaming 'shadow' -> 'ot' or 'external'.
	shadowTr   *shadowTracer
	shadowSpan opentracing.Span
}

// Span is the tracing Span that we use in CockroachDB. Depending on the tracing configuration,
// it can hold anywhere between zero and three destinations for trace information.
//
// The net/trace and opentracing spans are straightforward. If they are
// set, we forward information to them; and depending on whether they are
// set, spans descending from a parent will have these created as well.
//
// The CockroachDB-internal Span (crdbSpan) is more complex as it has multiple features:
//
// 1. recording: crdbSpan supports "recordings", meaning that it provides a way to extract
//    the data logged into a trace Span.
// 2. optimizations for the non-tracing case. If tracing is off and the Span is not required
//    to support recording (NoRecording), we still want to be able to have a cheap Span
//    to give to the caller. This is a) because it frees the caller from
//    distinguishing the tracing and non-tracing cases, and b) because the Span
//    has the dual purpose of propagating the *Tracer around, which is needed
//    in case at some point down the line there is a need to create an actual
//    Span (for example, because a "recordable" child Span is requested).
//
//    In these cases, we return a singleton Span that is empty save for the tracer.
// 3. snowball recording. As a special case of 1), we support a recording mode
//    (SnowballRecording) which propagates to child spans across RPC boundaries.
// 4. parent Span recording. To make matters even more complex, there is a single-node
//    recording option (SingleNodeRecording) in which the parent Span keeps track of
//    its local children and returns their recording in its own.
//
// TODO(tbg): investigate whether the tracer in 2) is really needed.
// TODO(tbg): simplify the functionality of crdbSpan, which seems overly complex.
type Span struct {
	tracer *Tracer // never nil

	// Internal trace Span. Can be zero.
	crdb crdbSpan
	// x/net/trace.Trace instance; nil if not tracing to x/net/trace.
	netTr trace.Trace
	// Shadow tracer and Span; zero if not using a shadow tracer.
	ot otSpan
}

// TODO(tbg): remove this. We don't need *Span to be an opentracing.Span.
var _ opentracing.Span = &Span{}

func (s *Span) isBlackHole() bool {
	return !s.crdb.isRecording() && s.netTr == nil && s.ot == (otSpan{})
}

func (s *Span) isNoop() bool {
	// NB: this is the same as `s` being zero with the exception
	// of the `tracer` field. However, `Span` is not comparable,
	// so this can't be expressed easily.
	return s.isBlackHole() && s.crdb.TraceID == 0
}

// IsRecording returns true if the Span is recording its events.
func IsRecording(s opentracing.Span) bool {
	return s.(*Span).crdb.isRecording()
}

// enableRecording start recording on the Span. From now on, log events and child spans
// will be stored.
//
// If parent != nil, the Span will be registered as a child of the respective
// parent.
// If separate recording is specified, the child is not registered with the
// parent. Thus, the parent's recording will not include this child.
func (s *crdbSpan) enableRecording(
	parent *crdbSpan, recType RecordingType, separateRecording bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	atomic.StoreInt32(&s.recording, 1)
	s.mu.recording.recordingType = recType
	if parent != nil && !separateRecording {
		parent.addChild(s)
	}
	if recType == SnowballRecording {
		s.setBaggageItemLocked(Snowball, "1")
	}
	// Clear any previously recorded info. This is needed by SQL SessionTracing,
	// who likes to start and stop recording repeatedly on the same Span, and
	// collect the (separate) recordings every time.
	s.mu.recording.recordedLogs = nil
	s.mu.recording.children = nil
	s.mu.recording.remoteSpans = nil
}

// StartRecording enables recording on the Span. Events from this point forward
// are recorded; also, all direct and indirect child spans started from now on
// will be part of the same recording.
//
// Recording is not supported by noop spans; to ensure a real Span is always
// created, use the Recordable option to StartSpan.
//
// If recording was already started on this Span (either directly or because a
// parent Span is recording), the old recording is lost.
//
// Children spans created from the Span while it is *not* recording will not
// necessarily be recordable.
func StartRecording(os opentracing.Span, recType RecordingType) {
	if recType == NoRecording {
		panic("StartRecording called with NoRecording")
	}
	sp := os.(*Span)
	if sp.isNoop() {
		panic("StartRecording called on NoopSpan; use the Recordable option for StartSpan")
	}

	// If we're already recording (perhaps because the parent was recording when
	// this Span was created), there's nothing to do.
	if !sp.crdb.isRecording() {
		sp.crdb.enableRecording(nil /* parent */, recType, false /* separateRecording */)
	}
}

// StopRecording disables recording on this Span. Child spans that were created
// since recording was started will continue to record until they finish.
//
// Calling this after StartRecording is not required; the recording will go away
// when all the spans finish.
//
// StopRecording() can be called on a Finish()ed Span.
func StopRecording(os opentracing.Span) {
	os.(*Span).disableRecording()
}

func (s *Span) disableRecording() {
	if s.isNoop() {
		panic("can't disable recording a noop Span")
	}
	s.crdb.disableRecording()
}

func (s *crdbSpan) disableRecording() {
	s.mu.Lock()
	defer s.mu.Unlock()
	atomic.StoreInt32(&s.recording, 0)
	// We test the duration as a way to check if the Span has been finished. If it
	// has, we don't want to do the call below as it might crash (at least if
	// there's a netTr).
	if (s.mu.duration == -1) && (s.mu.recording.recordingType == SnowballRecording) {
		// Clear the Snowball baggage item, assuming that it was set by
		// enableRecording().
		s.setBaggageItemLocked(Snowball, "")
	}
}

// IsRecordable returns true if {Start,Stop}Recording() can be called on this
// Span.
//
// In other words, this tests if the Span is our custom type, and not a noopSpan
// or anything else.
func IsRecordable(os opentracing.Span) bool {
	sp, isCockroachSpan := os.(*Span)
	return isCockroachSpan && !sp.isNoop()
}

// Recording represents a group of RecordedSpans, as returned by GetRecording.
// Spans are sorted by StartTime.
type Recording []tracingpb.RecordedSpan

// GetRecording retrieves the current recording, if the Span has recording
// enabled. This can be called while spans that are part of the recording are
// still open; it can run concurrently with operations on those spans.
func GetRecording(os opentracing.Span) Recording {
	return os.(*Span).crdb.getRecording()
}

func (s *crdbSpan) getRecording() Recording {
	if !s.isRecording() {
		return nil
	}
	s.mu.Lock()
	// The capacity here is approximate since we don't know how many grandchildren
	// there are.
	result := make(Recording, 0, 1+len(s.mu.recording.children)+len(s.mu.recording.remoteSpans))
	// Shallow-copy the children so we can process them without the lock.
	children := s.mu.recording.children
	result = append(result, s.getRecordingLocked())
	result = append(result, s.mu.recording.remoteSpans...)
	s.mu.Unlock()

	for _, child := range children {
		result = append(result, child.getRecording()...)
	}

	// Sort the spans by StartTime, except the first Span (the root of this
	// recording) which stays in place.
	toSort := result[1:]
	sort.Slice(toSort, func(i, j int) bool {
		return toSort[i].StartTime.Before(toSort[j].StartTime)
	})
	return result
}

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
func (r Recording) ToJaegerJSON(stmt string) (string, error) {
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
		t.Spans = append(t.Spans, s)
	}

	data := TraceCollection{
		Data: []jaegerjson.Trace{t},
		// Add a comment that will show-up at the top of the JSON file, is someone opens the file.
		// NOTE: This comment is scarce on newlines because they appear as \n in the
		// generated file doing more harm than good.
		Comment: fmt.Sprintf(`This is a trace for SQL statement: %s
This trace can be imported into Jaeger for visualization. From the Jaeger Search screen, select JSON File.
Jaeger can be started using docker with: docker run -d --name jaeger -p 16686:16686 jaegertracing/all-in-one:1.17
The UI can then be accessed at http://localhost:16686/search`,
			stmt),
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

// ImportRemoteSpans adds RecordedSpan data to the recording of the given Span;
// these spans will be part of the result of GetRecording. Used to import
// recorded traces from other nodes.
func ImportRemoteSpans(os opentracing.Span, remoteSpans []tracingpb.RecordedSpan) error {
	return os.(*Span).crdb.ImportRemoteSpans(remoteSpans)
}

func (s *crdbSpan) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) error {
	if !s.isRecording() {
		return errors.AssertionFailedf("adding Raw Spans to a Span that isn't recording")
	}
	// Change the root of the remote recording to be a child of this Span. This is
	// usually already the case, except with DistSQL traces where remote
	// processors run in spans that FollowFrom an RPC Span that we don't collect.
	remoteSpans[0].ParentSpanID = s.SpanID

	s.mu.Lock()
	s.mu.recording.remoteSpans = append(s.mu.recording.remoteSpans, remoteSpans...)
	s.mu.Unlock()
	return nil
}

// IsBlackHoleSpan returns true if events for this Span are just dropped. This
// is the case when the Span is not recording and no external tracer is configured.
// Tracing clients can use this method to figure out if they can short-circuit some
// tracing-related work that would be discarded anyway.
//
// The child of a blackhole Span is a non-recordable blackhole Span[*]. These incur
// only minimal overhead. It is therefore not worth it to call this method to avoid
// starting spans.
func IsBlackHoleSpan(s opentracing.Span) bool {
	sp := s.(*Span)
	return sp.isBlackHole()
}

// IsNoopContext returns true if the Span context is from a "no-op" Span. If
// this is true, any Span derived from this context will be a "black hole Span".
//
// You should never need to care about this method. It is exported for technical
// reasons.
func IsNoopContext(spanCtx opentracing.SpanContext) bool {
	sc := spanCtx.(*SpanContext)
	return sc.isNoop()
}

func (sc *SpanContext) isNoop() bool {
	return sc.recordingType == NoRecording && sc.shadowTr == nil
}

// SetSpanStats sets the stats on a Span. stats.Stats() will also be added to
// the Span tags.
func SetSpanStats(os opentracing.Span, stats SpanStats) {
	s := os.(*Span)
	if s.isNoop() {
		return
	}
	s.crdb.mu.Lock()
	s.crdb.mu.stats = stats
	for name, value := range stats.Stats() {
		s.setTagInner(StatTagPrefix+name, value, true /* locked */)
	}
	s.crdb.mu.Unlock()
}

// Finish is part of the opentracing.Span interface.
func (s *Span) Finish() {
	s.FinishWithOptions(opentracing.FinishOptions{})
}

// FinishWithOptions is part of the opentracing.Span interface.
func (s *Span) FinishWithOptions(opts opentracing.FinishOptions) {
	if s.isNoop() {
		return
	}
	finishTime := opts.FinishTime
	if finishTime.IsZero() {
		finishTime = time.Now()
	}
	s.crdb.mu.Lock()
	s.crdb.mu.duration = finishTime.Sub(s.crdb.startTime)
	s.crdb.mu.Unlock()
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.Finish()
	}
	if s.netTr != nil {
		s.netTr.Finish()
	}
}

// Context is part of the opentracing.Span interface.
//
// TODO(andrei, radu): Should this return noopSpanContext for a Recordable Span
// that's not currently recording? That might save work and allocations when
// creating child spans.
func (s *Span) Context() opentracing.SpanContext {
	s.crdb.mu.Lock()
	defer s.crdb.mu.Unlock()
	sc := s.SpanContext()
	return &sc
}

// SpanContext returns a SpanContext. Note that this returns a value,
// not a pointer, which the caller can use to avoid heap allocations.
func (s *Span) SpanContext() SpanContext {
	n := len(s.crdb.mu.Baggage)
	// In the common case, we have no baggage, so avoid making an empty map.
	var baggageCopy map[string]string
	if n > 0 {
		baggageCopy = make(map[string]string, n)
	}
	for k, v := range s.crdb.mu.Baggage {
		baggageCopy[k] = v
	}
	sc := SpanContext{
		spanMeta: s.crdb.spanMeta,
		span:     s,
		Baggage:  baggageCopy,
	}
	if s.ot.shadowSpan != nil {
		sc.shadowTr = s.ot.shadowTr
		sc.shadowCtx = s.ot.shadowSpan.Context()
	}

	if s.crdb.isRecording() {
		sc.recordingType = s.crdb.mu.recording.recordingType
	}
	return sc
}

// SetOperationName is part of the opentracing.Span interface.
func (s *Span) SetOperationName(operationName string) opentracing.Span {
	if s.isNoop() {
		return s
	}
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.SetOperationName(operationName)
	}
	s.crdb.operation = operationName
	return s
}

// SetTag is part of the opentracing.Span interface.
func (s *Span) SetTag(key string, value interface{}) opentracing.Span {
	if s.isNoop() {
		return s
	}
	return s.setTagInner(key, value, false /* locked */)
}

func (s *crdbSpan) setTagLocked(key string, value interface{}) {
	if s.mu.tags == nil {
		s.mu.tags = make(opentracing.Tags)
	}
	s.mu.tags[key] = value
}

func (s *Span) setTagInner(key string, value interface{}, locked bool) opentracing.Span {
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.SetTag(key, value)
	}
	if s.netTr != nil {
		s.netTr.LazyPrintf("%s:%v", key, value)
	}
	// The internal tags will be used if we start a recording on this Span.
	if !locked {
		s.crdb.mu.Lock()
		defer s.crdb.mu.Unlock()
	}
	s.crdb.setTagLocked(key, value)
	return s
}

// LogFields is part of the opentracing.Span interface.
func (s *Span) LogFields(fields ...otlog.Field) {
	if s.isNoop() {
		return
	}
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.LogFields(fields...)
	}
	if s.netTr != nil {
		// TODO(radu): when LightStep supports arbitrary fields, we should make
		// the formatting of the message consistent with that. Until then we treat
		// legacy events that just have an "event" key specially.
		if len(fields) == 1 && fields[0].Key() == tracingpb.LogMessageField {
			s.netTr.LazyPrintf("%s", fields[0].Value())
		} else {
			var buf bytes.Buffer
			for i, f := range fields {
				if i > 0 {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%s:%v", f.Key(), f.Value())
			}

			s.netTr.LazyPrintf("%s", buf.String())
		}
	}
	s.crdb.LogFields(fields...)
}

func (s *crdbSpan) LogFields(fields ...otlog.Field) {
	if !s.isRecording() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mu.recording.recordedLogs) < maxLogsPerSpan {
		s.mu.recording.recordedLogs = append(s.mu.recording.recordedLogs, opentracing.LogRecord{
			Timestamp: time.Now(),
			Fields:    fields,
		})
	}
}

// LogKV is part of the opentracing.Span interface.
func (s *Span) LogKV(alternatingKeyValues ...interface{}) {
	if s.isNoop() {
		return
	}
	fields, err := otlog.InterleavedKVToFields(alternatingKeyValues...)
	if err != nil {
		s.LogFields(otlog.Error(err), otlog.String("function", "LogKV"))
		return
	}
	s.LogFields(fields...)
}

// SetBaggageItem is part of the opentracing.Span interface.
func (s *Span) SetBaggageItem(restrictedKey, value string) opentracing.Span {
	if s.isNoop() {
		return s
	}
	s.crdb.SetBaggageItemAndTag(restrictedKey, value)
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.SetBaggageItem(restrictedKey, value)
		s.ot.shadowSpan.SetTag(restrictedKey, value)
	}
	// NB: nothing to do for net/trace.

	return s
}

func (s *crdbSpan) SetBaggageItemAndTag(restrictedKey, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setBaggageItemLocked(restrictedKey, value)
	s.setTagLocked(restrictedKey, value)
}

func (s *crdbSpan) setBaggageItemLocked(restrictedKey, value string) {
	if oldVal, ok := s.mu.Baggage[restrictedKey]; ok && oldVal == value {
		// No-op.
		return
	}
	if s.mu.Baggage == nil {
		s.mu.Baggage = make(map[string]string)
	}
	s.mu.Baggage[restrictedKey] = value
	s.setTagLocked(restrictedKey, value)
}

// BaggageItem is part of the opentracing.Span interface.
func (s *Span) BaggageItem(restrictedKey string) string {
	if s := s.crdb.BaggageItem(restrictedKey); s != "" {
		return s
	}
	if s.ot.shadowSpan == nil {
		return ""
	}
	return s.ot.shadowSpan.BaggageItem(restrictedKey)
}

func (s *crdbSpan) BaggageItem(restrictedKey string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.Baggage[restrictedKey]
}

// Tracer is part of the opentracing.Span interface.
func (s *Span) Tracer() opentracing.Tracer {
	return s.tracer
}

// LogEvent is part of the opentracing.Span interface. Deprecated.
func (s *Span) LogEvent(event string) {
	if s.isNoop() {
		return
	}
	s.LogFields(otlog.String(tracingpb.LogMessageField, event))
}

// LogEventWithPayload is part of the opentracing.Span interface. Deprecated.
func (s *Span) LogEventWithPayload(event string, payload interface{}) {
	if s.isNoop() {
		return
	}
	s.LogFields(otlog.String(tracingpb.LogMessageField, event), otlog.Object("payload", payload))
}

// Log is part of the opentracing.Span interface. Deprecated.
func (s *Span) Log(data opentracing.LogData) {
	panic("unimplemented")
}

// getRecordingLocked returns the Span's recording. This does not include
// children.
func (s *crdbSpan) getRecordingLocked() tracingpb.RecordedSpan {
	rs := tracingpb.RecordedSpan{
		TraceID:      s.TraceID,
		SpanID:       s.SpanID,
		ParentSpanID: s.parentSpanID,
		Operation:    s.operation,
		StartTime:    s.startTime,
		Duration:     s.mu.duration,
	}

	addTag := func(k, v string) {
		if rs.Tags == nil {
			rs.Tags = make(map[string]string)
		}
		rs.Tags[k] = v
	}

	switch rs.Duration {
	case -1:
		// -1 indicates an unfinished Span. For a recording it's better to put some
		// duration in it, otherwise tools get confused. For example, we export
		// recordings to Jaeger, and spans with a zero duration don't look nice.
		rs.Duration = timeutil.Now().Sub(rs.StartTime)
		addTag("unfinished", "")
	}

	if s.mu.stats != nil {
		stats, err := types.MarshalAny(s.mu.stats)
		if err != nil {
			panic(err)
		}
		rs.Stats = stats
	}

	if len(s.mu.Baggage) > 0 {
		rs.Baggage = make(map[string]string)
		for k, v := range s.mu.Baggage {
			rs.Baggage[k] = v
		}
	}
	if s.logTags != nil {
		tags := s.logTags.Get()
		for i := range tags {
			tag := &tags[i]
			addTag(tagName(tag.Key()), tag.ValueStr())
		}
	}
	if len(s.mu.tags) > 0 {
		for k, v := range s.mu.tags {
			// We encode the tag values as strings.
			addTag(k, fmt.Sprint(v))
		}
	}
	rs.Logs = make([]tracingpb.LogRecord, len(s.mu.recording.recordedLogs))
	for i, r := range s.mu.recording.recordedLogs {
		rs.Logs[i].Time = r.Timestamp
		rs.Logs[i].Fields = make([]tracingpb.LogRecord_Field, len(r.Fields))
		for j, f := range r.Fields {
			rs.Logs[i].Fields[j] = tracingpb.LogRecord_Field{
				Key:   f.Key(),
				Value: fmt.Sprint(f.Value()),
			}
		}
	}

	return rs
}

func (s *crdbSpan) addChild(child *crdbSpan) {
	s.mu.Lock()
	s.mu.recording.children = append(s.mu.recording.children, child)
	s.mu.Unlock()
}
