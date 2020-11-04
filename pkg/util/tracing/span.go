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
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	proto "github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"golang.org/x/net/trace"
)

// SpanMeta is information about a Span that is not local to this
// process. Typically, SpanMeta is populated from information
// about a Span on the other end of an RPC, and is used to derive
// a child span via `Tracer.StartSpan`. For local spans, SpanMeta
// is not used, as the *Span directly can be derived from.
//
// SpanMeta contains the trace and span identifiers of the parent,
// along with additional metadata. In particular, this specifies
// whether the child should be recording, in which case the contract
// is that the recording is to be returned to the caller when the
// child finishes, so that the caller can inductively construct the
// entire trace.
type SpanMeta struct {
	traceID uint64
	spanID  uint64

	// Underlying shadow tracer info and span context (optional). This
	// will only be populated when the remote Span is reporting to an
	// external opentracing tracer. We hold on to the type of tracer to
	// avoid mixing spans when the tracer is reconfigured, as impls are
	// not typically robust to being shown spans they did not create.
	shadowTracerType string
	shadowCtx        opentracing.SpanContext

	// If set, all spans derived from this context are being recorded.
	//
	// NB: at the time of writing, this is only ever set to SnowballTracing
	// and only if Baggage[Snowball] is set.
	recordingType RecordingType

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

type crdbSpanMu struct {
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

type crdbSpan struct {
	// The traceID, probabilistically unique.
	traceID uint64
	// The spanID, probabilistically unique.
	spanID uint64

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

	mu crdbSpanMu
}

func (s *crdbSpan) isRecording() bool {
	return s != nil && atomic.LoadInt32(&s.recording) != 0
}

// otSpan is a span for an external opentracing compatible tracer
// such as lightstep, zipkin, jaeger, etc.
type otSpan struct {
	// shadowTr is the shadowTracer this span was created from. We need
	// to hold on to it separately because shadowSpan.Tracer() returns
	// the wrapper tracer and we lose the ability to find out
	// what tracer it is. This is important when deriving children from
	// this span, as we want to avoid mixing different tracers, which
	// would otherwise be the result of cluster settings changed.
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

	// Internal trace Span; nil if not tracing to crdb.
	// When not-nil, allocated together with the surrounding Span for
	// performance.
	crdb *crdbSpan
	// x/net/trace.Trace instance; nil if not tracing to x/net/trace.
	netTr trace.Trace
	// Shadow tracer and Span; zero if not using a shadow tracer.
	ot otSpan
}

func (s *Span) isBlackHole() bool {
	return !s.crdb.isRecording() && s.netTr == nil && s.ot == (otSpan{})
}

func (s *Span) isNoop() bool {
	return s.crdb == nil && s.netTr == nil && s.ot == (otSpan{})
}

// IsRecording returns true if the Span is recording its events.
func (s *Span) IsRecording() bool {
	return s.crdb.isRecording()
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
// created, use the WithForceRealSpan option to StartSpan.
//
// If recording was already started on this Span (either directly or because a
// parent Span is recording), the old recording is lost.
//
// Children spans created from the Span while it is *not* recording will not
// necessarily be recordable.
func (s *Span) StartRecording(recType RecordingType) {
	if recType == NoRecording {
		panic("StartRecording called with NoRecording")
	}
	if s.isNoop() {
		panic("StartRecording called on NoopSpan; use the WithForceRealSpan option for StartSpan")
	}

	// If we're already recording (perhaps because the parent was recording when
	// this Span was created), there's nothing to do.
	if !s.crdb.isRecording() {
		s.crdb.enableRecording(nil /* parent */, recType, false /* separateRecording */)
	}
}

// StopRecording disables recording on this Span. Child spans that were created
// since recording was started will continue to record until they finish.
//
// Calling this after StartRecording is not required; the recording will go away
// when all the spans finish.
//
// StopRecording() can be called on a Finish()ed Span.
func (s *Span) StopRecording() {
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

// GetRecording retrieves the current recording, if the Span has recording
// enabled. This can be called while spans that are part of the recording are
// still open; it can run concurrently with operations on those spans.
func (s *Span) GetRecording() Recording {
	return s.crdb.getRecording()
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

func (s *crdbSpan) getRecordingType() RecordingType {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.recording.recordingType
}

// ImportRemoteSpans adds RecordedSpan data to the recording of the given Span;
// these spans will be part of the result of GetRecording. Used to import
// recorded traces from other nodes.
func (s *Span) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) error {
	return s.crdb.ImportRemoteSpans(remoteSpans)
}

func (s *crdbSpan) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) error {
	if !s.isRecording() {
		return errors.AssertionFailedf("adding Raw Spans to a Span that isn't recording")
	}
	// Change the root of the remote recording to be a child of this Span. This is
	// usually already the case, except with DistSQL traces where remote
	// processors run in spans that FollowFrom an RPC Span that we don't collect.
	remoteSpans[0].ParentSpanID = s.spanID

	s.mu.Lock()
	s.mu.recording.remoteSpans = append(s.mu.recording.remoteSpans, remoteSpans...)
	s.mu.Unlock()
	return nil
}

// IsBlackHole returns true if events for this Span are just dropped. This
// is the case when the Span is not recording and no external tracer is configured.
// Tracing clients can use this method to figure out if they can short-circuit some
// tracing-related work that would be discarded anyway.
//
// The child of a blackhole Span is a non-recordable blackhole Span[*]. These incur
// only minimal overhead. It is therefore not worth it to call this method to avoid
// starting spans.
func (s *Span) IsBlackHole() bool {
	return s.isBlackHole()
}

// isNilOrNoop returns true if the Span context is either nil
// or corresponds to a "no-op" Span. If this is true, any Span
// derived from this context will be a "black hole Span".
func (sc *SpanMeta) isNilOrNoop() bool {
	return sc.recordingType == NoRecording && sc.shadowTracerType == ""
}

// SetSpanStats sets the stats on a Span. stats.Stats() will also be added to
// the Span tags.
func (s *Span) SetSpanStats(stats SpanStats) {
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

// Finish marks the Span as completed. Finishing a nil *Span is a noop.
func (s *Span) Finish() {
	if s == nil {
		return
	}
	if s.isNoop() {
		return
	}
	finishTime := time.Now()
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

// Meta returns the information which needs to be propagated across
// process boundaries in order to derive child spans from this Span.
// This may return nil, which is a valid input to `WithRemoteParent`,
// if the Span has been optimized out.
func (s *Span) Meta() *SpanMeta {
	var traceID uint64
	var spanID uint64
	var recordingType RecordingType
	var baggage map[string]string

	if s.crdb != nil {
		traceID, spanID = s.crdb.traceID, s.crdb.spanID
		s.crdb.mu.Lock()
		defer s.crdb.mu.Unlock()
		n := len(s.crdb.mu.Baggage)
		// In the common case, we have no baggage, so avoid making an empty map.
		if n > 0 {
			baggage = make(map[string]string, n)
		}
		for k, v := range s.crdb.mu.Baggage {
			baggage[k] = v
		}
		if s.crdb.isRecording() {
			recordingType = s.crdb.mu.recording.recordingType
		}
	}

	var shadowTrTyp string
	var shadowCtx opentracing.SpanContext
	if s.ot.shadowSpan != nil {
		shadowTrTyp, _ = s.ot.shadowTr.Type()
		shadowCtx = s.ot.shadowSpan.Context()
	}

	return &SpanMeta{
		traceID:          traceID,
		spanID:           spanID,
		shadowTracerType: shadowTrTyp,
		shadowCtx:        shadowCtx,
		recordingType:    recordingType,
		Baggage:          baggage,
	}
}

// SetOperationName is part of the opentracing.Span interface.
func (s *Span) SetOperationName(operationName string) *Span {
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
func (s *Span) SetTag(key string, value interface{}) *Span {
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

func (s *Span) setTagInner(key string, value interface{}, locked bool) *Span {
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
func (s *Span) SetBaggageItem(restrictedKey, value string) *Span {
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

// Tracer is part of the opentracing.Span interface.
func (s *Span) Tracer() *Tracer {
	return s.tracer
}

// getRecordingLocked returns the Span's recording. This does not include
// children.
func (s *crdbSpan) getRecordingLocked() tracingpb.RecordedSpan {
	rs := tracingpb.RecordedSpan{
		TraceID:      s.traceID,
		SpanID:       s.spanID,
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
		setLogTags(s.logTags.Get(), func(remappedKey string, tag *logtags.Tag) {
			addTag(remappedKey, tag.ValueStr())
		})
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
