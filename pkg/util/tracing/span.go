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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"golang.org/x/net/trace"
)

const (
	// TagPrefix is prefixed to all tags that should be output in SHOW TRACE.
	TagPrefix = "cockroach."
)

// Span is the tracing Span that we use in CockroachDB. Depending on the tracing
// configuration, it can hold anywhere between zero and three destinations for
// trace information:
//
// 1. external OpenTracing-compatible trace collector (Jaeger, Zipkin, Lightstep),
// 2. /debug/requests endpoint (net/trace package); mostly useful for local debugging
// 3. CRDB-internal trace span (powers SQL session tracing).
//
// When there is no need to allocate either of these three destinations,
// a "noop span", i.e. an immutable *Span wrapping the *Tracer, may be
// returned, to allow starting additional nontrivial Spans from the return
// value later, when direct access to the tracer may no longer be available.
//
// The CockroachDB-internal Span (crdbSpan) is more complex because
// rather than reporting to some external sink, the caller's "owner"
// must propagate the trace data back across process boundaries towards
// the root of the trace span tree; see WithParentAndAutoCollection
// and WithParentAndManualCollection, respectively.
//
// Additionally, the internal span type also supports turning on, stopping,
// and restarting its data collection (see Span.StartRecording), and this is
// used extensively in SQL session tracing.
type Span struct {
	tracer *Tracer // never nil

	// Internal trace Span; nil if not tracing to crdb.
	// When not-nil, allocated together with the surrounding Span for
	// performance.
	crdb *crdbSpan
	// x/net/trace.Trace instance; nil if not tracing to x/net/trace.
	netTr trace.Trace
	// External opentracing compatible tracer such as lightstep, zipkin, jaeger;
	// zero if not using one.
	ot otSpan
}

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
	// NB: at the time of writing, this is only ever set to RecordingVerbose
	// and only if Baggage[verboseTracingBaggageKey] is set.
	recordingType RecordingType

	// The Span's associated baggage.
	Baggage map[string]string
}

func (sm *SpanMeta) String() string {
	return fmt.Sprintf("[spanID: %d, traceID: %d]", sm.spanID, sm.traceID)
}

func (s *Span) isNoop() bool {
	return s.crdb == nil && s.netTr == nil && s.ot == (otSpan{})
}

// IsVerbose returns true if the Span is verbose. See SetVerbose for details.
func (s *Span) IsVerbose() bool {
	return s.crdb.recordingType() == RecordingVerbose
}

// SetVerbose toggles verbose recording on the Span, which must not be a noop
// span (see the WithForceRealSpan option).
//
// With 'true', future calls to Record are actually recorded, and any future
// descendants of this Span will do so automatically as well. This does not
// apply to past derived Spans, which may in fact be noop spans.
//
// As a side effect, calls to `SetVerbose(true)` on a span that was not already
// verbose will reset any past recording stored on this Span.
//
// When set to 'false', Record will cede to add data to the recording (though
// they may still be collected, should the Span have been set up with an
// auxiliary trace sink). This does not apply to Spans derived from this one
// when it was verbose.
func (s *Span) SetVerbose(to bool) {
	// TODO(tbg): when always-on tracing is firmly established, we can remove the ugly
	// caveat that SetVerbose(true) is a panic on a noop span because there will be no
	// noop span.
	if s.isNoop() {
		panic(errors.AssertionFailedf("SetVerbose called on NoopSpan; use the WithForceRealSpan option for StartSpan"))
	}
	if to {
		// If we're already recording (perhaps because the parent was recording when
		// this Span was created), there's nothing to do. Avoid the call to enableRecording
		// because it would clear the existing recording.
		recType := RecordingVerbose
		if recType != s.crdb.recordingType() {
			s.crdb.enableRecording(nil /* parent */, recType)
		}
	} else {
		s.crdb.disableRecording()
	}
}

// GetRecording retrieves the current recording, if the Span has recording
// enabled. This can be called while spans that are part of the recording are
// still open; it can run concurrently with operations on those spans.
func (s *Span) GetRecording() Recording {
	return s.crdb.getRecording(s.tracer.mode())
}

// ImportRemoteSpans adds RecordedSpan data to the recording of the given Span;
// these spans will be part of the result of GetRecording. Used to import
// recorded traces from other nodes.
func (s *Span) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) error {
	if s.tracer.mode() == modeLegacy && s.crdb.recordingType() == RecordingOff {
		return nil
	}
	return s.crdb.importRemoteSpans(remoteSpans)
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
	return s.crdb.recordingType() == RecordingOff && s.netTr == nil && s.ot == (otSpan{})
}

// isNilOrNoop returns true if the Span context is either nil
// or corresponds to a "no-op" Span. If this is true, any Span
// derived from this context will be a "black hole Span".
func (sm *SpanMeta) isNilOrNoop() bool {
	return sm == nil || (sm.recordingType == RecordingOff && sm.shadowTracerType == "")
}

// SpanStats are stats that can be added to a Span.
type SpanStats interface {
	protoutil.Message
	// StatsTags returns the stats that the object represents as a map of
	// key/value tags that will be added to the Span tags. The tag keys should
	// start with TagPrefix.
	StatsTags() map[string]string
}

// SetSpanStats sets the stats on a Span. stats.Stats() will also be added to
// the Span tags.
//
// This is deprecated. Use LogStructured instead.
//
// TODO(tbg): remove this in the 21.2 cycle.
func (s *Span) SetSpanStats(stats SpanStats) {
	if s.isNoop() {
		return
	}
	s.LogStructured(stats)
	s.crdb.mu.Lock()
	s.crdb.mu.stats = stats
	for name, value := range stats.StatsTags() {
		s.setTagInner(name, value, true /* locked */)
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
	s.tracer.activeSpans.Lock()
	delete(s.tracer.activeSpans.m, s)
	s.tracer.activeSpans.Unlock()
}

// Meta returns the information which needs to be propagated across process
// boundaries in order to derive child spans from this Span. This may return
// nil, which is a valid input to `WithParentAndManualCollection`, if the Span
// has been optimized out.
func (s *Span) Meta() *SpanMeta {
	var traceID uint64
	var spanID uint64
	var recordingType RecordingType
	var baggage map[string]string

	if s.crdb != nil {
		traceID, spanID = s.crdb.traceID, s.crdb.spanID
		s.crdb.mu.Lock()
		defer s.crdb.mu.Unlock()
		n := len(s.crdb.mu.baggage)
		// In the common case, we have no baggage, so avoid making an empty map.
		if n > 0 {
			baggage = make(map[string]string, n)
		}
		for k, v := range s.crdb.mu.baggage {
			baggage[k] = v
		}
		recordingType = s.crdb.mu.recording.recordingType.load()
	}

	var shadowTrTyp string
	var shadowCtx opentracing.SpanContext
	if s.ot.shadowSpan != nil {
		shadowTrTyp, _ = s.ot.shadowTr.Type()
		shadowCtx = s.ot.shadowSpan.Context()
	}

	if traceID == 0 &&
		spanID == 0 &&
		shadowTrTyp == "" &&
		shadowCtx == nil &&
		recordingType == 0 &&
		baggage == nil {
		return nil
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

// SetOperationName sets the name of the operation.
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

// SetTag adds a tag to the span. If there is a pre-existing tag set for the
// key, it is overwritten.
func (s *Span) SetTag(key string, value interface{}) *Span {
	if s.isNoop() {
		return s
	}
	return s.setTagInner(key, value, false /* locked */)
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

// Structured is an opaque protobuf that can be attached to a trace via
// `Span.LogStructured`. This is the only kind of data a Span carries when
// `trace.mode = background`.
type Structured interface {
	protoutil.Message
}

// LogStructured adds a Structured payload to the Span. It will be added to the
// recording even if the Span is not verbose; however it will be discarded if
// the underlying Span has been optimized out (i.e. is a noop span).
//
// The caller must not mutate the item once LogStructured has been called.
func (s *Span) LogStructured(item Structured) {
	if s.isNoop() {
		return
	}
	s.crdb.logStructured(item)
}

// Record provides a way to record free-form text into verbose spans.
//
// TODO(irfansharif): We don't currently have redactability with trace
// recordings (both here, and using LogStructured above). We'll want to do this
// soon.
func (s *Span) Record(msg string) {
	if !s.hasVerboseSink() {
		return
	}
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.LogFields(otlog.String(tracingpb.LogMessageField, msg))
	}
	if s.netTr != nil {
		s.netTr.LazyPrintf("%s", msg)
	}
	s.crdb.record(msg)
}

// Recordf is like Record, but accepts a format specifier.
func (s *Span) Recordf(format string, args ...interface{}) {
	if !s.hasVerboseSink() {
		return
	}
	str := fmt.Sprintf(format, args...)
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.LogFields(otlog.String(tracingpb.LogMessageField, str))
	}
	if s.netTr != nil {
		s.netTr.LazyPrintf(format, args)
	}
	s.crdb.record(str)
}

// hasVerboseSink returns false if there is no reason to even evaluate Record
// because the result wouldn't be used for anything.
func (s *Span) hasVerboseSink() bool {
	if s.netTr == nil && s.ot == (otSpan{}) && !s.IsVerbose() {
		return false
	}
	return true
}

// SetBaggageItem attaches "baggage" to this span, a key:value pair that's
// propagated to all future descendants of this Span. Any attached baggage
// crosses RPC boundaries, and is copied transitively for every remote
// descendant.
func (s *Span) SetBaggageItem(restrictedKey, value string) *Span {
	if s.isNoop() {
		return s
	}
	s.crdb.setBaggageItemAndTag(restrictedKey, value)
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.SetBaggageItem(restrictedKey, value)
		s.ot.shadowSpan.SetTag(restrictedKey, value)
	}
	// NB: nothing to do for net/trace.

	return s
}

// Tracer exports the tracer this span was created using.
func (s *Span) Tracer() *Tracer {
	return s.tracer
}
