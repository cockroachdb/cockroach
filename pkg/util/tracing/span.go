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
	"strings"
	"sync/atomic"
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
	// Span itself is a very thin wrapper around spanInner whose only job is
	// to guard spanInner against use-after-Finish.
	i               spanInner
	numFinishCalled int32 // atomic
}

func (sp *Span) done() bool {
	if sp == nil {
		return true
	}
	return atomic.LoadInt32(&sp.numFinishCalled) != 0
}

// Tracer exports the tracer this span was created using.
func (sp *Span) Tracer() *Tracer {
	return sp.i.Tracer()
}

// SetOperationName sets the name of the operation.
func (sp *Span) SetOperationName(operationName string) {
	if sp.done() {
		return
	}
	sp.i.SetOperationName(operationName)
}

// Finish idempotently marks the Span as completed (at which point it will
// silently drop any new data added to it). Finishing a nil *Span is a noop.
func (sp *Span) Finish() {
	if sp == nil || atomic.AddInt32(&sp.numFinishCalled, 1) != 1 {
		return
	}
	sp.i.Finish()
}

// GetRecording retrieves the current recording, if the Span has recording
// enabled. This can be called while spans that are part of the recording are
// still open; it can run concurrently with operations on those spans.
func (sp *Span) GetRecording() Recording {
	// It's always valid to get the recording, even for a finished span.
	return sp.i.GetRecording()
}

// ImportRemoteSpans adds RecordedSpan data to the recording of the given Span;
// these spans will be part of the result of GetRecording. Used to import
// recorded traces from other nodes.
func (sp *Span) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) error {
	if sp.done() {
		return nil
	}
	return sp.i.ImportRemoteSpans(remoteSpans)
}

// Meta returns the information which needs to be propagated across process
// boundaries in order to derive child spans from this Span. This may return
// nil, which is a valid input to `WithParentAndManualCollection`, if the Span
// has been optimized out.
func (sp *Span) Meta() *SpanMeta {
	// It shouldn't be done in practice, but it is allowed to call Meta on
	// a finished span.
	return sp.i.Meta()
}

// SetVerbose toggles verbose recording on the Span, which must not be a noop
// span (see the WithForceRealSpan option).
//
// With 'true', future calls to Record are actually recorded, and any future
// descendants of this Span will do so automatically as well. This does not
// apply to past derived Spans, which may in fact be noop spans.
//
// When set to 'false', Record will cede to add data to the recording (though
// they may still be collected, should the Span have been set up with an
// auxiliary trace sink). This does not apply to Spans derived from this one
// when it was verbose.
func (sp *Span) SetVerbose(to bool) {
	// We allow toggling verbosity on and off for a finished span. This shouldn't
	// matter either way as a finished span drops all new data, but if we
	// prevented the toggling we could end up in weird states since IsVerbose()
	// won't reflect what the caller asked for.
	sp.i.SetVerbose(to)
}

// ResetRecording clears any previously recorded information. This doesn't
// affect any auxiliary trace sinks such as net/trace or zipkin.
func (sp *Span) ResetRecording() {
	sp.i.ResetRecording()
}

// IsVerbose returns true if the Span is verbose. See SetVerbose for details.
func (sp *Span) IsVerbose() bool {
	return sp.i.IsVerbose()
}

// Record provides a way to record free-form text into verbose spans.
//
// TODO(irfansharif): We don't currently have redactability with trace
// recordings (both here, and using RecordStructured above). We'll want to do this
// soon.
func (sp *Span) Record(msg string) {
	if sp.done() {
		return
	}
	sp.i.Record(msg)
}

// Recordf is like Record, but accepts a format specifier.
func (sp *Span) Recordf(format string, args ...interface{}) {
	if sp.done() {
		return
	}
	sp.i.Recordf(format, args...)
}

// RecordStructured adds a Structured payload to the Span. It will be added to
// the recording even if the Span is not verbose; however it will be discarded
// if the underlying Span has been optimized out (i.e. is a noop span).
//
// The caller must not mutate the item once RecordStructured has been called.
func (sp *Span) RecordStructured(item Structured) {
	if sp.done() {
		return
	}
	sp.i.RecordStructured(item)
}

// SetSpanStats sets the stats on a Span. stats.Stats() will also be added to
// the Span tags.
//
// This is deprecated. Use RecordStructured instead.
//
// TODO(tbg): remove this in the 21.2 cycle.
func (sp *Span) SetSpanStats(stats SpanStats) {
	if sp.done() {
		return
	}
	sp.i.SetSpanStats(stats)
}

// SetTag adds a tag to the span. If there is a pre-existing tag set for the
// key, it is overwritten.
func (sp *Span) SetTag(key string, value interface{}) {
	if sp.done() {
		return
	}
	sp.i.SetTag(key, value)
}

// SetBaggageItem attaches "baggage" to this span, a key:value pair that's
// propagated to all future descendants of this Span. Any attached baggage
// crosses RPC boundaries, and is copied transitively for every remote
// descendant.
func (sp *Span) SetBaggageItem(restrictedKey, value string) {
	if sp.done() {
		return
	}
	sp.i.SetBaggageItem(restrictedKey, value)
}

// TODO(tbg): move spanInner and its methods into a separate file.

type spanInner struct {
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

func (s *spanInner) isNoop() bool {
	return s.crdb == nil && s.netTr == nil && s.ot == (otSpan{})
}

func (s *spanInner) IsVerbose() bool {
	return s.crdb.recordingType() == RecordingVerbose
}

func (s *spanInner) SetVerbose(to bool) {
	// TODO(tbg): when always-on tracing is firmly established, we can remove the ugly
	// caveat that SetVerbose(true) is a panic on a noop span because there will be no
	// noop span.
	if s.isNoop() {
		panic(errors.AssertionFailedf("SetVerbose called on NoopSpan; use the WithForceRealSpan option for StartSpan"))
	}
	if to {
		s.crdb.enableRecording(nil /* parent */, RecordingVerbose)
	} else {
		s.crdb.disableRecording()
	}
}

func (s *spanInner) ResetRecording() {
	s.crdb.resetRecording()
}

func (s *spanInner) GetRecording() Recording {
	return s.crdb.getRecording(s.tracer.TracingVerbosityIndependentSemanticsIsActive())
}

func (s *spanInner) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) error {
	return s.crdb.importRemoteSpans(remoteSpans)
}

// SpanStats are stats that can be added to a Span.
type SpanStats interface {
	protoutil.Message
	// StatsTags returns the stats that the object represents as a map of
	// key/value tags that will be added to the Span tags. The tag keys should
	// start with TagPrefix.
	StatsTags() map[string]string
}

func (s *spanInner) SetSpanStats(stats SpanStats) {
	if s.isNoop() {
		return
	}
	s.RecordStructured(stats)
	s.crdb.mu.Lock()
	s.crdb.mu.stats = stats
	for name, value := range stats.StatsTags() {
		s.setTagInner(name, value, true /* locked */)
	}
	s.crdb.mu.Unlock()
}

func (s *spanInner) Finish() {
	if s == nil {
		return
	}
	if s.isNoop() {
		return
	}
	finishTime := time.Now()

	s.crdb.mu.Lock()
	if alreadyFinished := s.crdb.mu.duration >= 0; alreadyFinished {
		s.crdb.mu.Unlock()

		// External spans and net/trace are not always forgiving about spans getting
		// finished twice, but it may happen so let's be resilient to it.
		return
	}
	s.crdb.mu.duration = finishTime.Sub(s.crdb.startTime)
	if s.crdb.mu.duration == 0 {
		s.crdb.mu.duration = time.Nanosecond
	}
	s.crdb.mu.Unlock()

	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.Finish()
	}
	if s.netTr != nil {
		s.netTr.Finish()
	}
	s.tracer.activeSpans.Lock()
	delete(s.tracer.activeSpans.m, s.crdb.spanID)
	s.tracer.activeSpans.Unlock()
}

func (s *spanInner) Meta() *SpanMeta {
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

func (s *spanInner) SetOperationName(operationName string) *spanInner {
	if s.isNoop() {
		return s
	}
	if s.ot.shadowSpan != nil {
		s.ot.shadowSpan.SetOperationName(operationName)
	}
	s.crdb.operation = operationName
	return s
}

func (s *spanInner) SetTag(key string, value interface{}) *spanInner {
	if s.isNoop() {
		return s
	}
	return s.setTagInner(key, value, false /* locked */)
}

func (s *spanInner) setTagInner(key string, value interface{}, locked bool) *spanInner {
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
// `Span.RecordStructured`. This is the only kind of data a Span carries when
// `trace.mode = background`.
type Structured interface {
	protoutil.Message
}

func (s *spanInner) RecordStructured(item Structured) {
	if s.isNoop() {
		return
	}
	s.crdb.recordStructured(item)
	if s.hasVerboseSink() {
		// NB: TrimSpace avoids the trailing whitespace generated by the
		// protobuf stringers.
		s.Record(strings.TrimSpace(item.String()))
	}
}

func (s *spanInner) Record(msg string) {
	s.Recordf("%s", msg)
}

func (s *spanInner) Recordf(format string, args ...interface{}) {
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
func (s *spanInner) hasVerboseSink() bool {
	if s.netTr == nil && s.ot == (otSpan{}) && !s.IsVerbose() {
		return false
	}
	return true
}

func (s *spanInner) SetBaggageItem(restrictedKey, value string) *spanInner {
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
func (s *spanInner) Tracer() *Tracer {
	return s.tracer
}
