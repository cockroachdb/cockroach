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

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	// TagPrefix is prefixed to all tags that should be output in SHOW TRACE.
	TagPrefix = "cockroach."
)

// Span is the tracing Span that we use in CockroachDB. Depending on the tracing
// configuration, it can hold anywhere between zero and three destinations for
// trace information:
//
// 1. external OpenTelemetry-compatible trace collector (Jaeger, Zipkin, Lightstep),
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

// IsNoop returns true if this span is a black hole - it doesn't correspond to a
// CRDB span and it doesn't output either to an OpenTelemetry tracer, or to
// net.Trace.
func (sp *Span) IsNoop() bool {
	return sp.i.isNoop()
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
	if sp == nil || sp.IsNoop() || atomic.AddInt32(&sp.numFinishCalled, 1) != 1 {
		return
	}
	sp.i.Finish()
}

// GetRecording retrieves the current recording, if the Span has recording
// enabled. This can be called while spans that are part of the recording are
// still open; it can run concurrently with operations on those spans.
//
// As a performance optimization, GetRecording does not return tags when the
// underlying Span is not verbose. Returning tags requires expensive
// stringification.
//
// A few internal tags are added to denote span properties:
//
//    "_unfinished"	The span was never Finish()ed
//    "_verbose"	The span is a verbose one
//    "_dropped"	The span dropped recordings due to sizing constraints
func (sp *Span) GetRecording() Recording {
	// It's always valid to get the recording, even for a finished span.
	return sp.i.GetRecording()
}

// ImportRemoteSpans adds RecordedSpan data to the recording of the given Span;
// these spans will be part of the result of GetRecording. Used to import
// recorded traces from other nodes.
func (sp *Span) ImportRemoteSpans(remoteSpans []tracingpb.RecordedSpan) {
	if !sp.done() {
		sp.i.ImportRemoteSpans(remoteSpans)
	}
}

// Meta returns the information which needs to be propagated across process
// boundaries in order to derive child spans from this Span. This may return
// nil, which is a valid input to `WithParentAndManualCollection`, if the Span
// has been optimized out.
func (sp *Span) Meta() SpanMeta {
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

// SetVerboseRecursively is like SetVerbose, except it does so for all
// descendant spans as well.
func (sp *Span) SetVerboseRecursively(to bool) {
	sp.i.SetVerboseRecursively(to)
}

// ResetRecording clears any previously recorded information. This doesn't
// affect any auxiliary trace sinks such as net/trace or zipkin.
//
// TODO(irfansharif): Remove this, it's no longer used.
func (sp *Span) ResetRecording() {
	sp.i.ResetRecording()
}

// IsVerbose returns true if the Span is verbose. See SetVerbose for details.
func (sp *Span) IsVerbose() bool {
	return sp.i.IsVerbose()
}

// Record provides a way to record free-form text into verbose spans. Recordings
// may be dropped due to sizing constraints.
//
// TODO(tbg): make sure `msg` is lint-forced to be const.
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
// if the underlying Span has been optimized out (i.e. is a noop span). Payloads
// may also be dropped due to sizing constraints.
//
// The caller must not mutate the item once RecordStructured has been called.
func (sp *Span) RecordStructured(item Structured) {
	if sp.done() {
		return
	}
	sp.i.RecordStructured(item)
}

// SetTag adds a tag to the span. If there is a pre-existing tag set for the
// key, it is overwritten.
func (sp *Span) SetTag(key string, value attribute.Value) {
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

// TraceID retrieves a span's trace ID.
func (sp *Span) TraceID() uint64 {
	return sp.i.TraceID()
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

	// otelCtx is the OpenTelemetry span context. This is only populated when the
	// remote Span is reporting to an external OpenTelemetry tracer. Setting this
	// will cause child spans to also get an OpenTelemetry span.
	otelCtx oteltrace.SpanContext

	// If set, all spans derived from this context are being recorded.
	//
	// NB: at the time of writing, this is only ever set to RecordingVerbose
	// and only if Baggage[verboseTracingBaggageKey] is set.
	recordingType RecordingType

	// The Span's associated baggage.
	Baggage map[string]string
}

// Empty returns whether or not the SpanMeta is a zero value.
func (sm SpanMeta) Empty() bool {
	return sm.spanID == 0 && sm.traceID == 0
}

func (sm SpanMeta) String() string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("[spanID: %d, traceID: %d", sm.spanID, sm.traceID))
	hasOtelSpan := sm.otelCtx.IsValid()
	if hasOtelSpan {
		s.WriteString(" hasOtel")
		s.WriteString(fmt.Sprintf(" trace: %d span: %d", sm.otelCtx.TraceID(), sm.otelCtx.SpanID()))
	}
	s.WriteRune(']')
	return s.String()
}

// Structured is an opaque protobuf that can be attached to a trace via
// `Span.RecordStructured`.
type Structured interface {
	protoutil.Message
}
