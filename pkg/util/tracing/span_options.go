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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// spanReferenceType describes the relationship between a parent and a child
// span. This is an OpenTracing concept that we've maintained back from when
// CRDB was exporting traces to OpenTracing collectors. We no longer use
// OpenTracing, and OpenTelemetry doesn't (yet?) have a perfectly analogous
// concept.
//
// TODO(andrei): Follow
// https://github.com/open-telemetry/opentelemetry-specification/issues/65 and
// see how the OpenTelemetry concepts evolve.
type spanReferenceType int

const (
	// childOfRef means that the parent span will wait for the child span's
	// termination.
	childOfRef spanReferenceType = iota
	// followsFromRef means the child operation will run asynchronously with
	// respect to the parent span.
	followsFromRef
)

var followsFromAttribute = []attribute.KeyValue{attribute.String("follows-from", "")}

// spanOptions are the options to `Tracer.StartSpan`. This struct is
// typically not used directly. Instead, the methods mentioned on each
// field comment below are invoked as arguments to `Tracer.StartSpan`.
// See the SpanOption interface for a synopsis.
type spanOptions struct {
	// Parent, if set, indicates the parent span of the span being created with
	// these spanOptions.
	// If parent is set, its refCnt is assumed to have been incremented with the
	// reference held here (see WithParent()). Thus, this `spanOptions` cannot be
	// discarded; it must be used exactly once. StartSpan() will decrement refCnt.
	Parent spanRef // see WithParent
	// ParentDoesNotCollectRecording is set by WithDetachedRecording. It means
	// that, although this span has a parent, the parent should generally not
	// include this child's recording when the parent is asked for its own
	// recording. Usually, a parent span includes all its children in its
	// recording. However, sometimes that's not desired; sometimes the creator of
	// a child span has a different plan for how the recording of that child will
	// end up being collected and reported to where it ultimately needs to go.
	// Still, even in these cases, a parent-child relationship is still useful
	// (for example for the purposes of the active spans registry), so the child
	// span cannot simply be created as a root.
	//
	// For example, in the case of DistSQL, each processor in a flow has its own
	// span, as a child of the flow. The DistSQL infrastructure organizes the
	// collection of each processor span recording independently, without relying
	// on collecting the recording of the flow's span.
	ParentDoesNotCollectRecording bool
	RemoteParent                  SpanMeta               // see WithRemoteParentFromSpanMeta
	RefType                       spanReferenceType      // see WithFollowsFrom
	LogTags                       *logtags.Buffer        // see WithLogTags
	Tags                          map[string]interface{} // see WithTags
	ForceRealSpan                 bool                   // see WithForceRealSpan
	SpanKind                      oteltrace.SpanKind     // see WithSpanKind
	Sterile                       bool                   // see WithSterile

	// recordingTypeExplicit is set if the WithRecording() option was used. In
	// that case, spanOptions.recordingType() returns recordingTypeOpt below. If
	// not set, recordingType() looks at the parent.
	recordingTypeExplicit bool
	recordingTypeOpt      RecordingType
}

func (opts *spanOptions) parentTraceID() tracingpb.TraceID {
	if !opts.Parent.empty() && !opts.Parent.IsNoop() {
		return opts.Parent.i.crdb.traceID
	} else if !opts.RemoteParent.Empty() {
		return opts.RemoteParent.traceID
	}
	return 0
}

func (opts *spanOptions) parentSpanID() tracingpb.SpanID {
	if !opts.Parent.empty() && !opts.Parent.IsNoop() {
		return opts.Parent.i.crdb.spanID
	} else if !opts.RemoteParent.Empty() {
		return opts.RemoteParent.spanID
	}
	return 0
}

func (opts *spanOptions) recordingType() RecordingType {
	if opts.recordingTypeExplicit {
		return opts.recordingTypeOpt
	}

	recordingType := RecordingOff
	if !opts.Parent.empty() && !opts.Parent.IsNoop() {
		recordingType = opts.Parent.i.crdb.recordingType()
	} else if !opts.RemoteParent.Empty() {
		recordingType = opts.RemoteParent.recordingType
	}
	return recordingType
}

// otelContext returns information about the OpenTelemetry parent span. If there
// is a local parent with an otel Span, that Span is returned. If there is a
// RemoteParent,  a SpanContext is returned. If there's no OpenTelemetry parent,
// both return values will be empty.
func (opts *spanOptions) otelContext() (oteltrace.Span, oteltrace.SpanContext) {
	if !opts.Parent.empty() && opts.Parent.i.otelSpan != nil {
		return opts.Parent.i.otelSpan, oteltrace.SpanContext{}
	}
	if !opts.RemoteParent.Empty() && opts.RemoteParent.otelCtx.IsValid() {
		return nil, opts.RemoteParent.otelCtx
	}
	return nil, oteltrace.SpanContext{}
}

// SpanOption is the interface satisfied by options to `Tracer.StartSpan`.
// A synopsis of the options follows. For details, see their comments.
//
// - WithParent: create a child Span with a local parent.
// - WithRemoteParentFromSpanMeta: create a child Span with a remote parent.
// - WithFollowsFrom: hint that child may outlive parent.
// - WithLogTags: populates the Span tags from a `logtags.Buffer`.
// - WithCtxLogTags: like WithLogTags, but takes a `context.Context`.
// - WithTags: adds tags to a Span on creation.
// - WithForceRealSpan: prevents optimizations that can avoid creating a real span.
// - WithDetachedRecording: don't include the recording in the parent.
type SpanOption interface {
	apply(spanOptions) spanOptions
}

type parentOption spanRef

// WithParent instructs StartSpan to create a child Span from a (local) parent
// Span.
//
// In case when the parent span is created with a different Tracer (generally,
// when the parent lives in a different process), WithRemoteParentFromSpanMeta should be
// used.
//
// WithParent will be a no-op (i.e. the span resulting from
// applying this option will be a root span, just as if this option hadn't been
// specified) in the following cases:
// - if `sp` is nil
// - if `sp` is a no-op span
// - if `sp` is a sterile span (i.e. a span explicitly marked as not wanting
//   children). Note that the singleton Tracer.noop span is marked as sterile,
//   which makes this condition mostly encompass the previous one, however in
//   theory there could be no-op spans other than the singleton one.
//
// The child inherits the parent's log tags. The data collected in the
// child trace will be retrieved automatically when the parent's data is
// retrieved, meaning that the caller has no obligation (and in fact
// must not) manually propagate the recording to the parent Span.
//
// The child will start recording if the parent is recording at the time
// of child instantiation. If the parent span is not recording, the child
// could be a "noop span" (depending on whether the Tracer is configured
// to trace to an external tracing system) which does not support
// recording, unless the WithForceRealSpan option is passed to StartSpan.
//
// By default, children are derived using a ChildOf relationship,
// which corresponds to the expectation that the parent span will
// wait for the child to Finish(). If this expectation does not hold,
// WithFollowsFrom should be added to the StartSpan invocation.
//
// WithParent increments sp's reference count. As such, the resulting option
// must be passed to StartSpan(opt). Once passed to StartSpan(opt), opt cannot
// be reused. The child span will be responsible for ultimately doing the
// decrement. The fact that WithParent takes a reference on sp is important to
// avoid the possibility of deadlocks when buggy code uses a Span after Finish()
// (which is illegal). See comments on Span.refCnt for details. By taking the
// reference, it becomes safe (from a deadlock perspective) to call
// WithParent(sp) concurrently with sp.Finish(). Note that calling
// WithParent(sp) after sp was re-allocated cannot result in deadlocks
// regardless of the reference counting.
func WithParent(sp *Span) SpanOption {
	if sp == nil {
		return (parentOption)(spanRef{})
	}

	// Panic if the parent has already been finished, if configured to do so. If
	// the parent has finished and we're configured not to panic, StartSpan() will
	// deal with it when passed this WithParent option.
	//
	// Note that this check is best-effort (like all done() checks). More checking
	// below.
	_ = sp.detectUseAfterFinish()

	// Sterile spans don't get children. Noop spans also don't, but that case is
	// handled in StartSpan, not here, because we want to assert that the parent's
	// Tracer is the same as the child's. In contrast, in the IsSterile() case, it
	// is allowed for the "child" to be created with a different Tracer than the
	// parent.
	if sp.IsSterile() {
		return (parentOption)(spanRef{})
	}

	ref, _ /* ok */ := tryMakeSpanRef(sp)
	// Note that ref will be empty if tryMakeSpanRef() failed. In that case, the
	// resulting span will not have a parent.
	return (parentOption)(ref)
}

func (p parentOption) apply(opts spanOptions) spanOptions {
	opts.Parent = (spanRef)(p)
	return opts
}

type remoteParent SpanMeta

// WithRemoteParentFromSpanMeta instructs StartSpan to create a child span
// descending from a parent described via a SpanMeta. Generally this parent span
// lives in a different process.
//
// For the purposes of trace recordings, there's no mechanism ensuring that the
// child's recording will be passed to the parent span. When that's desired, it
// has to be done manually by calling Span.GetRecording() and propagating the
// result to the parent by calling Span.ImportRemoteSpans().
//
// The canonical use case for this is around RPC boundaries, where a server
// handling a request wants to create a child span descending from a parent on a
// remote machine.
//
// node 1                     (network)          node 2
// --------------------------------------------------------------------------
// Span.Meta()               ----------> sp2 := Tracer.StartSpan(
//                                       		WithRemoteParentFromSpanMeta(.))
//                                       doSomething(sp2)
// Span.ImportRemoteSpans(.) <---------- sp2.FinishAndGetRecording()
//
// By default, the child span is derived using a ChildOf relationship, which
// corresponds to the expectation that the parent span will usually wait for the
// child to Finish(). If this expectation does not hold, WithFollowsFrom should
// be added to the StartSpan invocation.
//
// If you're in possession of a TraceInfo instead of a SpanMeta, prefer using
// WithRemoteParentFromTraceInfo instead. If the TraceInfo is heap-allocated,
// WithRemoteParentFromTraceInfo will not allocate (whereas
// WithRemoteParentFromSpanMeta allocates).
func WithRemoteParentFromSpanMeta(parent SpanMeta) SpanOption {
	if parent.Empty() || parent.sterile {
		return nil
	}
	return (remoteParent)(parent)
}

func (p remoteParent) apply(opts spanOptions) spanOptions {
	opts.RemoteParent = (SpanMeta)(p)
	return opts
}

type remoteParentFromTraceInfoOpt tracingpb.TraceInfo

var _ SpanOption = &remoteParentFromTraceInfoOpt{}

// WithRemoteParentFromTraceInfo is like WithRemoteParentFromSpanMeta, except the remote
// parent info is passed in as *TraceInfo. This is equivalent to
// WithRemoteParentFromSpanMeta(SpanMetaFromProto(ti)), but more efficient because it
// doesn't allocate.
func WithRemoteParentFromTraceInfo(ti *tracingpb.TraceInfo) SpanOption {
	return (*remoteParentFromTraceInfoOpt)(ti)
}

func (r *remoteParentFromTraceInfoOpt) apply(opts spanOptions) spanOptions {
	opts.RemoteParent = SpanMetaFromProto(*(*tracingpb.TraceInfo)(r))
	return opts
}

type detachedRecording struct{}

var detachedRecordingSingleton = SpanOption(detachedRecording{})

func (o detachedRecording) apply(opts spanOptions) spanOptions {
	opts.ParentDoesNotCollectRecording = true
	return opts
}

// WithDetachedRecording configures the span to not be included in the parent's
// recording (if any) under most circumstances. Usually, a parent span includes
// all its children in its recording. However, sometimes that's not desired;
// sometimes the creator of a child span has a different plan for how the
// recording of that child will end up being collected and reported to where it
// ultimately needs to go. Still, even in these cases, a parent-child
// relationship is still useful (for example for the purposes of the active
// spans registry), so the child span cannot simply be created as a root.
//
// For example, in the case of DistSQL, each processor in a flow has its own
// span, as a child of the flow. The DistSQL infrastructure organizes the
// collection of each processor span recording independently, without relying
// on collecting the recording of the flow's span.
//
// In the case when the parent's recording is collected through the span
// registry, this option is ignore since, in that case, we want as much info as
// possible.
func WithDetachedRecording() SpanOption {
	return detachedRecordingSingleton
}

type followsFromOpt struct{}

var followsFromSingleton = SpanOption(followsFromOpt{})

// WithFollowsFrom instructs StartSpan to link the child span to its parent
// using a different kind of relationship than the regular parent-child one,
// should a child span be created (i.e. should WithParent or
// WithRemoteParentFromSpanMeta be supplied as well). This relationship was
// called "follows-from" in the old OpenTracing API. This only matters if the
// trace is sent to an OpenTelemetry tracer; CRDB itself ignores it (what
// matters for CRDB is the WithDetachedTrace option).
// OpenTelemetry does not have a concept of a follows-from relationship at the
// moment; specifying this option results in the child having a Link to the
// parent.
// TODO(andrei): OpenTelemetry used to have a FollowsFrom relationship, but then
// it was removed for the topic to be reconsidered more deeply. Let's keep an
// eye on
// https://github.com/open-telemetry/opentelemetry-specification/issues/65 and
// see how the thinking evolves.
//
// A WithFollowsFrom child is expected to run asynchronously with respect to the
// parent span (for example: asynchronous cleanup work), whereas a "regular"
// child span is not (i.e. the parent span typically waits for the child to
// Finish()).
//
// There is no penalty for getting this wrong, but it can help external trace
// systems visualize the traces better.
func WithFollowsFrom() SpanOption {
	return followsFromSingleton
}

func (o followsFromOpt) apply(opts spanOptions) spanOptions {
	opts.RefType = followsFromRef
	return opts
}

type forceRealSpanOption struct{}

var forceRealSpanSingleton = SpanOption(forceRealSpanOption{})

// WithForceRealSpan forces StartSpan to create of a real Span regardless of the
// Tracer's tracing mode (instead of a low-overhead non-recordable noop span).
//
// When tracing is disabled all spans are noopSpans; these spans aren't
// capable of recording, so this option should be passed to StartSpan if the
// caller wants to be able to call SetVerbose(true) on the span later. If the
// span should be recording from the beginning, use WithRecording() instead.
func WithForceRealSpan() SpanOption {
	return forceRealSpanSingleton
}

func (forceRealSpanOption) apply(opts spanOptions) spanOptions {
	opts.ForceRealSpan = true
	return opts
}

type recordingSpanOption struct {
	recType RecordingType
}

var structuredRecordingSingleton = SpanOption(recordingSpanOption{recType: RecordingStructured})
var verboseRecordingSingleton = SpanOption(recordingSpanOption{recType: RecordingVerbose})

// WithRecording configures the span to record in the given mode.
//
// The recording mode can be changed later with SetVerbose().
func WithRecording(recType RecordingType) SpanOption {
	switch recType {
	case RecordingStructured:
		return structuredRecordingSingleton
	case RecordingVerbose:
		return verboseRecordingSingleton
	case RecordingOff:
		panic("invalid recording option: RecordingOff")
	default:
		recCpy := recType // copy excaping to the heap
		panic(fmt.Sprintf("invalid recording option: %d", recCpy))
	}
}

func (o recordingSpanOption) apply(opts spanOptions) spanOptions {
	opts.recordingTypeExplicit = true
	opts.recordingTypeOpt = o.recType
	return opts
}

// withSpanKindOption configures a span with a specific kind.
type withSpanKindOption struct {
	kind oteltrace.SpanKind
}

// WithSpanKind configures a span with an OpenTelemetry kind. This option only
// matters if OpenTelemetry tracing is enabled; the CRDB tracer ignores it
// otherwise.
func WithSpanKind(kind oteltrace.SpanKind) SpanOption {
	return withSpanKindOption{kind: kind}
}

func (w withSpanKindOption) apply(opts spanOptions) spanOptions {
	opts.SpanKind = w.kind
	return opts
}

// WithServerSpanKind is a shorthand for server spans, frequently saving
// allocations.
var WithServerSpanKind = WithSpanKind(oteltrace.SpanKindServer)

// WithClientSpanKind is a shorthand for server spans, frequently saving
// allocations.
var WithClientSpanKind = WithSpanKind(oteltrace.SpanKindClient)

type withSterileOption struct{}

// WithSterile configures the span to not permit any child spans. The would-be
// children of a sterile span end up being root spans.
//
// Since WithParent(<sterile span>) is a noop, it is allowed to create children
// of sterile span with any Tracer. This is unlike children of any other spans,
// which must be created with the same Tracer as the parent.
func WithSterile() SpanOption {
	return withSterileOption{}
}

func (w withSterileOption) apply(opts spanOptions) spanOptions {
	opts.Sterile = true
	return opts
}
