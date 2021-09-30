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
	Parent        *Span                  // see WithParentAndAutoCollection
	RemoteParent  SpanMeta               // see WithParentAndManualCollection
	RefType       spanReferenceType      // see WithFollowsFrom
	LogTags       *logtags.Buffer        // see WithLogTags
	Tags          map[string]interface{} // see WithTags
	ForceRealSpan bool                   // see WithForceRealSpan
	SpanKind      oteltrace.SpanKind     // see WithSpanKind
	Sterile       bool                   // see WithSterile
}

func (opts *spanOptions) parentTraceID() uint64 {
	if opts.Parent != nil && !opts.Parent.IsNoop() {
		return opts.Parent.i.crdb.traceID
	} else if !opts.RemoteParent.Empty() {
		return opts.RemoteParent.traceID
	}
	return 0
}

func (opts *spanOptions) parentSpanID() uint64 {
	if opts.Parent != nil && !opts.Parent.IsNoop() {
		return opts.Parent.i.crdb.spanID
	} else if !opts.RemoteParent.Empty() {
		return opts.RemoteParent.spanID
	}
	return 0
}

func (opts *spanOptions) deriveRootSpan() *crdbSpan {
	if opts.Parent != nil && !opts.Parent.IsNoop() {
		return opts.Parent.i.crdb.rootSpan
	}
	return nil
}

func (opts *spanOptions) recordingType() RecordingType {
	recordingType := RecordingOff
	if opts.Parent != nil && !opts.Parent.IsNoop() {
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
	if opts.Parent != nil && opts.Parent.i.otelSpan != nil {
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
// - WithParentAndAutoCollection: create a child Span from a Span.
// - WithParentAndManualCollection: create a child Span from a SpanMeta.
// - WithFollowsFrom: indicate that child may outlive parent.
// - WithLogTags: populates the Span tags from a `logtags.Buffer`.
// - WithCtxLogTags: like WithLogTags, but takes a `context.Context`.
// - WithTags: adds tags to a Span on creation.
// - WithForceRealSpan: prevents optimizations that can avoid creating a real span.
type SpanOption interface {
	apply(spanOptions) spanOptions
}

type parentAndAutoCollectionOption Span

// WithParentAndAutoCollection instructs StartSpan to create a child Span
// from a parent Span.
//
// WithParentAndAutoCollection will be a no-op (i.e. the span resulting from
// applying this option will be a root span, just as if this option hadn't been
// specified) in the following cases:
// - if `sp` is nil
// - if `sp` is a no-op span
// - if `sp` is a sterile span (i.e. a span explicitly marked as not wanting
//   children). Note that the singleton Tracer.noop span is marked as sterile,
//   which makes this condition mostly encompass the previous one, however in
//   theory there could be no-op spans other than the singleton one.
//
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
// When the parent Span is not available at the caller,
// WithParentAndManualCollection should be used, which incurs an
// obligation to manually propagate the trace data to the parent Span.
func WithParentAndAutoCollection(sp *Span) SpanOption {
	if sp == nil || sp.IsNoop() || sp.IsSterile() {
		return (*parentAndAutoCollectionOption)(nil)
	}
	return (*parentAndAutoCollectionOption)(sp)
}

func (p *parentAndAutoCollectionOption) apply(opts spanOptions) spanOptions {
	opts.Parent = (*Span)(p)
	return opts
}

type parentAndManualCollectionOption SpanMeta

// WithParentAndManualCollection instructs StartSpan to create a
// child span descending from a parent described via a SpanMeta. In
// contrast with WithParentAndAutoCollection, the caller must call
// `Span.GetRecording` when finishing the returned Span, and propagate the
// result to the parent Span by calling `Span.ImportRemoteSpans` on it.
//
// The canonical use case for this is around RPC boundaries, where a
// server handling a request wants to create a child span descending
// from a parent on a remote machine.
//
// node 1                     (network)          node 2
// --------------------------------------------------------------------------
// Span.Meta()               ----------> sp2 := Tracer.StartSpan(
//                                       		WithParentAndManualCollection(.))
//                                       doSomething(sp2)
//                                       sp2.Finish()
// Span.ImportRemoteSpans(.) <---------- sp2.GetRecording()
//
// By default, the child span is derived using a ChildOf relationship,
// which corresponds to the expectation that the parent span will
// wait for the child to Finish(). If this expectation does not hold,
// WithFollowsFrom should be added to the StartSpan invocation.
func WithParentAndManualCollection(parent SpanMeta) SpanOption {
	if parent.sterile {
		return parentAndManualCollectionOption{}
	}
	return (parentAndManualCollectionOption)(parent)
}

func (p parentAndManualCollectionOption) apply(opts spanOptions) spanOptions {
	opts.RemoteParent = (SpanMeta)(p)
	return opts
}

type followsFromOpt struct{}

var followsFromSingleton = SpanOption(followsFromOpt{})

// WithFollowsFrom instructs StartSpan to link the child span to its parent
// using a different kind of relationship than the regular parent-child one,
// should a child span be created (i.e. should WithParentAndAutoCollection or
// WithParentAndManualCollection be supplied as well). This relationship was
// called "follows-from" in the old OpenTracing API. This only matters if the
// trace is sent to an OpenTelemetry tracer; CRDB itself ignores it (what
// matters for CRDB is the AutoCollection vs ManualCollection distinction).
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

// WithForceRealSpan forces StartSpan to create of a real Span instead of
// a low-overhead non-recordable noop span.
//
// When tracing is disabled all spans are noopSpans; these spans aren't
// capable of recording, so this option should be passed to StartSpan if the
// caller wants to be able to call StartRecording on the resulting Span.
func WithForceRealSpan() SpanOption {
	return forceRealSpanSingleton
}

func (forceRealSpanOption) apply(opts spanOptions) spanOptions {
	opts.ForceRealSpan = true
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
func WithSterile() SpanOption {
	return withSterileOption{}
}

func (w withSterileOption) apply(opts spanOptions) spanOptions {
	opts.Sterile = true
	return opts
}
