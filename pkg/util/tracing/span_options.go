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
	"github.com/opentracing/opentracing-go"
)

// spanOptions are the options to `Tracer.StartSpan`. This struct is
// typically not used directly. Instead, the methods mentioned on each
// field comment below are invoked as arguments to `Tracer.StartSpan`.
// See the SpanOption interface for a synopsis.
type spanOptions struct {
	Parent        *Span                         // see WithParentAndAutoCollection
	RemoteParent  SpanMeta                      // see WithParentAndManualCollection
	RefType       opentracing.SpanReferenceType // see WithFollowsFrom
	LogTags       *logtags.Buffer               // see WithLogTags
	Tags          map[string]interface{}        // see WithTags
	ForceRealSpan bool                          // see WithForceRealSpan
}

func (opts *spanOptions) parentTraceID() uint64 {
	if opts.Parent != nil && !opts.Parent.i.isNoop() {
		return opts.Parent.i.crdb.traceID
	} else if !opts.RemoteParent.Empty() {
		return opts.RemoteParent.traceID
	}
	return 0
}

func (opts *spanOptions) parentSpanID() uint64 {
	if opts.Parent != nil && !opts.Parent.i.isNoop() {
		return opts.Parent.i.crdb.spanID
	} else if !opts.RemoteParent.Empty() {
		return opts.RemoteParent.spanID
	}
	return 0
}

func (opts *spanOptions) deriveRootSpan() *crdbSpan {
	if opts.Parent != nil && !opts.Parent.i.isNoop() {
		return opts.Parent.i.crdb.rootSpan
	}
	return nil
}

func (opts *spanOptions) recordingType() RecordingType {
	recordingType := RecordingOff
	if opts.Parent != nil && !opts.Parent.i.isNoop() {
		recordingType = opts.Parent.i.crdb.recordingType()
	} else if !opts.RemoteParent.Empty() {
		recordingType = opts.RemoteParent.recordingType
	}
	return recordingType
}

func (opts *spanOptions) shadowTrTyp() (string, bool) {
	if opts.Parent != nil {
		return opts.Parent.i.ot.shadowTr.Type()
	} else if !opts.RemoteParent.Empty() {
		s := opts.RemoteParent.shadowTracerType
		return s, s != ""
	}
	return "", false
}

func (opts *spanOptions) shadowContext() opentracing.SpanContext {
	if opts.Parent != nil && opts.Parent.i.ot.shadowSpan != nil {
		return opts.Parent.i.ot.shadowSpan.Context()
	}
	if !opts.RemoteParent.Empty() && opts.RemoteParent.shadowCtx != nil {
		return opts.RemoteParent.shadowCtx
	}
	return nil
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
	return (parentAndManualCollectionOption)(parent)
}

func (p parentAndManualCollectionOption) apply(opts spanOptions) spanOptions {
	opts.RemoteParent = (SpanMeta)(p)
	return opts
}

type followsFromOpt struct{}

// WithFollowsFrom instructs StartSpan to use a FollowsFrom relationship
// should a child span be created (i.e. should WithParentAndAutoCollection or
// WithParentAndManualCollection be supplied as well). A WithFollowsFrom child
// is expected to, in the common case, outlive the parent span (for example:
// asynchronous cleanup work), whereas a "regular" child span is not (i.e. the
// parent span typically waits for the child to Finish()).
//
// There is no penalty for getting this wrong, but it can help external
// trace systems visualize the traces better.
func WithFollowsFrom() SpanOption {
	return followsFromOpt{}
}

func (o followsFromOpt) apply(opts spanOptions) spanOptions {
	opts.RefType = opentracing.FollowsFromRef
	return opts
}

type forceRealSpanOption struct{}

// WithForceRealSpan forces StartSpan to create of a real Span instead of
// a low-overhead non-recordable noop span.
//
// When tracing is disabled all spans are noopSpans; these spans aren't
// capable of recording, so this option should be passed to StartSpan if the
// caller wants to be able to call StartRecording on the resulting Span.
func WithForceRealSpan() SpanOption {
	return forceRealSpanOption{}
}

func (forceRealSpanOption) apply(opts spanOptions) spanOptions {
	opts.ForceRealSpan = true
	return opts
}
