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

// SpanOptions are the options to `Tracer.StartSpan`. This struct is
// typically not used directly. Instead, the methods mentioned on each
// field comment below are invoked as arguments to `Tracer.StartSpan`.
type SpanOptions struct {
	Parent            *Span                         // see WithParent
	RemoteParent      *SpanMeta                     // see WithRemoteParent
	RefType           opentracing.SpanReferenceType // see WithFollowsFrom
	LogTags           *logtags.Buffer               // see WithLogTags
	Tags              map[string]interface{}        // see WithTags
	SeparateRecording bool                          // see WithSeparateRecording
	ForceRealSpan     bool                          // see WithForceRealSpan
}

func (opts *SpanOptions) parentTraceID() uint64 {
	if opts.Parent != nil {
		return opts.Parent.crdb.traceID
	} else if opts.RemoteParent != nil {
		return opts.RemoteParent.traceID
	}
	return 0
}

func (opts *SpanOptions) parentSpanID() uint64 {
	if opts.Parent != nil {
		return opts.Parent.crdb.spanID
	} else if opts.RemoteParent != nil {
		return opts.RemoteParent.spanID
	}
	return 0
}

func (opts *SpanOptions) recordingType() RecordingType {
	var recordingType RecordingType
	if opts.Parent != nil {
		recordingType = opts.Parent.crdb.getRecordingType()
	} else if opts.RemoteParent != nil {
		recordingType = opts.RemoteParent.recordingType
	}
	return recordingType
}

func (opts *SpanOptions) shadowTrTyp() (string, bool) {
	if opts.Parent != nil {
		return opts.Parent.ot.shadowTr.Typ()
	} else if opts.RemoteParent != nil {
		s := opts.RemoteParent.shadowTracerType
		return s, s != ""
	}
	return "", false
}

// SpanOption is the interface satisfied by options to `Tracer.StartSpan`.
type SpanOption interface {
	Apply(*SpanOptions)
}

type parentOption Span

var _ SpanOption = (*parentOption)(nil)

func (p *parentOption) Apply(opts *SpanOptions) {
	opts.Parent = (*Span)(p)
}

// WithParent instructs StartSpan to create a child span referring to the
// given local parent Span.
//
// Children of local parents inherit the parent's log tags, and will
// share their recording with the parent (unless WithSeparateRecording is
// used). They will also start recording if the parent is recording at
// the time of child instantiation. If the parent span is not recording,
// the child could be a "noop span" (depending on whether the Tracer is
// configured to trace to an external tracing system) which does not support
// recording, unless the WithForceRealSpan option is passed to StartSpan.
//
// By default, children are derived using a ChildOf relationship,
// which corresponds to the expectation that the parent span will
// wait for the child to Finish(). If this expectation does not hold,
// WithFollowsFrom should be added to the StartSpan invocation.
//
// When no local Span is available, WithRemoteParent should be used.
func WithParent(sp *Span) SpanOption {
	return (*parentOption)(sp)
}

type remoteParentOption SpanMeta

var _ SpanOption = (*remoteParentOption)(nil)

func (p *remoteParentOption) Apply(opts *SpanOptions) {
	opts.RemoteParent = (*SpanMeta)(p)
}

// WithRemoteParent instructs StartSpan to create child span descending
// from a parent described via SpanMeta. Since no local parent is
// available (in contrast to WithParent), this Span will not share a
// recording with any other Span. Typically RPC middleware ensures that the
// child's recording is collected and propagated back to the parent Span.
func WithRemoteParent(parent *SpanMeta) SpanOption {
	return (*remoteParentOption)(parent)
}

type tagsOption []opentracing.Tag

var _ SpanOption = (*tagsOption)(nil)

func (o tagsOption) Apply(opts *SpanOptions) {
	if len(o) == 0 {
		return
	}
	if opts.Tags == nil {
		opts.Tags = map[string]interface{}{}
	}
	for _, tag := range o {
		opts.Tags[tag.Key] = tag.Value
	}
}

// WithTags is an option to Tracer.StartSpan which populates the
// tags on the newly created Span.
func WithTags(tags ...opentracing.Tag) SpanOption {
	return (tagsOption)(tags)
}

type followsFromOpt struct{}

var _ SpanOption = (*followsFromOpt)(nil)

func (o followsFromOpt) Apply(opts *SpanOptions) {
	opts.RefType = opentracing.FollowsFromRef
}

// WithFollowsFrom instructs StartSpan to use a FollowsFrom relationship
// should a child span be created (i.e. should WithParent or WithRemoteParent
// be supplied as well). A WithFollowsFrom child is expected to, in the common
// case, outlive the parent span (for example: asynchronous cleanup work),
// whereas a "regular" child span is not (i.e. the parent span typically
// waits for the child to Finish()).
//
// There is no penalty for getting this wrong, but it can help external
// trace systems visualize the traces better.
func WithFollowsFrom() SpanOption {
	return followsFromOpt{}
}

type forceRealSpanOption struct{}

var _ SpanOption = (*forceRealSpanOption)(nil)

func (forceRealSpanOption) Apply(opts *SpanOptions) {
	opts.ForceRealSpan = true
}

// WithForceRealSpan forces StartSpan to create of a real Span instead of
// a low-overhead non-recordable noop span.
//
// When tracing is disabled all spans are noopSpans; these spans aren't
// capable of recording, so this option should be passed to StartSpan if the
// caller wants to be able to call StartRecording on the resulting Span.
func WithForceRealSpan() SpanOption {
	return forceRealSpanOption{}
}

type withSeparateRecordingOpt struct{}

var _ SpanOption = (*withSeparateRecordingOpt)(nil)

func (o withSeparateRecordingOpt) Apply(opts *SpanOptions) {
	opts.SeparateRecording = true
}

// WithSeparateRecording instructs StartSpan to configure any child span
// started via WithParent to *not* share the recording with that parent.
//
// See WithParent and WithRemoteParent for details about recording inheritance.
func WithSeparateRecording() SpanOption {
	return withSeparateRecordingOpt{}
}
