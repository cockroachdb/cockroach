// Copyright 2015 The Cockroach Authors.
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
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/logtags"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/trace"
)

// Snowball is set as Baggage on traces which are used for snowball tracing.
const Snowball = "sb"

// maxLogsPerSpan limits the number of logs in a Span; use a comfortable limit.
const maxLogsPerSpan = 1000

// These constants are used to form keys to represent tracing context
// information in carriers supporting opentracing.HTTPHeaders format.
const (
	prefixTracerState = "crdb-tracer-"
	prefixBaggage     = "crdb-baggage-"
	// prefixShadow is prepended to the keys for the context of the shadow tracer
	// (e.g. LightStep).
	prefixShadow = "crdb-shadow-"

	fieldNameTraceID = prefixTracerState + "traceid"
	fieldNameSpanID  = prefixTracerState + "spanid"
	// fieldNameShadow is the name of the shadow tracer.
	fieldNameShadowType = prefixTracerState + "shadowtype"
)

var enableNetTrace = settings.RegisterPublicBoolSetting(
	"trace.debug.enable",
	"if set, traces for recent requests can be seen in the /debug page",
	false,
)

var lightstepToken = settings.RegisterPublicStringSetting(
	"trace.lightstep.token",
	"if set, traces go to Lightstep using this token",
	envutil.EnvOrDefaultString("COCKROACH_TEST_LIGHTSTEP_TOKEN", ""),
)

var zipkinCollector = settings.RegisterPublicStringSetting(
	"trace.zipkin.collector",
	"if set, traces go to the given Zipkin instance (example: '127.0.0.1:9411'); ignored if trace.lightstep.token is set",
	envutil.EnvOrDefaultString("COCKROACH_TEST_ZIPKIN_COLLECTOR", ""),
)

// Tracer is our own custom implementation of opentracing.Tracer. It supports:
//
//  - forwarding events to x/net/trace instances
//
//  - recording traces. Recording is started automatically for spans that have
//    the Snowball baggage and can be started explicitly as well. Recorded
//    events can be retrieved at any time.
//
//  - lightstep traces. This is implemented by maintaining a "shadow" lightstep
//    Span inside each of our spans.
//
// Even when tracing is disabled, we still use this Tracer (with x/net/trace and
// lightstep disabled) because of its recording capability (snowball
// tracing needs to work in all cases).
//
// Tracer is currently stateless so we could have a single instance; however,
// this won't be the case if the cluster settings move away from using global
// state.
type Tracer struct {
	// Preallocated noopSpan, used to avoid creating spans when we are not using
	// x/net/trace or lightstep and we are not recording.
	noopSpan *Span

	// True if tracing to the debug/requests endpoint. Accessed via t.useNetTrace().
	_useNetTrace int32 // updated atomically

	// Pointer to shadowTracer, if using one.
	shadowTracer unsafe.Pointer
}

// NewTracer creates a Tracer. It initially tries to run with minimal overhead
// and collects essentially nothing; use Configure() to enable various tracing
// backends.
func NewTracer() *Tracer {
	t := &Tracer{}
	t.noopSpan = &Span{tracer: t}
	return t
}

// Configure sets up the Tracer according to the cluster settings (and keeps
// it updated if they change).
func (t *Tracer) Configure(sv *settings.Values) {
	reconfigure := func() {
		if lsToken := lightstepToken.Get(sv); lsToken != "" {
			t.setShadowTracer(createLightStepTracer(lsToken))
		} else if zipkinAddr := zipkinCollector.Get(sv); zipkinAddr != "" {
			t.setShadowTracer(createZipkinTracer(zipkinAddr))
		} else {
			t.setShadowTracer(nil, nil)
		}
		var nt int32
		if enableNetTrace.Get(sv) {
			nt = 1
		}
		atomic.StoreInt32(&t._useNetTrace, nt)
	}

	reconfigure()

	enableNetTrace.SetOnChange(sv, reconfigure)
	lightstepToken.SetOnChange(sv, reconfigure)
	zipkinCollector.SetOnChange(sv, reconfigure)
}

func (t *Tracer) useNetTrace() bool {
	return atomic.LoadInt32(&t._useNetTrace) != 0
}

// Close cleans up any resources associated with a Tracer.
func (t *Tracer) Close() {
	// Clean up any shadow tracer.
	t.setShadowTracer(nil, nil)
}

func (t *Tracer) setShadowTracer(manager shadowTracerManager, tr opentracing.Tracer) {
	var shadow *shadowTracer
	if manager != nil {
		shadow = &shadowTracer{
			Tracer:  tr,
			manager: manager,
		}
	}
	if old := atomic.SwapPointer(&t.shadowTracer, unsafe.Pointer(shadow)); old != nil {
		(*shadowTracer)(old).Close()
	}
}

func (t *Tracer) getShadowTracer() *shadowTracer {
	return (*shadowTracer)(atomic.LoadPointer(&t.shadowTracer))
}

type forceRealSpanOption struct{}

// Apply is part of the opentracing.StartSpanOption interface.
func (forceRealSpanOption) Apply(opts *SpanOptions) {
	opts.ForceRealSpan = true
}

// WithRealSpan is a SpanOption that forces creation of a real Span.
//
// When tracing is disabled all spans are noopSpans; these spans aren't
// capable of recording, so this option should be passed to StartSpan if the
// caller wants to be able to call StartRecording on the resulting Span.
var WithRealSpan SpanOption = forceRealSpanOption{}

type SpanOptions struct {
	Parent            *Span
	RemoteParent      *SpanContext // TODO thin this out
	RefType           opentracing.SpanReferenceType
	LogTags           *logtags.Buffer
	Tags              map[string]interface{}
	SeparateRecording bool
	ForceRealSpan     bool
}

func (opts *SpanOptions) parentMeta() spanMeta {
	if opts.Parent != nil {
		return opts.Parent.crdb.spanMeta
	} else if opts.RemoteParent != nil {
		return opts.RemoteParent.spanMeta
	}
	return spanMeta{}
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

type SpanOptionFunc func(*SpanOptions)

func (f SpanOptionFunc) Apply(o *SpanOptions) {
	f(o)
}

type parentOption Span

func (p *parentOption) Apply(opts *SpanOptions) {
	opts.Parent = (*Span)(p)
}

func WithParent(sp *Span) SpanOption {
	return (*parentOption)(sp)
}

type remoteParentOption SpanContext

func (p *remoteParentOption) Apply(opts *SpanOptions) {
	opts.RemoteParent = (*SpanContext)(p)
}

func WithRemoteParent(parent *SpanContext) SpanOption {
	return (*remoteParentOption)(parent)
}

type tagsOption []opentracing.Tag

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

func WithTags(tags ...opentracing.Tag) SpanOption {
	return (tagsOption)(tags)
}

type followsFromOpt struct{}

func (o followsFromOpt) Apply(opts *SpanOptions) {
	opts.RefType = opentracing.FollowsFromRef
}

func WithFollowsFrom() SpanOption {
	return followsFromOpt{}
}

// StartSpan starts a Span.
func (t *Tracer) StartSpan(operationName string, os ...SpanOption) *Span {
	// Fast paths to avoid the allocation of StartSpanOptions below when tracing
	// is disabled: if we have no options or a single SpanReference (the common
	// case) with a black hole span, return a noop Span now.
	if len(os) == 1 {
		switch o := os[0].(type) {
		case *parentOption:
			if (*Span)(o).IsBlackHole() {
				return t.noopSpan
			}
		case *remoteParentOption:
			if (*SpanContext)(o).isNilOrNoop() {
				return t.noopSpan
			}
		}
	}
	if len(os) == 0 && !t.AlwaysTrace() {
		return t.noopSpan
	}

	var opts SpanOptions
	for _, o := range os {
		o.Apply(&opts)
	}

	if opts.RefType != opentracing.ChildOfRef && opts.RefType != opentracing.FollowsFromRef {
		panic(fmt.Sprintf("unexpected RefType %v", opts.RefType))
	}

	if opts.Parent != nil {
		if opts.RemoteParent != nil {
			panic("can't specify both Parent and RemoteParent")
		}
	}

	return t.startSpanGeneric(operationName, opts)
}

// AlwaysTrace returns true if operations should be traced regardless of the
// context.
func (t *Tracer) AlwaysTrace() bool {
	shadowTracer := t.getShadowTracer()
	return t.useNetTrace() || shadowTracer != nil
}

// StartRootSpan creates a root Span. This is functionally equivalent to:
// parentSpan.Tracer().(*Tracer).StartSpan(opName, LogTags(...), [WithRealSpan])
// Compared to that, it's more efficient, particularly in terms of memory
// allocations because the opentracing.StartSpanOption interface is not used.
//
// logTags can be nil.
func (t *Tracer) StartRootSpan(opName string, logTags *logtags.Buffer, recordable bool) *Span {
	return t.StartChildSpan(opName, nil /* parent */, logTags, recordable, false /* separateRecording */)
}

// StartChildSpan creates a child Span of the given parent Span. This is
// functionally equivalent to:
// parentSpan.Tracer().(*Tracer).StartSpan(opName, opentracing.ChildOf(parentSpan.Context()))
// Compared to that, it's more efficient, particularly in terms of memory
// allocations; among others, it saves the call to parentSpan.Context.
//
// This only works for creating children of local parents (i.e. the caller needs
// to have a reference to the parent Span).
//
// If separateRecording is true and the parent Span is recording, the child's
// recording will not be part of the parent's recording. This is useful when the
// child's recording will be reported to a collector separate from the parent's
// recording; for example DistSQL processors each report their own recording,
// and we don't want the parent's recording to include a child's because then we
// might double-report that child.
//
// TODO(tbg): I don't think we need separateRecording because children can consume
// and (atomically) clear their recording to avoid it getting consumed twice.
func (t *Tracer) StartChildSpan(
	opName string, parent *Span, logTags *logtags.Buffer, forceRealSpan bool, separateRecording bool,
) *Span {
	var opts SpanOptions
	opts.Parent = parent
	opts.LogTags = logTags
	opts.ForceRealSpan = forceRealSpan
	opts.SeparateRecording = separateRecording
	return t.startSpanGeneric(opName, opts)
}

// startSpanGeneric is the internal workhorse for creating spans. It serves two purposes:
//
// 1. creating root spans. In this case, parentContext and parentType are zero. A noop Span
//    is returned when nothing forces an actual Span to be created, i.e. there is no shadow
//    tracer and internal tracing active, plus no recordability is requested.
// 2. creating derived spans. In this case, parentContext and parentType are nonzero. If the
//    parent is not recording and 'recordable' is zero, and nothing else forces a real Span,
//    a noopSpan results.
func (t *Tracer) startSpanGeneric(opName string, opts SpanOptions) *Span {
	// If tracing is disabled, avoid overhead and return a noop Span.
	if !t.AlwaysTrace() &&
		opts.parentMeta().TraceID == 0 &&
		opts.recordingType() == NoRecording &&
		!opts.ForceRealSpan {
		return t.noopSpan
	}

	s := &Span{
		tracer: t,
		crdb: crdbSpan{
			operation:    opName,
			startTime:    time.Now(),
			parentSpanID: opts.parentMeta().SpanID,
			logTags:      opts.LogTags,
		},
	}
	s.crdb.mu.duration = -1 // unfinished

	traceID := opts.parentMeta().TraceID
	if traceID == 0 {
		traceID = uint64(rand.Int63())
	}
	s.crdb.TraceID = traceID
	s.crdb.SpanID = uint64(rand.Int63())

	shadowTr := t.getShadowTracer()
	{
		// Make sure not to derive spans created using an old
		// shadow tracer via a new one.
		typ1, ok1 := opts.shadowTrTyp() // old
		typ2, ok2 := shadowTr.Typ()     // new
		if ok1 && ok2 && typ1 != typ2 {
			// If both are set and don't agree, ignore shadow tracer
			// for the new span. It's fine if the old one isn't set
			// but the new one is (we won't use it, though we could)
			// or if the old one is and new one isn't (in which case
			// the supposedly latest config has no shadow tracing
			// enabled).
			shadowTr = nil
		}
	}

	if shadowTr != nil {
		var shadowCtx opentracing.SpanContext
		if opts.Parent != nil && opts.Parent.ot.shadowSpan != nil {
			shadowCtx = opts.Parent.ot.shadowSpan.Context()
		}
		linkShadowSpan(s, shadowTr, shadowCtx, opts.RefType)
	}

	// Start recording if necessary. We inherit the recording type of the local parent, if any,
	// over the remote parent, if any. If neither are specified, we're not recording.
	recordingType := opts.recordingType()

	if recordingType != NoRecording {
		var p *crdbSpan
		if opts.Parent != nil {
			p = &opts.Parent.crdb
		}
		s.crdb.enableRecording(p, recordingType, opts.SeparateRecording)
	}

	if t.useNetTrace() {
		s.netTr = trace.New("tracing", opName)
		s.netTr.SetMaxEvents(maxLogsPerSpan)
		if opts.LogTags != nil {
			tags := opts.LogTags.Get()
			for i := range tags {
				tag := &tags[i]
				s.netTr.LazyPrintf("%s:%v", tagName(tag.Key()), tag.Value())
			}
		}
	}

	// Set initial tags.
	//
	// NB: this could be optimized.
	for k, v := range opts.Tags {
		s.SetTag(k, v)
	}

	// Copy baggage from parent.
	//
	// NB: this could be optimized.
	if opts.Parent != nil {
		opts.Parent.crdb.mu.Lock()
		m := opts.Parent.crdb.mu.Baggage
		for k, v := range m {
			s.SetBaggageItem(k, v)
		}
		opts.Parent.crdb.mu.Unlock()
	} else if opts.RemoteParent != nil {
		for k, v := range opts.RemoteParent.Baggage {
			s.SetBaggageItem(k, v)
		}
	}

	return s
}

type textMapWriterFn func(key, val string)

var _ opentracing.TextMapWriter = textMapWriterFn(nil)

// Set is part of the opentracing.TextMapWriter interface.
func (fn textMapWriterFn) Set(key, val string) {
	fn(key, val)
}

// Inject is part of the opentracing.Tracer interface.
func (t *Tracer) Inject(sc *SpanContext, format interface{}, carrier interface{}) error {
	if sc.isNilOrNoop() {
		// Fast path when tracing is disabled. Extract will accept an empty map as a
		// noop context.
		return nil
	}

	// We only support the HTTPHeaders/TextMap format.
	if format != opentracing.HTTPHeaders && format != opentracing.TextMap {
		return opentracing.ErrUnsupportedFormat
	}

	mapWriter, ok := carrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}

	mapWriter.Set(fieldNameTraceID, strconv.FormatUint(sc.TraceID, 16))
	mapWriter.Set(fieldNameSpanID, strconv.FormatUint(sc.SpanID, 16))

	for k, v := range sc.Baggage {
		mapWriter.Set(prefixBaggage+k, v)
	}

	shadowTr := t.getShadowTracer()
	if shadowTr != nil {
		// Don't use a different shadow tracer than the one that created the parent span
		// to put information on the wire. If something changes out from under us, forget
		// about shadow tracing.
		curTyp, _ := shadowTr.Typ()
		if typ := sc.shadowTracerType; typ == curTyp {
			mapWriter.Set(fieldNameShadowType, sc.shadowTracerType)
			// Encapsulate the shadow text map, prepending a prefix to the keys.
			if err := shadowTr.Inject(sc.shadowCtx, format, textMapWriterFn(func(key, val string) {
				mapWriter.Set(prefixShadow+key, val)
			})); err != nil {
				return err
			}
		}
	}

	return nil
}

type textMapReaderFn func(handler func(key, val string) error) error

var _ opentracing.TextMapReader = textMapReaderFn(nil)

// ForeachKey is part of the opentracing.TextMapReader interface.
func (fn textMapReaderFn) ForeachKey(handler func(key, val string) error) error {
	return fn(handler)
}

var noopSpanContext = &SpanContext{}

// Extract is part of the opentracing.Tracer interface.
// It always returns a valid context, even in error cases (this is assumed by the
// grpc-opentracing interceptor).
func (t *Tracer) Extract(format interface{}, carrier interface{}) (*SpanContext, error) {
	// We only support the HTTPHeaders/TextMap format.
	if format != opentracing.HTTPHeaders && format != opentracing.TextMap {
		return noopSpanContext, opentracing.ErrUnsupportedFormat
	}

	mapReader, ok := carrier.(opentracing.TextMapReader)
	if !ok {
		return noopSpanContext, opentracing.ErrInvalidCarrier
	}

	var shadowType string
	var shadowCarrier opentracing.TextMapCarrier

	var traceID uint64
	var spanID uint64
	var baggage map[string]string
	err := mapReader.ForeachKey(func(k, v string) error {
		switch k = strings.ToLower(k); k {
		case fieldNameTraceID:
			var err error
			traceID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return opentracing.ErrSpanContextCorrupted
			}
		case fieldNameSpanID:
			var err error
			spanID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return opentracing.ErrSpanContextCorrupted
			}
		case fieldNameShadowType:
			shadowType = v
		default:
			if strings.HasPrefix(k, prefixBaggage) {
				if baggage == nil {
					baggage = make(map[string]string)
				}
				baggage[strings.TrimPrefix(k, prefixBaggage)] = v
			} else if strings.HasPrefix(k, prefixShadow) {
				if shadowCarrier == nil {
					shadowCarrier = make(opentracing.TextMapCarrier)
				}
				// We build a shadow textmap with the original shadow keys.
				shadowCarrier.Set(strings.TrimPrefix(k, prefixShadow), v)
			}
		}
		return nil
	})
	if err != nil {
		return noopSpanContext, err
	}
	if traceID == 0 && spanID == 0 {
		return noopSpanContext, nil
	}

	var recordingType RecordingType
	if baggage[Snowball] != "" {
		recordingType = SnowballRecording
	}

	var shadowCtx opentracing.SpanContext
	if shadowType != "" {
		shadowTr := t.getShadowTracer()
		curShadowTyp, _ := shadowTr.Typ()

		if shadowType != curShadowTyp {
			// If either the incoming context or tracer disagree on which
			// shadow tracer (if any) is active, scrub shadow tracing from
			// consideration.
			shadowType = ""
		} else {
			// Shadow tracing is active on this node and the incoming information
			// was created using the same type of tracer.
			//
			// Extract the shadow context using the un-encapsulated textmap.
			shadowCtx, err = shadowTr.Extract(format, shadowCarrier)
			if err != nil {
				return noopSpanContext, err
			}
		}
	}

	return &SpanContext{
		spanMeta: spanMeta{
			TraceID: traceID,
			SpanID:  spanID,
		},
		shadowTracerType: shadowType,
		shadowCtx:        shadowCtx,
		recordingType:    recordingType,
		Baggage:          baggage,
	}, nil
}

// ForkCtxSpan checks if ctx has a Span open; if it does, it creates a new Span
// that "follows from" the original Span. This allows the resulting context to be
// used in an async task that might outlive the original operation.
//
// Returns the new context and the new Span (if any). The Span should be
// closed via FinishSpan.
//
// See also ChildSpan() for a "parent-child relationship".
func ForkCtxSpan(ctx context.Context, opName string) (context.Context, *Span) {
	if sp := SpanFromContext(ctx); sp != nil {
		if sp.isNoop() {
			// Optimization: avoid ContextWithSpan call if tracing is disabled.
			return ctx, sp
		}
		tr := sp.Tracer()
		newSpan := tr.StartSpan(opName, WithParent(sp), WithCtxLogTags(ctx))
		return ContextWithSpan(ctx, newSpan), newSpan
	}
	return ctx, nil
}

// ChildSpan opens a Span as a child of the current Span in the context (if
// there is one).
// The Span's tags are inherited from the ctx's log tags automatically.
//
// Returns the new context and the new Span (if any). The Span should be
// closed via FinishSpan.
func ChildSpan(ctx context.Context, opName string) (context.Context, *Span) {
	return childSpan(ctx, opName, false /* separateRecording */)
}

// ChildSpanSeparateRecording is like ChildSpan but the new Span has separate
// recording (see StartChildSpan).
func ChildSpanSeparateRecording(ctx context.Context, opName string) (context.Context, *Span) {
	return childSpan(ctx, opName, true /* separateRecording */)
}

func childSpan(
	ctx context.Context, opName string, separateRecording bool,
) (context.Context, *Span) {
	sp := SpanFromContext(ctx)
	if sp == nil || sp.isNoop() {
		// Optimization: avoid ContextWithSpan call if tracing is disabled.
		return ctx, sp
	}
	tr := sp.Tracer()
	if sp.IsBlackHole() {
		ns := tr.noopSpan
		return ContextWithSpan(ctx, ns), ns
	}
	newSpan := tr.StartChildSpan(opName, sp, logtags.FromContext(ctx), false /* forceRealSpan */, separateRecording)
	return ContextWithSpan(ctx, newSpan), newSpan
}

// EnsureContext checks whether the given context.Context contains a Span. If
// not, it creates one using the provided Tracer and wraps it in the returned
// Span. The returned closure must be called after the request has been fully
// processed.
//
// Note that, if there's already a Span in the context, this method does nothing
// even if the current context's log tags are different from that Span's tags.
func EnsureContext(ctx context.Context, tracer *Tracer, opName string) (context.Context, func()) {
	if SpanFromContext(ctx) == nil {
		sp := tracer.StartRootSpan(opName, logtags.FromContext(ctx), false /* forceRealSpan */)
		return ContextWithSpan(ctx, sp), sp.Finish
	}
	return ctx, func() {}
}

// EnsureChildSpan is the same as EnsureContext, except it creates a child
// Span for the input context if the input context already has an active
// trace.
//
// The caller is responsible for closing the Span (via Span.Finish).
func EnsureChildSpan(ctx context.Context, tracer *Tracer, name string) (context.Context, *Span) {
	if SpanFromContext(ctx) == nil {
		sp := tracer.StartRootSpan(name, logtags.FromContext(ctx), false /* forceRealSpan */)
		return ContextWithSpan(ctx, sp), sp
	}
	return ChildSpan(ctx, name)
}

type activeSpanKey struct{}

// SpanFromContext returns the *Span contained in the Context, if any.
func SpanFromContext(ctx context.Context) *Span {
	val := ctx.Value(activeSpanKey{})
	if sp, ok := val.(*Span); ok {
		return sp
	}
	return nil
}

// ContextWithSpan returns a Context wrapping the supplied Span.
func ContextWithSpan(ctx context.Context, sp *Span) context.Context {
	return context.WithValue(ctx, activeSpanKey{}, sp)
}

// StartSnowballTrace takes in a context and returns a derived one with a
// "snowball Span" in it. The caller takes ownership of this Span from the
// returned context and is in charge of Finish()ing it. The Span has recording
// enabled.
//
// TODO(andrei): remove this method once EXPLAIN(TRACE) is gone.
func StartSnowballTrace(
	ctx context.Context, tracer *Tracer, opName string,
) (context.Context, *Span) {
	var span *Span
	if sp := SpanFromContext(ctx); sp != nil {
		span = sp.Tracer().StartSpan(
			opName, WithParent(sp), WithRealSpan, WithCtxLogTags(ctx),
		)
	} else {
		span = tracer.StartSpan(opName, WithRealSpan, WithCtxLogTags(ctx))
	}
	span.StartRecording(SnowballRecording)
	return ContextWithSpan(ctx, span), span
}

// ContextWithRecordingSpan returns a context with an embedded trace Span which
// returns its contents when getRecording is called and must be stopped by
// calling the cancel method when done with the context (getRecording() needs to
// be called before cancel()).
//
// Note that to convert the recorded spans into text, you can use
// Recording.String(). Tests can also use FindMsgInRecording().
func ContextWithRecordingSpan(
	ctx context.Context, opName string,
) (retCtx context.Context, getRecording func() Recording, cancel func()) {
	tr := NewTracer()
	sp := tr.StartSpan(opName, WithRealSpan, WithCtxLogTags(ctx))
	sp.StartRecording(SnowballRecording)
	ctx, cancelCtx := context.WithCancel(ctx)
	ctx = ContextWithSpan(ctx, sp)

	cancel = func() {
		cancelCtx()
		sp.StopRecording()
		sp.Finish()
		tr.Close()
	}
	return ctx, sp.GetRecording, cancel
}
