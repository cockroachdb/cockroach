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

// StartSpan starts a Span. See spanOptions for details.
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
			if (*SpanMeta)(o).isNilOrNoop() {
				return t.noopSpan
			}
		}
	}
	if len(os) == 0 && !t.AlwaysTrace() {
		return t.noopSpan
	}

	// NB: apply takes and returns a value to avoid forcing
	// `opts` on the heap here.
	var opts spanOptions
	for _, o := range os {
		opts = o.apply(opts)
	}

	return t.startSpanGeneric(operationName, opts)
}

// AlwaysTrace returns true if operations should be traced regardless of the
// context.
func (t *Tracer) AlwaysTrace() bool {
	shadowTracer := t.getShadowTracer()
	return t.useNetTrace() || shadowTracer != nil
}

// startSpanGeneric is the implementation of StartSpan.
func (t *Tracer) startSpanGeneric(opName string, opts spanOptions) *Span {
	if opts.RefType != opentracing.ChildOfRef && opts.RefType != opentracing.FollowsFromRef {
		panic(fmt.Sprintf("unexpected RefType %v", opts.RefType))
	}

	if opts.Parent != nil {
		if opts.RemoteParent != nil {
			panic("can't specify both Parent and RemoteParent")
		}
	}

	// If tracing is disabled, avoid overhead and return a noop Span.
	if !t.AlwaysTrace() &&
		opts.parentTraceID() == 0 &&
		opts.recordingType() == NoRecording &&
		!opts.ForceRealSpan {
		return t.noopSpan
	}

	if opts.LogTags == nil && opts.Parent != nil && !opts.Parent.isNoop() {
		// If no log tags are specified in the options, use the parent
		// span's, if any. This behavior is the reason logTags are
		// fundamentally different from tags, which are strictly per span,
		// for better or worse.
		opts.LogTags = opts.Parent.crdb.logTags
	}

	startTime := time.Now()

	// First, create any external spans that we may need (opentracing, net/trace).
	// We do this early so that they are available when we construct the main Span,
	// which makes it easier to avoid one-offs when populating the tags and baggage
	// items for the top-level Span.
	var ot otSpan
	{
		shadowTr := t.getShadowTracer()

		// Make sure not to derive spans created using an old
		// shadow tracer via a new one.
		typ1, ok1 := opts.shadowTrTyp() // old
		typ2, ok2 := shadowTr.Type()    // new
		// If both are set and don't agree, ignore shadow tracer
		// for the new span to avoid compat issues between the
		// two underlying tracers.
		if ok2 && (!ok1 || typ1 == typ2) {
			var shadowCtx opentracing.SpanContext
			if opts.Parent != nil && opts.Parent.ot.shadowSpan != nil {
				shadowCtx = opts.Parent.ot.shadowSpan.Context()
			}
			ot = makeShadowSpan(shadowTr, shadowCtx, opts.RefType, opName, startTime)
			// If LogTags are given, pass them as tags to the shadow span.
			// Regular tags are populated later, via the top-level Span.
			if opts.LogTags != nil {
				setLogTags(opts.LogTags.Get(), func(remappedKey string, tag *logtags.Tag) {
					_ = ot.shadowSpan.SetTag(remappedKey, tag.Value())
				})
			}
		}
	}

	var netTr trace.Trace
	if t.useNetTrace() {
		netTr = trace.New("tracing", opName)
		netTr.SetMaxEvents(maxLogsPerSpan)

		// If LogTags are given, pass them as tags to the shadow span.
		// Regular tags are populated later, via the top-level Span.
		if opts.LogTags != nil {
			setLogTags(opts.LogTags.Get(), func(remappedKey string, tag *logtags.Tag) {
				netTr.LazyPrintf("%s:%v", remappedKey, tag)
			})
		}
	}

	// Now that `ot` and `netTr` are properly set up, make the Span.

	traceID := opts.parentTraceID()
	if traceID == 0 {
		// NB: it is tempting to use the traceID and spanID from the
		// possibly populated otSpan in this case, but the opentracing
		// interface doesn't give us a good way to extract these.
		traceID = uint64(rand.Int63())
	}
	spanID := uint64(rand.Int63())

	// Now allocate the main *Span and contained crdbSpan.
	// Allocate these together to save on individual allocs.
	//
	// NB: at the time of writing, it's not possible to start a Span
	// that *only* contains `ot` or `netTr`. This is just an artifact
	// of the history of this code and may change in the future.
	helper := struct {
		Span     Span
		crdbSpan crdbSpan
	}{}

	helper.crdbSpan = crdbSpan{
		traceID:      traceID,
		spanID:       spanID,
		operation:    opName,
		startTime:    startTime,
		parentSpanID: opts.parentSpanID(),
		logTags:      opts.LogTags,
		mu: crdbSpanMu{
			duration: -1, // unfinished
		},
	}
	helper.Span = Span{
		tracer: t,
		crdb:   &helper.crdbSpan,
		ot:     ot,
		netTr:  netTr,
	}

	s := &helper.Span

	// Start recording if necessary. We inherit the recording type of the local parent, if any,
	// over the remote parent, if any. If neither are specified, we're not recording.
	recordingType := opts.recordingType()

	if recordingType != NoRecording {
		var p *crdbSpan
		if opts.Parent != nil {
			p = opts.Parent.crdb
		}
		s.crdb.enableRecording(p, recordingType, opts.SeparateRecording)
	}

	// Set initial tags. These will propagate to the crdbSpan, ot, and netTr
	// as appropriate.
	//
	// NB: this could be optimized.
	for k, v := range opts.Tags {
		s.SetTag(k, v)
	}

	// Copy baggage from parent. This similarly fans out over the various
	// spans contained in Span.
	//
	// NB: this could be optimized.
	if opts.Parent != nil && !opts.Parent.isNoop() {
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
func (t *Tracer) Inject(sc *SpanMeta, format interface{}, carrier interface{}) error {
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

	mapWriter.Set(fieldNameTraceID, strconv.FormatUint(sc.traceID, 16))
	mapWriter.Set(fieldNameSpanID, strconv.FormatUint(sc.spanID, 16))

	for k, v := range sc.Baggage {
		mapWriter.Set(prefixBaggage+k, v)
	}

	shadowTr := t.getShadowTracer()
	if shadowTr != nil {
		// Don't use a different shadow tracer than the one that created the parent span
		// to put information on the wire. If something changes out from under us, forget
		// about shadow tracing.
		curTyp, _ := shadowTr.Type()
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

var noopSpanContext = &SpanMeta{}

// Extract is part of the opentracing.Tracer interface.
// It always returns a valid context, even in error cases (this is assumed by the
// grpc-opentracing interceptor).
func (t *Tracer) Extract(format interface{}, carrier interface{}) (*SpanMeta, error) {
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

	// TODO(tbg): ForeachKey forces things on the heap. We can do better
	// by using an explicit carrier.
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
		curShadowTyp, _ := shadowTr.Type()

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

	return &SpanMeta{
		traceID:          traceID,
		spanID:           spanID,
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
	return childSpan(ctx, opName, spanOptions{})
}

// ChildSpanSeparateRecording is like ChildSpan but the new Span has separate
// recording (see StartChildSpan).
func ChildSpanSeparateRecording(ctx context.Context, opName string) (context.Context, *Span) {
	return childSpan(ctx, opName, spanOptions{SeparateRecording: true})
}

func childSpan(ctx context.Context, opName string, opts spanOptions) (context.Context, *Span) {
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
	opts.LogTags = logtags.FromContext(ctx)
	opts.Parent = sp
	newSpan := tr.startSpanGeneric(opName, opts)
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
		sp := tracer.StartSpan(opName, WithCtxLogTags(ctx))
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
		sp := tracer.StartSpan(name, WithCtxLogTags(ctx))
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
			opName, WithParent(sp), WithForceRealSpan(), WithCtxLogTags(ctx),
		)
	} else {
		span = tracer.StartSpan(opName, WithForceRealSpan(), WithCtxLogTags(ctx))
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
	sp := tr.StartSpan(opName, WithForceRealSpan(), WithCtxLogTags(ctx))
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
