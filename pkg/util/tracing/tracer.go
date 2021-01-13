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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/logtags"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/trace"
)

// verboseTracingBaggageKey is set as Baggage on traces which are used for verbose tracing,
// meaning that a) spans derived from this one will not be no-op spans and b) they will
// start recording.
//
// This is "sb" for historical reasons; this concept used to be called "[S]now[b]all" tracing
// and since this string goes on the wire, it's a hassle to change it now.
const verboseTracingBaggageKey = "sb"

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

type mode int32

const (
	modeLegacy mode = iota
	modeBackground
)

// tracingMode informs the creation of noop spans and the default recording mode
// of created spans.
//
// If set to 'background', trace spans will be created for all operations, but
// these will record sparse structured information, unless an operation
// explicitly requests the verbose from. It's optimized for low overhead, and
// powers fine-grained statistics and alerts.
//
// If set to 'legacy', trace spans will not be created by default. This is
// unless an internal code path explicitly requests for it, or if an auxiliary
// tracer (such as lightstep or zipkin) is configured. This tracing mode always
// records in the verbose form. Using this mode has two effects: the
// observability of the cluster may be degraded (as most trace spans are elided)
// and where trace spans are created, they may consume large amounts of memory.
//
// Note that regardless of this setting, configuring an auxiliary trace sink
// will cause verbose traces to be created for all operations, which may lead to
// high memory consumption. It is not currently possible to send non-verbose
// traces to auxiliary sinks.
var tracingMode = settings.RegisterEnumSetting(
	"trace.mode",
	"if set to 'background', traces will be created for all operations (in"+
		"'legacy' mode it's created when explicitly requested or when auxiliary tracers are configured)",
	"background",
	map[int64]string{
		int64(modeLegacy):     "legacy",
		int64(modeBackground): "background",
	})

var enableNetTrace = settings.RegisterBoolSetting(
	"trace.debug.enable",
	"if set, traces for recent requests can be seen at https://<ui>/debug/requests",
	false,
).WithPublic()

var lightstepToken = settings.RegisterStringSetting(
	"trace.lightstep.token",
	"if set, traces go to Lightstep using this token",
	envutil.EnvOrDefaultString("COCKROACH_TEST_LIGHTSTEP_TOKEN", ""),
).WithPublic()

var zipkinCollector = settings.RegisterStringSetting(
	"trace.zipkin.collector",
	"if set, traces go to the given Zipkin instance (example: '127.0.0.1:9411'); ignored if trace.lightstep.token is set",
	envutil.EnvOrDefaultString("COCKROACH_TEST_ZIPKIN_COLLECTOR", ""),
).WithPublic()

// Tracer is our own custom implementation of opentracing.Tracer. It supports:
//
//  - forwarding events to x/net/trace instances
//
//  - recording traces. Recording is started automatically for spans that have
//    the verboseTracingBaggageKey baggage and can be started explicitly as well. Recorded
//    events can be retrieved at any time.
//
//  - lightstep traces. This is implemented by maintaining a "shadow" lightstep
//    Span inside each of our spans.
//
// Even when tracing is disabled, we still use this Tracer (with x/net/trace and
// lightstep disabled) because of its recording capability (verbose tracing needs
// to work in all cases).
//
// Tracer is currently stateless so we could have a single instance; however,
// this won't be the case if the cluster settings move away from using global
// state.
type Tracer struct {
	// Preallocated noopSpan, used to avoid creating spans when we are not using
	// x/net/trace or lightstep and we are not recording.
	noopSpan *Span

	_mode int32 // modeLegacy or modeBackground, accessed atomically

	// True if tracing to the debug/requests endpoint. Accessed via t.useNetTrace().
	_useNetTrace int32 // updated atomically

	// Pointer to shadowTracer, if using one.
	shadowTracer unsafe.Pointer

	// activeSpans is a map that references all non-Finish'ed local root spans,
	// i.e. those for which no WithLocalParent(<non-nil>) option was supplied.
	// It also elides spans created using WithBypassRegistry.
	//
	// In normal operation, a local root Span is inserted on creation and
	// removed on .Finish().
	//
	// The map can be introspected by `Tracer.VisitSpans`.
	activeSpans struct {
		// NB: it might be tempting to use a sync.Map here, but
		// this incurs an allocation per Span (sync.Map does
		// not use a sync.Pool for its internal *entry type).
		//
		// The bare map approach is essentially allocation-free once the map
		// has grown to accommodate the usual number of active local root spans,
		// and the critical sections of the mutex are very small.
		syncutil.Mutex
		m map[*Span]struct{}
	}
}

// NewTracer creates a Tracer. It initially tries to run with minimal overhead
// and collects essentially nothing; use Configure() to enable various tracing
// backends.
func NewTracer() *Tracer {
	t := &Tracer{}
	t.activeSpans.m = map[*Span]struct{}{}
	t.noopSpan = &Span{tracer: t}
	return t
}

// Configure sets up the Tracer according to the cluster settings (and keeps
// it updated if they change).
func (t *Tracer) Configure(sv *settings.Values) {
	reconfigure := func() {
		atomic.StoreInt32(&t._mode, int32(tracingMode.Get(sv)))
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

// StartSpan starts a Span. See SpanOption for details.
func (t *Tracer) StartSpan(operationName string, os ...SpanOption) *Span {
	_, sp := t.StartSpanCtx(noCtx, operationName, os...)
	return sp
}

// StartSpanCtx starts a Span and returns it alongside a wrapping Context
// derived from the supplied Context. Any log tags found in the supplied
// Context are propagated into the Span; this behavior can be modified by
// passing WithLogTags explicitly.
//
// See SpanOption for other options that can be passed.
func (t *Tracer) StartSpanCtx(
	ctx context.Context, operationName string, os ...SpanOption,
) (context.Context, *Span) {
	// NB: apply takes and returns a value to avoid forcing
	// `opts` on the heap here.
	var opts spanOptions
	for _, o := range os {
		opts = o.apply(opts)
	}

	return t.startSpanGeneric(ctx, operationName, opts)
}

func (t *Tracer) mode() mode {
	return mode(atomic.LoadInt32(&t._mode))
}

// AlwaysTrace returns true if operations should be traced regardless of the
// context.
func (t *Tracer) AlwaysTrace() bool {
	shadowTracer := t.getShadowTracer()
	return t.useNetTrace() || shadowTracer != nil
}

// startSpanGeneric is the implementation of StartSpanCtx and StartSpan. In
// the latter case, ctx == noCtx and the returned Context is the supplied one;
// otherwise the returned Context embeds the returned Span.
func (t *Tracer) startSpanGeneric(
	ctx context.Context, opName string, opts spanOptions,
) (context.Context, *Span) {
	if opts.RefType != opentracing.ChildOfRef && opts.RefType != opentracing.FollowsFromRef {
		panic(fmt.Sprintf("unexpected RefType %v", opts.RefType))
	}

	if opts.Parent != nil {
		if opts.RemoteParent != nil {
			panic("can't specify both Parent and RemoteParent")
		}
	}

	if t.mode() == modeBackground {
		opts.ForceRealSpan = true
	}
	if opts.LogTags == nil {
		opts.LogTags = logtags.FromContext(ctx)
	}

	// Avoid creating a real span when possible. If tracing is globally
	// enabled, we always need to create spans. If the incoming
	// span is recording (which implies that there is a parent) then
	// we also have to create a real child. Additionally, if the
	// caller explicitly asked for a real span they need to get one.
	// In all other cases, a noop span will do.
	if !t.AlwaysTrace() &&
		opts.recordingType() == RecordingOff &&
		!opts.ForceRealSpan {
		return maybeWrapCtx(ctx, nil /* octx */, t.noopSpan)
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
		span     Span
		crdbSpan crdbSpan
		octx     optimizedContext
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
	helper.span = Span{
		tracer: t,
		crdb:   &helper.crdbSpan,
		ot:     ot,
		netTr:  netTr,
	}

	s := &helper.span

	// Start recording if necessary. We inherit the recording type of the local parent, if any,
	// over the remote parent, if any. If neither are specified, we're not recording.
	recordingType := opts.recordingType()

	if recordingType != RecordingOff {
		var p *crdbSpan
		if opts.Parent != nil {
			p = opts.Parent.crdb
		}
		s.crdb.enableRecording(p, recordingType)
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
	if opts.Parent != nil {
		if !opts.Parent.isNoop() {
			opts.Parent.crdb.mu.Lock()
			m := opts.Parent.crdb.mu.baggage
			for k, v := range m {
				s.SetBaggageItem(k, v)
			}
			opts.Parent.crdb.mu.Unlock()
		}
	} else {
		if !opts.BypassRegistry {
			// Local root span - put it into the registry of active local root
			// spans. `Span.Finish` takes care of deleting it again.
			t.activeSpans.Lock()
			t.activeSpans.m[s] = struct{}{}
			t.activeSpans.Unlock()
		}

		if opts.RemoteParent != nil {
			for k, v := range opts.RemoteParent.Baggage {
				s.SetBaggageItem(k, v)
			}
		}
	}

	return maybeWrapCtx(ctx, &helper.octx, s)
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
	if baggage[verboseTracingBaggageKey] != "" {
		recordingType = RecordingVerbose
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

// VisitSpans invokes the visitor with all active Spans.
func (t *Tracer) VisitSpans(visitor func(*Span)) {
	t.activeSpans.Lock()
	sl := make([]*Span, 0, len(t.activeSpans.m))
	for sp := range t.activeSpans.m {
		sl = append(sl, sp)
	}
	t.activeSpans.Unlock()

	for _, sp := range sl {
		visitor(sp)
	}
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
	sp := SpanFromContext(ctx)
	if sp == nil {
		return ctx, nil
	}
	return sp.Tracer().StartSpanCtx(
		ctx, opName, WithParentAndAutoCollection(sp), WithFollowsFrom(),
	)
}

// ChildSpan opens a Span as a child of the current Span in the context (if
// there is one), via the WithParentAndAutoCollection option.
// The Span's tags are inherited from the ctx's log tags automatically.
//
// Returns the new context and the new Span (if any). If a non-nil Span is
// returned, it is the caller's duty to eventually call Finish() on it.
func ChildSpan(ctx context.Context, opName string) (context.Context, *Span) {
	sp := SpanFromContext(ctx)
	if sp == nil {
		return ctx, nil
	}
	return sp.Tracer().StartSpanCtx(ctx, opName, WithParentAndAutoCollection(sp))
}

// ChildSpanRemote is like ChildSpan but the new Span is created using WithParentAndManualCollection
// instead of WithParentAndAutoCollection. When this is used, it's the caller's duty to collect this span's
// recording and return it to the root span of the trace.
func ChildSpanRemote(ctx context.Context, opName string) (context.Context, *Span) {
	sp := SpanFromContext(ctx)
	if sp == nil {
		return ctx, nil
	}
	return sp.Tracer().StartSpanCtx(ctx, opName, WithParentAndManualCollection(sp.Meta()))
}

// EnsureChildSpan looks at the supplied Context. If it contains a Span, returns
// a child span via the WithParentAndAutoCollection option; otherwise starts a
// new Span. In both cases, a context wrapping the Span is returned along with
// the newly created Span.
//
// The caller is responsible for closing the Span (via Span.Finish).
func EnsureChildSpan(
	ctx context.Context, tr *Tracer, name string, os ...SpanOption,
) (context.Context, *Span) {
	slp := optsPool.Get().(*[]SpanOption)
	*slp = append(*slp, WithParentAndAutoCollection(SpanFromContext(ctx)))
	*slp = append(*slp, os...)
	ctx, sp := tr.StartSpanCtx(ctx, name, *slp...)
	// Clear and zero-length the slice. Note that we have to clear
	// explicitly or the options will continue to be referenced by
	// the slice.
	for i := range *slp {
		(*slp)[i] = nil
	}
	*slp = (*slp)[0:0:cap(*slp)]
	optsPool.Put(slp)
	return ctx, sp
}

var optsPool = sync.Pool{
	New: func() interface{} {
		// It is unusual to pass more than 5 SpanOptions.
		sl := make([]SpanOption, 0, 5)
		return &sl
	},
}

// StartVerboseTrace takes in a context and returns a derived one with a
// Span in it that is recording verbosely. The caller takes ownership of
// this Span from the returned context and is in charge of Finish()ing it.
//
// TODO(tbg): remove this method. It adds very little over EnsureChildSpan.
func StartVerboseTrace(ctx context.Context, tr *Tracer, opName string) (context.Context, *Span) {
	ctx, sp := EnsureChildSpan(ctx, tr, opName, WithForceRealSpan())
	sp.SetVerbose(true)
	return ctx, sp
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
) (_ context.Context, getRecording func() Recording, cancel func()) {
	tr := NewTracer()
	ctx, sp := tr.StartSpanCtx(ctx, opName, WithForceRealSpan())
	sp.SetVerbose(true)
	ctx, cancelCtx := context.WithCancel(ctx)

	cancel = func() {
		cancelCtx()
		sp.SetVerbose(false)
		sp.Finish()
		tr.Close()
	}
	return ctx, sp.GetRecording, cancel
}
