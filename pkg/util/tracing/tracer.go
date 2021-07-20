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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/petermattis/goid"
	"golang.org/x/net/trace"
)

// verboseTracingBaggageKey is set as Baggage on traces which are used for verbose tracing,
// meaning that a) spans derived from this one will not be no-op spans and b) they will
// start recording.
//
// This is "sb" for historical reasons; this concept used to be called "[S]now[b]all" tracing
// and since this string goes on the wire, it's a hassle to change it now.
const verboseTracingBaggageKey = "sb"

const (
	// maxRecordedBytesPerSpan limits the size of logs and structured in a span;
	// use a comfortable limit.
	maxLogBytesPerSpan        = 256 * (1 << 10) // 256 KiB
	maxStructuredBytesPerSpan = 10 * (1 << 10)  // 10 KiB
	// maxChildrenPerSpan limits the number of (direct) child spans in a Span.
	maxChildrenPerSpan = 1000
	// maxSpanRegistrySize limits the number of local root spans tracked in
	// a Tracer's registry.
	maxSpanRegistrySize = 5000
	// maxLogsPerSpanExternal limits the number of logs in a Span for external
	// tracers (net/trace, lightstep); use a comfortable limit.
	maxLogsPerSpanExternal = 1000
)

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

// ZipkinCollector is the cluster setting that specifies the Zipkin instance
// to send traces to, if any.
var ZipkinCollector = settings.RegisterStringSetting(
	"trace.zipkin.collector",
	"if set, traces go to the given Zipkin instance (example: '127.0.0.1:9411'). "+
		"Only one tracer can be configured at a time.",
	envutil.EnvOrDefaultString("COCKROACH_TEST_ZIPKIN_COLLECTOR", ""),
).WithPublic()

var dataDogAgentAddr = settings.RegisterStringSetting(
	"trace.datadog.agent",
	"if set, traces will be sent to this DataDog agent; use <host>:<port> or \"default\" for localhost:8126. "+
		"Only one tracer can be configured at a time.",
	envutil.EnvOrDefaultString("COCKROACH_DATADOG_AGENT", ""),
).WithPublic()

var dataDogProjectName = settings.RegisterStringSetting(
	"trace.datadog.project",
	"the project under which traces will be reported to the DataDog agent if trace.datadog.agent is set. "+
		"Only one tracer can be configured at a time.",
	envutil.EnvOrDefaultString("COCKROACH_DATADOG_PROJECT", "CockroachDB"),
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

	// True if tracing to the debug/requests endpoint. Accessed via t.useNetTrace().
	_useNetTrace int32 // updated atomically

	// Pointer to shadowTracer, if using one.
	shadowTracer unsafe.Pointer

	// activeSpans is a map that references all non-Finish'ed local root spans,
	// i.e. those for which no WithLocalParent(<non-nil>) option was supplied.
	// The map is keyed on the span ID, which is deterministically unique.
	//
	// In normal operation, a local root Span is inserted on creation and
	// removed on .Finish().
	//
	// The map can be introspected by `Tracer.VisitSpans`. A Span can also be
	// retrieved from its ID by `Tracer.GetActiveSpanFromID`.
	activeSpans struct {
		// NB: it might be tempting to use a sync.Map here, but
		// this incurs an allocation per Span (sync.Map does
		// not use a sync.Pool for its internal *entry type).
		//
		// The bare map approach is essentially allocation-free once the map
		// has grown to accommodate the usual number of active local root spans,
		// and the critical sections of the mutex are very small.
		syncutil.Mutex
		m map[uint64]*Span
	}
	// TracingVerbosityIndependentSemanticsIsActive is really
	// version.IsActive(TracingVerbosityIndependentSemanticsIsActive)
	// but gets injected this way to avoid import cycles. It defaults
	// to a function that returns `true`.
	TracingVerbosityIndependentSemanticsIsActive func() bool

	testingMu               syncutil.Mutex // protects testingRecordAsyncSpans
	testingRecordAsyncSpans bool           // see TestingRecordAsyncSpans

	testing *testingKnob
}

// NewTracer creates a Tracer. It initially tries to run with minimal overhead
// and collects essentially nothing; use Configure() to enable various tracing
// backends.
func NewTracer() *Tracer {
	t := &Tracer{}
	t.activeSpans.m = make(map[uint64]*Span)
	t.TracingVerbosityIndependentSemanticsIsActive = func() bool { return true }
	// The noop span is marked as finished so that even in the case of a bug,
	// it won't soak up data.
	t.noopSpan = &Span{numFinishCalled: 1, i: spanInner{tracer: t}}
	return t
}

// Configure sets up the Tracer according to the cluster settings (and keeps
// it updated if they change).
func (t *Tracer) Configure(ctx context.Context, sv *settings.Values) {
	reconfigure := func(ctx context.Context) {
		if lsToken := lightstepToken.Get(sv); lsToken != "" {
			t.setShadowTracer(createLightStepTracer(lsToken))
		} else if zipkinAddr := ZipkinCollector.Get(sv); zipkinAddr != "" {
			t.setShadowTracer(createZipkinTracer(zipkinAddr))
		} else if ddAddr := dataDogAgentAddr.Get(sv); ddAddr != "" {
			t.setShadowTracer(createDataDogTracer(ddAddr, dataDogProjectName.Get(sv)))
		} else {
			t.setShadowTracer(nil, nil)
		}
		var nt int32
		if enableNetTrace.Get(sv) {
			nt = 1
		}
		atomic.StoreInt32(&t._useNetTrace, nt)
	}

	reconfigure(ctx)

	enableNetTrace.SetOnChange(sv, reconfigure)
	lightstepToken.SetOnChange(sv, reconfigure)
	ZipkinCollector.SetOnChange(sv, reconfigure)
	dataDogAgentAddr.SetOnChange(sv, reconfigure)
	dataDogProjectName.SetOnChange(sv, reconfigure)
}

// HasExternalSink returns whether the tracer is configured to report
// to an external tracing collector.
func (t *Tracer) HasExternalSink() bool {
	return t.getShadowTracer() != nil || t.useNetTrace()
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
	if manager != nil && tr != nil {
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
		if !opts.RemoteParent.Empty() {
			panic("can't specify both Parent and RemoteParent")
		}
	}

	// Are we tracing everything, or have a parent, or want a real span? Then
	// we create a real trace span. In all other cases, a noop span will do.
	if !(t.AlwaysTrace() || opts.parentTraceID() != 0 || opts.ForceRealSpan) {
		return maybeWrapCtx(ctx, nil /* octx */, t.noopSpan)
	}

	if opts.LogTags == nil {
		opts.LogTags = logtags.FromContext(ctx)
	}

	if opts.LogTags == nil && opts.Parent != nil && !opts.Parent.i.isNoop() {
		// If no log tags are specified in the options, use the parent
		// span's, if any. This behavior is the reason logTags are
		// fundamentally different from tags, which are strictly per span,
		// for better or worse.
		opts.LogTags = opts.Parent.i.crdb.logTags
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
			ot = makeShadowSpan(shadowTr, opts.shadowContext(), opts.RefType, opName, startTime)
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
		netTr.SetMaxEvents(maxLogsPerSpanExternal)

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
	goroutineID := uint64(goid.Get())

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
		goroutineID:  goroutineID,
		startTime:    startTime,
		parentSpanID: opts.parentSpanID(),
		logTags:      opts.LogTags,
		mu: crdbSpanMu{
			duration: -1, // unfinished
		},
		testing: t.testing,
	}
	helper.crdbSpan.mu.operation = opName
	helper.crdbSpan.mu.recording.logs = newSizeLimitedBuffer(maxLogBytesPerSpan)
	helper.crdbSpan.mu.recording.structured = newSizeLimitedBuffer(maxStructuredBytesPerSpan)
	helper.span.i = spanInner{
		tracer: t,
		crdb:   &helper.crdbSpan,
		ot:     ot,
		netTr:  netTr,
	}

	// Copy over the parent span's root span reference, and if there isn't one
	// (we're creating a new root span), set a reference to ourselves.
	//
	// TODO(irfansharif): Given we have a handle on the root span, we should
	// reconsider the maxChildrenPerSpan limit, which only limits the branching
	// factor. To bound the total memory usage for pkg/tracing, we could instead
	// limit the number of spans per trace (no-oping all subsequent ones) and
	// do the same for the total number of root spans.
	if rootSpan := opts.deriveRootSpan(); rootSpan != nil {
		helper.crdbSpan.rootSpan = rootSpan
	} else {
		helper.crdbSpan.rootSpan = &helper.crdbSpan
	}

	s := &helper.span

	{
		// Link the newly created span to the parent, if necessary,
		// and start recording, if requested.
		// We inherit the recording type of the local parent, if any,
		// over the remote parent, if any. If neither are specified, we're not recording.
		var p *crdbSpan
		if opts.Parent != nil {
			p = opts.Parent.i.crdb
		}
		s.i.crdb.enableRecording(p, opts.recordingType())
	}

	// Set initial tags (has to happen after instantiating the recording type).
	// These will propagate to the crdbSpan, ot, and netTr as appropriate.
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
		if !opts.Parent.i.isNoop() {
			opts.Parent.i.crdb.mu.Lock()
			m := opts.Parent.i.crdb.mu.baggage
			opts.Parent.i.crdb.mu.Unlock()

			for k, v := range m {
				s.SetBaggageItem(k, v)
			}
		}
	} else {
		// Local root span - put it into the registry of active local root
		// spans. `Span.Finish` takes care of deleting it again.
		t.activeSpans.Lock()

		// Ensure that the registry does not grow unboundedly in case there
		// is a leak. When the registry reaches max size, each new span added
		// kicks out some old span. We rely on map iteration order here to
		// make this cheap.
		if toDelete := len(t.activeSpans.m) - maxSpanRegistrySize + 1; toDelete > 0 {
			for k := range t.activeSpans.m {
				delete(t.activeSpans.m, k)
				toDelete--
				if toDelete <= 0 {
					break
				}
			}
		}
		t.activeSpans.m[spanID] = s
		t.activeSpans.Unlock()

		if !opts.RemoteParent.Empty() {
			for k, v := range opts.RemoteParent.Baggage {
				s.SetBaggageItem(k, v)
			}
		}
	}

	return maybeWrapCtx(ctx, &helper.octx, s)
}

// serializationFormat is the format used by the Tracer to {de,}serialize span
// metadata across process boundaries. This takes place within
// Tracer.{InjectMetaInto,ExtractMetaFrom}. Each format is inextricably linked
// to a corresponding Carrier, which is the thing that actually captures the
// serialized data and crosses process boundaries.
//
// The usage pattern is as follows:
//
//     // One end of the RPC.
//     carrier := MapCarrier{...}
//     tracer.InjectMetaInto(sp.Meta(), carrier)
//
//     // carrier crosses RPC boundary.
//
//     // Other end of the RPC.
//     spMeta, _ := Tracer.ExtractMetaFrom(carrier)
//     ctx, sp := tracer.StartSpanCtx(..., spMeta)
//
type serializationFormat = opentracing.BuiltinFormat

const (
	_ serializationFormat = iota

	// metadataFormat is used to {de,}serialize data as HTTP header string
	// pairs. It's used with gRPC (the carrier must be metadataCarrier), for
	// when operations straddle RPC boundaries.
	metadataFormat = opentracing.HTTPHeaders

	// mapFormat is used to serialize data as a map of string pairs. The carrier
	// must be MapCarrier.
	mapFormat = opentracing.TextMap
)

// Carrier is what's used to capture the serialized data. Each carrier is
// inextricably linked to a corresponding format. See serializationFormat for
// more details.
type Carrier interface {
	Set(key, val string)
	ForEach(fn func(key, val string) error) error
}

// MapCarrier is an implementation of the Carrier interface for a map of string
// pairs.
type MapCarrier struct {
	Map map[string]string
}

// Set implements the Carrier interface.
func (c MapCarrier) Set(key, val string) {
	c.Map[key] = val
}

// ForEach implements the Carrier interface.
func (c MapCarrier) ForEach(fn func(key, val string) error) error {
	for k, v := range c.Map {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

type textMapWriterFn func(key, val string)

var _ opentracing.TextMapWriter = textMapWriterFn(nil)

// Set is part of the opentracing.TextMapWriter interface.
func (fn textMapWriterFn) Set(key, val string) {
	fn(key, val)
}

// InjectMetaInto is used to serialize the given span metadata into the given
// Carrier. This, alongside ExtractMetaFrom, can be used to carry span metadata
// across process boundaries. See serializationFormat for more details.
func (t *Tracer) InjectMetaInto(sm SpanMeta, carrier Carrier) error {
	if sm.Empty() {
		// Fast path when tracing is disabled. ExtractMetaFrom will accept an
		// empty map as a noop context.
		return nil
	}

	var format serializationFormat
	switch carrier.(type) {
	case MapCarrier:
		format = mapFormat
	case metadataCarrier:
		format = metadataFormat
	default:
		return errors.New("unsupported carrier")
	}

	carrier.Set(fieldNameTraceID, strconv.FormatUint(sm.traceID, 16))
	carrier.Set(fieldNameSpanID, strconv.FormatUint(sm.spanID, 16))

	for k, v := range sm.Baggage {
		carrier.Set(prefixBaggage+k, v)
	}

	shadowTr := t.getShadowTracer()
	if shadowTr != nil {
		// Don't use a different shadow tracer than the one that created the parent span
		// to put information on the wire. If something changes out from under us, forget
		// about shadow tracing.
		curTyp, _ := shadowTr.Type()
		if typ := sm.shadowTracerType; typ == curTyp {
			carrier.Set(fieldNameShadowType, sm.shadowTracerType)
			// Encapsulate the shadow text map, prepending a prefix to the keys.
			if err := shadowTr.Inject(sm.shadowCtx, format, textMapWriterFn(func(key, val string) {
				carrier.Set(prefixShadow+key, val)
			})); err != nil {
				return err
			}
		}
	}

	return nil
}

// var noopSpanMeta = (*SpanMeta)(nil)
var noopSpanMeta = SpanMeta{}

// ExtractMetaFrom is used to deserialize a span metadata (if any) from the
// given Carrier. This, alongside InjectMetaFrom, can be used to carry span
// metadata across process boundaries. See serializationFormat for more details.
func (t *Tracer) ExtractMetaFrom(carrier Carrier) (SpanMeta, error) {
	var shadowType string
	var shadowCarrier opentracing.TextMapCarrier
	var traceID uint64
	var spanID uint64
	var baggage map[string]string

	iterFn := func(k, v string) error {
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
	}

	// Instead of iterating through the interface type, we prefer to do so with
	// the explicit types to avoid heap allocations.
	switch c := carrier.(type) {
	case MapCarrier:
		if err := c.ForEach(iterFn); err != nil {
			return noopSpanMeta, err
		}
	case metadataCarrier:
		if err := c.ForEach(iterFn); err != nil {
			return noopSpanMeta, err
		}
	default:
		return noopSpanMeta, errors.New("unsupported carrier")
	}

	if traceID == 0 && spanID == 0 {
		return noopSpanMeta, nil
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
			var format serializationFormat
			switch carrier.(type) {
			case MapCarrier:
				format = mapFormat
			case metadataCarrier:
				format = metadataFormat
			default:
				return noopSpanMeta, errors.New("unsupported carrier")
			}
			// Shadow tracing is active on this node and the incoming information
			// was created using the same type of tracer.
			//
			// Extract the shadow context using the un-encapsulated textmap.
			var err error
			shadowCtx, err = shadowTr.Extract(format, shadowCarrier)
			if err != nil {
				return noopSpanMeta, err
			}
		}
	}

	return SpanMeta{
		traceID:          traceID,
		spanID:           spanID,
		shadowTracerType: shadowType,
		shadowCtx:        shadowCtx,
		recordingType:    recordingType,
		Baggage:          baggage,
	}, nil
}

// GetActiveSpanFromID retrieves any active root span given its ID.
func (t *Tracer) GetActiveSpanFromID(spanID uint64) (*Span, bool) {
	t.activeSpans.Lock()
	span, found := t.activeSpans.m[spanID]
	t.activeSpans.Unlock()
	return span, found
}

// VisitSpans invokes the visitor with all active Spans. The function will
// gracefully exit if the visitor returns iterutil.StopIteration().
func (t *Tracer) VisitSpans(visitor func(*Span) error) error {
	t.activeSpans.Lock()
	sl := make([]*Span, 0, len(t.activeSpans.m))
	for _, sp := range t.activeSpans.m {
		sl = append(sl, sp)
	}
	t.activeSpans.Unlock()

	for _, sp := range sl {
		if err := visitor(sp); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// TestingRecordAsyncSpans is a test-only helper that configures
// the tracer to include recordings from forked/async child spans, when
// retrieving the recording for a parent span.
func (t *Tracer) TestingRecordAsyncSpans() {
	t.testingMu.Lock()
	defer t.testingMu.Unlock()

	t.testingRecordAsyncSpans = true
}

// ShouldRecordAsyncSpans returns whether or not we should include recordings
// from async child spans in the parent span. See TestingRecordAsyncSpans, this
// mode is only used in tests.
func (t *Tracer) ShouldRecordAsyncSpans() bool {
	t.testingMu.Lock()
	defer t.testingMu.Unlock()

	return t.testingRecordAsyncSpans
}

// ForkSpan forks the current span, if any[1]. Forked spans "follow from" the
// original, and are typically used to trace operations that may outlive the
// parent (think async tasks). See the package-level documentation for more
// details.
//
// The recordings from these spans will not be automatically propagated to the
// parent span[2]. Also see `ChildSpan`, for the other kind of derived span
// relation.
//
// A context wrapping the newly created span is returned, along with the span
// itself. If non-nil, the caller is responsible for eventually Finish()ing it.
//
// [1]: Looking towards the provided context to see if one exists.
// [2]: Unless configured differently by tests, see
//      TestingRecordAsyncSpans.
func ForkSpan(ctx context.Context, opName string) (context.Context, *Span) {
	sp := SpanFromContext(ctx)
	if sp == nil {
		return ctx, nil
	}
	collectionOpt := WithParentAndManualCollection(sp.Meta())
	if sp.Tracer().ShouldRecordAsyncSpans() {
		// Using auto collection here ensures that recordings from async spans
		// also show up at the parent.
		collectionOpt = WithParentAndAutoCollection(sp)
	}
	return sp.Tracer().StartSpanCtx(ctx, opName, WithFollowsFrom(), collectionOpt)
}

// ChildSpan creates a child span of the current one, if any. Recordings from
// child spans are automatically propagated to the parent span, and the tags are
// inherited from the context's log tags automatically. Also see `ForkSpan`,
// for the other kind of derived span relation.
//
// A context wrapping the newly created span is returned, along with the span
// itself. If non-nil, the caller is responsible for eventually Finish()ing it.
func ChildSpan(ctx context.Context, opName string) (context.Context, *Span) {
	sp := SpanFromContext(ctx)
	if sp == nil {
		return ctx, nil
	}
	return sp.Tracer().StartSpanCtx(ctx, opName, WithParentAndAutoCollection(sp))
}

// ChildSpanRemote is like ChildSpan but the new Span is created using
// WithParentAndManualCollection instead of WithParentAndAutoCollection. When
// this is used, it's the caller's duty to collect this span's recording and
// return it to the root span of the trace.
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

// ContextWithRecordingSpan returns a context with an embedded trace Span.
// The Span is derived from the provided Tracer. The Span returns its contents
// when `getRecording` is called, and must be stopped using `cancel`, when done
// with the context (`getRecording` needs to be called before `cancel`).
//
// Note that to convert the recorded spans into text, you can use
// Recording.String(). Tests can also use FindMsgInRecording().
func ContextWithRecordingSpan(
	ctx context.Context, tr *Tracer, opName string,
) (_ context.Context, getRecording func() Recording, cancel func()) {
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
