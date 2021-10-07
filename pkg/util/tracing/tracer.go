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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/petermattis/goid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/sdk/resource"
	otelsdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
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
	// tracers (net/trace, OpenTelemetry); use a comfortable limit.
	maxLogsPerSpanExternal = 1000
)

// These constants are used to form keys to represent tracing context
// information in "carriers" to be transported across RPC boundaries.
const (
	prefixTracerState = "crdb-tracer-"
	prefixBaggage     = "crdb-baggage-"

	fieldNameTraceID = prefixTracerState + "traceid"
	fieldNameSpanID  = prefixTracerState + "spanid"
	// fieldNameOtel{TraceID,SpanID} will contain the OpenTelemetry span info, hex
	// encoded.
	fieldNameOtelTraceID = prefixTracerState + "otel_traceid"
	fieldNameOtelSpanID  = prefixTracerState + "otel_spanid"

	spanKindTagKey = "span.kind"
)

var enableNetTrace = settings.RegisterBoolSetting(
	"trace.debug.enable",
	"if set, traces for recent requests can be seen at https://<ui>/debug/requests",
	false,
).WithPublic()

var openTelemetryCollector = settings.RegisterValidatedStringSetting(
	"trace.opentelemetry.collector",
	"address of an OpenTelemetry trace collector to receive "+
		"traces using the otel gRPC protocol, as <host>:<port>. "+
		"If no port is specified, 4317 will be used.",
	envutil.EnvOrDefaultString("COCKROACH_OTLP_COLLECTOR", ""),
	func(_ *settings.Values, s string) error {
		if s == "" {
			return nil
		}
		_, _, err := addr.SplitHostPort(s, "4317")
		return err
	},
).WithPublic()

var jaegerAgent = settings.RegisterValidatedStringSetting(
	"trace.jaeger.agent",
	"the address of a Jaeger agent to receive traces using the "+
		"Jaeger UDP Thrift protocol, as <host>:<port>. "+
		"If no port is specified, 6381 will be used.",
	envutil.EnvOrDefaultString("COCKROACH_JAEGER", ""),
	func(_ *settings.Values, s string) error {
		if s == "" {
			return nil
		}
		_, _, err := addr.SplitHostPort(s, "6381")
		return err
	},
).WithPublic()

// ZipkinCollector is the cluster setting that specifies the Zipkin instance
// to send traces to, if any.
var ZipkinCollector = settings.RegisterValidatedStringSetting(
	"trace.zipkin.collector",
	"the address of a Zipkin instance to receive traces, as <host>:<port>. "+
		"If no port is specified, 9411 will be used.",
	envutil.EnvOrDefaultString("COCKROACH_ZIPKIN", ""),
	func(_ *settings.Values, s string) error {
		if s == "" {
			return nil
		}
		_, _, err := addr.SplitHostPort(s, "9411")
		return err
	},
).WithPublic()

// Tracer implements tracing requests. It supports:
//
//  - forwarding events to x/net/trace instances
//
//  - recording traces. Recording is started automatically for spans that have
//    the verboseTracingBaggageKey baggage and can be started explicitly as well. Recorded
//    events can be retrieved at any time.
//
//  - OpenTelemetry tracing. This is implemented by maintaining a "shadow"
//    OpenTelemetry Span inside each of our spans.
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

	// Pointer to an OpenTelemetry tracer used as a "shadow tracer", if any. If
	// not nil, the respective *otel.Tracer will be used to create mirror spans
	// for all spans that the parent Tracer creates.
	otelTracer unsafe.Pointer

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

	testingMu               syncutil.Mutex // protects testingRecordAsyncSpans
	testingRecordAsyncSpans bool           // see TestingRecordAsyncSpans

	testing TracerTestingKnobs
}

// TracerTestingKnobs contains knobs for a Tracer.
type TracerTestingKnobs struct {
	// Clock allows the time source for spans to be controlled.
	Clock timeutil.TimeSource
	// ForceRealSpans, if set, forces the Tracer to create spans even when tracing
	// is otherwise disabled.
	ForceRealSpans bool
}

// NewTracer creates a Tracer. It initially tries to run with minimal overhead
// and collects essentially nothing; use Configure() to enable various tracing
// backends.
func NewTracer() *Tracer {
	t := &Tracer{}
	t.activeSpans.m = make(map[uint64]*Span)
	// The noop span is marked as finished so that even in the case of a bug,
	// it won't soak up data.
	t.noopSpan = &Span{numFinishCalled: 1, i: spanInner{tracer: t}}
	return t
}

// NewTracerWithOpt creates a Tracer and configures it according to the
// passed-in options.
func NewTracerWithOpt(ctx context.Context, opts ...TracerOption) *Tracer {
	var o tracerOptions
	for _, opt := range opts {
		opt.apply(&o)
	}

	t := NewTracer()
	if o.sv != nil {
		t.Configure(ctx, o.sv)
	}
	t.testing = o.knobs
	return t
}

// tracerOptions groups configuration for Tracer construction.
type tracerOptions struct {
	sv    *settings.Values
	knobs TracerTestingKnobs
}

// TracerOption is implemented by the arguments to the Tracer constructor.
type TracerOption interface {
	apply(opt *tracerOptions)
}

type clusterSettingsOpt struct {
	sv *settings.Values
}

func (o clusterSettingsOpt) apply(opt *tracerOptions) {
	opt.sv = o.sv
}

var _ TracerOption = clusterSettingsOpt{}

// WithClusterSettings configures the Tracer according to the relevant cluster
// settings. Future changes to those cluster settings will update the Tracer.
func WithClusterSettings(sv *settings.Values) TracerOption {
	return clusterSettingsOpt{sv: sv}
}

type knobsOpt struct {
	knobs TracerTestingKnobs
}

func (o knobsOpt) apply(opt *tracerOptions) {
	opt.knobs = o.knobs
}

var _ TracerOption = knobsOpt{}

// WithTestingKnobs configures the Tracer with the specified knobs.
func WithTestingKnobs(knobs TracerTestingKnobs) TracerOption {
	return knobsOpt{knobs: knobs}
}

// Configure sets up the Tracer according to the cluster settings (and keeps
// it updated if they change).
func (t *Tracer) Configure(ctx context.Context, sv *settings.Values) {
	// traceProvider is captured by the function below.
	var traceProvider *otelsdk.TracerProvider

	// reconfigure will be called every time a cluster setting affecting tracing
	// is updated.
	reconfigure := func(ctx context.Context) {
		jaegerAgentAddr := jaegerAgent.Get(sv)
		otlpCollectorAddr := openTelemetryCollector.Get(sv)
		zipkinAddr := ZipkinCollector.Get(sv)

		var nt int32
		if enableNetTrace.Get(sv) {
			nt = 1
		}
		atomic.StoreInt32(&t._useNetTrace, nt)

		// Return early if the OpenTelemetry tracer is disabled.
		if jaegerAgentAddr == "" && otlpCollectorAddr == "" && zipkinAddr == "" {
			if traceProvider != nil {
				t.SetOpenTelemetryTracer(nil)
				if err := traceProvider.Shutdown(ctx); err != nil {
					fmt.Fprintf(os.Stderr, "error shutting down tracer: %s", err)
				}
			}
			return
		}

		opts := []otelsdk.TracerProviderOption{otelsdk.WithSampler(otelsdk.AlwaysSample())}
		resource, err := resource.New(ctx,
			resource.WithAttributes(semconv.ServiceNameKey.String("CockroachDB")),
		)
		if err == nil {
			opts = append(opts, otelsdk.WithResource(resource))
		} else {
			fmt.Fprintf(os.Stderr, "failed to create OpenTelemetry resource: %s\n", err)
		}

		if otlpCollectorAddr != "" {
			spanProcessor, err := createOTLPSpanProcessor(ctx, otlpCollectorAddr)
			if err == nil {
				opts = append(opts, otelsdk.WithSpanProcessor(spanProcessor))
			} else {
				fmt.Fprintf(os.Stderr, "failed to create OTLP processor: %s", err)
			}
		}

		if jaegerAgentAddr != "" {
			spanProcessor, err := createJaegerSpanCollector(ctx, jaegerAgentAddr)
			if err == nil {
				opts = append(opts, otelsdk.WithSpanProcessor(spanProcessor))
			} else {
				fmt.Fprintf(os.Stderr, "failed to create Jaeger processor: %s", err)
			}
		}

		if zipkinAddr != "" {
			spanProcessor, err := createZipkinCollector(ctx, zipkinAddr)
			if err == nil {
				opts = append(opts, otelsdk.WithSpanProcessor(spanProcessor))
			} else {
				fmt.Fprintf(os.Stderr, "failed to create Zipkin processor: %s", err)
			}
		}

		oldTP := traceProvider
		traceProvider = otelsdk.NewTracerProvider(opts...)

		// Canonical OpenTelemetry wants every module to have its own Tracer
		// instance, with each one initialized with a different name. We're not
		// doing that though, because our code creates all the spans through a
		// single Tracer (the receiver of this method). So, we're creating a
		// single Tracer here.
		otelTracer := traceProvider.Tracer("crdb")
		t.SetOpenTelemetryTracer(otelTracer)

		// Shutdown the old tracer.
		if oldTP != nil {
			_ = oldTP.Shutdown(context.TODO())
		}

		// TODO(andrei): Figure out how to cleanup the tracer when the server
		// exits. It unfortunately seems hard to plumb the Stopper to here to put
		// a closer on it.
	}

	reconfigure(ctx)

	enableNetTrace.SetOnChange(sv, reconfigure)
	openTelemetryCollector.SetOnChange(sv, reconfigure)
	ZipkinCollector.SetOnChange(sv, reconfigure)
	jaegerAgent.SetOnChange(sv, reconfigure)
}

func createOTLPSpanProcessor(
	ctx context.Context, otlpCollectorAddr string,
) (otelsdk.SpanProcessor, error) {
	host, port, err := addr.SplitHostPort(otlpCollectorAddr, "4317")
	if err != nil {
		return nil, err
	}

	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(fmt.Sprintf("%s:%s", host, port)),
		// TODO(andrei): Add support for secure connections to the collector.
		otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	spanProcessor := otelsdk.NewBatchSpanProcessor(exporter)
	return spanProcessor, nil
}

func createJaegerSpanCollector(
	ctx context.Context, agentAddr string,
) (otelsdk.SpanProcessor, error) {
	host, port, err := addr.SplitHostPort(agentAddr, "6831")
	if err != nil {
		return nil, err
	}
	exporter, err := jaeger.New(jaeger.WithAgentEndpoint(
		jaeger.WithAgentHost(host),
		jaeger.WithAgentPort(port)))
	if err != nil {
		return nil, err
	}
	spanProcessor := otelsdk.NewBatchSpanProcessor(exporter)
	return spanProcessor, nil
}

func createZipkinCollector(ctx context.Context, zipkinAddr string) (otelsdk.SpanProcessor, error) {
	host, port, err := addr.SplitHostPort(zipkinAddr, "9411")
	if err != nil {
		return nil, err
	}
	exporter, err := zipkin.New(fmt.Sprintf("http://%s:%s/api/v2/spans", host, port))
	if err != nil {
		return nil, err
	}
	spanProcessor := otelsdk.NewBatchSpanProcessor(exporter)
	return spanProcessor, nil
}

// HasExternalSink returns whether the tracer is configured to report
// to an external tracing collector.
func (t *Tracer) HasExternalSink() bool {
	return t.getOtelTracer() != nil || t.useNetTrace()
}

func (t *Tracer) useNetTrace() bool {
	return atomic.LoadInt32(&t._useNetTrace) != 0
}

// Close cleans up any resources associated with a Tracer.
func (t *Tracer) Close() {
	// Clean up the OpenTelemetry tracer, if any.
	t.SetOpenTelemetryTracer(nil)
}

// SetOpenTelemetryTracer sets the OpenTelemetry tracer to use as a "shadow
// tracer". A nil value means that no otel tracer will be used.
func (t *Tracer) SetOpenTelemetryTracer(tr oteltrace.Tracer) {
	var p *oteltrace.Tracer
	if tr == nil {
		p = nil
	} else {
		p = &tr
	}
	atomic.StorePointer(&t.otelTracer, unsafe.Pointer(p))
}

// getOtelTracer returns the OpenTelemetry tracer to use, or nil.
func (t *Tracer) getOtelTracer() oteltrace.Tracer {
	p := atomic.LoadPointer(&t.otelTracer)
	if p == nil {
		return nil
	}
	return *(*oteltrace.Tracer)(p)
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
	if t.testing.ForceRealSpans {
		return true
	}
	otelTracer := t.getOtelTracer()
	return t.useNetTrace() || otelTracer != nil
}

// startSpanGeneric is the implementation of StartSpanCtx and StartSpan. In
// the latter case, ctx == noCtx and the returned Context is the supplied one;
// otherwise the returned Context embeds the returned Span.
func (t *Tracer) startSpanGeneric(
	ctx context.Context, opName string, opts spanOptions,
) (context.Context, *Span) {
	if opts.RefType != childOfRef && opts.RefType != followsFromRef {
		panic(fmt.Sprintf("unexpected RefType %v", opts.RefType))
	}

	if opts.Parent != nil {
		if !opts.RemoteParent.Empty() {
			panic("can't specify both Parent and RemoteParent")
		}
		if opts.Parent.IsNoop() {
			// This method relies on the parent, if any, not being a no-op. A no-op
			// parent should have been optimized away by the
			// WithParentAndAutoCollection option.
			panic("invalid no-op parent")
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

	if opts.LogTags == nil && opts.Parent != nil {
		// If no log tags are specified in the options, use the parent
		// span's, if any. This behavior is the reason logTags are
		// fundamentally different from tags, which are strictly per span,
		// for better or worse.
		opts.LogTags = opts.Parent.i.crdb.logTags
	}

	startTime := time.Now()

	// First, create any external spans that we may need (OpenTelemetry, net/trace).
	// We do this early so that they are available when we construct the main Span,
	// which makes it easier to avoid one-offs when populating the tags and baggage
	// items for the top-level Span.
	var otelSpan oteltrace.Span
	if otelTr := t.getOtelTracer(); otelTr != nil {
		parentSpan, parentContext := opts.otelContext()
		otelSpan = makeOtelSpan(otelTr, opName, parentSpan, parentContext, opts.RefType, startTime, opts.SpanKind)
		// If LogTags are given, pass them as tags to the otel span.
		// Regular tags are populated later, via the top-level Span.
		if opts.LogTags != nil {
			setLogTags(opts.LogTags.Get(), func(remappedKey string, tag *logtags.Tag) {
				otelSpan.SetAttributes(attribute.String(remappedKey, tag.ValueStr()))
			})
		}
	}

	var netTr trace.Trace
	if t.useNetTrace() {
		netTr = trace.New("tracing", opName)
		netTr.SetMaxEvents(maxLogsPerSpanExternal)

		// If LogTags are given, pass them as tags to the otel span.
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
		// tagsAlloc preallocates space for crdbSpan.mu.tags.
		tagsAlloc [3]attribute.KeyValue
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
		testing: &t.testing,
	}
	helper.crdbSpan.mu.operation = opName
	helper.crdbSpan.mu.recording.logs = newSizeLimitedBuffer(maxLogBytesPerSpan)
	helper.crdbSpan.mu.recording.structured = newSizeLimitedBuffer(maxStructuredBytesPerSpan)
	helper.crdbSpan.mu.tags = helper.tagsAlloc[:0]
	if opts.SpanKind != oteltrace.SpanKindUnspecified {
		helper.crdbSpan.setTagLocked(spanKindTagKey, attribute.StringValue(opts.SpanKind.String()))
	}
	helper.span.i = spanInner{
		tracer:   t,
		crdb:     &helper.crdbSpan,
		otelSpan: otelSpan,
		netTr:    netTr,
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
		// If a parent is specified, link the newly created Span to the parent. This
		// is done even when not recording because recording could be started later.
		//
		// We inherit the recording type of the local parent, if any, over the
		// remote parent, if any. If neither are specified, we're not recording.
		if opts.Parent != nil && opts.Parent.i.crdb != nil {
			defer opts.Parent.i.crdb.addChild(s.i.crdb)
		}
		s.i.crdb.enableRecording(opts.recordingType())
	}

	// Copy baggage from parent. This similarly fans out over the various
	// spans contained in Span.
	//
	// NB: this could be optimized.
	// NB: (opts.Parent != nil && opts.Parent.i.crdb == nil) is not possible at
	// the moment, but let's not rely on that.
	if opts.Parent != nil && opts.Parent.i.crdb != nil {
		opts.Parent.i.crdb.mu.Lock()
		m := opts.Parent.i.crdb.mu.baggage
		opts.Parent.i.crdb.mu.Unlock()

		for k, v := range m {
			s.SetBaggageItem(k, v)
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

// InjectMetaInto is used to serialize the given span metadata into the given
// Carrier. This, alongside ExtractMetaFrom, can be used to carry span metadata
// across process boundaries. See serializationFormat for more details.
func (t *Tracer) InjectMetaInto(sm SpanMeta, carrier Carrier) error {
	if sm.Empty() {
		// Fast path when tracing is disabled. ExtractMetaFrom will accept an
		// empty map as a noop context.
		return nil
	}

	carrier.Set(fieldNameTraceID, strconv.FormatUint(sm.traceID, 16))
	carrier.Set(fieldNameSpanID, strconv.FormatUint(sm.spanID, 16))

	for k, v := range sm.Baggage {
		carrier.Set(prefixBaggage+k, v)
	}
	if sm.otelCtx.TraceID().IsValid() {
		carrier.Set(fieldNameOtelTraceID, sm.otelCtx.TraceID().String())
		carrier.Set(fieldNameOtelSpanID, sm.otelCtx.SpanID().String())
	}

	return nil
}

var noopSpanMeta = SpanMeta{}

// ExtractMetaFrom is used to deserialize a span metadata (if any) from the
// given Carrier. This, alongside InjectMetaFrom, can be used to carry span
// metadata across process boundaries. See serializationFormat for more details.
func (t *Tracer) ExtractMetaFrom(carrier Carrier) (SpanMeta, error) {
	var traceID uint64
	var spanID uint64
	var otelTraceID oteltrace.TraceID
	var otelSpanID oteltrace.SpanID
	var baggage map[string]string

	iterFn := func(k, v string) error {
		switch k = strings.ToLower(k); k {
		case fieldNameTraceID:
			var err error
			traceID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return errors.Errorf("invalid trace id: %s", v)
			}
		case fieldNameSpanID:
			var err error
			spanID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return errors.Errorf("invalid span id: %s", v)
			}
		case fieldNameOtelTraceID:
			var err error
			otelTraceID, err = oteltrace.TraceIDFromHex(v)
			if err != nil {
				return err
			}
		case fieldNameOtelSpanID:
			var err error
			otelSpanID, err = oteltrace.SpanIDFromHex(v)
			if err != nil {
				return err
			}
		default:
			if strings.HasPrefix(k, prefixBaggage) {
				if baggage == nil {
					baggage = make(map[string]string)
				}
				baggage[strings.TrimPrefix(k, prefixBaggage)] = v
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

	var otelCtx oteltrace.SpanContext
	if otelTraceID.IsValid() && otelSpanID.IsValid() {
		otelCtx = otelCtx.WithRemote(true).WithTraceID(otelTraceID).WithSpanID(otelSpanID)
	}

	return SpanMeta{
		traceID:       traceID,
		spanID:        spanID,
		otelCtx:       otelCtx,
		recordingType: recordingType,
		Baggage:       baggage,
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

// makeOtelSpan creates an OpenTelemetry span. If either of localParent or
// remoteParent are not empty, the returned span will be a child of that parent.
//
// End() needs to be called on the returned span once the span is complete.
func makeOtelSpan(
	otelTr oteltrace.Tracer,
	opName string,
	localParent oteltrace.Span,
	remoteParent oteltrace.SpanContext,
	refType spanReferenceType,
	startTime time.Time,
	kind oteltrace.SpanKind,
) oteltrace.Span {
	ctx := context.Background()
	var parentSpanContext oteltrace.SpanContext
	if localParent != nil {
		parentSpanContext = localParent.SpanContext()
	} else if remoteParent.IsValid() {
		parentSpanContext = remoteParent
	}

	opts := make([]oteltrace.SpanStartOption, 0, 3)
	opts = append(opts, oteltrace.WithTimestamp(startTime), oteltrace.WithSpanKind(kind))
	switch refType {
	case childOfRef:
		// If a parent was passed in, put it in the context. That's where Start()
		// will take it from.
		if parentSpanContext.IsValid() {
			ctx = oteltrace.ContextWithSpanContext(ctx, parentSpanContext)
		}

	case followsFromRef:
		opts = append(opts, oteltrace.WithLinks(oteltrace.Link{
			SpanContext: parentSpanContext,
			Attributes:  followsFromAttribute,
		}))
	default:
		panic(fmt.Sprintf("unsupported span reference type: %v", refType))
	}

	_ /* ctx */, sp := otelTr.Start(ctx, opName, opts...)
	return sp
}
