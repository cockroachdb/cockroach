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
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
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

const (
	// maxRecordedSpansPerTrace limits the number of spans per recording, keeping
	// recordings from getting too large.
	maxRecordedSpansPerTrace = 1000
	// maxRecordedBytesPerSpan limits the size of logs and structured in a span;
	// use a comfortable limit.
	maxLogBytesPerSpan        = 256 * (1 << 10) // 256 KiB
	maxStructuredBytesPerSpan = 10 * (1 << 10)  // 10 KiB
	// maxSpanRegistrySize limits the number of local root spans tracked in
	// a Tracer's registry.
	maxSpanRegistrySize = 5000
	// maxLogsPerSpanExternal limits the number of logs in a Span for external
	// tracers (net/trace, OpenTelemetry); use a comfortable limit.
	maxLogsPerSpanExternal = 1000
	// maxSnapshots limits the number of snapshots that a Tracer will hold in
	// memory. Beyond this limit, each new snapshot evicts the oldest one.
	maxSnapshots = 10
)

// These constants are used to form keys to represent tracing context
// information in "carriers" to be transported across RPC boundaries.
const (
	prefixTracerState = "crdb-tracer-"

	fieldNameTraceID = prefixTracerState + "traceid"
	fieldNameSpanID  = prefixTracerState + "spanid"
	// fieldNameRecordingType will contain the desired type of trace recording.
	fieldNameRecordingType = "rec"

	// fieldNameOtel{TraceID,SpanID} will contain the OpenTelemetry span info, hex
	// encoded.
	fieldNameOtelTraceID = prefixTracerState + "otel_traceid"
	fieldNameOtelSpanID  = prefixTracerState + "otel_spanid"

	// fieldNameDeprecatedVerboseTracing is the carrier key indicating that the trace
	// has verbose recording enabled. It means that a) spans derived from this one
	// will not be no-op spans and b) they will start recording.
	//
	// The key is named the way it is for backwards compatibility reasons.
	// TODO(andrei): remove in 22.2, once we no longer need to set this key for
	// compatibility with 21.2.
	fieldNameDeprecatedVerboseTracing = "crdb-baggage-sb"

	spanKindTagKey = "span.kind"
)

// TODO(davidh): Once performance issues around redaction are
// resolved via #58610, this setting can be removed so that all traces
// have redactability enabled.
var enableTraceRedactable = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"trace.redactable.enabled",
	"set to true to enable redactability for unstructured events "+
		"in traces and to redact traces sent to tenants. "+
		"Set to false to coarsely mark unstructured events as redactable "+
		" and eliminate them from tenant traces.",
	false,
)

var enableNetTrace = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"trace.debug.enable",
	"if set, traces for recent requests can be seen at https://<ui>/debug/requests",
	false,
).WithPublic()

var openTelemetryCollector = settings.RegisterValidatedStringSetting(
	settings.TenantWritable,
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
	settings.TenantWritable,
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
	settings.TenantWritable,
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

// EnableActiveSpansRegistry controls Tracers configured as
// WithTracingMode(TracingModeFromEnv) (which is the default). When enabled,
// spans are allocated and registered with the active spans registry until
// finished. When disabled, span creation is short-circuited for a small
// performance improvement.
var EnableActiveSpansRegistry = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"trace.span_registry.enabled",
	"if set, ongoing traces can be seen at https://<ui>/debug/tracez",
	envutil.EnvOrDefaultBool("COCKROACH_REAL_SPANS", true),
).WithPublic()

// panicOnUseAfterFinish, if set, causes use of a span after Finish() to panic
// if detected.
var panicOnUseAfterFinish = buildutil.CrdbTestBuild ||
	envutil.EnvOrDefaultBool("COCKROACH_CRASH_ON_SPAN_USE_AFTER_FINISH", false)

// debugUseAfterFinish controls whether to debug uses of Span values after finishing.
// FOR DEBUGGING ONLY. This will slow down the program.
var debugUseAfterFinish = envutil.EnvOrDefaultBool("COCKROACH_DEBUG_SPAN_USE_AFTER_FINISH", false)

// reuseSpans controls whether spans can be re-allocated after they've been
// Finish()ed. See Tracer.spanReusePercent for details.
//
// Span reuse is incompatible with the deadlock detector because, with reuse,
// the span mutexes end up being reused and locked repeatedly in random order on
// the same goroutine. This erroneously looks like a potential deadlock to the
// detector.
var reuseSpans = !syncutil.DeadlockEnabled && envutil.EnvOrDefaultBool("COCKROACH_REUSE_TRACING_SPANS", true)

// detectSpanRefLeaks enables the detection of Span reference leaks - i.e.
// failures to decrement a Span's reference count. If detection is enabled, such
// bugs will cause crashes. Otherwise, such bugs silently prevent reallocations
// of spans, at a memory cost.
//
// The detection mechanism uses runtime finalizers; they're disabled under race
// builds elsewhere.
var detectSpanRefLeaks = buildutil.CrdbTestBuild

// TracingMode specifies whether span creation is enabled or disabled by
// default, when other conditions that don't explicitly turn tracing on don't
// apply.
type TracingMode int

const (
	// TracingModeFromEnv configures tracing according to enableTracingByDefault.
	TracingModeFromEnv TracingMode = iota
	// TracingModeOnDemand means that Spans will no be created unless there's a
	// particular reason to create them (i.e. a span being created with
	// WithForceRealSpan(), a net.Trace or OpenTelemetry tracers attached).
	TracingModeOnDemand
	// TracingModeActiveSpansRegistry means that Spans are always created.
	// Currently-open spans are accessible through the active spans registry.
	//
	// If no net.Trace/OpenTelemetry tracer is attached, spans do not record
	// events by default (i.e. do not accumulate log messages via Span.Record() or
	// a history of finished child spans). In order for a span to record events,
	// it must be started with the WithRecording() option).
	TracingModeActiveSpansRegistry
)

// SpanReusePercentOpt is the option produced by WithSpanReusePercent(),
// configuring the Tracer's span reuse policy.
type SpanReusePercentOpt uint

var _ TracerOption = SpanReusePercentOpt(0)

// WithSpanReusePercent configures the Tracer span reuse ratio, overriding the
// environmental default.
func WithSpanReusePercent(percent uint32) TracerOption {
	if percent > 100 {
		panic(fmt.Sprintf("invalid percent: %d", percent))
	}
	return SpanReusePercentOpt(percent)
}

func (s SpanReusePercentOpt) apply(opt *tracerOptions) {
	val := uint32(s)
	opt.spanReusePercent = &val
}

// spanReusePercent controls the span reuse probability for Tracers that are not
// explicitly configured with a reuse probability. Tests vary this away from the
// default of 100% in order to make the use-after-Finish detection reliable (see
// panicOnUseAfterFinish).
var spanReusePercent = util.ConstantWithMetamorphicTestRange(
	"span-reuse-rate",
	100, /* defaultValue - always reuse spans. This turns to 0 if reuseSpans is not set. */
	0,   /* metamorphic min */
	101, /* metamorphic max */
)

// Tracer implements tracing requests. It supports:
//
//  - forwarding events to x/net/trace instances
//
//  - recording traces. Recorded events can be retrieved at any time.
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
	// Preallocated noopSpans, used to avoid creating spans when we are not using
	// x/net/trace or OpenTelemetry and we are not recording.
	noopSpan        *Span
	sterileNoopSpan *Span

	// backardsCompatibilityWith211, if set, makes the Tracer
	// work with 21.1 remote nodes.
	//
	// Accessed atomically.
	backwardsCompatibilityWith211 int64

	// True if tracing to the debug/requests endpoint. Accessed via t.useNetTrace().
	_useNetTrace int32 // updated atomically

	// True if we would like spans created from this tracer to be marked
	// as redactable. This will make unstructured events logged to those
	// spans redactable.
	// Currently, this is used to mark spans as redactable and to redact
	// them at the network boundary from KV.
	_redactable int32 // accessed atomically

	// Pointer to an OpenTelemetry tracer used as a "shadow tracer", if any. If
	// not nil, the respective *otel.Tracer will be used to create mirror spans
	// for all spans that the parent Tracer creates.
	otelTracer unsafe.Pointer

	// activeSpansRegistryEnabled controls whether spans are created and
	// registered with activeSpansRegistry until they're Finish()ed. If not
	// enabled, span creation is generally a no-op unless a recording span is
	// explicitly requested.
	activeSpansRegistryEnabled bool
	// activeSpans is a map that references all non-Finish'ed local root spans,
	// i.e. those for which no WithParent(<non-nil>) option was supplied.
	activeSpansRegistry *SpanRegistry
	snapshotsMu         struct {
		syncutil.Mutex
		// snapshots stores the activeSpansRegistry snapshots taken during the
		// Tracer's lifetime. The ring buffer will contain snapshots with contiguous
		// IDs, from the oldest one to <oldest id> + maxSnapshots - 1.
		snapshots ring.Buffer // snapshotWithID
	}

	testingMu               syncutil.Mutex // protects testingRecordAsyncSpans
	testingRecordAsyncSpans bool           // see TestingRecordAsyncSpans

	// panicOnUseAfterFinish configures the Tracer to crash when a Span is used
	// after it was previously Finish()ed. Crashing is best-effort if reuseSpan is
	// set - use-after-Finish detection doesn't work across span re-allocations.
	panicOnUseAfterFinish bool
	// debugUseAfterFinish configures the Tracer to collect expensive extra info
	// to help debug use-after-Finish() bugs - stack traces will be collected and
	// stored on every Span.Finish().
	debugUseAfterFinish bool

	// spanReusePercent controls the probability that a Finish()ed Span is made
	// available for reuse. A value of 100 configures the Tracer to always reuse
	// spans; a value of 0 configures it to not reuse any spans. Values in between
	// will cause spans to be reused randomly.
	//
	// Span reuse works by pooling finished spans in a sync.Pool and re-allocate
	// them in order to avoid large dynamic memory allocations. When reusing
	// spans, buggy code that uses previously-finished spans can result in trace
	// corruption, if the span in question has been reallocated by the time it's
	// erroneously used.
	//
	// Span reuse saves dynamic memory allocations for span creation. Creating a
	// span generally still needs to allocate the Context that the span is
	// associated with; contexts cannot be reused because they are immutable and
	// don't have a clear lifetime. Before span reuse was introduced, we had a way
	// to allocate a Span and its Context at once. Compared to that, span reuse
	// doesn't save on the absolute number of allocations, but saves on the size
	// of these allocations: spans are large and contexts are small. This amounts
	// to savings of around 10KB worth of heap for simple queries.
	//
	// When a span is reused, use-after-Finish detection (as configured by
	// panicOnUseAfterFinish) becomes unreliable. That's why tests metamorphically
	// set this to less than 100.
	spanReusePercent uint32
	// spanPool holds spanAllocHelper's. If reuseSpans is set, spans are
	// allocated through this pool to reduce dynamic memory allocations.
	spanPool sync.Pool
	// spansCreated/spansAllocated counts how many spans have been created and how
	// many have been allocated (i.e. not reused through the spanPool) since the
	// last time TestingGetStatsAndReset() was called. These counters are only
	// incremented if the MaintainAllocationCounters testing knob was set.
	spansCreated, spansAllocated int32 // atomics

	testing TracerTestingKnobs

	// stack is populated in NewTracer and is printed in assertions related to
	// mixing tracers.
	stack string
	// closed is set on Close().
	_closed int32 // accessed atomically
}

// SpanRegistry is a map that references all non-Finish'ed local root spans,
// i.e. those for which no WithLocalParent(<non-nil>) option was supplied. The
// map is keyed on the span ID, which is deterministically unique.
//
// In normal operation, a local root crdbSpan is inserted on creation and
// removed on .Finish().
//
// The map can be introspected by `Tracer.VisitSpans`. A Span can also be
// retrieved from its ID by `Tracer.GetActiveSpanByID`.
type SpanRegistry struct {
	mu struct {
		syncutil.Mutex
		m map[tracingpb.SpanID]*crdbSpan
	}
}

func makeSpanRegistry() *SpanRegistry {
	r := &SpanRegistry{}
	r.mu.m = make(map[tracingpb.SpanID]*crdbSpan)
	return r
}

func (r *SpanRegistry) removeSpanLocked(id tracingpb.SpanID) {
	delete(r.mu.m, id)
}

func (r *SpanRegistry) addSpan(s *crdbSpan) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addSpanLocked(s)
}

func (r *SpanRegistry) addSpanLocked(s *crdbSpan) {
	// Ensure that the registry does not grow unboundedly in case there is a leak.
	// When the registry reaches max size, each new span added kicks out an
	// arbitrary existing span. We rely on map iteration order here to make this
	// cheap.
	if len(r.mu.m) == maxSpanRegistrySize {
		for k := range r.mu.m {
			delete(r.mu.m, k)
			break
		}
	}
	r.mu.m[s.spanID] = s
}

// getSpanByID looks up a span in the registry. Returns nil if not found.
func (r *SpanRegistry) getSpanByID(id tracingpb.SpanID) RegistrySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	crdbSpan, ok := r.mu.m[id]
	if !ok {
		// Avoid returning a typed nil pointer.
		return nil
	}
	return crdbSpan
}

// VisitSpans calls the visitor callback for every local root span in the
// registry. Iterations stops when the visitor returns an error. If that error
// is iterutils.StopIteration(), then VisitSpans() returns nil.
//
// The callback should not hold on to the span after it returns.
func (r *SpanRegistry) VisitSpans(visitor func(span RegistrySpan) error) error {
	// Take a snapshot of the registry and release the lock.
	r.mu.Lock()
	spans := make([]spanRef, 0, len(r.mu.m))
	for _, sp := range r.mu.m {
		// We'll keep the spans alive while we're visiting them below.
		spans = append(spans, makeSpanRef(sp.sp))
	}
	r.mu.Unlock()

	defer func() {
		for i := range spans {
			spans[i].release()
		}
	}()

	for _, sp := range spans {
		if err := visitor(sp.Span.i.crdb); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

// testingAll returns (pointers to) all the spans in the registry, in an
// arbitrary order. Since spans can generally finish at any point and use of a
// finished span is not permitted, this method is only suitable for tests.
func (r *SpanRegistry) testingAll() []*crdbSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := make([]*crdbSpan, 0, len(r.mu.m))
	for _, sp := range r.mu.m {
		res = append(res, sp)
	}
	return res
}

// swap atomically swaps a span with its children. This is called when a parent
// finishes for promoting its (still open) children into the registry. Before
// removing the parent from the registry, the children are accessible in the
// registry through that parent; if we didn't do this swap when the parent is
// removed, the children would not be part of the registry anymore.
//
// The children are passed as spanRef's, so they're not going to be reallocated
// concurrently with this call. swap takes ownership of the spanRefs, and will
// release() them.
func (r *SpanRegistry) swap(parentID tracingpb.SpanID, children []spanRef) {
	r.mu.Lock()
	r.removeSpanLocked(parentID)
	for _, c := range children {
		sp := c.Span.i.crdb
		sp.withLock(func() {
			if !sp.mu.finished {
				r.addSpanLocked(sp)
			}
		})
	}
	r.mu.Unlock()
	for _, c := range children {
		c.release()
	}
}

// TracerTestingKnobs contains knobs for a Tracer.
type TracerTestingKnobs struct {
	// Clock allows the time source for spans to be controlled.
	Clock timeutil.TimeSource
	// UseNetTrace, if set, forces the Traces to always create spans which record
	// to net.Trace objects.
	UseNetTrace bool
	// MaintainAllocationCounters, if set,  configures the Tracer to maintain
	// counters about span creation. See Tracer.GetStatsAndReset().
	MaintainAllocationCounters bool
	// ReleaseSpanToPool, if set, if called just before a span is put in the
	// sync.Pool for reuse. If the hook returns false, the span will not be put in
	// the pool.
	ReleaseSpanToPool func(*Span) bool
}

// Redactable returns true if the tracer is configured to emit
// redactable logs. This value will affect the redactability of messages
// from already open Spans.
func (t *Tracer) Redactable() bool {
	return atomic.LoadInt32(&t._redactable) != 0
}

// SetRedactable changes the redactability of the Tracer. This
// affects any future trace spans created.
func (t *Tracer) SetRedactable(to bool) {
	var n int32
	if to {
		n = 1
	}
	atomic.StoreInt32(&t._redactable, n)
}

// NewTracer creates a Tracer with default options.
//
// See NewTracerWithOpt() for controlling various configuration options.
func NewTracer() *Tracer {
	var defaultSpanReusePercent uint32
	if reuseSpans {
		defaultSpanReusePercent = uint32(spanReusePercent)
	} else {
		defaultSpanReusePercent = 0
	}

	t := &Tracer{
		stack:                      string(debug.Stack()),
		activeSpansRegistryEnabled: true,
		activeSpansRegistry:        makeSpanRegistry(),
		// These might be overridden in NewTracerWithOpt.
		panicOnUseAfterFinish: panicOnUseAfterFinish,
		debugUseAfterFinish:   debugUseAfterFinish,
		spanReusePercent:      defaultSpanReusePercent,
	}

	t.spanPool = sync.Pool{
		New: func() interface{} {
			if t.testing.MaintainAllocationCounters {
				atomic.AddInt32(&t.spansAllocated, 1)
			}
			h := new(spanAllocHelper)
			// Read-only span fields are assigned here. The rest are initialized in Span.reset().
			h.span.helper = h
			sp := &h.span
			sp.i.tracer = t
			c := &h.crdbSpan
			*c = crdbSpan{
				tracer: t,
				sp:     sp,
			}
			sp.i.crdb = c
			return h
		},
	}
	t.noopSpan = &Span{i: spanInner{tracer: t}}
	t.sterileNoopSpan = &Span{i: spanInner{tracer: t, sterile: true}}
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
	if o.useAfterFinishOpt != nil {
		t.panicOnUseAfterFinish = o.useAfterFinishOpt.panicOnUseAfterFinish
		t.debugUseAfterFinish = o.useAfterFinishOpt.debugUseAfterFinish
	}
	if o.spanReusePercent != nil {
		t.spanReusePercent = *o.spanReusePercent
	}
	t.testing = o.knobs
	t.activeSpansRegistryEnabled = o.tracingDefault != TracingModeOnDemand
	if o.sv != nil {
		t.configure(ctx, o.sv, o.tracingDefault)
	}
	return t
}

// tracerOptions groups configuration for Tracer construction.
type tracerOptions struct {
	sv                *settings.Values
	knobs             TracerTestingKnobs
	tracingDefault    TracingMode
	useAfterFinishOpt *useAfterFinishOpt
	// spanReusePercent, if not nil, controls the probability of span reuse. A
	// negative value indicates that the default should come from the environment.
	// If nil, the probability comes from the environment (test vs production).
	spanReusePercent *uint32
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

type tracingModeOpt TracingMode

var _ TracerOption = tracingModeOpt(TracingModeFromEnv)

func (o tracingModeOpt) apply(opt *tracerOptions) {
	opt.tracingDefault = TracingMode(o)
}

// WithTracingMode configures the Tracer's tracing mode.
func WithTracingMode(opt TracingMode) TracerOption {
	return tracingModeOpt(opt)
}

type useAfterFinishOpt struct {
	panicOnUseAfterFinish bool
	debugUseAfterFinish   bool
}

var _ TracerOption = useAfterFinishOpt{}

func (o useAfterFinishOpt) apply(opt *tracerOptions) {
	opt.useAfterFinishOpt = &o
}

// WithUseAfterFinishOpt allows control over the Tracer's behavior when a Span
// is used after it had been Finish()ed.
//
// panicOnUseAfterFinish configures the Tracer to panic when it detects that any
// method on a Span is called after that Span was Finish()ed. If not set, such
// uses are tolerated as silent no-ops.
//
// debugUseAfterFinish configures the Tracer to collect stack traces on every
// Span.Finish() call. These are presented when the finished span is reused.
// This option is expensive. It's invalid to debugUseAfterFinish if
// panicOnUseAfterFinish is not set.
func WithUseAfterFinishOpt(panicOnUseAfterFinish, debugUseAfterFinish bool) TracerOption {
	if debugUseAfterFinish && !panicOnUseAfterFinish {
		panic("it is nonsensical to set debugUseAfterFinish when panicOnUseAfterFinish is not set, " +
			"as the collected stacks will never be used")
	}
	return useAfterFinishOpt{
		panicOnUseAfterFinish: panicOnUseAfterFinish,
		debugUseAfterFinish:   debugUseAfterFinish,
	}
}

// configure sets up the Tracer according to the cluster settings (and keeps
// it updated if they change).
func (t *Tracer) configure(ctx context.Context, sv *settings.Values, tracingDefault TracingMode) {
	// traceProvider is captured by the function below.
	var traceProvider *otelsdk.TracerProvider

	// reconfigure will be called every time a cluster setting affecting tracing
	// is updated.
	reconfigure := func(ctx context.Context) {
		jaegerAgentAddr := jaegerAgent.Get(sv)
		otlpCollectorAddr := openTelemetryCollector.Get(sv)
		zipkinAddr := ZipkinCollector.Get(sv)
		enableRedactable := enableTraceRedactable.Get(sv)

		switch tracingDefault {
		case TracingModeFromEnv:
			t.activeSpansRegistryEnabled = EnableActiveSpansRegistry.Get(sv)
		case TracingModeOnDemand:
			t.activeSpansRegistryEnabled = false
		case TracingModeActiveSpansRegistry:
			t.activeSpansRegistryEnabled = true
		default:
			panic(fmt.Sprintf("unrecognized tracing option: %v", tracingDefault))
		}

		t.SetRedactable(enableRedactable)

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

	EnableActiveSpansRegistry.SetOnChange(sv, reconfigure)
	enableNetTrace.SetOnChange(sv, reconfigure)
	openTelemetryCollector.SetOnChange(sv, reconfigure)
	ZipkinCollector.SetOnChange(sv, reconfigure)
	jaegerAgent.SetOnChange(sv, reconfigure)
	enableTraceRedactable.SetOnChange(sv, reconfigure)
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
	return t.testing.UseNetTrace || atomic.LoadInt32(&t._useNetTrace) != 0
}

// Close cleans up any resources associated with a Tracer.
func (t *Tracer) Close() {
	atomic.StoreInt32(&t._closed, 1)
	// Clean up the OpenTelemetry tracer, if any.
	t.SetOpenTelemetryTracer(nil)
}

// closed returns true if Close() has been called.
func (t *Tracer) closed() bool {
	return atomic.LoadInt32(&t._closed) == 1
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

// spanAllocHelper
type spanAllocHelper struct {
	span     Span
	crdbSpan crdbSpan
	// Pre-allocated buffers for the span.
	tagsAlloc             [3]attribute.KeyValue
	childrenAlloc         [4]childRef
	structuredEventsAlloc [3]interface{}
}

// newSpan allocates a span using the Tracer's sync.Pool. A span that was
// previously Finish()ed be returned if the Tracer is configured for Span reuse.
//+(...) must be called on the returned span before further use.
func (t *Tracer) newSpan(
	traceID tracingpb.TraceID,
	spanID tracingpb.SpanID,
	operation string,
	goroutineID uint64,
	startTime time.Time,
	logTags *logtags.Buffer,
	kind oteltrace.SpanKind,
	otelSpan oteltrace.Span,
	netTr trace.Trace,
	sterile bool,
) *Span {
	if t.testing.MaintainAllocationCounters {
		atomic.AddInt32(&t.spansCreated, 1)
	}
	h := t.spanPool.Get().(*spanAllocHelper)
	h.span.reset(
		traceID, spanID, operation, goroutineID,
		startTime, logTags, kind,
		otelSpan, netTr, sterile)
	return &h.span
}

// releaseSpanToPool makes sp available for re-allocation. If the Tracer was not
// configured for span reuse, this is a no-op.
func (t *Tracer) releaseSpanToPool(sp *Span) {
	switch t.spanReusePercent {
	case 0:
		return
	case 100:
		break
	default:
		rnd := randutil.FastUint32() % 100
		if rnd >= t.spanReusePercent {
			return
		}
	}

	// Nil out various slices inside the span to make them available for GC (both
	// the backing storage for the slices and the individual elements in the
	// slices). The backing arrays might be shared with the spanAllocHelper, in
	// which case they can't be GCed (and we don't want them to be GC'ed either
	// because we'll reuse them when the span is re-allocated from the pool).
	// We'll zero-out the spanAllocHelper, though, to make all the elements
	// available for GC.
	c := sp.i.crdb
	// Nobody is supposed to have a reference to the span at this point, but let's
	// take the lock anyway to protect against buggy clients accessing the span
	// after Finish().
	c.mu.Lock()
	c.mu.openChildren = nil
	c.mu.recording.finishedChildren = nil
	c.mu.tags = nil
	c.mu.recording.logs.Discard()
	c.mu.recording.structured.Discard()
	c.mu.Unlock()

	// Zero out the spanAllocHelper buffers to make the elements inside the
	// arrays, if any, available for GC.
	h := sp.helper
	h.tagsAlloc = [3]attribute.KeyValue{}
	h.childrenAlloc = [4]childRef{}
	h.structuredEventsAlloc = [3]interface{}{}

	release := true
	if fn := t.testing.ReleaseSpanToPool; fn != nil {
		release = fn(sp)
	}
	if release {
		t.spanPool.Put(h)
	}
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
		if o == nil {
			continue
		}
		opts = o.apply(opts)
	}

	return t.startSpanGeneric(ctx, operationName, opts)
}

// AlwaysTrace returns true if operations should be traced regardless of the
// context.
func (t *Tracer) AlwaysTrace() bool {
	if t.activeSpansRegistryEnabled {
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

	if !opts.Parent.empty() {
		// If we don't panic, opts.Parent will be moved into the child, and this
		// release() will be a no-op.
		defer opts.Parent.release()

		if !opts.RemoteParent.Empty() {
			panic("can't specify both Parent and RemoteParent")
		}
		if opts.Parent.IsSterile() {
			// A sterile parent should have been optimized away by
			// WithParent.
			panic("invalid sterile parent")
		}
		if s := opts.Parent.Tracer(); s != t {
			// Creating a child with a different Tracer than the parent is not allowed
			// because it would become unclear which active span registry the new span
			// should belong to. In particular, the child could end up in the parent's
			// registry if the parent Finish()es before the child, and then it would
			// be leaked because Finish()ing the child would attempt to remove the
			// span from the child tracer's registry.
			panic(fmt.Sprintf(`attempting to start span with parent from different Tracer.
parent operation: %s, tracer created at:

%s

child operation: %s, tracer created at:
%s`,
				opts.Parent.OperationName(), s.stack, opName, t.stack))
		}
		if opts.Parent.IsNoop() {
			// If the parent is a no-op, we'll create a root span.
			opts.Parent.decRef()
			opts.Parent = spanRef{}
		}
	}

	// Are we tracing everything, or have a parent, or want a real span, or were
	// asked for a recording? Then we create a real trace span. In all other
	// cases, a noop span will do.
	if !(t.AlwaysTrace() || opts.parentTraceID() != 0 || opts.ForceRealSpan || opts.recordingType() != RecordingOff) {
		if !opts.Sterile {
			return maybeWrapCtx(ctx, t.noopSpan)
		}
		return maybeWrapCtx(ctx, t.sterileNoopSpan)
	}

	if opts.LogTags == nil {
		opts.LogTags = logtags.FromContext(ctx)
	}

	if opts.LogTags == nil && !opts.Parent.empty() {
		// If no log tags are specified in the options, use the parent
		// span's, if any. This behavior is the reason logTags are
		// fundamentally different from tags, which are strictly per span,
		// for better or worse.
		opts.LogTags = opts.Parent.i.crdb.logTags
	}

	startTime := time.Now()

	// First, create any external spans that we may need (OpenTelemetry, net/trace).
	// We do this early so that they are available when we construct the main Span,
	// which makes it easier to avoid one-offs when populating the tags items for
	// the top-level Span.
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
		traceID = tracingpb.TraceID(randutil.FastInt63())
	}
	spanID := tracingpb.SpanID(randutil.FastInt63())

	s := t.newSpan(
		traceID, spanID, opName, uint64(goid.Get()),
		startTime, opts.LogTags, opts.SpanKind,
		otelSpan, netTr, opts.Sterile)

	s.i.crdb.enableRecording(opts.recordingType())

	s.i.crdb.parentSpanID = opts.parentSpanID()

	var localRoot bool
	{
		// If a parent is specified, link the newly created Span to the parent.
		// While the parent is alive, the child will be part of the
		// activeSpansRegistry indirectly through this link. If the parent is
		// recording, it will also use this link to collect the child's recording.
		//
		// NB: (!opts.Parent.empty() && opts.Parent.i.crdb == nil) is not possible at
		// the moment, but let's not rely on that.
		if !opts.Parent.empty() && opts.Parent.i.crdb != nil {

			// Panic if the parent has already been finished, if configured to do so.
			// If the parent has finished and we're configured not to panic,
			// addChildLocked we'll return false below and we'll deal with it.
			_ = opts.Parent.detectUseAfterFinish()

			parent := opts.Parent.i.crdb
			if s.i.crdb == parent {
				panic(fmt.Sprintf("attempting to link a child to itself: %s", s.i.crdb.operation))
			}

			// We're going to hold the parent's lock while we link both the parent
			// to the child and the child to the parent.
			parent.withLock(func() {
				added := parent.addChildLocked(s, !opts.ParentDoesNotCollectRecording)
				if added {
					localRoot = false
					// We take over the reference in opts.Parent. The child will release
					// it once when it nils out s.i.crdb.mu.parent (i.e. when either the
					// parent of the child finish). Note that some methods on opts cannot
					// be used from this moment on.
					s.i.crdb.mu.parent = opts.Parent.move()
				} else {
					// The parent has already finished, so make this "child" a root.
					localRoot = true
				}
			})
		} else {
			localRoot = true
		}
	}

	// If the span is a local root, put it into the registry of active local root
	// spans. Span.Finish will take care of removing it.
	if localRoot {
		t.activeSpansRegistry.addSpan(s.i.crdb)
	}

	return maybeWrapCtx(ctx, s)
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
func (t *Tracer) InjectMetaInto(sm SpanMeta, carrier Carrier) {
	if sm.Empty() {
		// Fast path when tracing is disabled. ExtractMetaFrom will accept an
		// empty map as a noop context.
		return
	}
	// If the span has been marked as not wanting children, we don't propagate any
	// information about it through the carrier (the point of propagating span
	// info is to create a child from it).
	if sm.sterile {
		return
	}

	if sm.otelCtx.TraceID().IsValid() {
		carrier.Set(fieldNameOtelTraceID, sm.otelCtx.TraceID().String())
		carrier.Set(fieldNameOtelSpanID, sm.otelCtx.SpanID().String())
	}

	compatMode := atomic.LoadInt64(&t.backwardsCompatibilityWith211) == 1

	// For compatibility with 21.1, we don't want to propagate the traceID when
	// we're not recording. A 21.1 node interprets a traceID as wanting structured
	// recording (or verbose recording if fieldNameDeprecatedVerboseTracing is also
	// set).
	if compatMode && sm.recordingType == RecordingOff {
		return
	}

	carrier.Set(fieldNameTraceID, strconv.FormatUint(uint64(sm.traceID), 16))
	carrier.Set(fieldNameSpanID, strconv.FormatUint(uint64(sm.spanID), 16))
	carrier.Set(fieldNameRecordingType, sm.recordingType.ToCarrierValue())

	if compatMode && sm.recordingType == RecordingVerbose {
		carrier.Set(fieldNameDeprecatedVerboseTracing, "1")
	}
}

var noopSpanMeta = SpanMeta{}

// ExtractMetaFrom is used to deserialize a span metadata (if any) from the
// given Carrier. This, alongside InjectMetaFrom, can be used to carry span
// metadata across process boundaries. See serializationFormat for more details.
func (t *Tracer) ExtractMetaFrom(carrier Carrier) (SpanMeta, error) {
	var traceID tracingpb.TraceID
	var spanID tracingpb.SpanID
	var otelTraceID oteltrace.TraceID
	var otelSpanID oteltrace.SpanID
	var recordingTypeExplicit bool
	var recordingType RecordingType

	iterFn := func(k, v string) error {
		switch k = strings.ToLower(k); k {
		case fieldNameTraceID:
			var err error
			id, err := strconv.ParseUint(v, 16, 64)
			if err != nil {
				return errors.Errorf("invalid trace id: %s", v)
			}
			traceID = tracingpb.TraceID(id)
		case fieldNameSpanID:
			var err error
			id, err := strconv.ParseUint(v, 16, 64)
			if err != nil {
				return errors.Errorf("invalid span id: %s", v)
			}
			spanID = tracingpb.SpanID(id)
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
		case fieldNameRecordingType:
			recordingTypeExplicit = true
			recordingType = RecordingTypeFromCarrierValue(v)
		case fieldNameDeprecatedVerboseTracing:
			// Compatibility with 21.2.
			if !recordingTypeExplicit {
				recordingType = RecordingVerbose
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

	if !recordingTypeExplicit && recordingType == RecordingOff {
		// A 21.1 node (or a 21.2 mode running in backwards-compatibility mode)
		// that passed a TraceID but not fieldNameDeprecatedVerboseTracing wants the
		// structured events.
		recordingType = RecordingStructured
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
		// The sterile field doesn't make it across the wire. The simple fact that
		// there was any tracing info in the carrier means that the parent span was
		// not sterile.
		sterile: false,
	}, nil
}

// RegistrySpan is the interface used by clients of the active span registry.
type RegistrySpan interface {
	// TraceID returns an identifier for the trace that this span is part of.
	TraceID() tracingpb.TraceID

	// GetFullRecording returns the recording of the trace rooted at this span.
	//
	// This includes the recording of child spans created with the
	// WithDetachedRecording option. In other situations, the recording of such
	// children is not included in the parent's recording but, in the case of the
	// span registry, we want as much information as possible to be included.
	GetFullRecording(recType RecordingType) Recording

	// SetRecordingType sets the recording mode of the span and its children,
	// recursively. Setting it to RecordingOff disables further recording.
	// Everything recorded so far remains in memory.
	SetRecordingType(to RecordingType)
}

var _ RegistrySpan = &crdbSpan{}

// GetActiveSpanByID retrieves any active root span given its ID.
func (t *Tracer) GetActiveSpanByID(spanID tracingpb.SpanID) RegistrySpan {
	return t.activeSpansRegistry.getSpanByID(spanID)
}

// VisitSpans calls the visitor callback for every local root span in the
// registry. Iterations stops when the visitor returns an error. If that error
// is iterutils.StopIteration(), then VisitSpans() returns nil.
//
// The callback should not hold on to the span after it returns.
func (t *Tracer) VisitSpans(visitor func(span RegistrySpan) error) error {
	return t.activeSpansRegistry.VisitSpans(visitor)
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

// SetBackwardsCompatibilityWith211 toggles the compatibility mode.
func (t *Tracer) SetBackwardsCompatibilityWith211(to bool) {
	if to {
		atomic.StoreInt64(&t.backwardsCompatibilityWith211, 1)
	} else {
		atomic.StoreInt64(&t.backwardsCompatibilityWith211, 0)
	}
}

// PanicOnUseAfterFinish returns true if the Tracer is configured to crash when
// a Span is used after it was previously Finish()ed. Crashing is supposed to be
// best-effort as, in the future, reliably detecting use-after-Finish might not
// be possible (i.e. it's not possible if the Span is reused for a different
// operation before the use-after-Finish occurs).
func (t *Tracer) PanicOnUseAfterFinish() bool {
	return t.panicOnUseAfterFinish
}

// SpanRegistry exports the registry containing all currently-open spans.
func (t *Tracer) SpanRegistry() *SpanRegistry {
	return t.activeSpansRegistry
}

// TestingGetStatsAndReset returns the number of spans created and allocated
// since the previous TestingGetStatsAndReset() call. The difference
// created-allocated indicates how many times spans have been reused through the
// Tracer's pool.
//
// Panics if the MaintainAllocationCounters testing knob was not set.
func (t *Tracer) TestingGetStatsAndReset() (int, int) {
	if !t.testing.MaintainAllocationCounters {
		panic("GetStatsAndReset() needs the Tracer to have been configured with MaintainAllocationCounters")
	}
	created := atomic.SwapInt32(&t.spansCreated, 0)
	allocs := atomic.SwapInt32(&t.spansAllocated, 0)
	return int(created), int(allocs)
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
	opts := make([]SpanOption, 0, 3)
	if sp.Tracer().ShouldRecordAsyncSpans() {
		opts = append(opts, WithParent(sp))
	} else {
		opts = append(opts, WithParent(sp), WithDetachedRecording())
	}
	opts = append(opts, WithFollowsFrom())
	return sp.Tracer().StartSpanCtx(ctx, opName, opts...)
}

// EnsureForkSpan is like ForkSpan except that, if there is no span in ctx, it
// creates a root span.
func EnsureForkSpan(ctx context.Context, tr *Tracer, opName string) (context.Context, *Span) {
	sp := SpanFromContext(ctx)
	opts := make([]SpanOption, 0, 3)
	// If there's a span in ctx, we use it as a parent.
	if sp != nil {
		tr = sp.Tracer()
		if tr.ShouldRecordAsyncSpans() {
			opts = append(opts, WithParent(sp))
		} else {
			// Using auto collection here ensures that recordings from async spans
			// also show up at the parent.
			opts = append(opts, WithParent(sp), WithDetachedRecording())
		}
		opts = append(opts, WithFollowsFrom())
	}
	return tr.StartSpanCtx(ctx, opName, opts...)
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
	return sp.Tracer().StartSpanCtx(ctx, opName, WithParent(sp))
}

// EnsureChildSpan looks at the supplied Context. If it contains a Span, returns
// a child span via the WithParent option; otherwise starts a
// new Span. In both cases, a context wrapping the Span is returned along with
// the newly created Span.
//
// The caller is responsible for closing the Span (via Span.Finish).
func EnsureChildSpan(
	ctx context.Context, tr *Tracer, name string, os ...SpanOption,
) (context.Context, *Span) {
	// NB: Making the capacity dynamic, based on len(os), makes this allocate.
	// With a fixed length, it doesn't allocate.
	opts := make([]SpanOption, 0, 3)
	opts = append(opts, WithParent(SpanFromContext(ctx)))
	opts = append(opts, os...)
	ctx, sp := tr.StartSpanCtx(ctx, name, opts...)
	return ctx, sp
}

// ContextWithRecordingSpan returns a context with an embedded trace Span. The
// Span is derived from the provided Tracer. The recording is collected and the
// span is Finish()ed through the returned callback.
//
// The returned callback can be called multiple times.
//
// Note that to convert the recorded spans into text, you can use
// Recording.String(). Tests can also use FindMsgInRecording().
func ContextWithRecordingSpan(
	ctx context.Context, tr *Tracer, opName string,
) (_ context.Context, finishAndGetRecording func() Recording) {
	ctx, sp := tr.StartSpanCtx(ctx, opName, WithRecording(RecordingVerbose))
	var rec Recording
	return ctx,
		func() Recording {
			if rec != nil {
				return rec
			}
			rec = sp.FinishAndGetConfiguredRecording()
			return rec
		}
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
