// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
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
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/sdk/resource"
	otelsdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/metadata"
)

const (

	// maxRecordedSpansPerTrace limits the number of spans per recording, keeping
	// recordings from getting too large.
	maxRecordedSpansPerTrace = 1000
	// maxRecordedBytesPerSpan limits the size of unstructured logs in a span.
	maxLogBytesPerSpan = 256 * (1 << 10) // 256 KiB
	// maxStructuredBytesPerSpan limits the size of structured logs in a span.
	// This limit applies to records directly logged into the span; it does not
	// apply to records in child span (including structured records copied from
	// the child into the parent when the child is dropped because of the number
	// of spans limit).
	// See also maxStructuredBytesPerTrace.
	maxStructuredBytesPerSpan = 10 * (1 << 10) // 10 KiB
	// maxStructuredBytesPerTrace limits the total size of structured logs in a
	// trace recording, across all spans. This limit is enforced at the time when
	// a span is finished and its recording is copied to the parent, and at the
	// time when an open span's recording is collected - which calls into all its
	// open children. Thus, if there are multiple open spans that are part of the
	// same trace, each one of them can temporarily have up to
	// maxStructuredBytesPerTrace worth of messages under it. Each open span is
	// also subject to the maxStructuredBytesPerSpan limit.
	maxStructuredBytesPerTrace = 1 << 20 // 1 MiB

	// maxSpanRegistrySize limits the number of local root spans tracked in
	// a Tracer's registry.
	maxSpanRegistrySize = 5000
	// maxLogsPerSpanExternal limits the number of logs in a Span for external
	// tracers (net/trace, OpenTelemetry); use a comfortable limit.
	maxLogsPerSpanExternal = 1000
	// maxSnapshots limits the number of snapshots that a Tracer will hold in
	// memory. Beyond this limit, each new snapshot evicts the oldest one.
	maxSnapshots          = 10
	maxAutomaticSnapshots = 20 // TODO(dt): make this a setting
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

	SpanKindTagKey = "span.kind"
)

// TODO(davidh): Once performance issues around redaction are
// resolved via #58610, this setting can be removed so that all traces
// have redactability enabled.
var enableTraceRedactable = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"trace.redactable.enabled",
	"set to true to enable finer-grainer redactability for unstructured events in traces",
	true,
)

var enableNetTrace = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"trace.debug.enable",
	"if set, traces for recent requests can be seen at https://<ui>/debug/requests",
	false,
	settings.WithName("trace.debug_http_endpoint.enabled"),
	settings.WithPublic)

var openTelemetryCollector = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"trace.opentelemetry.collector",
	"address of an OpenTelemetry trace collector to receive "+
		"traces using the otel gRPC protocol, as <host>:<port>. "+
		"If no port is specified, 4317 will be used.",
	envutil.EnvOrDefaultString("COCKROACH_OTLP_COLLECTOR", ""),
	settings.WithValidateString(func(_ *settings.Values, s string) error {
		if s == "" {
			return nil
		}
		_, _, err := addr.SplitHostPort(s, "4317")
		return err
	}),
	settings.WithPublic,
)

// ZipkinCollector is the cluster setting that specifies the Zipkin instance
// to send traces to, if any.
var ZipkinCollector = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"trace.zipkin.collector",
	"the address of a Zipkin instance to receive traces, as <host>:<port>. "+
		"If no port is specified, 9411 will be used.",
	envutil.EnvOrDefaultString("COCKROACH_ZIPKIN", ""),
	settings.WithValidateString(func(_ *settings.Values, s string) error {
		if s == "" {
			return nil
		}
		_, _, err := addr.SplitHostPort(s, "9411")
		return err
	}),
	settings.WithPublic,
)

var forceVerboseSpanRegexp = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"trace.span.force_verbose_regexp",
	"a regular expression representing trace span operation names that should be "+
		"forced into a verbose recording mode. If a span is created whose operation name matches "+
		"the provided regular expression, the span and all of its children are forced into a verbose "+
		"recording mode, meaning that the spans will capture all structured logs and events recorded "+
		"during the lifetime of the span(s). NOTE: the verbose recording mode is known to cause non-trivial "+
		"amounts of overhead. Using this setting could have a negative impact on cluster latency.",
	"", /*defaultValue*/
	settings.WithValidateString(func(_ *settings.Values, s string) error {
		if s == "" {
			return nil
		}
		_, err := regexp.Compile(s)
		return err
	}),
	settings.WithVisibility(settings.Reserved),
)

// EnableActiveSpansRegistry controls Tracers configured as
// WithTracingMode(TracingModeFromEnv) (which is the default). When enabled,
// spans are allocated and registered with the active spans registry until
// finished. When disabled, span creation is short-circuited for a small
// performance improvement.
var EnableActiveSpansRegistry = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"trace.span_registry.enabled",
	"if set, ongoing traces can be seen at https://<ui>/#/debug/tracez",
	envutil.EnvOrDefaultBool("COCKROACH_REAL_SPANS", false),
	settings.WithPublic)

var periodicSnapshotInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"trace.snapshot.rate",
	"if non-zero, interval at which background trace snapshots are captured",
	0,
	settings.NonNegativeDuration,
	settings.WithPublic)

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
		panic(errors.AssertionFailedf("invalid percent: %d", percent))
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
var spanReusePercent = metamorphic.ConstantWithTestRange(
	"span-reuse-rate",
	100, /* defaultValue - always reuse spans. This turns to 0 if reuseSpans is not set. */
	0,   /* metamorphic min */
	101, /* metamorphic max */
)

// Tracer implements tracing requests. It supports:
//
//   - forwarding events to x/net/trace instances
//
//   - recording traces. Recorded events can be retrieved at any time.
//
//   - OpenTelemetry tracing. This is implemented by maintaining a "shadow"
//     OpenTelemetry Span inside each of our spans.
//
// Even when tracing is disabled, we still use this Tracer (with x/net/trace and
// lightstep disabled) because of its recording capability (verbose tracing needs
// to work in all cases).
//
// Tracer is currently stateless so we could have a single instance; however,
// this won't be the case if the cluster settings move away from using global
// state.
type Tracer struct {
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

	// _activeSpansRegistryEnabled controls whether spans are created and
	// registered with activeSpansRegistry until they're Finish()ed. If not
	// enabled, span creation is generally a no-op unless a recording span is
	// explicitly requested.
	_activeSpansRegistryEnabled int32 // accessed atomically

	opNameRegexp atomic.Pointer[regexp.Regexp]

	// activeSpans is a map that references all non-Finish'ed local root spans,
	// i.e. those for which no WithParent(<non-nil>) option was supplied.
	activeSpansRegistry *SpanRegistry
	snapshotsMu         struct {
		syncutil.Mutex
		// snapshots stores the activeSpansRegistry snapshots taken during the
		// Tracer's lifetime. The ring buffer will contain snapshots with contiguous
		// IDs, from the oldest one to <oldest id> + maxSnapshots - 1.
		snapshots     ring.Buffer[snapshotWithID]
		autoSnapshots ring.Buffer[snapshotWithID]
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
	stack debugutil.SafeStack
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
		// m stores all the currently open spans. Note that a span being present
		// here proves that the corresponding Span.Finish() call has not yet
		// completed (but crdbSpan.Finish() might have finished), therefor the span
		// cannot be reused while present in the registry. At the same time, note
		// that Span.Finish() can be called on these spans so, when using one of
		// these spans, we need to be prepared for that use to be concurrent with
		// the Finish() call.
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

// VisitRoots calls the visitor callback for every local root span in the
// registry. Iterations stops when the visitor returns an error. If that error
// is iterutils.StopIteration(), then VisitRoots() returns nil.
//
// The callback should not hold on to the span after it returns.
func (r *SpanRegistry) VisitRoots(visitor func(span RegistrySpan) error) error {
	// Take a snapshot of the registry and release the lock.
	spans := r.getSpanRefs()

	// Keep the spans alive while visting them below.
	defer func() {
		for i := range spans {
			spans[i].release()
		}
	}()

	for _, sp := range spans {
		if err := visitor(sp.Span.i.crdb); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// visitTrace recursively calls the visitor on sp and all its descendants.
func visitTrace(sp *crdbSpan, visitor func(sp RegistrySpan)) {
	visitor(sp)
	sp.visitOpenChildren(func(child *crdbSpan) {
		visitTrace(child, visitor)
	})
}

// VisitSpans calls the visitor callback for every span in the
// registry.
//
// The callback should not hold on to the span after it returns.
func (r *SpanRegistry) VisitSpans(visitor func(span RegistrySpan)) {
	// Take a snapshot of the registry and release the lock.
	spans := r.getSpanRefs()

	// Keep the spans alive while visting them below.
	defer func() {
		for i := range spans {
			spans[i].release()
		}
	}()

	for _, sp := range spans {
		visitTrace(sp.Span.i.crdb, visitor)
	}
}

// getSpanRefs collects references to all spans in the SpanRegistry map and
// returns a slice of them.
func (r *SpanRegistry) getSpanRefs() []spanRef {
	r.mu.Lock()
	defer r.mu.Unlock()
	spans := make([]spanRef, 0, len(r.mu.m))
	for _, sp := range r.mu.m {
		spans = append(spans, makeSpanRef(sp.sp))
	}
	return spans
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
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.removeSpanLocked(parentID)
		for _, c := range children {
			sp := c.Span.i.crdb
			sp.withLock(func() {
				if !sp.mu.finished {
					r.addSpanLocked(sp)
				}
			})
		}
	}()
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

// SetActiveSpansRegistryEnabled controls whether spans are created and
// registered with activeSpansRegistry.
func (t *Tracer) SetActiveSpansRegistryEnabled(to bool) {
	var n int32
	if to {
		n = 1
	}
	atomic.StoreInt32(&t._activeSpansRegistryEnabled, n)
}

// ActiveSpansRegistryEnabled returns true if this tracer is configured
// to register spans with the activeSpansRegistry
func (t *Tracer) ActiveSpansRegistryEnabled() bool {
	return atomic.LoadInt32(&t._activeSpansRegistryEnabled) != 0
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
		stack:               debugutil.Stack(),
		activeSpansRegistry: makeSpanRegistry(),
		// These might be overridden in NewTracerWithOpt.
		panicOnUseAfterFinish: panicOnUseAfterFinish,
		debugUseAfterFinish:   debugUseAfterFinish,
		spanReusePercent:      defaultSpanReusePercent,
	}
	t.SetActiveSpansRegistryEnabled(true)

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
			h.childrenMetadataAlloc = make(map[string]tracingpb.OperationMetadata)
			return h
		},
	}
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
	t.SetActiveSpansRegistryEnabled(o.tracingDefault == TracingModeActiveSpansRegistry)
	if o.sv != nil {
		t.configure(ctx, o.sv, o.tracingDefault)
		forceVerboseSpanRegexp.SetOnChange(o.sv, func(ctx context.Context) {
			if err := t.setVerboseOpNameRegexp(forceVerboseSpanRegexp.Get(o.sv)); err != nil {
				fmt.Fprintf(os.Stderr, "error compiling verbose span operation name regexp: %s\n", err)
			}
		})
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
		panic(errors.AssertionFailedf("it is nonsensical to set debugUseAfterFinish when panicOnUseAfterFinish is not set, " +
			"as the collected stacks will never be used"))
	}
	return useAfterFinishOpt{
		panicOnUseAfterFinish: panicOnUseAfterFinish,
		debugUseAfterFinish:   debugUseAfterFinish,
	}
}

func (t *Tracer) setVerboseOpNameRegexp(s string) error {
	if s == "" {
		t.opNameRegexp.Store(nil)
		return nil
	}
	compiled, err := regexp.Compile(s)
	if err != nil {
		return err
	}
	t.opNameRegexp.Store(compiled)
	return nil
}

// configure sets up the Tracer according to the cluster settings (and keeps
// it updated if they change).
func (t *Tracer) configure(ctx context.Context, sv *settings.Values, tracingDefault TracingMode) {
	// traceProvider is captured by the function below.
	var traceProvider *otelsdk.TracerProvider

	// reconfigure will be called every time a cluster setting affecting tracing
	// is updated.
	reconfigure := func(ctx context.Context) {
		otlpCollectorAddr := openTelemetryCollector.Get(sv)
		zipkinAddr := ZipkinCollector.Get(sv)
		enableRedactable := enableTraceRedactable.Get(sv)

		switch tracingDefault {
		case TracingModeFromEnv:
			t.SetActiveSpansRegistryEnabled(EnableActiveSpansRegistry.Get(sv))
		case TracingModeOnDemand:
			t.SetActiveSpansRegistryEnabled(false)
		case TracingModeActiveSpansRegistry:
			t.SetActiveSpansRegistryEnabled(true)
		default:
			panic(errors.AssertionFailedf("unrecognized tracing option: %v", tracingDefault))
		}

		t.SetRedactable(enableRedactable)

		var nt int32
		if enableNetTrace.Get(sv) {
			nt = 1
		}
		atomic.StoreInt32(&t._useNetTrace, nt)

		// Return early if the OpenTelemetry tracer is disabled.
		if otlpCollectorAddr == "" && zipkinAddr == "" {
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
	structuredEventsAlloc [3]*tracingpb.StructuredRecord
	childrenMetadataAlloc map[string]tracingpb.OperationMetadata
}

// newSpan allocates a span using the Tracer's sync.Pool. A span that was
// previously Finish()ed be returned if the Tracer is configured for Span reuse.
// +(...) must be called on the returned span before further use.
func (t *Tracer) newSpan(
	traceID tracingpb.TraceID,
	spanID tracingpb.SpanID,
	operation string,
	goroutineID uint64,
	startTime time.Time,
	logTags *logtags.Buffer,
	eventListeners []EventListener,
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
		startTime, logTags, eventListeners, kind,
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
	c.eventListeners = nil
	// Nobody is supposed to have a reference to the span at this point, but let's
	// take the lock anyway to protect against buggy clients accessing the span
	// after Finish().
	func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.openChildren = nil
		c.mu.recording.finishedChildren = Trace{}
		c.mu.tags = nil
		c.mu.lazyTags = nil
		c.mu.recording.logs.Discard()
		c.mu.recording.structured.Discard()
	}()

	// Zero out the spanAllocHelper buffers to make the elements inside the
	// arrays, if any, available for GC.
	h := sp.helper
	h.tagsAlloc = [3]attribute.KeyValue{}
	h.childrenAlloc = [4]childRef{}
	h.structuredEventsAlloc = [3]*tracingpb.StructuredRecord{}
	for op := range h.childrenMetadataAlloc {
		delete(h.childrenMetadataAlloc, op)
	}

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
	if t == nil {
		return ctx, nil
	}
	// NB: apply takes and returns a value to avoid forcing
	// `opts` on the heap here.
	var opts spanOptions
	for i := range os {
		if os[i] == nil {
			continue
		}
		opts = os[i].apply(opts)
	}

	return t.startSpanFast(ctx, operationName, &opts)
}

// AlwaysTrace returns true if operations should be traced regardless of the
// context.
func (t *Tracer) AlwaysTrace() bool {
	if t.ActiveSpansRegistryEnabled() {
		return true
	}
	otelTracer := t.getOtelTracer()
	return t.useNetTrace() || otelTracer != nil
}

func (t *Tracer) forceOpNameVerbose(opName string) bool {
	if r := t.opNameRegexp.Load(); r != nil {
		return r.MatchString(opName)
	}
	return false
}

// startSpanFast implements a fast path for the common case of tracing
// being disabled on the current span and its parent. We make only the
// checks necessary to ensure that recording is disabled and wrap the
// context in a nil span.
func (t *Tracer) startSpanFast(
	ctx context.Context, opName string, opts *spanOptions,
) (context.Context, *Span) {
	if opts.RefType != childOfRef && opts.RefType != followsFromRef {
		panic(errors.AssertionFailedf("unexpected RefType %v", opts.RefType))
	}

	// This is a fast path for the common case where there's no parent
	// span. This contains duplicated logic from
	// `(*spanOptions).recordingType()`. Specifically, we are aiming to
	// avoid creating local copies of `opts.Parent` and
	// `opts.RemoteParent` hence conditional logic is being inlined and
	// checked locally. This logic should only change if
	// `recordingType()` is being modified.
	if opts.Parent.Span == nil && opts.RemoteParent.Empty() {
		var recordingType tracingpb.RecordingType

		if opts.recordingTypeExplicit {
			recordingType = opts.recordingTypeOpt
		}

		if recordingType < opts.minRecordingTypeOpt {
			recordingType = opts.minRecordingTypeOpt
		}

		shouldBeNilSpan := !(t.AlwaysTrace() || opts.ForceRealSpan || recordingType != tracingpb.RecordingOff)
		forceVerbose := t.forceOpNameVerbose(opName)
		if shouldBeNilSpan && !forceVerbose && !opts.Sterile {
			return maybeWrapCtx(ctx, nil)
		}
	}
	return t.startSpanGeneric(ctx, opName, opts)
}

// startSpanGeneric is the implementation of StartSpanCtx and StartSpan. In
// the latter case, ctx == noCtx and the returned Context is the supplied one;
// otherwise the returned Context embeds the returned Span.
func (t *Tracer) startSpanGeneric(
	ctx context.Context, opName string, opts *spanOptions,
) (context.Context, *Span) {
	if opts.RefType != childOfRef && opts.RefType != followsFromRef {
		panic(errors.AssertionFailedf("unexpected RefType %v", opts.RefType))
	}

	if !opts.Parent.empty() {
		// If we don't panic, opts.Parent will be moved into the child, and this
		// release() will be a no-op.
		defer opts.Parent.release()

		if !opts.RemoteParent.Empty() {
			panic(errors.AssertionFailedf("can't specify both Parent and RemoteParent"))
		}

		if opts.Parent.i.sterile {
			// A sterile parent should have been optimized away by
			// WithParent.
			panic(errors.AssertionFailedf("invalid sterile parent"))
		}
		if s := opts.Parent.Tracer(); s != t {
			// Creating a child with a different Tracer than the parent is not allowed
			// because it would become unclear which active span registry the new span
			// should belong to. In particular, the child could end up in the parent's
			// registry if the parent Finish()es before the child, and then it would
			// be leaked because Finish()ing the child would attempt to remove the
			// span from the child tracer's registry.
			panic(errors.AssertionFailedf(`attempting to start span with parent from different Tracer.
parent operation: %s, tracer created at:

%s

child operation: %s, tracer created at:
%s`,
				opts.Parent.OperationName(), s.stack, opName, t.stack))
		}
	}

	// Are we tracing everything, or have a parent, or want a real span, or were
	// asked for a recording? Then we create a real trace span. In all other
	// cases, a nil span will do.
	shouldBeNilSpan := !(t.AlwaysTrace() || opts.parentTraceID() != 0 || opts.ForceRealSpan || opts.recordingType() != tracingpb.RecordingOff)
	// Finally, we should check to see if this opName is configured to always be forced to the
	// tracingpb.RecordingVerbose RecordingType. If it is, we'll want to create a real trace
	// span.
	forceVerbose := t.forceOpNameVerbose(opName)
	if shouldBeNilSpan && !forceVerbose {
		return maybeWrapCtx(ctx, nil)
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

	startTime := timeutil.Now()

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
		startTime, opts.LogTags, opts.EventListeners, opts.SpanKind,
		otelSpan, netTr, opts.Sterile)

	var recType tracingpb.RecordingType
	if forceVerbose {
		recType = tracingpb.RecordingVerbose
	} else {
		recType = opts.recordingType()
	}
	s.i.crdb.SetRecordingType(recType)
	s.i.crdb.parentSpanID = opts.parentSpanID()

	var localRoot bool
	{
		// If a parent is specified, link the newly created Span to the parent.
		// While the parent is alive, the child will be part of the
		// activeSpansRegistry indirectly through this link. If the parent is
		// recording, it will also use this link to collect the child's recording.
		//
		// NB: (!opts.Parent.Empty() && opts.Parent.i.crdb == nil) is not possible at
		// the moment, but let's not rely on that.
		if !opts.Parent.empty() && opts.Parent.i.crdb != nil {

			// Panic if the parent has already been finished, if configured to do so.
			// If the parent has finished and we're configured not to panic,
			// addChildLocked we'll return false below and we'll deal with it.
			_ = opts.Parent.detectUseAfterFinish()

			parent := opts.Parent.i.crdb
			if s.i.crdb == parent {
				panic(errors.AssertionFailedf("attempting to link a child to itself: %s", s.i.crdb.operation))
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
					s.i.crdb.mu.recording.notifyParentOnStructuredEvent = parent.wantEventNotificationsLocked()
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
		// Empty map as a noop context.
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

	carrier.Set(fieldNameTraceID, strconv.FormatUint(uint64(sm.traceID), 16))
	carrier.Set(fieldNameSpanID, strconv.FormatUint(uint64(sm.spanID), 16))
	carrier.Set(fieldNameRecordingType, sm.recordingType.ToCarrierValue())
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
	var recordingType tracingpb.RecordingType

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
			recordingType = tracingpb.RecordingTypeFromCarrierValue(v)
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
	case MetadataCarrier:
		if err := c.ForEach(iterFn); err != nil {
			return noopSpanMeta, err
		}
	default:
		return noopSpanMeta, errors.New("unsupported carrier")
	}

	if traceID == 0 && spanID == 0 {
		return noopSpanMeta, nil
	}

	if !recordingTypeExplicit && recordingType == tracingpb.RecordingOff {
		// A 21.1 node (or a 21.2 mode running in backwards-compatibility mode)
		// that passed a TraceID but not fieldNameDeprecatedVerboseTracing wants the
		// structured events.
		recordingType = tracingpb.RecordingStructured
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
//
// Note that RegistrySpans can be Finish()ed concurrently with their use, so all
// methods must work with concurrent Finish() calls.
type RegistrySpan interface {
	// TraceID returns the id of the trace that this span is part of.
	TraceID() tracingpb.TraceID

	// SpanID returns the span's ID.
	SpanID() tracingpb.SpanID

	// GetFullRecording returns the recording of the trace rooted at this span.
	//
	// This includes the recording of child spans created with the
	// WithDetachedRecording option. In other situations, the recording of such
	// children is not included in the parent's recording but, in the case of the
	// span registry, we want as much information as possible to be included.
	GetFullRecording(recType tracingpb.RecordingType) Trace

	// SetRecordingType sets the recording mode of the span and its children,
	// recursively. Setting it to RecordingOff disables further recording.
	// Everything recorded so far remains in memory.
	SetRecordingType(to tracingpb.RecordingType)

	// RecordingType returns the span's current recording type.
	RecordingType() tracingpb.RecordingType
}

var _ RegistrySpan = &crdbSpan{}

// GetActiveSpansRegistry returns a pointer to the registry containing all
// in-flight on the node.
func (t *Tracer) GetActiveSpansRegistry() *SpanRegistry {
	return t.activeSpansRegistry
}

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
	return t.activeSpansRegistry.VisitRoots(visitor)
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
		panic(errors.AssertionFailedf("GetStatsAndReset() needs the Tracer to have been configured with MaintainAllocationCounters"))
	}
	created := atomic.SwapInt32(&t.spansCreated, 0)
	allocs := atomic.SwapInt32(&t.spansAllocated, 0)
	return int(created), int(allocs)
}

func (t *Tracer) now() time.Time {
	if clock := t.testing.Clock; clock != nil {
		return clock.Now()
	}
	return timeutil.Now()
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
//
//	TestingRecordAsyncSpans.
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

// ChildSpan creates a child span of the current one, if any, via the WithParent
// option. Recordings from child spans are automatically propagated to the
// parent span, and the tags are inherited from the context's log tags
// automatically. Also see `ForkSpan`, for the other kind of derived span
// relation.
//
// A context wrapping the newly created span is returned, along with the span
// itself. If non-nil, the caller is responsible for eventually Finish()ing it.
func ChildSpan(ctx context.Context, opName string, os ...SpanOption) (context.Context, *Span) {
	sp := SpanFromContext(ctx)
	if sp == nil {
		return ctx, nil
	}
	if len(os) == 0 {
		return sp.Tracer().StartSpanCtx(ctx, opName, WithParent(sp))
	}
	os = append(os[:len(os):len(os)], WithParent(sp))
	return sp.Tracer().StartSpanCtx(ctx, opName, os...)
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
) (_ context.Context, finishAndGetRecording func() tracingpb.Recording) {
	ctx, sp := tr.StartSpanCtx(ctx, opName, WithRecording(tracingpb.RecordingVerbose))
	var rec tracingpb.Recording
	return ctx,
		func() tracingpb.Recording {
			if rec != nil {
				return rec
			}
			rec = sp.FinishAndGetConfiguredRecording()
			return rec
		}
}

// makeOtelSpan creates an OpenTelemetry span. If either of localParent or
// remoteParent are not Empty, the returned span will be a child of that parent.
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
		panic(errors.AssertionFailedf("unsupported span reference type: %v", refType))
	}

	_ /* ctx */, sp := otelTr.Start(ctx, opName, opts...)
	return sp
}

// MetadataCarrier is an implementation of the Carrier interface for gRPC
// metadata.
type MetadataCarrier struct {
	metadata.MD
}

// Set implements the Carrier interface.
func (w MetadataCarrier) Set(key, val string) {
	// The GRPC HPACK implementation rejects any uppercase keys here.
	//
	// As such, since the HTTP_HEADERS format is case-insensitive anyway, we
	// blindly lowercase the key.
	key = strings.ToLower(key)
	w.MD[key] = append(w.MD[key], val)
}

// ForEach implements the Carrier interface.
func (w MetadataCarrier) ForEach(fn func(key, val string) error) error {
	for k, vals := range w.MD {
		for _, v := range vals {
			if err := fn(k, v); err != nil {
				return err
			}
		}
	}

	return nil
}

// SpanInclusionFuncForClient is used as a SpanInclusionFunc for the client-side
// of RPCs, deciding for which operations the gRPC tracing interceptor should
// create a span.
//
// We use this to circumvent the interceptor's work when tracing is
// disabled. Otherwise, the interceptor causes an increase in the
// number of packets (even with an Empty context!).
//
// See #17177.
func SpanInclusionFuncForClient(parent *Span) bool {
	return parent != nil
}

// SpanInclusionFuncForServer is used as a SpanInclusionFunc for the server-side
// of RPCs, deciding for which operations the gRPC tracing interceptor should
// create a span.
func SpanInclusionFuncForServer(t *Tracer, spanMeta SpanMeta) bool {
	// If there is an incoming trace on the RPC (spanMeta) or the tracer is
	// configured to always trace, return true. The second part is particularly
	// useful for calls coming through the HTTP->RPC gateway (i.e. the AdminUI),
	// where client is never tracing.
	return !spanMeta.Empty() || t.AlwaysTrace()
}
