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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
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
//    span inside each of our spans.
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
	noopSpan noopSpan

	// If forceRealSpans is set, this Tracer will always create real spans (never
	// noopSpans), regardless of the recording or lightstep configuration. Used
	// by tests for situations when they need to indirectly create spans and don't
	// have the option of passing the Recordable option to their constructor.
	forceRealSpans bool

	// True if tracing to the debug/requests endpoint. Accessed via t.useNetTrace().
	_useNetTrace int32 // updated atomically

	// Pointer to shadowTracer, if using one.
	shadowTracer unsafe.Pointer
}

var _ opentracing.Tracer = &Tracer{}

// NewTracer creates a Tracer. It initially tries to run with minimal overhead
// and collects essentially nothing; use Configure() to enable various tracing
// backends.
func NewTracer() *Tracer {
	t := &Tracer{}
	t.noopSpan.tracer = t
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

// SetForceRealSpans sets forceRealSpans option to v and returns the previous
// value.
func (t *Tracer) SetForceRealSpans(v bool) bool {
	prevVal := t.forceRealSpans
	t.forceRealSpans = v
	return prevVal
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

type recordableOption struct{}

// Apply is part of the opentracing.StartSpanOption interface.
func (recordableOption) Apply(*opentracing.StartSpanOptions) {}

// Recordable is a StartSpanOption that forces creation of a real span.
//
// When tracing is disabled all spans are noopSpans; these spans aren't
// capable of recording, so this option should be passed to StartSpan if the
// caller wants to be able to call StartRecording on the resulting span.
var Recordable opentracing.StartSpanOption = recordableOption{}

// StartSpan is part of the opentracing.Tracer interface.
//
// Avoid using this method in favor of Tracer.StartRootSpan() or
// tracing.StartChildSpan() (or higher-level methods like EnsureContext() or
// AmbientContext.AnnotateCtxWithSpan()) when possible. Those are more efficient
// because they don't have to use the StartSpanOption interface and so a bunch
// of allocations are avoided. However, we need to implement this standard
// StartSpan() too because grpc integrates with a generic opentracing.Tracer.
func (t *Tracer) StartSpan(
	operationName string, opts ...opentracing.StartSpanOption,
) opentracing.Span {
	// Fast paths to avoid the allocation of StartSpanOptions below when tracing
	// is disabled: if we have no options or a single SpanReference (the common
	// case) with a noop context, return a noop span now.
	if len(opts) == 1 {
		if o, ok := opts[0].(opentracing.SpanReference); ok {
			if IsNoopContext(o.ReferencedContext) {
				return &t.noopSpan
			}
		}
	}

	shadowTr := t.getShadowTracer()

	if len(opts) == 0 && !t.useNetTrace() && shadowTr == nil && !t.forceRealSpans {
		return &t.noopSpan
	}

	var sso opentracing.StartSpanOptions
	var recordable bool
	var logTags *logtags.Buffer
	for _, o := range opts {
		switch to := o.(type) {
		case recordableOption:
			recordable = true
		case *logTagsOption:
			// NOTE: calling logTagsOption.Apply() would be functionally equivalent,
			// but less efficient.
			logTags = (*logtags.Buffer)(to)
		default:
			o.Apply(&sso)
		}
	}

	var hasParent bool
	var parentType opentracing.SpanReferenceType
	var parentCtx *spanContext
	var recordingGroup *spanGroup
	var recordingType RecordingType

	for _, r := range sso.References {
		if r.Type != opentracing.ChildOfRef && r.Type != opentracing.FollowsFromRef {
			continue
		}
		if r.ReferencedContext == nil {
			continue
		}
		if IsNoopContext(r.ReferencedContext) {
			continue
		}
		hasParent = true
		parentType = r.Type
		parentCtx = r.ReferencedContext.(*spanContext)
		if parentCtx.recordingGroup != nil {
			recordingGroup = parentCtx.recordingGroup
			recordingType = parentCtx.recordingType
		} else if parentCtx.Baggage[Snowball] != "" {
			// Automatically enable recording if we have the Snowball baggage item.
			recordingGroup = new(spanGroup)
			recordingType = SnowballRecording
		}
		// TODO(radu): can we do something for multiple references?
		break
	}
	if hasParent {
		// We use the parent's shadow tracer, to avoid inconsistency inside a
		// trace when the shadow tracer changes.
		shadowTr = parentCtx.shadowTr
	}

	// If tracing is disabled, the Recordable option wasn't passed, and we're not
	// part of a recording or snowball trace, avoid overhead and return a noop
	// span.
	if !recordable && recordingGroup == nil && shadowTr == nil && !t.useNetTrace() && !t.forceRealSpans {
		return &t.noopSpan
	}

	s := &span{
		tracer:    t,
		operation: operationName,
		startTime: sso.StartTime,
		logTags:   logTags,
	}
	if s.startTime.IsZero() {
		s.startTime = time.Now()
	}
	s.mu.duration = -1

	if !hasParent {
		// No parent Span; allocate new trace id.
		s.TraceID = uint64(rand.Int63())
	} else {
		s.TraceID = parentCtx.TraceID
	}
	s.SpanID = uint64(rand.Int63())

	// Start recording if necessary.
	if recordingGroup != nil {
		s.enableRecording(recordingGroup, recordingType)
	}

	if t.useNetTrace() {
		s.netTr = trace.New("tracing", operationName)
		s.netTr.SetMaxEvents(maxLogsPerSpan)
	}

	if hasParent {
		s.parentSpanID = parentCtx.SpanID
		// Copy baggage from parent.
		if l := len(parentCtx.Baggage); l > 0 {
			s.mu.Baggage = make(map[string]string, l)
			for k, v := range parentCtx.Baggage {
				s.mu.Baggage[k] = v
			}
		}
	}

	for k, v := range sso.Tags {
		s.SetTag(k, v)
	}

	// Copy baggage items to tags so they show up in the shadow tracer UI,
	// x/net/trace, or recordings.
	for k, v := range s.mu.Baggage {
		s.SetTag(k, v)
	}

	if shadowTr != nil {
		var parentShadowCtx opentracing.SpanContext
		if hasParent {
			parentShadowCtx = parentCtx.shadowCtx
		}
		linkShadowSpan(s, shadowTr, parentShadowCtx, parentType)
	}

	return s
}

// RecordableOpt specifies whether a root span should be recordable.
type RecordableOpt bool

const (
	// RecordableSpan means that the root span will be recordable. This means that
	// a real span will be created (and so it carries a cost).
	RecordableSpan RecordableOpt = true
	// NonRecordableSpan means that the root span will not be recordable. This
	// means that the static noop span might be returned.
	NonRecordableSpan RecordableOpt = false
)

// AlwaysTrace returns true if operations should be traced regardless of the
// context.
func (t *Tracer) AlwaysTrace() bool {
	shadowTracer := t.getShadowTracer()
	return t.useNetTrace() || shadowTracer != nil || t.forceRealSpans
}

// StartRootSpan creates a root span. This is functionally equivalent to:
// parentSpan.Tracer().(*Tracer).StartSpan(opName, LogTags(...), [Recordable])
// Compared to that, it's more efficient, particularly in terms of memory
// allocations because the opentracing.StartSpanOption interface is not used.
//
// logTags can be nil.
func (t *Tracer) StartRootSpan(
	opName string, logTags *logtags.Buffer, recordable RecordableOpt,
) opentracing.Span {
	// In the usual case, we return noopSpan.
	if !t.AlwaysTrace() && recordable == NonRecordableSpan {
		return &t.noopSpan
	}

	s := &span{
		spanMeta: spanMeta{
			TraceID: uint64(rand.Int63()),
			SpanID:  uint64(rand.Int63()),
		},
		tracer:    t,
		operation: opName,
		startTime: time.Now(),
		logTags:   logTags,
	}
	s.mu.duration = -1

	shadowTracer := t.getShadowTracer()
	if shadowTracer != nil {
		linkShadowSpan(
			s, shadowTracer, nil, /* parentShadowCtx */
			opentracing.SpanReferenceType(0) /* parentType - ignored*/)
	}

	if t.useNetTrace() {
		var tags []logtags.Tag
		if logTags != nil {
			tags = logTags.Get()
		}

		s.netTr = trace.New("tracing", opName)
		s.netTr.SetMaxEvents(maxLogsPerSpan)
		for i := range tags {
			tag := &tags[i]
			s.netTr.LazyPrintf("%s:%v", tag.Key(), tag.Value())
		}
	}

	return s
}

// StartChildSpan creates a child span of the given parent span. This is
// functionally equivalent to:
// parentSpan.Tracer().(*Tracer).StartSpan(opName, opentracing.ChildOf(parentSpan.Context()))
// Compared to that, it's more efficient, particularly in terms of memory
// allocations; among others, it saves the call to parentSpan.Context.
//
// This only works for creating children of local parents (i.e. the caller needs
// to have a reference to the parent span).
//
// If separateRecording is true and the parent span is recording, we start a
// new recording for the child span. If separateRecording is false (the
// default), then the child span will be part of the same recording.
func StartChildSpan(
	opName string, parentSpan opentracing.Span, logTags *logtags.Buffer, separateRecording bool,
) opentracing.Span {
	tr := parentSpan.Tracer().(*Tracer)
	// If tracing is disabled, avoid overhead and return a noop span.
	if IsBlackHoleSpan(parentSpan) {
		return &tr.noopSpan
	}

	pSpan := parentSpan.(*span)

	s := &span{
		tracer:       tr,
		operation:    opName,
		startTime:    time.Now(),
		parentSpanID: pSpan.SpanID,
		logTags:      logTags,
	}

	// Copy baggage from parent.
	pSpan.mu.Lock()
	if l := len(pSpan.mu.Baggage); l > 0 {
		s.mu.Baggage = make(map[string]string, l)
		for k, v := range pSpan.mu.Baggage {
			s.mu.Baggage[k] = v
		}
	}

	s.TraceID = pSpan.TraceID
	s.SpanID = uint64(rand.Int63())

	if pSpan.shadowTr != nil {
		linkShadowSpan(s, pSpan.shadowTr, pSpan.shadowSpan.Context(), opentracing.ChildOfRef)
	}

	// Start recording if necessary.
	if pSpan.isRecording() {
		recordingGroup := pSpan.mu.recordingGroup
		if separateRecording {
			recordingGroup = new(spanGroup)
		}
		s.enableRecording(recordingGroup, pSpan.mu.recordingType)
	}

	if pSpan.netTr != nil {
		s.netTr = trace.New("tracing", opName)
		s.netTr.SetMaxEvents(maxLogsPerSpan)
		if startTags := s.logTags; startTags != nil {
			tags := startTags.Get()
			for i := range tags {
				tag := &tags[i]
				s.netTr.LazyPrintf("%s:%v", tagName(tag.Key()), tag.Value())
			}
		}
	}

	if pSpan.netTr != nil || pSpan.shadowTr != nil {
		// Copy baggage items to tags so they show up in the shadow tracer UI or x/net/trace.
		for k, v := range s.mu.Baggage {
			s.SetTag(k, v)
		}
	}

	pSpan.mu.Unlock()
	return s
}

type textMapWriterFn func(key, val string)

var _ opentracing.TextMapWriter = textMapWriterFn(nil)

// Set is part of the opentracing.TextMapWriter interface.
func (fn textMapWriterFn) Set(key, val string) {
	fn(key, val)
}

// Inject is part of the opentracing.Tracer interface.
func (t *Tracer) Inject(
	osc opentracing.SpanContext, format interface{}, carrier interface{},
) error {
	if IsNoopContext(osc) {
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

	sc, ok := osc.(*spanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}

	mapWriter.Set(fieldNameTraceID, strconv.FormatUint(sc.TraceID, 16))
	mapWriter.Set(fieldNameSpanID, strconv.FormatUint(sc.SpanID, 16))

	for k, v := range sc.Baggage {
		mapWriter.Set(prefixBaggage+k, v)
	}

	if sc.shadowTr != nil {
		mapWriter.Set(fieldNameShadowType, sc.shadowTr.Typ())
		// Encapsulate the shadow text map, prepending a prefix to the keys.
		if err := sc.shadowTr.Inject(sc.shadowCtx, format, textMapWriterFn(func(key, val string) {
			mapWriter.Set(prefixShadow+key, val)
		})); err != nil {
			return err
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

// Extract is part of the opentracing.Tracer interface.
// It always returns a valid context, even in error cases (this is assumed by the
// grpc-opentracing interceptor).
func (t *Tracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	// We only support the HTTPHeaders/TextMap format.
	if format != opentracing.HTTPHeaders && format != opentracing.TextMap {
		return noopSpanContext{}, opentracing.ErrUnsupportedFormat
	}

	mapReader, ok := carrier.(opentracing.TextMapReader)
	if !ok {
		return noopSpanContext{}, opentracing.ErrInvalidCarrier
	}

	var sc spanContext
	var shadowType string
	var shadowCarrier opentracing.TextMapCarrier

	err := mapReader.ForeachKey(func(k, v string) error {
		switch k = strings.ToLower(k); k {
		case fieldNameTraceID:
			var err error
			sc.TraceID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return opentracing.ErrSpanContextCorrupted
			}
		case fieldNameSpanID:
			var err error
			sc.SpanID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return opentracing.ErrSpanContextCorrupted
			}
		case fieldNameShadowType:
			shadowType = v
		default:
			if strings.HasPrefix(k, prefixBaggage) {
				if sc.Baggage == nil {
					sc.Baggage = make(map[string]string)
				}
				sc.Baggage[strings.TrimPrefix(k, prefixBaggage)] = v
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
		return noopSpanContext{}, err
	}
	if sc.TraceID == 0 && sc.SpanID == 0 {
		return noopSpanContext{}, nil
	}

	if shadowType != "" {
		// Using a shadow tracer only works if all hosts use the same shadow tracer.
		// If that's not the case, ignore the shadow context.
		if shadowTr := t.getShadowTracer(); shadowTr != nil &&
			strings.EqualFold(shadowType, shadowTr.Typ()) {
			sc.shadowTr = shadowTr
			// Extract the shadow context using the un-encapsulated textmap.
			sc.shadowCtx, err = shadowTr.Extract(format, shadowCarrier)
			if err != nil {
				return noopSpanContext{}, err
			}
		}
	}

	return &sc, nil
}

// FinishSpan closes the given span (if not nil). It is a convenience wrapper
// for span.Finish() which tolerates nil spans.
func FinishSpan(span opentracing.Span) {
	if span != nil {
		span.Finish()
	}
}

// ForkCtxSpan checks if ctx has a Span open; if it does, it creates a new Span
// that "follows from" the original Span. This allows the resulting context to be
// used in an async task that might outlive the original operation.
//
// Returns the new context and the new span (if any). The span should be
// closed via FinishSpan.
//
// See also ChildSpan() for a "parent-child relationship".
func ForkCtxSpan(ctx context.Context, opName string) (context.Context, opentracing.Span) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		if _, noop := span.(*noopSpan); noop {
			// Optimization: avoid ContextWithSpan call if tracing is disabled.
			return ctx, span
		}
		tr := span.Tracer()
		if IsBlackHoleSpan(span) {
			ns := &tr.(*Tracer).noopSpan
			return opentracing.ContextWithSpan(ctx, ns), ns
		}
		newSpan := tr.StartSpan(opName, opentracing.FollowsFrom(span.Context()), LogTagsFromCtx(ctx))
		return opentracing.ContextWithSpan(ctx, newSpan), newSpan
	}
	return ctx, nil
}

// ChildSpan opens a span as a child of the current span in the context (if
// there is one).
// The span's tags are inherited from the ctx's log tags automatically.
//
// Returns the new context and the new span (if any). The span should be
// closed via FinishSpan.
func ChildSpan(ctx context.Context, opName string) (context.Context, opentracing.Span) {
	return childSpan(ctx, opName, false /* separateRecording */)
}

// ChildSpanSeparateRecording is like ChildSpan but the new span has separate
// recording (see StartChildSpan).
func ChildSpanSeparateRecording(
	ctx context.Context, opName string,
) (context.Context, opentracing.Span) {
	return childSpan(ctx, opName, true /* separateRecording */)
}

func childSpan(
	ctx context.Context, opName string, separateRecording bool,
) (context.Context, opentracing.Span) {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return ctx, nil
	}
	if _, noop := span.(*noopSpan); noop {
		// Optimization: avoid ContextWithSpan call if tracing is disabled.
		return ctx, span
	}
	tr := span.Tracer()
	if IsBlackHoleSpan(span) {
		ns := &tr.(*Tracer).noopSpan
		return opentracing.ContextWithSpan(ctx, ns), ns
	}
	newSpan := StartChildSpan(opName, span, logtags.FromContext(ctx), separateRecording)
	return opentracing.ContextWithSpan(ctx, newSpan), newSpan
}

// EnsureContext checks whether the given context.Context contains a Span. If
// not, it creates one using the provided Tracer and wraps it in the returned
// Span. The returned closure must be called after the request has been fully
// processed.
//
// Note that, if there's already a span in the context, this method does nothing
// even if the current context's log tags are different from that span's tags.
func EnsureContext(
	ctx context.Context, tracer opentracing.Tracer, opName string,
) (context.Context, func()) {
	if opentracing.SpanFromContext(ctx) == nil {
		sp := tracer.(*Tracer).StartRootSpan(opName, logtags.FromContext(ctx), NonRecordableSpan)
		return opentracing.ContextWithSpan(ctx, sp), sp.Finish
	}
	return ctx, func() {}
}

// EnsureChildSpan is the same as EnsureContext, except it creates a child
// span for the input context if the input context already has an active
// trace.
//
// The caller is responsible for closing the span (via Span.Finish).
func EnsureChildSpan(
	ctx context.Context, tracer opentracing.Tracer, name string,
) (context.Context, opentracing.Span) {
	if opentracing.SpanFromContext(ctx) == nil {
		sp := tracer.(*Tracer).StartRootSpan(name, logtags.FromContext(ctx), NonRecordableSpan)
		return opentracing.ContextWithSpan(ctx, sp), sp
	}
	return ChildSpan(ctx, name)
}

// StartSnowballTrace takes in a context and returns a derived one with a
// "snowball span" in it. The caller takes ownership of this span from the
// returned context and is in charge of Finish()ing it. The span has recording
// enabled.
//
// TODO(andrei): remove this method once EXPLAIN(TRACE) is gone.
func StartSnowballTrace(
	ctx context.Context, tracer opentracing.Tracer, opName string,
) (context.Context, opentracing.Span) {
	var span opentracing.Span
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		span = parentSpan.Tracer().StartSpan(
			opName, opentracing.ChildOf(parentSpan.Context()), Recordable, LogTagsFromCtx(ctx),
		)
	} else {
		span = tracer.StartSpan(opName, Recordable, LogTagsFromCtx(ctx))
	}
	StartRecording(span, SnowballRecording)
	return opentracing.ContextWithSpan(ctx, span), span
}

// TestingCheckRecordedSpans checks whether a recording looks like an expected
// one represented by a string with one line per expected span and one line per
// expected event (i.e. log message).
//
// Use with something like:
// 	 if err := TestingCheckRecordedSpans(tracing.GetRecording(span), `
//     span root:
//       event: a
//       event: c
//     span child:
//       event: [ambient] b
//   `); err != nil {
//   	t.Fatal(err)
//   }
//
// The event lines can (and generally should) omit the file:line part that they
// might contain (depending on the level at which they were logged).
//
// Note: this test function is in this file because it needs to be used by
// both tests in the tracing package and tests outside of it, and the function
// itself depends on tracing.
func TestingCheckRecordedSpans(recSpans []RecordedSpan, expected string) error {
	expected = strings.TrimSpace(expected)
	var rows []string
	row := func(format string, args ...interface{}) {
		rows = append(rows, fmt.Sprintf(format, args...))
	}

	for _, rs := range recSpans {
		row("span %s:", rs.Operation)
		if len(rs.Tags) > 0 {
			var tags []string
			for k, v := range rs.Tags {
				tags = append(tags, fmt.Sprintf("%s=%v", k, v))
			}
			sort.Strings(tags)
			row("  tags: %s", strings.Join(tags, " "))
		}
		for _, l := range rs.Logs {
			msg := ""
			for _, f := range l.Fields {
				msg = msg + fmt.Sprintf("  %s: %v", f.Key, f.Value)
			}
			row("%s", msg)
		}
	}
	var expRows []string
	if expected != "" {
		expRows = strings.Split(expected, "\n")
	}
	match := false
	if len(expRows) == len(rows) {
		match = true
		for i := range expRows {
			e := strings.Trim(expRows[i], " \t")
			r := strings.Trim(rows[i], " \t")
			if e != r && !matchesWithoutFileLine(r, e) {
				match = false
				break
			}
		}
	}
	if !match {
		file, line, _ := caller.Lookup(1)
		return errors.Errorf(
			"%s:%d expected:\n%s\ngot:\n%s",
			file, line, expected, strings.Join(rows, "\n"))
	}
	return nil
}

// matchesWithoutFileLine tries to match an event by stripping a file:line from
// it. For example:
// "event: util/log/trace_test.go:111 log" will match "event: log".
//
// Returns true if it matches.
func matchesWithoutFileLine(msg string, expected string) bool {
	groups := regexp.MustCompile(`^(event: ).*:[0-9]* (.*)$`).FindStringSubmatch(msg)
	return len(groups) == 3 && fmt.Sprintf("event: %s", groups[2]) == expected
}

// ContextWithRecordingSpan returns a context with an embedded trace span which
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
	sp := tr.StartSpan(opName, Recordable, LogTagsFromCtx(ctx))
	StartRecording(sp, SnowballRecording)
	ctx, cancelCtx := context.WithCancel(ctx)
	ctx = opentracing.ContextWithSpan(ctx, sp)

	getRecording = func() Recording {
		return GetRecording(sp)
	}
	cancel = func() {
		cancelCtx()
		StopRecording(sp)
		sp.Finish()
		tr.Close()
	}
	return ctx, getRecording, cancel
}
