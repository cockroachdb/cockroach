// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Andrei Matei (andreimatei1@gmail.com)
// Author: Radu Berinde (radu@cockroachlabs.com)

package tracing

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/lightstep/lightstep-tracer-go"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
)

// Snowball is set as Baggage on traces which are used for snowball tracing.
const Snowball = "sb"

// maxLogsPerSpan limits the number of logs in a Span; use a comfortable limit.
const maxLogsPerSpan = 1000

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
type Tracer struct {
	// If set, we set up an x/net/trace for each span.
	netTrace bool
	// If set, we are using a lightstep tracer for most operations.
	lightstep opentracing.Tracer

	// Preallocated noopSpan, used to avoid creating spans when we are not using
	// x/net/trace or lightstep and we are not recording.
	noopSpan noopSpan
}

var _ opentracing.Tracer = &Tracer{}

func newTracer(netTrace bool, lightstep opentracing.Tracer) *Tracer {
	t := &Tracer{
		netTrace:  netTrace,
		lightstep: lightstep,
	}
	t.noopSpan.tracer = t
	return t
}

// isNoop returns if this is a noop tracer, which means that span events don't
// go anywhere, unless they are being recorded. Such a tracer is still capable
// of snowball tracing.
func (t *Tracer) isNoop() bool {
	return !t.netTrace && t.lightstep == nil
}

func (t *Tracer) getLightstepSpanIDs(
	spanCtx opentracing.SpanContext,
) (traceID uint64, spanID uint64) {
	// Retrieve the trace metadata from lightstep.
	carrier := &SpanContextCarrier{}
	if err := t.lightstep.Inject(spanCtx, basictracer.Delegator, carrier); err != nil {
		panic(fmt.Sprintf("error injecting lightstep context %s", err))
	}
	return carrier.TraceID, carrier.SpanID
}

type forceOption struct{}

// Force is a StartSpanOption that forces creation of a real span.
//
// When tracing is disabled all spans are noopSpans; these spans aren't
// capable of recording, so this option should be passed to StartSpan if the
// caller wants to be able to call StartRecording on the resulting span.
var Force opentracing.StartSpanOption = forceOption{}

func (forceOption) Apply(*opentracing.StartSpanOptions) {}

// StartSpan is part of the opentracing.Tracer interface.
func (t *Tracer) StartSpan(
	operationName string, opts ...opentracing.StartSpanOption,
) opentracing.Span {
	// We don't use the StartSpanOption.Apply() pattern because it causes a heap
	// allocation which we want to avoid in the noopSpan case (it shows up in
	// profiles to the tune of 0.2% with read-only kv load). This pattern is
	// pretty useless anyway because the members of StartSpanOptions don't allow
	// for any extensibility.

	var startTime time.Time
	var tags map[string]interface{}
	var parentType opentracing.SpanReferenceType
	var parentCtx spanContext
	hasParent := false
	force := false

	// Decode the options.
	for _, o := range opts {
		switch o := o.(type) {
		case opentracing.SpanReference:
			if o.Type != opentracing.ChildOfRef && o.Type != opentracing.FollowsFromRef {
				break
			}
			if _, noopCtx := o.ReferencedContext.(noopSpanContext); noopCtx {
				break
			}
			hasParent = true
			parentType = o.Type
			parentCtx = o.ReferencedContext.(spanContext)
			// TODO(radu): can we do something for multiple references?

		case opentracing.StartTime:
			startTime = time.Time(o)

		case opentracing.Tags:
			tags = o

		case forceOption:
			force = true

		default:
			panic(fmt.Sprintf("unknown StartSpanOption %T", o))
		}
	}

	if !force && !hasParent && t.isNoop() {
		return &t.noopSpan
	}

	s := &span{
		tracer:    t,
		operation: operationName,
		startTime: startTime,
		duration:  -1,
	}
	if s.startTime.IsZero() {
		s.startTime = time.Now()
	}

	// Start recording if necessary.
	if hasParent {
		if parentCtx.recordingGroup != nil {
			// If the parent span is recording, add this span to the recording group.
			s.enableRecording(parentCtx.recordingGroup)
		} else if parentCtx.Baggage[Snowball] != "" {
			// Automatically enable recording if we have the Snowball baggage item.
			s.enableRecording(new(spanGroup))
		}
	}

	// If we are using lightstep, we create a new lightstep span and use the
	// metadata (TraceID, SpanID, Baggage) from that span. Otherwise, we generate
	// our own IDs.
	if t.lightstep != nil {
		// Create the shadow lightstep span.
		var lsOpts []opentracing.StartSpanOption
		// Replicate the options, using the lightstep context in the reference.
		if !startTime.IsZero() {
			lsOpts = append(lsOpts, opentracing.StartTime(startTime))
		}
		if tags != nil {
			lsOpts = append(lsOpts, opentracing.Tags(tags))
		}
		if hasParent {
			if parentCtx.lightstep == nil {
				panic("lightstep span derived from non-lightstep span")
			}
			lsOpts = append(lsOpts, opentracing.SpanReference{
				Type:              parentType,
				ReferencedContext: parentCtx.lightstep,
			})
		}
		s.lightstep = t.lightstep.StartSpan(operationName, lsOpts...)
		s.TraceID, s.SpanID = t.getLightstepSpanIDs(s.lightstep.Context())
		if hasParent && s.TraceID != parentCtx.TraceID {
			panic(fmt.Sprintf(
				"TraceID doesn't match between parent (%d) and child (%d) spans",
				parentCtx.TraceID, s.TraceID,
			))
		}
	} else {
		s.SpanID = uint64(rand.Int63())

		if !hasParent {
			// No parent Span; allocate new trace id.
			s.TraceID = uint64(rand.Int63())
		} else {
			s.TraceID = parentCtx.TraceID
		}
	}

	// Copy Baggage from parent context.
	if hasParent && len(parentCtx.Baggage) > 0 {
		s.Baggage = make(map[string]string, len(parentCtx.Baggage))
		for k, v := range parentCtx.Baggage {
			s.Baggage[k] = v
		}
	}

	if t.netTrace {
		s.netTr = trace.New("tracing", operationName)
		s.netTr.SetMaxEvents(maxLogsPerSpan)
	}

	if hasParent {
		s.parentSpanID = parentCtx.SpanID
		if l := len(parentCtx.Baggage); l > 0 {
			s.Baggage = make(map[string]string, l)
			for k, v := range parentCtx.Baggage {
				s.Baggage[k] = v
			}
		}
	}

	// Set tags but only if they actually go somewhere (x/net/trace or lightstep).
	if t.netTrace || t.lightstep != nil {
		for k, v := range tags {
			s.SetTag(k, v)
		}
		// Copy baggage items to tags so they show up in the Lightstep UI or x/net/trace.
		for k, v := range s.Baggage {
			s.SetTag(k, v)
		}
	}

	return s
}

var dummyTracer = basictracer.New(nil)

// Inject is part of the opentracing.Tracer interface.
func (t *Tracer) Inject(
	osc opentracing.SpanContext, format interface{}, carrier interface{},
) error {
	if _, noopCtx := osc.(noopSpanContext); noopCtx {
		return nil
	}
	sc := osc.(spanContext)
	if t.lightstep != nil {
		// If using lightstep, serialize that context.
		return t.lightstep.Inject(sc.lightstep, format, carrier)
	}
	// We use the Inject/Extract functionality of a dummy basictracer.
	bc := basictracer.SpanContext{
		TraceID: sc.TraceID,
		SpanID:  sc.SpanID,
		Baggage: sc.Baggage,
	}
	return dummyTracer.Inject(bc, format, carrier)
}

// Extract is part of the opentracing.Tracer interface.
func (t *Tracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	if t.lightstep != nil {
		// Extract the lightstep context. Note that lightstep uses basictracer
		// underneath so things won't break even if one side is using lightstep and
		// one isn't.
		lightstepCtx, err := t.lightstep.Extract(format, carrier)
		if err != nil {
			return nil, err
		}

		sc := spanContext{lightstep: lightstepCtx}
		sc.TraceID, sc.SpanID = t.getLightstepSpanIDs(lightstepCtx)
		lightstepCtx.ForeachBaggageItem(func(k, v string) bool {
			if sc.Baggage == nil {
				sc.Baggage = make(map[string]string)
			}
			sc.Baggage[k] = v
			return true
		})
		return sc, nil
	}

	// We use the Inject/Extract functionality of a dummy basictracer.
	extracted, err := dummyTracer.Extract(format, carrier)
	if err != nil {
		return nil, err
	}
	bc := extracted.(basictracer.SpanContext)
	if bc.TraceID == 0 {
		return noopSpanContext{}, nil
	}
	return spanContext{
		spanMeta: spanMeta{
			TraceID: bc.TraceID,
			SpanID:  bc.SpanID,
			Baggage: bc.Baggage,
		},
	}, nil
}

// FinishSpan closes the given span (if not nil). It is a convenience wrapper
// for span.Finish() which tolerates nil spans.
func FinishSpan(span opentracing.Span) {
	if span != nil {
		span.Finish()
	}
}

// ForkCtxSpan checks if ctx has a Span open; if it does, it creates a new Span
// that follows from the original Span. This allows the resulting context to be
// used in an async task that might outlive the original operation.
//
// Returns the new context and the new span (if any). The span should be
// closed via FinishSpan.
func ForkCtxSpan(ctx context.Context, opName string) (context.Context, opentracing.Span) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		if IsNoopSpan(span) {
			// Optimization: avoid ContextWithSpan call if tracing is disabled.
			return ctx, span
		}
		tr := span.Tracer()
		newSpan := tr.StartSpan(opName, opentracing.FollowsFrom(span.Context()))
		return opentracing.ContextWithSpan(ctx, newSpan), newSpan
	}
	return ctx, nil
}

// ChildSpan opens a span as a child of the current span in the context (if
// there is one).
//
// Returns the new context and the new span (if any). The span should be
// closed via FinishSpan.
func ChildSpan(ctx context.Context, opName string) (context.Context, opentracing.Span) {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return ctx, nil
	}
	if IsNoopSpan(span) {
		// Optimization: avoid ContextWithSpan call if tracing is disabled.
		return ctx, span
	}
	newSpan := span.Tracer().StartSpan(opName, opentracing.ChildOf(span.Context()))
	return opentracing.ContextWithSpan(ctx, newSpan), newSpan
}

var lightstepToken = envutil.EnvOrDefaultString("COCKROACH_LIGHTSTEP_TOKEN", "")
var enableTracing = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_TRACING", false)

// NewTracer creates a Tracer which records to the net/trace
// endpoint.
func NewTracer() opentracing.Tracer {
	if !enableTracing {
		// Create a tracer that drops all events unless we enable
		// recording on a span.
		return newTracer(false /* netTrace */, nil /* lightstep */)
	}
	var lsTr opentracing.Tracer
	if lightstepToken != "" {
		lsTr = lightstep.NewTracer(lightstep.Options{
			AccessToken:    lightstepToken,
			MaxLogsPerSpan: maxLogsPerSpan,
			UseGRPC:        true,
		})
	}
	return newTracer(true /* netTrace */, lsTr)
}

// EnsureContext checks whether the given context.Context contains a Span. If
// not, it creates one using the provided Tracer and wraps it in the returned
// Span. The returned closure must be called after the request has been fully
// processed.
func EnsureContext(
	ctx context.Context, tracer opentracing.Tracer, name string,
) (context.Context, func()) {
	if opentracing.SpanFromContext(ctx) == nil {
		sp := tracer.StartSpan(name)
		return opentracing.ContextWithSpan(ctx, sp), sp.Finish
	}
	return ctx, func() {}
}

// Disable is for benchmarking use and causes all future tracers to deal in
// no-ops. Calling the returned closure undoes this effect. There is no
// synchronization, so no moving parts are allowed while Disable and the
// closure are called.
func Disable() func() {
	return SetEnabled(false)
}

// SetEnabled enables or disables tracing. Returns a function that restores
// the previous setting.
func SetEnabled(enabled bool) func() {
	orig := enableTracing
	enableTracing = enabled
	return func() {
		enableTracing = orig
	}
}

// EncodeRawSpan encodes a raw span into bytes, using the given dest slice
// as a buffer.
func EncodeRawSpan(rawSpan *basictracer.RawSpan, dest []byte) ([]byte, error) {
	// This is not a greatly efficient (but convenient) use of gob.
	buf := bytes.NewBuffer(dest[:0])
	e := gob.NewEncoder(buf)
	var err error

	encode := func(arg interface{}) {
		if err == nil {
			err = e.Encode(arg)
		}
	}

	// We cannot use gob for the Logs (because Field doesn't export the necessary
	// fields). We use it for the other fields.
	encode(rawSpan.Context)
	encode(rawSpan.ParentSpanID)
	encode(rawSpan.Operation)
	encode(rawSpan.Start)
	encode(rawSpan.Duration)
	encode(rawSpan.Tags)

	// Encode the number of LogRecords, then the records.
	encode(int32(len(rawSpan.Logs)))
	for _, lr := range rawSpan.Logs {
		encode(lr.Timestamp)
		// Encode the number of Fields.
		encode(int32(len(lr.Fields)))
		for _, f := range lr.Fields {
			encode(f.Key())
			// Encode the field value as a string.
			encode(fmt.Sprint(f.Value()))
		}
	}

	return buf.Bytes(), err
}

// DecodeRawSpan unmarshals into the given RawSpan.
func DecodeRawSpan(enc []byte, dest *basictracer.RawSpan) error {
	d := gob.NewDecoder(bytes.NewBuffer(enc))
	var err error
	decode := func(arg interface{}) {
		if err == nil {
			err = d.Decode(arg)
		}
	}

	decode(&dest.Context)
	decode(&dest.ParentSpanID)
	decode(&dest.Operation)
	decode(&dest.Start)
	decode(&dest.Duration)
	decode(&dest.Tags)

	var numLogs int32
	decode(&numLogs)
	dest.Logs = make([]opentracing.LogRecord, numLogs)
	for i := range dest.Logs {
		lr := &dest.Logs[i]
		decode(&lr.Timestamp)

		var numFields int32
		decode(&numFields)
		lr.Fields = make([]otlog.Field, numFields)
		for j := range lr.Fields {
			var key, val string
			decode(&key)
			decode(&val)
			lr.Fields[j] = otlog.String(key, val)
		}
	}

	return err
}

// StartSnowballTrace takes in a context and returns a derived one with a
// "snowball span" in it. The caller takes ownership of this span from the
// returned context and is in charge of Finish()ing it. The span has recording
// enabled.
func StartSnowballTrace(
	ctx context.Context, tracer opentracing.Tracer, opName string,
) (context.Context, opentracing.Span, error) {
	var span opentracing.Span
	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
		span = parentSpan.Tracer().StartSpan(opName, opentracing.ChildOf(parentSpan.Context()), Force)
	} else {
		span = tracer.StartSpan(opName, Force)
	}
	StartRecording(span)
	span.SetBaggageItem(Snowball, "1")
	return opentracing.ContextWithSpan(ctx, span), span, nil
}

// JoinRemoteTrace takes a Context and returns a derived one with a Span that's
// a child of a remote span carried over by carrier. The caller is responsible
// for Finish()ing this span.
//
// If the trace is a "snowball trace", then the created span will have recording
// enabled (GetRecording() can be used).
func JoinRemoteTrace(
	ctx context.Context, tr opentracing.Tracer, carrier *SpanContextCarrier, opName string,
) (context.Context, opentracing.Span, error) {
	if tr == nil {
		return ctx, nil, errors.Errorf("JoinRemoteTrace called with nil Tracer")
	}

	wireContext, err := tr.Extract(basictracer.Delegator, carrier)
	if err != nil {
		return ctx, nil, err
	}

	span := tr.StartSpan(opName, opentracing.FollowsFrom(wireContext))
	return opentracing.ContextWithSpan(ctx, span), span, nil
}

// IngestRemoteSpans takes a bunch of encoded spans and adds them to the record
// for the span in the context. The context is expected to contain a span for
// which recording is enabled.
func IngestRemoteSpans(ctx context.Context, remoteSpans [][]byte) error {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return errors.Errorf("trying to ingest remote spans but there is no recording span set up")
	}
	rawSpans := make([]basictracer.RawSpan, len(remoteSpans))
	for i, encSp := range remoteSpans {
		if err := DecodeRawSpan(encSp, &rawSpans[i]); err != nil {
			return err
		}
	}
	return ImportRemoteSpans(span, rawSpans)
}
