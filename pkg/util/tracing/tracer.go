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

package tracing

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/lightstep/lightstep-tracer-go"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	otext "github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
)

// Snowball is set as Baggage on traces which are used for snowball tracing.
const Snowball = "sb"

// maxLogsPerSpan limits the number of logs in a Span; use a comfortable limit.
const maxLogsPerSpan = 1000

// A CallbackRecorder immediately invokes itself on received trace spans.
type CallbackRecorder func(sp basictracer.RawSpan)

// RecordSpan implements basictracer.SpanRecorder.
func (cr CallbackRecorder) RecordSpan(sp basictracer.RawSpan) {
	if cr != nil {
		cr(sp)
	}
}

// NewTracerAndSpanFor7881 creates a new tracer and a root span. The tracer is
// to be used for tracking down #7881.
// Note that this is not a "snowball" trace; otherwise this function mirrors
// StartSnowballTrace().
func NewTracerAndSpanFor7881(ctx context.Context) (context.Context, *RecordedTrace, error) {
	if recorderFromCtx(ctx) != nil {
		// Unclear what to do if there's already a recorder. Are we intending to
		// overwrite it or should we record spans to both of them? For now, we don't
		// support multiple snowballs on the same trace.
		return ctx, nil, errors.Errorf("there's already a recorder in the ctx.")
	}
	if span := opentracing.SpanFromContext(ctx); span != nil {
		teeSp, recorder := createTeeSpanWithRecorder(
			span, false /* adopt */, "root-for-7881", false /* snowball*/)
		return opentracing.ContextWithSpan(
			withRecorder(ctx, recorder), teeSp), recorder, nil
	}
	recordingTr, recorder := NewRecordingTracer()
	sp := recordingTr.StartSpan("root-for-7881")
	return opentracing.ContextWithSpan(
		withRecorder(ctx, recorder), sp), recorder, nil
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
	newSpan := span.Tracer().StartSpan(opName, opentracing.ChildOf(span.Context()))
	return opentracing.ContextWithSpan(ctx, newSpan), newSpan
}

// netTraceIntegrator is passed into basictracer as NewSpanEventListener
// and causes all traces to be registered with the net/trace endpoint.
func netTraceIntegrator() func(basictracer.SpanEvent) {
	var tr trace.Trace
	return func(e basictracer.SpanEvent) {
		switch t := e.(type) {
		case basictracer.EventCreate:
			tr = trace.New("tracing", t.OperationName)
			tr.SetMaxEvents(maxLogsPerSpan)
		case basictracer.EventFinish:
			tr.Finish()
		case basictracer.EventTag:
			tr.LazyPrintf("%s:%v", t.Key, t.Value)
		case basictracer.EventLogFields:
			// TODO(radu): when LightStep supports arbitrary fields, we should make
			// the formatting of the message consistent with that. Until then we treat
			// legacy events that just have an "event" key specially.
			if len(t.Fields) == 1 && t.Fields[0].Key() == "event" {
				tr.LazyPrintf("%s", t.Fields[0].Value())
			} else {
				var buf bytes.Buffer
				for i, f := range t.Fields {
					if i > 0 {
						buf.WriteByte(' ')
					}
					fmt.Fprintf(&buf, "%s:%v", f.Key(), f.Value())
				}

				tr.LazyPrintf("%s", buf.String())
			}
		case basictracer.EventLog:
			panic("EventLog is deprecated")
		}
	}
}

// basicTracerOptions initializes options for basictracer.
// The recorder should be nil if we don't need to record spans.
func basictracerOptions(recorder func(basictracer.RawSpan)) basictracer.Options {
	opts := basictracer.DefaultOptions()
	opts.ShouldSample = func(traceID uint64) bool { return false }
	opts.TrimUnsampledSpans = true
	opts.NewSpanEventListener = netTraceIntegrator
	opts.DebugAssertUseAfterFinish = true // provoke crash on use-after-Finish
	if recorder == nil {
		opts.Recorder = CallbackRecorder(nil)
		// If we are not recording the spans, there is no need to keep them in
		// memory. Events still get passed to netTraceIntegrator.
		opts.DropAllLogs = true
	} else {
		opts.Recorder = CallbackRecorder(recorder)
		// Set a comfortable limit of log events per span.
		opts.MaxLogsPerSpan = maxLogsPerSpan
	}
	return opts
}

var lightstepToken = envutil.EnvOrDefaultString("COCKROACH_LIGHTSTEP_TOKEN", "")

// By default, if a lightstep token is specified we trace to both Lightstep and
// net/trace. If this flag is enabled, we will only trace to Lightstep.
var lightstepOnly = envutil.EnvOrDefaultBool("COCKROACH_LIGHTSTEP_ONLY", false)

var enableTracing = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_TRACING", false)

// newTracer implements NewTracer and allows that function to be mocked out via Disable().
var newTracer = func() opentracing.Tracer {
	if !enableTracing {
		return opentracing.NoopTracer{}
	}
	if lightstepToken != "" {
		lsTr := lightstep.NewTracer(lightstep.Options{
			AccessToken:    lightstepToken,
			MaxLogsPerSpan: maxLogsPerSpan,
			UseGRPC:        true,
		})
		if lightstepOnly {
			return lsTr
		}
		basicTr := basictracer.NewWithOptions(basictracerOptions(nil))
		// The TeeTracer uses the first tracer for serialization of span contexts;
		// lightspan needs to be first because it correlates spans between nodes.
		return NewTeeTracer(lsTr, basicTr)
	}
	return basictracer.NewWithOptions(basictracerOptions(nil))
}

// NewTracer creates a Tracer which records to the net/trace
// endpoint.
func NewTracer() opentracing.Tracer {
	return newTracer()
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
	orig := newTracer
	newTracer = func() opentracing.Tracer { return opentracing.NoopTracer{} }
	return func() {
		newTracer = orig
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

// RecordedTrace is associated with a span and records the respective span and
// all child spans.
// It is created, together with a request-specific tracer, by
// NewRecordingTracer().
// The addSpan() method is used as a tracer recorder.
type RecordedTrace struct {
	syncutil.Mutex
	spans []basictracer.RawSpan
	done  bool
}

func (tr *RecordedTrace) addSpan(rs basictracer.RawSpan) {
	tr.Lock()
	defer tr.Unlock()
	if tr.done {
		// This is a late span that we must discard because the request was already
		// completed.
		return
	}
	tr.spans = append(tr.spans, rs)
}

// AddDummySpan is an external version of addSpan.
// TODO(andrei): get rid of this when explainTraceNode goes away.
func (tr *RecordedTrace) AddDummySpan(rs basictracer.RawSpan) {
	tr.addSpan(rs)
}

// GetSpans returns all accumulated spans.
func (tr *RecordedTrace) GetSpans() []basictracer.RawSpan {
	tr.Lock()
	defer tr.Unlock()
	return tr.spans
}

// Done marks the recorder such that future spans are rejected.
func (tr *RecordedTrace) Done() {
	tr.Lock()
	defer tr.Unlock()
	tr.done = true
}

// ClearSpans clears the spans accumulated so far.
// TODO(andrei): remove this once the we get rid of explainTraceNode and its
// weird use case for this.
func (tr *RecordedTrace) ClearSpans() {
	tr.Lock()
	defer tr.Unlock()
	tr.spans = nil
}

// contextRecorderKeyType is an empty type for the handle associated with the
// RecordedTrace value (see context.Value).
type contextRecorderKeyType struct{}

// withRecorder returns a context derived from the given context, for which
// recorderFromCtx returns the given recorder.
func withRecorder(ctx context.Context, rec *RecordedTrace) context.Context {
	return context.WithValue(ctx, contextRecorderKeyType{}, rec)
}

// recorderFromCtx returns the tracer set on the context (or a parent context) via
// withRecorder.
func recorderFromCtx(ctx context.Context) *RecordedTrace {
	if recVal := ctx.Value(contextRecorderKeyType{}); recVal != nil {
		return recVal.(*RecordedTrace)
	}
	return nil
}

// NewRecordingTracer creates a request-specific tracer that will record its
// trace.
func NewRecordingTracer() (opentracing.Tracer, *RecordedTrace) {
	rec := new(RecordedTrace)
	opts := basictracerOptions(rec.addSpan)
	// Sample everything. This is required so that logs going to the spans don't
	// get trimmed.
	opts.ShouldSample = func(traceID uint64) bool { return true }
	return basictracer.NewWithOptions(opts), rec
}

// createTeeSpanWithRecorder tees a current span. It creates a per-request
// TeeTracer with a root span that's (current span, new recording span). This
// TeeSpan is returned and should be used instead of the passed-in span for all
// the subsequent operations that need recording. The caller takes ownership and
// is required to Finish() it.
//
// adopt specifies if the root TeeSpan takes ownership of the passed-in span. If
// set, then the caller is no longer in charge of Finish()ing the passed-in
// span; instead it's enough to Finish() the returned TeeSpan. If not set, then
// the TeeSpan does not take ownership of the passed-in span and so someone else
// remains in charge of Finish()ing it.
//
// snowball specifies if the new span should be snowballed.
func createTeeSpanWithRecorder(
	span opentracing.Span, adopt bool, opName string, snowball bool,
) (opentracing.Span, *RecordedTrace) {
	// We'll create a TeeTracer, having the existing tracer on the left and a
	// RecordingTracer on the right. We'll then use the TeeTracer to create a new
	// span, having the existing span on the left, and a new one on the right.
	recordingTr, recorder := NewRecordingTracer()
	recordingSpan := recordingTr.StartSpan(opName)
	// The span's tracer needs to be the first tracer in the Tee, and the
	// corresponding span needs to be the first argument to CreateSpanFrom, since
	// we want the span's SpanContext to be serialized when RPCs are invoked.
	tee := NewTeeTracer(span.Tracer(), recordingTr)
	teeSpan := tee.CreateSpanFrom(
		spanWithOpt{
			Span:  span,
			owned: adopt,
		},
		spanWithOpt{
			Span:  recordingSpan,
			owned: true,
		})
	if snowball {
		// We want to sample a Snowball trace because otherwise the logged events
		// are dropped. This must be set *before* SetBaggageItem, as that will
		// otherwise be ignored. This is a bit subtle: in principle we don't need
		// `span` to be sampled necessarily, we just need recordingSpan to be
		// sampled, and we need any remote child spans to be sampled. But to get the
		// remote spans to be sampled, we need to SetBaggageItem(Snowball) on
		// `span`, and for that call to work we need `span` to be sampled. So, we
		// just make `teeSpan` sampled, which will propagate to all constituent
		// spans.
		otext.SamplingPriority.Set(teeSpan, 1)
		teeSpan.SetBaggageItem(Snowball, "1")
	}
	return teeSpan, recorder
}

// StartSnowballTrace takes in a context and returns a derived one with a
// "snowball span" in it. The caller takes ownership of this span from the
// returned context and is in charge of Finish()ing it. A recorder that will
// receive both local and remote child spans is also returned.
//
// If there's already a span in the passed-in context, then a per-request
// TeeTracer will be used for creating this new snowball span; the existing span
// thus continues to be in effect. Note that ownership of the existing span is
// not transferred in any way - it will not be Finish()ed when the TeeSpan is
// Finish()ed.
func StartSnowballTrace(
	ctx context.Context, opName string,
) (context.Context, *RecordedTrace, error) {
	if recorderFromCtx(ctx) != nil {
		// Unclear what to do if there's already a recorder. Are we intending to
		// overwrite it or should we record spans to both of them? For now, we don't
		// support multiple snowballs on the same trace.
		return ctx, nil, errors.Errorf("there's already a recorder in the ctx.")
	}
	if span := opentracing.SpanFromContext(ctx); span != nil {
		// We're in the context of a span; we'll create a TeeTracer.
		teeSpan, recorder := createTeeSpanWithRecorder(
			span, false /* adopt */, opName, true /* snowball */)
		return opentracing.ContextWithSpan(
			withRecorder(ctx, recorder), teeSpan), recorder, nil
	}
	// There's no active trace so no need for a TeeTracer.
	tracer, recorder := NewRecordingTracer()
	rootSp := tracer.StartSpan(opName)
	// We definitely want to sample a Snowball trace.
	// This must be set *before* SetBaggageItem, as that will otherwise be ignored.
	otext.SamplingPriority.Set(rootSp, 1)
	rootSp.SetBaggageItem(Snowball, "1")
	return opentracing.ContextWithSpan(
		withRecorder(ctx, recorder), rootSp), recorder, nil
}

// JoinRemoteTrace takes a Context and returns a derived one with a Span that's
// a child of a remote span carried over by carrier. The caller is responsible
// for Finish()ing this span.
// If the trace is a "snowball trace", then the span will be created by a
// per-request TeeTracer, which both records the trace and also pipes everything
// to tr.
//
// tr must be a tracer able to extract the SpanContext from carrier.
//
// It returns the derived context and a RecordedTrace. This will be nil if the
// remote trace is not a "snowball trace"; otherwise, it will be a handle on the
// recording of the trace rooted by the span created here. When this span is
// closed, the caller should serialize the RecordedTrace and send it over the
// wire to the remote party.
func JoinRemoteTrace(
	ctx context.Context, tr opentracing.Tracer, carrier *SpanContextCarrier, opName string,
) (context.Context, *RecordedTrace, error) {
	if tr == nil {
		return ctx, nil, errors.Errorf("JoinRemoteTrace called with nil Tracer")
	}

	var sp opentracing.Span
	wireContext, err := tr.Extract(basictracer.Delegator, carrier)
	switch err {
	case nil:
		sp = tr.StartSpan(opName, opentracing.FollowsFrom(wireContext))
		// Copy baggage items to tags so they show up in the Lightstep UI.
		sp.Context().ForeachBaggageItem(func(k, v string) bool {
			sp.SetTag(k, v)
			return true
		})
		sp.LogFields(otlog.String("event", opName))
	case opentracing.ErrSpanContextNotFound:
		fallthrough
	default:
		return ctx, nil, err
	}
	if sp.BaggageItem(Snowball) == "" {
		return opentracing.ContextWithSpan(ctx, sp), nil, nil
	}
	// This is a "snowball trace"; we'll use a TeeTracer.
	if recorderFromCtx(ctx) != nil {
		// Unclear what to do if there's already a recorder. Are we intending to
		// overwrite it or should we record spans to both of them? For now, we don't
		// support multiple snowballs on the same trace.
		return ctx, nil, errors.Errorf("there's already a recorder in the ctx")
	}
	teeSpan, recorder := createTeeSpanWithRecorder(
		sp, true /* adopt */, opName, true /* snowball */)
	return opentracing.ContextWithSpan(
		withRecorder(ctx, recorder), teeSpan), recorder, nil
}

// IngestRemoteSpans takes a bunch of encoded spans and passes them to a
// Recorder in the Context.
func IngestRemoteSpans(ctx context.Context, remoteSpans [][]byte) error {
	rec := recorderFromCtx(ctx)
	if rec == nil {
		return errors.Errorf("asked to IngestRemoteSpan with a context without " +
			"a Recorder... Why did we get some spans to ingest if we weren't setup " +
			"for snowball tracing?")
	}
	for _, encSp := range remoteSpans {
		var newSp basictracer.RawSpan
		if err := DecodeRawSpan(encSp, &newSp); err != nil {
			return err
		}
		rec.addSpan(newSp)
	}
	return nil
}
