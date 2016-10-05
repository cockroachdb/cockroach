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

package tracing

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/lightstep/lightstep-tracer-go"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
)

// Snowball is set as Baggage on traces which are used for snowball tracing.
const Snowball = "sb"

// maxLogsPerSpan limits the number of logs in a Span; use a comfortable limit.
const maxLogsPerSpan = 1000

// A CallbackRecorder immediately invokes itself on received trace spans.
type CallbackRecorder func(sp basictracer.RawSpan)

// RecordSpan implements basictracer.SpanRecorder.
func (cr CallbackRecorder) RecordSpan(sp basictracer.RawSpan) {
	cr(sp)
}

// JoinOrNew creates a new Span joined to the provided DelegatingCarrier or
// creates Span from the given tracer.
func JoinOrNew(tr opentracing.Tracer, carrier *Span, opName string) (opentracing.Span, error) {
	if carrier != nil {
		wireContext, err := tr.Extract(basictracer.Delegator, carrier)
		switch err {
		case nil:
			sp := tr.StartSpan(opName, opentracing.FollowsFrom(wireContext))

			// Copy baggage items to tags so they show up in the Lightstep UI.
			sp.Context().ForeachBaggageItem(func(k, v string) bool { sp.SetTag(k, v); return true })

			sp.LogEvent(opName)
			return sp, nil
		case opentracing.ErrSpanContextNotFound:
		default:
			return nil, err
		}
	}
	return tr.StartSpan(opName), nil
}

// JoinOrNewSnowball returns a Span which records directly via the specified
// callback. If the given DelegatingCarrier is nil, a new Span is created.
// otherwise, the created Span is a child.
//
// The recorder should be nil if we don't need to record spans.
//
// TODO(andrei): JoinOrNewSnowball creates a new tracer, which is not kosher.
// Also this can't use the lightstep tracer.
func JoinOrNewSnowball(
	opName string, carrier *Span, recorder func(sp basictracer.RawSpan),
) (opentracing.Span, error) {
	tr := basictracer.NewWithOptions(basictracerOptions(recorder))
	sp, err := JoinOrNew(tr, carrier, opName)
	if err == nil {
		// We definitely want to sample a Snowball trace.
		// This must be set *before* SetBaggageItem, as that will otherwise be ignored.
		ext.SamplingPriority.Set(sp, 1)
		sp.SetBaggageItem(Snowball, "1")
	}
	return sp, err
}

// NewTracerAndSpanFor7881 creates a new tracer and a root span. The tracer is
// to be used for tracking down #7881; it runs a callback for each finished span
// (and the callback used accumulates the spans in a SQL txn).
func NewTracerAndSpanFor7881(
	callback func(sp basictracer.RawSpan),
) (opentracing.Span, opentracing.Tracer, error) {
	opts := basictracerOptions(callback)
	// Don't trim the logs in "unsampled" spans". Note that this tracer does not
	// use sampling; instead it uses an ad-hoc mechanism for marking spans of
	// interest.
	opts.TrimUnsampledSpans = false
	tr := basictracer.NewWithOptions(opts)
	sp, err := JoinOrNew(tr, nil, "sql txn")
	return sp, tr, err
}

// ForkCtxSpan checks if ctx has a Span open; if it does, it creates a new Span
// that follows from the original Span. This allows the resulting context to be
// used in an async task that might outlive the original operation.
//
// Returns the new context and a function that closes the span.
func ForkCtxSpan(ctx context.Context, opName string) (context.Context, func()) {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		if span.BaggageItem(Snowball) == "1" {
			// If we are doing snowball tracing, the span might outlive the snowball
			// tracer (calling the record function when it is no longer legal to do
			// so). Return a context with no span in this case.
			return opentracing.ContextWithSpan(ctx, nil), func() {}
		}
		tr := span.Tracer()
		newSpan := tr.StartSpan(opName, opentracing.FollowsFrom(span.Context()))
		return opentracing.ContextWithSpan(ctx, newSpan), func() { newSpan.Finish() }
	}
	return ctx, func() {}
}

// ChildSpan opens a span as a child of the current span in the context (if
// there is one).
//
// Returns the new context and a function that closes the span.
func ChildSpan(ctx context.Context, opName string) (context.Context, func()) {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return ctx, func() {}
	}
	newSpan := span.Tracer().StartSpan(opName, opentracing.ChildOf(span.Context()))
	return opentracing.ContextWithSpan(ctx, newSpan), func() { newSpan.Finish() }
}

// netTraceIntegrator is passed into basictracer as NewSpanEventListener
// and causes all traces to be registered with the net/trace endpoint.
func netTraceIntegrator() func(basictracer.SpanEvent) {
	var tr trace.Trace
	return func(e basictracer.SpanEvent) {
		switch t := e.(type) {
		case basictracer.EventCreate:
			tr = trace.New("tracing", t.OperationName)
			// TODO(radu): call SetMaxEvents when #9748 is fixed.
		case basictracer.EventFinish:
			tr.Finish()
		case basictracer.EventTag:
			tr.LazyPrintf("%s:%v", t.Key, t.Value)
		case basictracer.EventLogFields:
			var buf bytes.Buffer
			for i, f := range t.Fields {
				if i > 0 {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%s:%v", f.Key(), f.Value())
			}

			tr.LazyPrintf("%s", buf.String())
		case basictracer.EventLog:
			if t.Payload != nil {
				tr.LazyPrintf("%s (payload %v)", t.Event, t.Payload)
			} else {
				tr.LazyPrintf("%s", t.Event)
			}
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
		opts.Recorder = CallbackRecorder(func(_ basictracer.RawSpan) {})
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

// newTracer implements NewTracer and allows that function to be mocked out via Disable().
var newTracer = func() opentracing.Tracer {
	if lightstepToken != "" {
		lsTr := lightstep.NewTracer(lightstep.Options{
			AccessToken:    lightstepToken,
			MaxLogsPerSpan: maxLogsPerSpan,
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
func EnsureContext(ctx context.Context, tracer opentracing.Tracer) (context.Context, func()) {
	_, _, funcName := caller.Lookup(1)
	if opentracing.SpanFromContext(ctx) == nil {
		sp := tracer.StartSpan(funcName)
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

// contextTracerKeyType is an empty type for the handle associated with the
// tracer value (see context.Value).
type contextTracerKeyType struct{}

// WithTracer returns a context derived from the given context, for which
// TracerFromCtx returns the given tracer.
func WithTracer(ctx context.Context, tracer opentracing.Tracer) context.Context {
	return context.WithValue(ctx, contextTracerKeyType{}, tracer)
}

// TracerFromCtx returns the tracer set on the context (or a parent context) via
// WithTracer.
func TracerFromCtx(ctx context.Context) opentracing.Tracer {
	if tracerVal := ctx.Value(contextTracerKeyType{}); tracerVal != nil {
		return tracerVal.(opentracing.Tracer)
	}
	return nil
}
