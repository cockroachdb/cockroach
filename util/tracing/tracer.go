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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/caller"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// Snowball is set as Baggage on traces which are used for snowball tracing.
const Snowball = "sb"

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
		sp, err := tr.Join(opName, basictracer.Delegator, carrier)
		switch err {
		case nil:
			sp.LogEvent(opName)
			return sp, nil
		case opentracing.ErrTraceNotFound:
		default:
			return nil, err
		}
	}
	return tr.StartSpan(opName), nil
}

// JoinOrNewSnowball returns a Span which records directly via the specified
// callback. If the given DelegatingCarrier is nil, a new Span is created.
// otherwise, the created Span is a child.
func JoinOrNewSnowball(opName string, carrier *Span, callback func(sp basictracer.RawSpan)) (opentracing.Span, error) {
	tr := basictracer.NewWithOptions(defaultOptions(callback))
	sp, err := JoinOrNew(tr, carrier, opName)
	if err == nil {
		// We definitely want to sample a Snowball trace.
		// This must be set *before* SetBaggageItem, as that will otherwise be ignored.
		ext.SamplingPriority.Set(sp, 1)
		sp.SetBaggageItem(Snowball, "1")
	}
	return sp, err
}

func defaultOptions(recorder func(basictracer.RawSpan)) basictracer.Options {
	opts := basictracer.DefaultOptions()
	opts.ShouldSample = func(traceID uint64) bool { return false }
	opts.TrimUnsampledSpans = true
	opts.Recorder = CallbackRecorder(recorder)
	opts.NewSpanEventListener = basictracer.NetTraceIntegrator
	opts.DebugAssertUseAfterFinish = true // provoke crash on use-after-Finish
	return opts
}

// newTracer implements NewTracer and allows that function to be mocked out via Disable().
var newTracer = func() opentracing.Tracer {
	return basictracer.NewWithOptions(defaultOptions(func(_ basictracer.RawSpan) {}))
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
	err := gob.NewEncoder(buf).Encode(rawSpan)
	return buf.Bytes(), err
}

// DecodeRawSpan unmarshals into the given RawSpan.
func DecodeRawSpan(enc []byte, dest *basictracer.RawSpan) error {
	return gob.NewDecoder(bytes.NewBuffer(enc)).Decode(dest)
}
