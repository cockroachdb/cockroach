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
	tr := basictracer.New(CallbackRecorder(callback))
	sp, err := JoinOrNew(tr, carrier, opName)
	if err == nil {
		sp.SetBaggageItem(Snowball, "1")
		// We definitely want to sample a Snowball trace.
		ext.SamplingPriority.Set(sp, 1)
	}
	return sp, err
}

// newTracer implements NewTracer and allows that function to be mocked out via Disable().
var newTracer = func() opentracing.Tracer {
	opts := basictracer.DefaultOptions()
	opts.TrimUnsampledSpans = true
	opts.Recorder = CallbackRecorder(func(_ basictracer.RawSpan) {})
	opts.NewSpanEventListener = basictracer.NetTraceIntegrator
	opts.DebugAssertUseAfterFinish = true // provoke crash on use-after-Finish
	return basictracer.NewWithOptions(opts)
}

// NewTracer creates a Tracer which records to the net/trace
// endpoint.
func NewTracer() opentracing.Tracer {
	return newTracer()
}

// SpanFromContext returns the Span obtained from the context or, if none is
// found, a new one started through the tracer. Callers should call (or defer)
// the returned cleanup func as well to ensure that the span is Finish()ed, but
// callers should *not* attempt to call Finish directly -- in the case where the
// span was obtained from the context, it is not the caller's to Finish.
func SpanFromContext(opName string, tracer opentracing.Tracer, ctx context.Context) (opentracing.Span, func()) {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		sp = tracer.StartSpan(opName)
		return sp, sp.Finish
	}
	return sp, func() {}
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
