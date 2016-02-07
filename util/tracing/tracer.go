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
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/standardtracer"
	"golang.org/x/net/context"
)

const traceTimeFormat = "15:04:05.000000"

// A StderrRecorder prints received trace spans to stderr.
type StderrRecorder struct{}

// RecordSpan implements standardtracer.SpanRecorder.
func (StderrRecorder) RecordSpan(sp *standardtracer.RawSpan) {
	fmt.Fprintf(os.Stderr, "[Trace %s]\n", sp.Operation)
	for _, log := range sp.Logs {
		fmt.Fprintln(os.Stderr, " * ", log.Timestamp.Format(traceTimeFormat), log.Event)
	}
}

// A NoopRecorder silently drops received trace spans.
type NoopRecorder struct{}

// RecordSpan implements standardtracer.SpanRecorder.
func (NoopRecorder) RecordSpan(sp *standardtracer.RawSpan) {}

// A CallbackRecorder immediately invokes itself on received trace spans.
type CallbackRecorder func(sp *standardtracer.RawSpan)

// RecordSpan implements standardtracer.SpanRecorder.
func (cr CallbackRecorder) RecordSpan(sp *standardtracer.RawSpan) {
	cr(sp)
}

var recorder standardtracer.SpanRecorder = NoopRecorder{}

// SetTestTracing sets up subsequently created tracers returned by
// NewTracer() so that they record their traces to stderr.
// Not to be called concurrently with any tracer operations; the
// right place is init() in main_test.go.
func SetTestTracing() {
	recorder = StderrRecorder{}
}

// Disable changes the environment so that all newly created tracers are
// trivial (i.e. noop tracers). This is a crutch to turn off tracing during
// benchmarks until future iterations come with a tunable probabilistic tracing
// setting. Concurrent calls are not safe. Prescribed use is:
//
// func BenchmarkSomething(b *testing.B) {
//   defer tracing.Disable()() // note the double ()()
// }
func Disable() func() {
	storedRecorder := recorder
	recorder = NoopRecorder{}
	return func() {
		recorder = storedRecorder
	}
}

// NewTracer creates a new tracer.
func NewTracer() opentracing.Tracer {
	if _, tracingDisabled := recorder.(NoopRecorder); tracingDisabled {
		return opentracing.NoopTracer{}
	}
	return standardtracer.New(recorder)
}

// SpanFromContext wraps opentracing.SpanFromContext so that the returned
// Span is never nil (instead of a nil Span, a noop Span is returned).
func SpanFromContext(ctx context.Context) opentracing.Span {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return NoopSpan
	}
	return sp
}

// NoopSpan is a singleton span on which all operations are noops.
// Its existence is owed to the (currently) unwieldy API of opentracing-go,
// where serialization logic sits on the tracer and barfs when it gets
// passed a Span which doesn't have the right underlying type.
// Since we use noop-spans for requests we don't want to trace (or
// pay measurable overhead for), comparing with NoopSpan has to guard
// serialization calls.
// TODO(tschottdorf): hopefully remove when upstream discussion through.
var NoopSpan = opentracing.NoopTracer{}.StartTrace("")

// EncodeRawSpan encodes a raw span into bytes, using the given dest slice
// as a buffer.
func EncodeRawSpan(rawSpan *standardtracer.RawSpan, dest []byte) ([]byte, error) {
	// This is not a greatly efficient (but convenient) use of gob.
	buf := bytes.NewBuffer(dest[:0])
	err := gob.NewEncoder(buf).Encode(rawSpan)
	return buf.Bytes(), err
}

// DecodeRawSpan unmarshals into the given RawSpan.
func DecodeRawSpan(enc []byte, dest *standardtracer.RawSpan) error {
	return gob.NewDecoder(bytes.NewBuffer(enc)).Decode(dest)
}

var dummyStdTracer = standardtracer.New(NoopRecorder{})

// PropagateSpanAsBinary exposes standardtracer's binary serialization.
var PropagateSpanAsBinary = dummyStdTracer.PropagateSpanAsBinary
