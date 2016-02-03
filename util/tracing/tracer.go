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
	"fmt"
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/standardtracer"
	"golang.org/x/net/context"
)

const traceTimeFormat = "15:04:05.000000"

type testRecorder struct{}

func (testRecorder) RecordSpan(sp *standardtracer.RawSpan) {
	fmt.Fprintf(os.Stderr, "[Trace %s]\n", sp.Operation)
	for _, log := range sp.Logs {
		fmt.Fprintln(os.Stderr, " * ", log.Timestamp.Format(traceTimeFormat), log.Event)
	}
}

type noopRecorder struct{}

func (noopRecorder) RecordSpan(sp *standardtracer.RawSpan) {}

var recorder standardtracer.SpanRecorder = noopRecorder{}

// SetTestTracing sets up subsequently created tracers returned by
// NewTracer() so that they record their traces to stderr.
// Not to be called concurrently with any tracer operations; the
// right place is init() in main_test.go.
func SetTestTracing() {
	recorder = testRecorder{}
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
	recorder = noopRecorder{}
	return func() {
		recorder = storedRecorder
	}
}

type noopTracer struct {
	// Lazy implementation: we don't use any of the SpanPropagator methods yet,
	// so we just embed an interface which will be nil in practice.
	opentracing.SpanPropagator
}

func (noopTracer) StartTrace(_ string) opentracing.Span {
	return noopSpan{}
}

// NewTracer creates a new tracer.
func NewTracer() opentracing.Tracer {
	if _, tracingDisabled := recorder.(noopRecorder); tracingDisabled {
		return noopTracer{}
	}
	return standardtracer.New(recorder)
}

type noopSpan struct{}

func (noopSpan) SetOperationName(_ string) opentracing.Span {
	return noopSpan{}
}
func (noopSpan) StartChild(_ string) opentracing.Span {
	return noopSpan{}
}
func (noopSpan) SetTag(_ string, _ interface{}) opentracing.Span {
	return noopSpan{}
}
func (noopSpan) Finish()                                     {}
func (noopSpan) LogEvent(_ string)                           {}
func (noopSpan) LogEventWithPayload(_ string, _ interface{}) {}
func (noopSpan) Log(_ opentracing.LogData)                   {}
func (noopSpan) SetTraceAttribute(_, _ string) opentracing.Span {
	return noopSpan{}
}
func (noopSpan) TraceAttribute(_ string) string {
	return ""
}

// SpanFromContext wraps opentracing.SpanFromContext so that the returned
// Span is never nil (instead of a nil Span, a noop Span is returned).
func SpanFromContext(ctx context.Context) opentracing.Span {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return noopSpan{}
	}
	return sp
}

// NilSpan returns a Span for which all methods are noops.
func NilSpan() opentracing.Span {
	return noopSpan{}
}
