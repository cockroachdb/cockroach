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

func (*testRecorder) RecordSpan(sp *standardtracer.RawSpan) {
	fmt.Fprintf(os.Stderr, "[Trace %s]\n", sp.Operation)
	for _, log := range sp.Logs {
		fmt.Fprintln(os.Stderr, " * ", log.Timestamp.Format(traceTimeFormat), log.Event)
	}
}

type noopRecorder struct{}

func (*noopRecorder) RecordSpan(sp *standardtracer.RawSpan) {}

var recorder standardtracer.SpanRecorder = (*noopRecorder)(nil)

// SetTestTracing sets up subsequently created tracers returned by
// NewTracer() so that they record their traces to stderr.
// Not to be called concurrently with any tracer operations; the
// right place is init() in main_test.go.
func SetTestTracing() {
	recorder = (*testRecorder)(nil)
}

// NewTracer creates a new tracer.
func NewTracer() opentracing.Tracer {
	return standardtracer.New(recorder)
}

type noopSpan struct{}

var dummySpan *noopSpan

func (*noopSpan) SetOperationName(operationName string) opentracing.Span         { return dummySpan }
func (*noopSpan) StartChild(operationName string) opentracing.Span               { return dummySpan }
func (*noopSpan) SetTag(key string, value interface{}) opentracing.Span          { return dummySpan }
func (*noopSpan) Finish()                                                        {}
func (*noopSpan) LogEvent(event string)                                          {}
func (*noopSpan) LogEventWithPayload(event string, payload interface{})          {}
func (*noopSpan) Log(data opentracing.LogData)                                   {}
func (*noopSpan) SetTraceAttribute(restrictedKey, value string) opentracing.Span { return dummySpan }
func (*noopSpan) TraceAttribute(restrictedKey string) string                     { return "" }

// SpanFromContext wraps opentracing.SpanFromContext so that the returned
// Span is never nil (instead of a nil Span, a noop Span is returned).
func SpanFromContext(ctx context.Context) opentracing.Span {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return dummySpan
	}
	return sp
}

// NilTrace returns a Span for which all methods are noops.
func NilTrace() opentracing.Span {
	return SpanFromContext(context.Background())
}
