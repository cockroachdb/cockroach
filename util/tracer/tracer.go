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

package tracer

import (
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/standardtracer"
	"golang.org/x/net/context"
)

type nilRecorder struct{}

func (*nilRecorder) RecordSpan(_ *standardtracer.RawSpan) {}

var recorder *nilRecorder

// NewTracer creates a new tracer.
func NewTracer() opentracing.Tracer {
	return standardtracer.New(recorder)
}

type nilSpan struct{}

var span *nilSpan

func (*nilSpan) SetOperationName(operationName string) opentracing.Span         { return span }
func (*nilSpan) StartChild(operationName string) opentracing.Span               { return span }
func (*nilSpan) SetTag(key string, value interface{}) opentracing.Span          { return span }
func (*nilSpan) Finish()                                                        {}
func (*nilSpan) LogEvent(event string)                                          {}
func (*nilSpan) LogEventWithPayload(event string, payload interface{})          {}
func (*nilSpan) Log(data opentracing.LogData)                                   {}
func (*nilSpan) SetTraceAttribute(restrictedKey, value string) opentracing.Span { return span }
func (*nilSpan) TraceAttribute(restrictedKey string) string                     { return "" }

// SpanFromContext wraps opentracing.SpanFromContext so that the returned
// trace is never nil (instead of a nil trace, a noop-trace is returned).
func SpanFromContext(ctx context.Context) opentracing.Span {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return span
	}
	return sp
}

// NilTrace returns a Span for which all methods are noops.
func NilTrace() opentracing.Span {
	return SpanFromContext(context.Background())
}
