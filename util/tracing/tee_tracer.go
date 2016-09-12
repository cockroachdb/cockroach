// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package tracing

import (
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

// TeeTracer is an opentracing.Tracer that sends events to multiple Tracers.
type TeeTracer struct {
	tracers []opentracing.Tracer
}

var _ opentracing.Tracer = &TeeTracer{}

// NewTeeTracer creates a Tracer that sends events to multiple Tracers.
func NewTeeTracer(tracers ...opentracing.Tracer) opentracing.Tracer {
	return &TeeTracer{tracers: tracers}
}

// StartSpan is part of the opentracing.Traer interface.
func (t *TeeTracer) StartSpan(
	operationName string, opts ...opentracing.StartSpanOption,
) opentracing.Span {
	spans := make([]opentracing.Span, len(t.tracers))
	for i := 0; i < len(t.tracers); i++ {
		spans[i] = t.tracers[i].StartSpan(operationName, opts...)
	}
	return &TeeSpan{tracer: t, spans: spans}
}

// Inject is part of the opentracing.Traer interface.
func (t *TeeTracer) Inject(
	sm opentracing.SpanContext, format interface{}, carrier interface{},
) error {
	// We use the fact that any tracers we use make use of basictracer spans, so
	// the SpanContexts should be the same.
	return t.tracers[0].Inject(sm, format, carrier)
}

// Extract is part of the opentracing.Traer interface.
func (t *TeeTracer) Extract(
	format interface{}, carrier interface{},
) (opentracing.SpanContext, error) {
	return t.tracers[0].Extract(format, carrier)
}

// TeeSpan is the opentracing.Span implementation used by the TeeTracer.
type TeeSpan struct {
	tracer *TeeTracer
	spans  []opentracing.Span
}

var _ opentracing.Span = &TeeSpan{}

// Finish is part of the opentracing.Span interface.
func (ts *TeeSpan) Finish() {
	for _, sp := range ts.spans {
		sp.Finish()
	}
}

// FinishWithOptions is part of the opentracing.Span interface.
func (ts *TeeSpan) FinishWithOptions(opts opentracing.FinishOptions) {
	for _, sp := range ts.spans {
		sp.FinishWithOptions(opts)
	}
}

// Context is part of the opentracing.Span interface.
func (ts *TeeSpan) Context() opentracing.SpanContext {
	// We are using the fact that underlying tracers are using
	// basictracer.SpanContext, which allows us to use one span's
	// context instead of making our own "composite" context.
	// Verify this assumption.
	for _, sp := range ts.spans {
		_ = sp.Context().(basictracer.SpanContext)
	}
	return ts.spans[0].Context()
}

// SetOperationName is part of the opentracing.Span interface.
func (ts *TeeSpan) SetOperationName(operationName string) opentracing.Span {
	for _, sp := range ts.spans {
		sp.SetOperationName(operationName)
	}
	return ts
}

// SetTag is part of the opentracing.Span interface.
func (ts *TeeSpan) SetTag(key string, value interface{}) opentracing.Span {
	for _, sp := range ts.spans {
		sp.SetTag(key, value)
	}
	return ts
}

// LogEvent is part of the opentracing.Span interface.
func (ts *TeeSpan) LogEvent(event string) {
	for _, sp := range ts.spans {
		sp.LogEvent(event)
	}
}

// LogEventWithPayload is part of the opentracing.Span interface.
func (ts *TeeSpan) LogEventWithPayload(event string, payload interface{}) {
	for _, sp := range ts.spans {
		sp.LogEventWithPayload(event, payload)
	}
}

// Log is part of the opentracing.Span interface.
func (ts *TeeSpan) Log(data opentracing.LogData) {
	for _, sp := range ts.spans {
		sp.Log(data)
	}
}

// SetBaggageItem is part of the opentracing.Span interface.
func (ts *TeeSpan) SetBaggageItem(restrictedKey, value string) opentracing.Span {
	for _, sp := range ts.spans {
		sp.SetBaggageItem(restrictedKey, value)
	}
	return ts
}

// BaggageItem is part of the opentracing.Span interface.
func (ts *TeeSpan) BaggageItem(restrictedKey string) string {
	return ts.spans[0].BaggageItem(restrictedKey)
}

// Tracer is part of the opentracing.Span interface.
func (ts *TeeSpan) Tracer() opentracing.Tracer {
	return ts.tracer
}
