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
	"fmt"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
)

// TeeTracer is an opentracing.Tracer that sends events to multiple Tracers.
type TeeTracer struct {
	tracers []opentracing.Tracer
}

var _ opentracing.Tracer = &TeeTracer{}

// NewTeeTracer creates a Tracer that sends events to multiple Tracers.
//
// Note that only the span from the first tracer is used for serialization
// purposes (Inject/Extract).
func NewTeeTracer(tracers ...opentracing.Tracer) *TeeTracer {
	return &TeeTracer{tracers: tracers}
}

// option is a wrapper for opentracing.StartSpanOptions that
// implements opentracing.StartSpanOption. This is the only way to pass options
// to StartSpan.
type option opentracing.StartSpanOptions

var _ opentracing.StartSpanOption = &option{}

// Apply is part of the StartSpanOption interface.
func (o *option) Apply(sso *opentracing.StartSpanOptions) {
	*sso = opentracing.StartSpanOptions(*o)
}

// StartSpan is part of the opentracing.Tracer interface.
func (t *TeeTracer) StartSpan(
	operationName string, opts ...opentracing.StartSpanOption,
) opentracing.Span {
	// Form the StartSpanOptions from the opts.
	sso := opentracing.StartSpanOptions{}
	for _, o := range opts {
		o.Apply(&sso)
	}

	spans := make([]spanWithOpt, len(t.tracers))
	for i := 0; i < len(t.tracers); i++ {
		o := option(sso)
		// Replace any references to the TeeSpanContext with the corresponding
		// SpanContext for tracer i.
		o.References = make([]opentracing.SpanReference, len(sso.References))
		for j := range sso.References {
			tsc := sso.References[j].ReferencedContext.(TeeSpanContext)
			o.References[j].ReferencedContext = tsc.contexts[i]
		}
		spans[i] = spanWithOpt{
			Span:  t.tracers[i].StartSpan(operationName, &o),
			owned: true,
		}
	}
	return &TeeSpan{tracer: t, spans: spans}
}

type spanWithOpt struct {
	opentracing.Span
	// If owned is set, the span needs to be Finish()ed by the TeeSpan that
	// contains it.
	owned bool
}

// CreateSpanFrom creates a TeeSpan that will contains the passed-in spans.
// Depending on span.owned, the TeeSpan takes ownership of some of the child
// spans.
func (t *TeeTracer) CreateSpanFrom(spans ...spanWithOpt) *TeeSpan {
	if len(spans) != len(t.tracers) {
		panic(fmt.Sprintf("non-matching spans for TeeTracer. spans: %d, tracers: %d",
			len(spans), len(t.tracers)))
	}
	for i, tracer := range t.tracers {
		if tracer != spans[i].Tracer() {
			panic(fmt.Sprintf("non-matching span at position: %d", i))
		}
	}
	return &TeeSpan{tracer: t, spans: spans}
}

// Inject is part of the opentracing.Tracer interface.
func (t *TeeTracer) Inject(
	sc opentracing.SpanContext, format interface{}, carrier interface{},
) error {
	tsc, ok := sc.(TeeSpanContext)
	if !ok {
		return errors.Errorf("SpanContext type %T incompatible with TeeTracer", sc)
	}
	// TODO(radu): we only serialize the span for the first tracer. Ideally we
	// would produce our own format that includes serializations for all the
	// underlying spans.
	return t.tracers[0].Inject(tsc.contexts[0], format, carrier)
}

// Extract is part of the opentracing.Tracer interface.
func (t *TeeTracer) Extract(
	format interface{}, carrier interface{},
) (opentracing.SpanContext, error) {
	decoded, err := t.tracers[0].Extract(format, carrier)
	if err != nil {
		return nil, err
	}
	contexts := make([]opentracing.SpanContext, len(t.tracers))
	contexts[0] = decoded
	// We use the fact that we only use tracers based on basictracer spans and
	// duplicate the same span.
	ctxBasic := decoded.(basictracer.SpanContext)
	for i := 1; i < len(contexts); i++ {
		ctxCopy := ctxBasic
		ctxCopy.Baggage = make(map[string]string)
		for k, v := range ctxBasic.Baggage {
			ctxCopy.Baggage[k] = v
		}
		contexts[i] = ctxCopy
	}
	return TeeSpanContext{contexts: contexts}, nil
}

// TeeSpanContext is an opentracing.SpanContext that keeps track of SpanContexts
// from multiple tracers.
type TeeSpanContext struct {
	contexts []opentracing.SpanContext
}

var _ opentracing.SpanContext = TeeSpanContext{}

// ForeachBaggageItem is part of the opentracing.SpanContext interface.
func (tsc TeeSpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	// All underlying SpanContexts have the same baggage.
	tsc.contexts[0].ForeachBaggageItem(handler)
}

// TeeSpan is the opentracing.Span implementation used by the TeeTracer.
type TeeSpan struct {
	tracer *TeeTracer
	spans  []spanWithOpt
}

var _ opentracing.Span = &TeeSpan{}

// Finish is part of the opentracing.Span interface.
func (ts *TeeSpan) Finish() {
	for _, sp := range ts.spans {
		if sp.owned {
			sp.Finish()
		}
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
	// Build a composite context.
	ctx := TeeSpanContext{}
	ctx.contexts = make([]opentracing.SpanContext, len(ts.spans))
	for i := range ts.spans {
		ctx.contexts[i] = ts.spans[i].Context()
	}
	return ctx
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

// LogFields is part of the opentracing.Span interface.
func (ts *TeeSpan) LogFields(fields ...otlog.Field) {
	for _, sp := range ts.spans {
		sp.LogFields(fields...)
	}
}

// LogKV is part of the opentracing.Span interface.
func (ts *TeeSpan) LogKV(alternatingKeyValues ...interface{}) {
	for _, sp := range ts.spans {
		sp.LogKV(alternatingKeyValues...)
	}
}

// LogEvent is part of the opentracing.Span interface.
//
// Deprecated: use LogKV/LogFields.
func (ts *TeeSpan) LogEvent(event string) {
	panic("deprecated")
}

// LogEventWithPayload is part of the opentracing.Span interface.
//
// Deprecated: use LogKV/LogFields.
func (ts *TeeSpan) LogEventWithPayload(event string, payload interface{}) {
	panic("deprecated")
}

// Log is part of the opentracing.Span interface.
//
// Deprecated: use LogKV/LogFields.
func (ts *TeeSpan) Log(data opentracing.LogData) {
	panic("deprecated")
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
