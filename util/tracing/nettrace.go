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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package tracing

import (
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/trace"
)

const family = "tracing"

// netTraceWrapTracer is a tracer which hooks up operations on the underlying
// tracer with the `net/trace` endpoint transparently.
type netTraceWrapTracer struct {
	wrap opentracing.Tracer
}

var _ opentracing.Tracer = &netTraceWrapTracer{}

func wrapWithNetTrace(tr opentracing.Tracer) opentracing.Tracer {
	return &netTraceWrapTracer{tr}
}

type netTraceWrapSpan struct {
	opentracing.Span
	tr trace.Trace
}

func (ntws *netTraceWrapSpan) LogEvent(event string) {
	ntws.tr.LazyPrintf(event)
	ntws.Span.LogEvent(event)
}

func (ntws *netTraceWrapSpan) LogEventWithPayload(event string, payload interface{}) {
	ntws.tr.LazyPrintf(event)
	ntws.Span.LogEventWithPayload(event, payload)
}

func (ntws *netTraceWrapSpan) Log(data opentracing.LogData) {
	ntws.tr.LazyPrintf(data.Event)
	ntws.Span.Log(data)
}

func (ntws *netTraceWrapSpan) Finish() {
	ntws.tr.Finish()
	ntws.Span.Finish()
}

func (nt *netTraceWrapTracer) StartSpan(opName string) opentracing.Span {
	return &netTraceWrapSpan{
		Span: nt.wrap.StartSpan(opName),
		tr:   trace.New(family, opName),
	}
}

func (nt *netTraceWrapTracer) StartSpanWithOptions(opts opentracing.StartSpanOptions) opentracing.Span {
	return &netTraceWrapSpan{Span: nt.wrap.StartSpanWithOptions(opts), tr: trace.New(family, opts.OperationName)}
}

func (nt *netTraceWrapTracer) Inject(span opentracing.Span, format, carrier interface{}) error {
	realSpan, ok := span.(*netTraceWrapSpan)
	if !ok {
		// If the incoming span wasn't wrapped, hope for the best and let the
		// injector do its thing.
		return nt.wrap.Inject(span, format, carrier)
	}
	return nt.wrap.Inject(realSpan.Span, format, carrier)
}

func (nt *netTraceWrapTracer) Join(opName string, format, carrier interface{}) (opentracing.Span, error) {
	sp, err := nt.wrap.Join(opName, format, carrier)
	if err != nil {
		return nil, err
	}
	return &netTraceWrapSpan{Span: sp, tr: trace.New(family, opName)}, nil
}
