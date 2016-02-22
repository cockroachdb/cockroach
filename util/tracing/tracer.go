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
	"golang.org/x/net/context"

	"github.com/opentracing/opentracing-go"
)

var netTracer = wrapWithNetTrace(opentracing.NoopTracer{})

// NewTracer creates a Tracer which records directly to the net/trace
// endpoint.
func NewTracer() opentracing.Tracer {
	return netTracer
}

// SpanFromContext wraps opentracing.SpanFromContext so that the returned
// Span is never nil.
func SpanFromContext(ctx context.Context) opentracing.Span {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return NoopSpan()
	}
	return sp
}

// NoopSpan returns a Span which discards all operations.
func NoopSpan() opentracing.Span {
	return (opentracing.NoopTracer{}).StartSpan("DefaultSpan")
}

// Disable is for benchmarking use and causes all future tracers to deal in
// no-ops. Calling the returned closure undoes this effect. There is no
// synchronization, so no moving parts are allowed while Disable and the
// closure are called.
func Disable() func() {
	orig := netTracer
	netTracer = opentracing.NoopTracer{}
	return func() {
		netTracer = orig
	}
}
