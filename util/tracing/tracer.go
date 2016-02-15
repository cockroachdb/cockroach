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
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/log"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/standardtracer"
)

const traceTimeFormat = "15:04:05.000000"

type testRecorder struct{}

func (testRecorder) RecordSpan(sp standardtracer.RawSpan) {
	if log.V(2) {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "[%s]", sp.Operation)
		for _, log := range sp.Logs {
			fmt.Fprint(&buf, "\n * ", log.Timestamp.Format(traceTimeFormat), " ", log.Event)
		}
		log.Info(buf.String())
	}
}

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

// Disable can be called by tests. All tracers created before the
// returned callback fires will deal exclusively in no-ops.
func Disable() func() {
	orig := netTracer
	netTracer = opentracing.NoopTracer{}
	return func() {
		netTracer = orig
	}
}
