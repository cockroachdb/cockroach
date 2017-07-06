// Copyright 2017 The Cockroach Authors.
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
//
// A "shadow" tracer can be any opentracing.Tracer implementation that is used
// in addition to the normal functionality of our tracer. It works by attaching
// a shadow span to every span, and attaching a shadow context to every span
// context. When injecting a span context, we encapsulate the shadow context
// inside ours.

package tracing

import (
	"bytes"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/settings"
	lightstep "github.com/lightstep/lightstep-tracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"
)

type shadowTracerManager interface {
	Name() string
	Close(tr opentracing.Tracer)
}

type lightStepManager struct{}

func (lightStepManager) Name() string {
	return "lightstep"
}

func (lightStepManager) Close(tr opentracing.Tracer) {
	// TODO(radu): these calls are not reliable. FlushLightstepTracer exits
	// immediately if a flush is in progress (see
	// github.com/lightstep/lightstep-tracer-go/issues/89), and CloseTracer always
	// exits immediately (see
	// https://github.com/lightstep/lightstep-tracer-go/pull/85#discussion_r123800322).
	_ = lightstep.FlushLightStepTracer(tr)
	_ = lightstep.CloseTracer(tr)
}

type zipkinManager struct {
	collector zipkin.Collector
}

func (zipkinManager) Name() string {
	return "zipkin"
}

func (m zipkinManager) Close(tr opentracing.Tracer) {
	_ = m.collector.Close()
}

type shadowTracer struct {
	opentracing.Tracer
	manager shadowTracerManager
}

func (st *shadowTracer) Typ() string {
	return st.manager.Name()
}

func (st *shadowTracer) Close() {
	st.manager.Close(st)
}

// linkShadowSpan creates and links a Shadow span to the passed-in span (i.e.
// fills in s.shadowTr and s.shadowSpan). This should only be called when
// shadow tracing is enabled.
//
// The Shadow span will have a parent if parentShadowCtx is not nil.
// parentType is ignored if parentShadowCtx is nil.
//
// The tags from s are copied to the Shadow span.
func linkShadowSpan(
	s *span,
	shadowTr *shadowTracer,
	parentShadowCtx opentracing.SpanContext,
	parentType opentracing.SpanReferenceType,
) {
	// Create the shadow lightstep span.
	var opts []opentracing.StartSpanOption
	// Replicate the options, using the lightstep context in the reference.
	opts = append(opts, opentracing.StartTime(s.startTime))
	if s.mu.tags != nil {
		opts = append(opts, s.mu.tags)
	}
	if parentShadowCtx != nil {
		opts = append(opts, opentracing.SpanReference{
			Type:              parentType,
			ReferencedContext: parentShadowCtx,
		})
	}
	s.shadowTr = shadowTr
	s.shadowSpan = shadowTr.StartSpan(s.operation, opts...)
}

var lightStepToken = settings.RegisterStringSetting(
	"trace.lightstep.token",
	"if set, traces go to Lightstep using this token",
	"",
)

func createLightStepTracer(token string) (shadowTracerManager, opentracing.Tracer) {
	return lightStepManager{}, lightstep.NewTracer(lightstep.Options{
		AccessToken:      token,
		MaxLogsPerSpan:   maxLogsPerSpan,
		MaxBufferedSpans: 10000,
		UseGRPC:          true,
	})
}

var zipkinCollector = settings.RegisterStringSetting(
	"trace.zipkin.collector",
	"if set, traces go to the given Zipkin instance (example: '127.0.0.1:9411')",
	"",
)

func createZipkinTracer(collectorAddr string) (shadowTracerManager, opentracing.Tracer) {
	// Create our HTTP collector.
	collector, err := zipkin.NewHTTPCollector(fmt.Sprintf("http://%s/api/v1/spans", collectorAddr))
	if err != nil {
		panic(err)
	}

	// Create our recorder.
	recorder := zipkin.NewRecorder(collector, false /* debug */, "0.0.0.0:0", "cockroach")

	// Create our tracer.
	zipkinTr, err := zipkin.NewTracer(
		recorder,
		zipkin.WithLogger(zipkin.LoggerFunc(func(keyvals ...interface{}) error {
			var buf bytes.Buffer
			for i := 0; i < len(keyvals); i += 2 {
				if i > 0 {
					buf.WriteByte(' ')
				}
				if i == len(keyvals)-1 {
					fmt.Fprintf(&buf, "%s=(MISSING)", keyvals[i])
				} else {
					fmt.Fprintf(&buf, "%s=%q", keyvals[i], keyvals[i+1])
				}
			}
			buf.WriteString("\n")
			_, _ = buf.WriteTo(os.Stderr)
			return nil
		})),
	)
	if err != nil {
		panic(err)
	}
	return zipkinManager{collector: collector}, zipkinTr
}

// We don't call OnChange inline above because it causes an "initialization
// loop" compile error.
var _ = lightStepToken.OnChange(updateShadowTracers)
var _ = zipkinCollector.OnChange(updateShadowTracers)

func updateShadowTracer(t *Tracer) {
	if lsToken := lightStepToken.Get(); lsToken != "" {
		t.setShadowTracer(createLightStepTracer(lsToken))
	} else if zipkinAddr := zipkinCollector.Get(); zipkinAddr != "" {
		t.setShadowTracer(createZipkinTracer(zipkinAddr))
	} else {
		t.setShadowTracer(nil, nil)
	}
}

func updateShadowTracers() {
	tracerRegistry.ForEach(updateShadowTracer)
}
