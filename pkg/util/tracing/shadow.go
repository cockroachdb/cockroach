// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	lightstep "github.com/lightstep/lightstep-tracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
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
	lightstep.Close(context.TODO(), tr)
}

type zipkinManager struct {
	collector zipkin.Collector
}

func (*zipkinManager) Name() string {
	return "zipkin"
}

func (m *zipkinManager) Close(tr opentracing.Tracer) {
	_ = m.collector.Close()
}

type dataDogManager struct{}

func (dataDogManager) Name() string {
	return "DataDog"
}

func (dataDogManager) Close(tr opentracing.Tracer) {
	// TODO(andrei): Figure out what to do here. The docs suggest that
	// ddtrace.tracer.Stop() flushes, but the problem is that it operates on
	// global state, and when shadow tracers are changed, we call this *after*
	// starting the new one.
}

// A shadowTracer can be any opentracing.Tracer implementation that is used in
// addition to the normal functionality of our tracer. It works by attaching a
// shadow Span to every Span, and attaching a shadow context to every Span
// context. When injecting a Span context, we encapsulate the shadow context
// inside ours.
type shadowTracer struct {
	opentracing.Tracer
	manager shadowTracerManager
}

// Type returns the underlying type of the shadow tracer manager.
// It is valid to call this on a nil shadowTracer; it will
// return zero values in this case.
func (st *shadowTracer) Type() (string, bool) {
	if st == nil || st.manager == nil {
		return "", false
	}
	return st.manager.Name(), true
}

func (st *shadowTracer) Close() {
	st.manager.Close(st)
}

// makeShadowSpan creates an otSpan for construction of a Span.
// This must be called with a non-nil shadowTr, which must have
// been confirmed to be compatible with parentShadowCtx.
//
// The span contained in `otSpan` will have a parent if parentShadowCtx
// is not nil. parentType is ignored if parentShadowCtx is nil.
func makeShadowSpan(
	shadowTr *shadowTracer,
	parentShadowCtx opentracing.SpanContext,
	parentType opentracing.SpanReferenceType,
	opName string,
	startTime time.Time,
) otSpan {
	// Create the shadow lightstep Span.
	opts := make([]opentracing.StartSpanOption, 0, 2)
	// Replicate the options, using the lightstep context in the reference.
	opts = append(opts, opentracing.StartTime(startTime))
	if parentShadowCtx != nil {
		opts = append(opts, opentracing.SpanReference{
			Type:              parentType,
			ReferencedContext: parentShadowCtx,
		})
	}
	return otSpan{
		shadowTr:   shadowTr,
		shadowSpan: shadowTr.StartSpan(opName, opts...),
	}
}

func createLightStepTracer(token string) (shadowTracerManager, opentracing.Tracer) {
	return lightStepManager{}, lightstep.NewTracer(lightstep.Options{
		AccessToken:      token,
		MaxLogsPerSpan:   maxLogsPerSpanExternal,
		MaxBufferedSpans: 10000,
		UseGRPC:          true,
	})
}

var zipkinLogEveryN = util.Every(5 * time.Second)

func createZipkinTracer(collectorAddr string) (shadowTracerManager, opentracing.Tracer) {
	// Create our HTTP collector.
	collector, err := zipkin.NewHTTPCollector(
		fmt.Sprintf("http://%s/api/v1/spans", collectorAddr),
		zipkin.HTTPLogger(zipkin.LoggerFunc(func(keyvals ...interface{}) error {
			if zipkinLogEveryN.ShouldProcess(timeutil.Now()) {
				// These logs are from the collector (e.g. errors sending data, dropped
				// traces). We can't use `log` from this package so print them to stderr.
				toPrint := append([]interface{}{"Zipkin collector"}, keyvals...)
				fmt.Fprintln(os.Stderr, toPrint)
			}
			return nil
		})),
	)
	if err != nil {
		panic(err)
	}

	// Create our recorder.
	recorder := zipkin.NewRecorder(collector, false /* debug */, "0.0.0.0:0", "cockroach")

	// Create our tracer.
	zipkinTr, err := zipkin.NewTracer(recorder)
	if err != nil {
		panic(err)
	}
	return &zipkinManager{collector: collector}, zipkinTr
}

func createDataDogTracer(agentAddr, project string) (shadowTracerManager, opentracing.Tracer) {
	opts := []tracer.StartOption{tracer.WithService(project)}
	if agentAddr != "" && agentAddr != "default" {
		opts = append(opts, tracer.WithAgentAddr(agentAddr))
	}
	return dataDogManager{}, opentracer.New(opts...)
}
