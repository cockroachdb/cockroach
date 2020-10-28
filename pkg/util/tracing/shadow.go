// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// A "shadow" tracer can be any opentracing.Tracer implementation that is used
// in addition to the normal functionality of our tracer. It works by attaching
// a shadow Span to every Span, and attaching a shadow context to every Span
// context. When injecting a Span context, we encapsulate the shadow context
// inside ours.

package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
	lightstep "github.com/lightstep/lightstep-tracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin-contrib/zipkin-go-opentracing"
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

// otLogTagsOption is an opentracing.StartSpanOption that inserts the log
// tags into newly created spans.
type otLogTagsOption logtags.Buffer

func (o *otLogTagsOption) Apply(opts *opentracing.StartSpanOptions) {
	tags := (*logtags.Buffer)(o).Get()
	if len(tags) == 0 {
		return
	}
	if opts.Tags == nil {
		opts.Tags = map[string]interface{}{}
	}
	for _, tag := range tags {
		opts.Tags[tagName(tag.Key())] = tag.Value()
	}
}

// linkShadowSpan creates and links a Shadow Span to the passed-in Span (i.e.
// fills in s.shadowTr and s.shadowSpan). This should only be called when
// shadow tracing is enabled.
//
// The Shadow Span will have a parent if parentShadowCtx is not nil.
// parentType is ignored if parentShadowCtx is nil.
//
// The tags (including logTags) from s are copied to the Shadow Span.
func linkShadowSpan(
	s *Span,
	shadowTr *shadowTracer,
	parentShadowCtx opentracing.SpanContext,
	parentType opentracing.SpanReferenceType,
) {
	// Create the shadow lightstep Span.
	var opts []opentracing.StartSpanOption
	// Replicate the options, using the lightstep context in the reference.
	opts = append(opts, opentracing.StartTime(s.crdb.startTime))
	if s.crdb.logTags != nil {
		opts = append(opts, (*otLogTagsOption)(s.crdb.logTags))
	}
	if s.crdb.mu.tags != nil {
		opts = append(opts, s.crdb.mu.tags)
	}
	if parentShadowCtx != nil {
		opts = append(opts, opentracing.SpanReference{
			Type:              parentType,
			ReferencedContext: parentShadowCtx,
		})
	}
	s.ot.shadowTr = shadowTr
	s.ot.shadowSpan = shadowTr.StartSpan(s.crdb.operation, opts...)
}

func createLightStepTracer(token string) (shadowTracerManager, opentracing.Tracer) {
	return lightStepManager{}, lightstep.NewTracer(lightstep.Options{
		AccessToken:      token,
		MaxLogsPerSpan:   maxLogsPerSpan,
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
