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
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings"
	lightstep "github.com/lightstep/lightstep-tracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

type shadowTracerManager interface {
	name() string
	flush(tr opentracing.Tracer)
}

type lightStepManager struct{}

func (lightStepManager) name() string {
	return "lightstep"
}

func (lightStepManager) flush(tr opentracing.Tracer) {
	_ = lightstep.FlushLightStepTracer(tr)
}

type shadowTracer struct {
	opentracing.Tracer
	manager shadowTracerManager
}

// Atomic pointer of type *shadowTracer. We don't use sync.Value because we
// can't set it to nil.
var shadowPtr unsafe.Pointer

func setShadowTracer(manager shadowTracerManager, tr opentracing.Tracer) {
	shadow := &shadowTracer{
		Tracer:  tr,
		manager: manager,
	}
	atomic.StorePointer(&shadowPtr, unsafe.Pointer(shadow))
}

func unsetShadowTracer() {
	atomic.StorePointer(&shadowPtr, nil)
}

func getShadowTracer() *shadowTracer {
	if ptr := atomic.LoadPointer(&shadowPtr); ptr != nil {
		return (*shadowTracer)(ptr)
	}
	return nil
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

func createLightStepTracer(token string) {
	lsTr := lightstep.NewTracer(lightstep.Options{
		AccessToken:      token,
		MaxLogsPerSpan:   maxLogsPerSpan,
		MaxBufferedSpans: 10000,
		UseGRPC:          true,
	})
	setShadowTracer(lightStepManager{}, lsTr)
}

// We don't call OnChange inline above because it causes an "initialization
// loop" compile error.
var _ = lightStepToken.OnChange(updateShadowTracer)

func updateShadowTracer() {
	if token := lightStepToken.Get(); token != "" {
		createLightStepTracer(token)
	} else {
		unsetShadowTracer()
	}
}

func init() {
	// If we want to hardcode a lightstep token (for testing), the OnChange
	// callback won't fire.
	updateShadowTracer()
}
