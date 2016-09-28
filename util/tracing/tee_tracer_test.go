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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

func TestTeeTracer(t *testing.T) {
	r1 := basictracer.NewInMemoryRecorder()
	t1 := basictracer.NewWithOptions(basictracer.Options{
		Recorder:     r1,
		ShouldSample: func(traceID uint64) bool { return true }, // always sample
	})
	r2 := basictracer.NewInMemoryRecorder()
	t2 := basictracer.NewWithOptions(basictracer.Options{
		Recorder:     r2,
		ShouldSample: func(traceID uint64) bool { return true }, // always sample
	})
	tr := NewTeeTracer(t1, t2)

	span := tr.StartSpan("x")
	span.LogKV("k1", "v1", "k2", "v2")
	span.SetTag("tag", "value")
	span.SetBaggageItem("baggage", "baggage-value")
	assert.Equal(t, "baggage-value", span.BaggageItem("baggage"))

	spanCtx := span.Context()
	var ctxBuffer bytes.Buffer
	if err := tr.Inject(spanCtx, opentracing.Binary, &ctxBuffer); err != nil {
		t.Fatal(err)
	}

	decodedCtx, err := tr.Extract(opentracing.Binary, &ctxBuffer)
	if err != nil {
		t.Fatal(err)
	}
	span2 := tr.StartSpan("y", opentracing.FollowsFrom(decodedCtx))
	span2.LogEvent("event2")
	assert.Equal(t, "baggage-value", span2.BaggageItem("baggage"))
	span.Finish()
	span2.Finish()

	for _, spans := range [][]basictracer.RawSpan{r1.GetSpans(), r2.GetSpans()} {
		require.Equal(t, 2, len(spans))

		assert.Equal(t, "x", spans[0].Operation)
		assert.Equal(t, opentracing.Tags{"tag": "value"}, spans[0].Tags)
		assert.Equal(t, "k1:v1", spans[0].Logs[0].Fields[0].String())
		assert.Equal(t, "k2:v2", spans[0].Logs[0].Fields[1].String())
		assert.Equal(t, 1, len(spans[0].Context.Baggage))

		assert.Equal(t, "y", spans[1].Operation)
		assert.Equal(t, opentracing.Tags(nil), spans[1].Tags)
		assert.Equal(t, "event:event2", spans[1].Logs[0].Fields[0].String())
		assert.Equal(t, 1, len(spans[1].Context.Baggage))
	}
}

// TestTeeTracerSpanRefs verifies that ChildOf/FollowsFrom relations are
// reflected correctly in the underlying spans.
func TestTeeTracerSpanRefs(t *testing.T) {
	r1 := basictracer.NewInMemoryRecorder()
	t1 := basictracer.NewWithOptions(basictracer.Options{
		Recorder:     r1,
		ShouldSample: func(traceID uint64) bool { return true }, // always sample
	})
	r2 := basictracer.NewInMemoryRecorder()
	t2 := basictracer.NewWithOptions(basictracer.Options{
		Recorder:     r2,
		ShouldSample: func(traceID uint64) bool { return true }, // always sample
	})
	tr := NewTeeTracer(t1, t2)

	root := tr.StartSpan("x")
	child := tr.StartSpan("x", opentracing.ChildOf(root.Context()))
	child.Finish()
	root.Finish()

	for _, spans := range [][]basictracer.RawSpan{r1.GetSpans(), r2.GetSpans()} {
		require.Equal(t, 2, len(spans))

		assert.Equal(t, spans[0].Context.TraceID, spans[1].Context.TraceID)
		assert.Equal(t, spans[1].Context.SpanID, spans[0].ParentSpanID)
	}
}
