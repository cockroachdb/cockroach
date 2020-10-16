// Copyright 2016 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/logtags"
	lightstep "github.com/lightstep/lightstep-tracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

func TestTracerRecording(t *testing.T) {
	tr := NewTracer()

	noop1 := tr.StartSpan("noop")
	if !noop1.isNoop() {
		t.Error("expected noop Span")
	}
	noop1.LogKV("hello", "void")

	noop2 := tr.StartSpan("noop2", opentracing.ChildOf(noop1.Context()))
	if !noop2.isNoop() {
		t.Error("expected noop child Span")
	}
	noop2.Finish()
	noop1.Finish()

	s1 := tr.StartSpan("a", Recordable)
	if s1.isNoop() {
		t.Error("Recordable (but not recording) Span should not be noop")
	}
	if !IsBlackHoleSpan(s1) {
		t.Error("Recordable Span should be black hole")
	}

	// Unless recording is actually started, child spans are still noop.
	noop3 := tr.StartSpan("noop3", opentracing.ChildOf(s1.Context()))
	if !noop3.isNoop() {
		t.Error("expected noop child Span")
	}
	noop3.Finish()

	s1.LogKV("x", 1)
	StartRecording(s1, SingleNodeRecording)
	s1.LogKV("x", 2)
	s2 := tr.StartSpan("b", opentracing.ChildOf(s1.Context()))
	if IsBlackHoleSpan(s2) {
		t.Error("recording Span should not be black hole")
	}
	s2.LogKV("x", 3)

	if err := TestingCheckRecordedSpans(GetRecording(s1), `
		Span a:
			tags: unfinished=
			x: 2
		Span b:
			tags: unfinished=
			x: 3
	`); err != nil {
		t.Fatal(err)
	}

	if err := TestingCheckRecordedSpans(GetRecording(s2), `
		Span b:
			tags: unfinished=
			x: 3
	`); err != nil {
		t.Fatal(err)
	}

	s3 := tr.StartSpan("c", opentracing.FollowsFrom(s2.Context()))
	s3.LogKV("x", 4)
	s3.SetTag("tag", "val")

	s2.Finish()

	if err := TestingCheckRecordedSpans(GetRecording(s1), `
		Span a:
			tags: unfinished=
			x: 2
		Span b:
			x: 3
		Span c:
			tags: tag=val unfinished=
			x: 4
	`); err != nil {
		t.Fatal(err)
	}
	s3.Finish()
	if err := TestingCheckRecordedSpans(GetRecording(s1), `
		Span a:
      tags: unfinished=
			x: 2
		Span b:
			x: 3
		Span c:
			tags: tag=val
			x: 4
	`); err != nil {
		t.Fatal(err)
	}
	StopRecording(s1)
	s1.LogKV("x", 100)
	if err := TestingCheckRecordedSpans(GetRecording(s1), ``); err != nil {
		t.Fatal(err)
	}

	// The child Span is still recording.
	s3.LogKV("x", 5)
	if err := TestingCheckRecordedSpans(GetRecording(s3), `
		Span c:
			tags: tag=val
			x: 4
			x: 5
	`); err != nil {
		t.Fatal(err)
	}
	s1.Finish()
}

func TestStartChildSpan(t *testing.T) {
	tr := NewTracer()
	sp1 := tr.StartSpan("parent", Recordable)
	StartRecording(sp1, SingleNodeRecording)
	sp2 := tr.StartChildSpan("child", sp1.SpanContext(), nil /* logTags */, false /* recordable */, false /*separateRecording*/)
	sp2.Finish()
	sp1.Finish()
	if err := TestingCheckRecordedSpans(GetRecording(sp1), `
		Span parent:
			Span child:
	`); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", Recordable)
	StartRecording(sp1, SingleNodeRecording)
	sp2 = tr.StartChildSpan("child", sp1.SpanContext(), nil /* logTags */, false /* recordable */, true /*separateRecording*/)
	sp2.Finish()
	sp1.Finish()
	if err := TestingCheckRecordedSpans(GetRecording(sp1), `
		Span parent:
	`); err != nil {
		t.Fatal(err)
	}
	if err := TestingCheckRecordedSpans(GetRecording(sp2), `
		Span child:
	`); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", Recordable)
	StartRecording(sp1, SingleNodeRecording)
	sp2 = tr.StartChildSpan(
		"child", sp1.SpanContext(), logtags.SingleTagBuffer("key", "val"), false /* recordable */, false, /*separateRecording*/
	)
	sp2.Finish()
	sp1.Finish()
	if err := TestingCheckRecordedSpans(GetRecording(sp1), `
		Span parent:
			Span child:
				tags: key=val
	`); err != nil {
		t.Fatal(err)
	}
}

func TestTracerInjectExtract(t *testing.T) {
	tr := NewTracer()
	tr2 := NewTracer()

	// Verify that noop spans become noop spans on the remote side.

	noop1 := tr.StartSpan("noop")
	if !noop1.isNoop() {
		t.Fatalf("expected noop Span: %+v", noop1)
	}
	carrier := make(opentracing.HTTPHeadersCarrier)
	if err := tr.Inject(noop1.Context(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}
	if len(carrier) != 0 {
		t.Errorf("noop Span has carrier: %+v", carrier)
	}

	wireContext, err := tr2.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}
	if !wireContext.isNoop() {
		t.Errorf("expected noop context: %v", wireContext)
	}
	noop2 := tr2.StartSpan("remote op", opentracing.FollowsFrom(wireContext))
	if !noop2.isNoop() {
		t.Fatalf("expected noop Span: %+v", noop2)
	}
	noop1.Finish()
	noop2.Finish()

	// Verify that snowball tracing is propagated and triggers recording on the
	// remote side.

	s1 := tr.StartSpan("a", Recordable)
	StartRecording(s1, SnowballRecording)

	carrier = make(opentracing.HTTPHeadersCarrier)
	if err := tr.Inject(s1.Context(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}

	wireContext, err = tr2.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}
	s2 := tr2.StartSpan("remote op", opentracing.FollowsFrom(wireContext))

	// Compare TraceIDs
	trace1 := s1.Context().TraceID
	trace2 := s2.Context().TraceID
	if trace1 != trace2 {
		t.Errorf("TraceID doesn't match: parent %d child %d", trace1, trace2)
	}
	s2.LogKV("x", 1)
	s2.Finish()

	// Verify that recording was started automatically.
	rec := GetRecording(s2)
	if err := TestingCheckRecordedSpans(rec, `
		Span remote op:
			tags: sb=1
			x: 1
	`); err != nil {
		t.Fatal(err)
	}

	if err := TestingCheckRecordedSpans(GetRecording(s1), `
		Span a:
			tags: sb=1 unfinished=
	`); err != nil {
		t.Fatal(err)
	}

	if err := ImportRemoteSpans(s1, rec); err != nil {
		t.Fatal(err)
	}
	s1.Finish()

	if err := TestingCheckRecordedSpans(GetRecording(s1), `
		Span a:
			tags: sb=1
		Span remote op:
			tags: sb=1
			x: 1
	`); err != nil {
		t.Fatal(err)
	}
}

func TestLightstepContext(t *testing.T) {
	tr := NewTracer()
	lsTr := lightstep.NewTracer(lightstep.Options{
		AccessToken: "invalid",
		Collector: lightstep.Endpoint{
			Host:      "127.0.0.1",
			Port:      65535,
			Plaintext: true,
		},
		MaxLogsPerSpan: maxLogsPerSpan,
		UseGRPC:        true,
	})
	tr.setShadowTracer(lightStepManager{}, lsTr)
	s := tr.StartSpan("test")

	const testBaggageKey = "test-baggage"
	const testBaggageVal = "test-val"
	s.SetBaggageItem(testBaggageKey, testBaggageVal)

	carrier := make(opentracing.HTTPHeadersCarrier)
	if err := tr.Inject(s.Context(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}

	// Extract also extracts the embedded lightstep context.
	wireContext, err := tr.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}

	s2 := tr.StartSpan("child", opentracing.FollowsFrom(wireContext))
	s2Ctx := s2.ot.shadowSpan.Context()

	// Verify that the baggage is correct in both the tracer context and in the
	// lightstep context.
	for i, spanCtx := range []interface {
		ForeachBaggageItem(func(string, string) bool)
	}{s2.Context(), s2Ctx} {
		baggage := make(map[string]string)
		spanCtx.ForeachBaggageItem(func(k, v string) bool {
			baggage[k] = v
			return true
		})
		if len(baggage) != 1 || baggage[testBaggageKey] != testBaggageVal {
			t.Errorf("%d: expected baggage %s=%s, got %v", i, testBaggageKey, testBaggageVal, baggage)
		}
	}
}
