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
	"github.com/stretchr/testify/require"
)

func TestStartSpanAlwaysTrace(t *testing.T) {
	// Regression test: if tracing is on, don't erroneously return a noopSpan
	// due to optimizations in StartSpan.
	tr := NewTracer()
	tr._useNetTrace = 1
	require.True(t, tr.AlwaysTrace())
	nilMeta := tr.noopSpan.Meta()
	require.Nil(t, nilMeta)
	sp := tr.StartSpan("foo", WithParentAndManualCollection(nilMeta))
	require.False(t, sp.IsBlackHole())
	require.False(t, sp.isNoop())
	sp = tr.StartSpan("foo", WithParentAndAutoCollection(tr.noopSpan))
	require.False(t, sp.IsBlackHole())
	require.False(t, sp.isNoop())
}

func TestTracerRecording(t *testing.T) {
	tr := NewTracer()

	noop1 := tr.StartSpan("noop")
	if !noop1.isNoop() {
		t.Error("expected noop Span")
	}
	noop1.LogKV("hello", "void")

	noop2 := tr.StartSpan("noop2", WithParentAndManualCollection(noop1.Meta()))
	if !noop2.isNoop() {
		t.Error("expected noop child Span")
	}
	noop2.Finish()
	noop1.Finish()

	s1 := tr.StartSpan("a", WithForceRealSpan())
	if s1.isNoop() {
		t.Error("WithForceRealSpan (but not recording) Span should not be noop")
	}
	if !s1.IsBlackHole() {
		t.Error("WithForceRealSpan Span should be black hole")
	}

	// Unless recording is actually started, child spans are still noop.
	noop3 := tr.StartSpan("noop3", WithParentAndManualCollection(s1.Meta()))
	if !noop3.isNoop() {
		t.Error("expected noop child Span")
	}
	noop3.Finish()

	s1.LogKV("x", 1)
	s1.StartRecording(SnowballRecording)
	s1.LogKV("x", 2)
	s2 := tr.StartSpan("b", WithParentAndAutoCollection(s1))
	if s2.IsBlackHole() {
		t.Error("recording Span should not be black hole")
	}
	s2.LogKV("x", 3)

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
			tags: sb=1 unfinished=
			x: 2
		Span b:
			tags: sb=1 unfinished=
			x: 3
	`); err != nil {
		t.Fatal(err)
	}

	if err := TestingCheckRecordedSpans(s2.GetRecording(), `
		Span b:
			tags: sb=1 unfinished=
			x: 3
	`); err != nil {
		t.Fatal(err)
	}

	s3 := tr.StartSpan("c", WithParentAndAutoCollection(s2))
	s3.LogKV("x", 4)
	s3.SetTag("tag", "val")

	s2.Finish()

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
			tags: sb=1 unfinished=
			x: 2
		Span b:
			tags: sb=1
			x: 3
		Span c:
			tags: sb=1 tag=val unfinished=
			x: 4
	`); err != nil {
		t.Fatal(err)
	}
	s3.Finish()
	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
      tags: sb=1 unfinished=
			x: 2
		Span b:
      tags: sb=1
			x: 3
		Span c:
			tags: sb=1 tag=val
			x: 4
	`); err != nil {
		t.Fatal(err)
	}
	s1.StopRecording()
	s1.LogKV("x", 100)
	if err := TestingCheckRecordedSpans(s1.GetRecording(), ``); err != nil {
		t.Fatal(err)
	}

	// The child Span is still recording.
	s3.LogKV("x", 5)
	if err := TestingCheckRecordedSpans(s3.GetRecording(), `
		Span c:
			tags: sb=1 tag=val
			x: 4
			x: 5
	`); err != nil {
		t.Fatal(err)
	}
	s1.Finish()
}

func TestStartChildSpan(t *testing.T) {
	tr := NewTracer()
	sp1 := tr.StartSpan("parent", WithForceRealSpan())
	sp1.StartRecording(SnowballRecording)
	sp2 := tr.StartSpan("child", WithParentAndAutoCollection(sp1))
	sp2.Finish()
	sp1.Finish()

	var exp = `
Span parent:
      tags: sb=1
    Span child:
      tags: sb=1
`

	if err := TestingCheckRecordedSpans(sp1.GetRecording(), exp); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", WithForceRealSpan())
	sp1.StartRecording(SnowballRecording)
	sp2 = tr.StartSpan("child", WithParentAndManualCollection(sp1.Meta()))
	sp2.Finish()
	sp1.Finish()
	if err := TestingCheckRecordedSpans(sp1.GetRecording(), `
		Span parent:
			tags: sb=1
	`); err != nil {
		t.Fatal(err)
	}
	if err := TestingCheckRecordedSpans(sp2.GetRecording(), `
		Span child:
			tags: sb=1
	`); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", WithForceRealSpan())
	sp1.StartRecording(SnowballRecording)
	sp2 = tr.StartSpan("child", WithParentAndAutoCollection(sp1),
		WithLogTags(logtags.SingleTagBuffer("key", "val")))
	sp2.Finish()
	sp1.Finish()
	if err := TestingCheckRecordedSpans(sp1.GetRecording(), `
		Span parent:
			tags: sb=1
			Span child:
				tags: key=val sb=1
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
	if err := tr.Inject(noop1.Meta(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}
	if len(carrier) != 0 {
		t.Errorf("noop Span has carrier: %+v", carrier)
	}

	wireContext, err := tr2.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}
	if !wireContext.isNilOrNoop() {
		t.Errorf("expected noop context: %v", wireContext)
	}
	noop2 := tr2.StartSpan("remote op", WithParentAndManualCollection(wireContext))
	if !noop2.isNoop() {
		t.Fatalf("expected noop Span: %+v", noop2)
	}
	noop1.Finish()
	noop2.Finish()

	// Verify that snowball tracing is propagated and triggers recording on the
	// remote side.

	s1 := tr.StartSpan("a", WithForceRealSpan())
	s1.StartRecording(SnowballRecording)

	carrier = make(opentracing.HTTPHeadersCarrier)
	if err := tr.Inject(s1.Meta(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}

	wireContext, err = tr2.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}
	s2 := tr2.StartSpan("remote op", WithParentAndManualCollection(wireContext))

	// Compare TraceIDs
	trace1 := s1.Meta().traceID
	trace2 := s2.Meta().traceID
	if trace1 != trace2 {
		t.Errorf("traceID doesn't match: parent %d child %d", trace1, trace2)
	}
	s2.LogKV("x", 1)
	s2.Finish()

	// Verify that recording was started automatically.
	rec := s2.GetRecording()
	if err := TestingCheckRecordedSpans(rec, `
		Span remote op:
			tags: sb=1
			x: 1
	`); err != nil {
		t.Fatal(err)
	}

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
			tags: sb=1 unfinished=
	`); err != nil {
		t.Fatal(err)
	}

	if err := s1.ImportRemoteSpans(rec); err != nil {
		t.Fatal(err)
	}
	s1.Finish()

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
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
	if err := tr.Inject(s.Meta(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}

	// Extract also extracts the embedded lightstep context.
	wireContext, err := tr.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}

	s2 := tr.StartSpan("child", WithParentAndManualCollection(wireContext))
	s2Ctx := s2.ot.shadowSpan.Context()

	// Verify that the baggage is correct in both the tracer context and in the
	// lightstep context.
	shadowBaggage := make(map[string]string)
	s2Ctx.ForeachBaggageItem(func(k, v string) bool {
		shadowBaggage[k] = v
		return true
	})
	exp := map[string]string{
		testBaggageKey: testBaggageVal,
	}
	require.Equal(t, exp, s2.Meta().Baggage)
	require.Equal(t, exp, shadowBaggage)
}
