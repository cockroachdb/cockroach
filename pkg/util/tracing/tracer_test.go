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
	"sort"
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
	noop1.Record("hello")

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

	s1.Recordf("x=%d", 1)
	s1.SetVerbose(true)
	s1.Recordf("x=%d", 2)
	s2 := tr.StartSpan("b", WithParentAndAutoCollection(s1))
	if s2.IsBlackHole() {
		t.Error("recording Span should not be black hole")
	}
	s2.Recordf("x=%d", 3)

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
			tags: _unfinished=1 _verbose=1
			event: x=2
		Span b:
			tags: _unfinished=1 _verbose=1
			event: x=3
	`); err != nil {
		t.Fatal(err)
	}

	if err := TestingCheckRecordedSpans(s2.GetRecording(), `
		Span b:
			tags: _unfinished=1 _verbose=1
			event: x=3
	`); err != nil {
		t.Fatal(err)
	}

	s3 := tr.StartSpan("c", WithParentAndAutoCollection(s2))
	s3.Recordf("x=%d", 4)
	s3.SetTag("tag", "val")

	s2.Finish()

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
			tags: _unfinished=1 _verbose=1
			event: x=2
		Span b:
			tags: _verbose=1
			event: x=3
		Span c:
			tags: _unfinished=1 _verbose=1 tag=val
			event: x=4
	`); err != nil {
		t.Fatal(err)
	}
	s3.Finish()
	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
      tags: _unfinished=1 _verbose=1
			event: x=2
		Span b:
      tags: _verbose=1
			event: x=3
		Span c:
			tags: _verbose=1 tag=val
			event: x=4
	`); err != nil {
		t.Fatal(err)
	}
	s1.SetVerbose(false)
	s1.Recordf("x=%d", 100)
	if err := TestingCheckRecordedSpans(s1.GetRecording(), ``); err != nil {
		t.Fatal(err)
	}

	// The child Span is still recording.
	s3.Recordf("x=%d", 5)
	if err := TestingCheckRecordedSpans(s3.GetRecording(), `
		Span c:
			tags: _verbose=1 tag=val
			event: x=4
			event: x=5
	`); err != nil {
		t.Fatal(err)
	}
	s1.Finish()
}

func TestStartChildSpan(t *testing.T) {
	tr := NewTracer()
	sp1 := tr.StartSpan("parent", WithForceRealSpan())
	sp1.SetVerbose(true)
	sp2 := tr.StartSpan("child", WithParentAndAutoCollection(sp1))
	sp2.Finish()
	sp1.Finish()

	var exp = `
Span parent:
      tags: _verbose=1
    Span child:
      tags: _verbose=1
`

	if err := TestingCheckRecordedSpans(sp1.GetRecording(), exp); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", WithForceRealSpan())
	sp1.SetVerbose(true)
	sp2 = tr.StartSpan("child", WithParentAndManualCollection(sp1.Meta()))
	sp2.Finish()
	sp1.Finish()
	if err := TestingCheckRecordedSpans(sp1.GetRecording(), `
		Span parent:
			tags: _verbose=1
	`); err != nil {
		t.Fatal(err)
	}
	if err := TestingCheckRecordedSpans(sp2.GetRecording(), `
		Span child:
			tags: _verbose=1
	`); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", WithForceRealSpan())
	sp1.SetVerbose(true)
	sp2 = tr.StartSpan("child", WithParentAndAutoCollection(sp1),
		WithLogTags(logtags.SingleTagBuffer("key", "val")))
	sp2.Finish()
	sp1.Finish()
	if err := TestingCheckRecordedSpans(sp1.GetRecording(), `
		Span parent:
			tags: _verbose=1
			Span child:
				tags: _verbose=1 key=val
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

	// Verify that verbose tracing is propagated and triggers verbosity on the
	// remote side.

	s1 := tr.StartSpan("a", WithForceRealSpan())
	s1.SetVerbose(true)

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
	s2.Recordf("x=%d", 1)
	s2.Finish()

	// Verify that recording was started automatically.
	rec := s2.GetRecording()
	if err := TestingCheckRecordedSpans(rec, `
		Span remote op:
			tags: _verbose=1
			event: x=1
	`); err != nil {
		t.Fatal(err)
	}

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
			tags: _unfinished=1 _verbose=1
	`); err != nil {
		t.Fatal(err)
	}

	if err := s1.ImportRemoteSpans(rec); err != nil {
		t.Fatal(err)
	}
	s1.Finish()

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		Span a:
			tags: _verbose=1
		Span remote op:
			tags: _verbose=1
			event: x=1
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

func getSortedActiveSpanOps(t *testing.T, tr *Tracer) []string {
	t.Helper()
	var sl []string

	require.NoError(t, tr.VisitSpans(func(sp *Span) error {
		for _, rec := range sp.GetRecording() {
			sl = append(sl, rec.Operation)
		}
		return nil
	}))

	sort.Strings(sl)
	return sl
}

// TestTracer_VisitSpans verifies that in-flight local root Spans
// are tracked by the Tracer, and that Finish'ed Spans are not.
func TestTracer_VisitSpans(t *testing.T) {
	tr1 := NewTracer()
	tr2 := NewTracer()

	root := tr1.StartSpan("root", WithForceRealSpan())
	root.SetVerbose(true)
	child := tr1.StartSpan("root.child", WithParentAndAutoCollection(root))
	require.Len(t, tr1.activeSpans.m, 1)

	childChild := tr2.StartSpan("root.child.remotechild", WithParentAndManualCollection(child.Meta()))
	childChildFinished := tr2.StartSpan("root.child.remotechilddone", WithParentAndManualCollection(child.Meta()))
	require.Len(t, tr2.activeSpans.m, 2)

	require.NoError(t, child.ImportRemoteSpans(childChildFinished.GetRecording()))

	childChildFinished.Finish()
	require.Len(t, tr2.activeSpans.m, 1)

	// Even though only `root` is tracked by tr1, we also reach
	// root.child and (via ImportRemoteSpans) the remote child.
	require.Equal(t, []string{"root", "root.child", "root.child.remotechilddone"}, getSortedActiveSpanOps(t, tr1))
	require.Equal(t, []string{"root.child.remotechild"}, getSortedActiveSpanOps(t, tr2))

	childChild.Finish()
	child.Finish()
	root.Finish()

	// Nothing is tracked any more.
	require.Len(t, getSortedActiveSpanOps(t, tr1), 0)
	require.Len(t, getSortedActiveSpanOps(t, tr2), 0)
	require.Len(t, tr1.activeSpans.m, 0)
	require.Len(t, tr2.activeSpans.m, 0)
}
