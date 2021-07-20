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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/logtags"
	lightstep "github.com/lightstep/lightstep-tracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	zipkin "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestStartSpanAlwaysTrace(t *testing.T) {
	// Regression test: if tracing is on, don't erroneously return a noopSpan
	// due to optimizations in StartSpan.
	tr := NewTracer()
	tr._useNetTrace = 1
	require.True(t, tr.AlwaysTrace())
	nilMeta := tr.noopSpan.Meta()
	require.True(t, nilMeta.Empty())
	sp := tr.StartSpan("foo", WithParentAndManualCollection(nilMeta))
	require.False(t, sp.IsVerbose()) // parent was not verbose, so neither is sp
	require.False(t, sp.i.isNoop())
	sp = tr.StartSpan("foo", WithParentAndAutoCollection(tr.noopSpan))
	require.False(t, sp.IsVerbose()) // parent was not verbose
	require.False(t, sp.i.isNoop())
}

func TestTracerRecording(t *testing.T) {
	tr := NewTracer()

	noop1 := tr.StartSpan("noop")
	if !noop1.i.isNoop() {
		t.Error("expected noop Span")
	}
	noop1.Record("hello")

	// Noop span returns empty recording.
	require.Equal(t, Recording(nil), noop1.GetRecording())

	noop2 := tr.StartSpan("noop2", WithParentAndManualCollection(noop1.Meta()))
	if !noop2.i.isNoop() {
		t.Error("expected noop child Span")
	}
	noop2.Finish()
	noop1.Finish()

	s1 := tr.StartSpan("a", WithForceRealSpan())
	if s1.i.isNoop() {
		t.Error("WithForceRealSpan (but not recording) Span should not be noop")
	}
	if s1.IsVerbose() {
		t.Error("WithForceRealSpan Span should not be verbose")
	}

	// Initial recording of this fresh (real) span.
	if err := TestingCheckRecordedSpans(s1.GetRecording(), ``); err != nil {
		t.Fatal(err)
	}

	s1.SetVerbose(true)
	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		span: a
			tags: _unfinished=1 _verbose=1
	`); err != nil {
		t.Fatal(err)
	}
	s1.SetVerbose(false)

	// Real parent --> real child.
	real3 := tr.StartSpan("noop3", WithParentAndManualCollection(s1.Meta()))
	if real3.i.isNoop() {
		t.Error("expected real child Span")
	}
	real3.Finish()

	s1.Recordf("x=%d", 1)
	s1.SetVerbose(true)
	s1.Recordf("x=%d", 2)
	s2 := tr.StartSpan("b", WithParentAndAutoCollection(s1))
	if !s2.IsVerbose() {
		t.Error("recording Span should be verbose")
	}
	s2.Recordf("x=%d", 3)

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		span: a
			tags: _unfinished=1 _verbose=1
			event: x=2
			span: b
				tags: _unfinished=1 _verbose=1
				event: x=3
	`); err != nil {
		t.Fatal(err)
	}

	if err := TestingCheckRecordedSpans(s2.GetRecording(), `
		span: b
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
		span: a
			tags: _unfinished=1 _verbose=1
			event: x=2
			span: b
				tags: _verbose=1
				event: x=3
				span: c
					tags: _unfinished=1 _verbose=1 tag=val
					event: x=4
	`); err != nil {
		t.Fatal(err)
	}
	s3.Finish()
	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		span: a
			tags: _unfinished=1 _verbose=1
			event: x=2
			span: b
				tags: _verbose=1
				event: x=3
				span: c
					tags: _verbose=1 tag=val
					event: x=4
	`); err != nil {
		t.Fatal(err)
	}
	s1.ResetRecording()
	s1.SetVerbose(false)
	s1.Recordf("x=%d", 100)
	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		span: a
	`); err != nil {
		t.Fatal(err)
	}

	// The child Span, now finished, will drop future recordings.
	s3.Recordf("x=%d", 5)
	if err := TestingCheckRecordedSpans(s3.GetRecording(), `
		span: c
			tags: _verbose=1 tag=val
			event: x=4
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

	if err := TestingCheckRecordedSpans(sp1.GetRecording(), `
		span: parent
			tags: _verbose=1
			span: child
				tags: _verbose=1
	`); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", WithForceRealSpan())
	sp1.SetVerbose(true)
	sp2 = tr.StartSpan("child", WithParentAndManualCollection(sp1.Meta()))
	sp2.Finish()
	sp1.Finish()
	if err := TestingCheckRecordedSpans(sp1.GetRecording(), `
		span: parent
			tags: _verbose=1
	`); err != nil {
		t.Fatal(err)
	}
	if err := TestingCheckRecordedSpans(sp2.GetRecording(), `
		span: child
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
		span: parent
			tags: _verbose=1
			span: child
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
	if !noop1.i.isNoop() {
		t.Fatalf("expected noop Span: %+v", noop1)
	}
	carrier := metadataCarrier{metadata.MD{}}
	if err := tr.InjectMetaInto(noop1.Meta(), carrier); err != nil {
		t.Fatal(err)
	}
	if len(carrier.MD) != 0 {
		t.Errorf("noop Span has carrier: %+v", carrier)
	}

	wireSpanMeta, err := tr2.ExtractMetaFrom(carrier)
	if err != nil {
		t.Fatal(err)
	}
	if !wireSpanMeta.Empty() {
		t.Errorf("expected no-op span meta: %v", wireSpanMeta)
	}
	noop2 := tr2.StartSpan("remote op", WithParentAndManualCollection(wireSpanMeta))
	if !noop2.i.isNoop() {
		t.Fatalf("expected noop Span: %+v", noop2)
	}
	noop1.Finish()
	noop2.Finish()

	// Verify that verbose tracing is propagated and triggers verbosity on the
	// remote side.

	s1 := tr.StartSpan("a", WithForceRealSpan())
	s1.SetVerbose(true)

	carrier = metadataCarrier{metadata.MD{}}
	if err := tr.InjectMetaInto(s1.Meta(), carrier); err != nil {
		t.Fatal(err)
	}

	wireSpanMeta, err = tr2.ExtractMetaFrom(carrier)
	if err != nil {
		t.Fatal(err)
	}
	s2 := tr2.StartSpan("remote op", WithParentAndManualCollection(wireSpanMeta))

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
		span: remote op
			tags: _verbose=1
			event: x=1
	`); err != nil {
		t.Fatal(err)
	}

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		span: a
			tags: _unfinished=1 _verbose=1
	`); err != nil {
		t.Fatal(err)
	}

	s1.ImportRemoteSpans(rec)
	s1.Finish()

	if err := TestingCheckRecordedSpans(s1.GetRecording(), `
		span: a
			tags: _verbose=1
			span: remote op
				tags: _verbose=1
				event: x=1
	`); err != nil {
		t.Fatal(err)
	}
}

func TestTracer_PropagateNonRecordingRealSpanAcrossRPCBoundaries(t *testing.T) {
	// Verify that when a span is put on the wire on one end, and is checked
	// against the span inclusion functions both on the client and server, a real
	// span results in a real span.
	tr1 := NewTracer()
	sp1 := tr1.StartSpan("tr1.root", WithForceRealSpan())
	defer sp1.Finish()
	carrier := metadataCarrier{MD: metadata.MD{}}
	require.True(t, spanInclusionFuncForClient(sp1))
	require.NoError(t, tr1.InjectMetaInto(sp1.Meta(), carrier))
	require.Equal(t, 2, carrier.Len(), "%+v", carrier) // trace id and span id

	tr2 := NewTracer()
	meta, err := tr2.ExtractMetaFrom(carrier)
	require.NoError(t, err)
	require.True(t, spanInclusionFuncForServer(tr2, meta))
	sp2 := tr2.StartSpan("tr2.child", WithParentAndManualCollection(meta))
	defer sp2.Finish()
	require.NotZero(t, sp2.i.crdb.spanID)
}

func TestShadowTracer(t *testing.T) {
	zipMgr, _ := createZipkinTracer("127.0.0.1:900000")
	zipRec := zipkin.NewInMemoryRecorder()
	zipTr, err := zipkin.NewTracer(zipRec)
	require.NoError(t, err)

	ddMgr, ddTr := createDataDogTracer("dummyaddr", "dummyproject")

	for _, tc := range []struct {
		mgr   shadowTracerManager
		str   opentracing.Tracer
		check func(t *testing.T, sp opentracing.Span)
	}{
		{
			mgr: lightStepManager{},
			str: lightstep.NewTracer(lightstep.Options{
				AccessToken: "invalid",
				// Massaged the creation here to not erroneously send crap to
				// lightstep's API. One of the ways below would've done it but
				// can't hurt to block it in multiple ways just in case.
				MinReportingPeriod: time.Hour,
				Collector: lightstep.Endpoint{
					Host:      "127.0.0.1",
					Port:      65535,
					Plaintext: true,
				},
				MaxLogsPerSpan: maxLogsPerSpanExternal,
				UseGRPC:        true,
			}),
		},
		{
			mgr: zipMgr,
			str: zipTr,
			check: func(t *testing.T, spi opentracing.Span) {
				rs := zipRec.GetSpans()
				require.Len(t, rs, 2)
				// The first span we opened is the second one to get recorded.
				parentSpan := rs[1]
				require.Len(t, parentSpan.Logs, 1)
				require.Equal(t, log.String("event", "hello"), parentSpan.Logs[0].Fields[0])
			},
		},
		{
			mgr: ddMgr,
			str: ddTr,
		},
	} {
		t.Run(tc.mgr.Name(), func(t *testing.T) {
			tr := NewTracer()
			tr.setShadowTracer(tc.mgr, tc.str)
			s := tr.StartSpan("test")
			defer func() {
				if tc.check != nil {
					tc.check(t, s.i.ot.shadowSpan)
				}
			}()
			defer s.Finish()
			// The span is not verbose, but has a sink (i.e. the log messages
			// go somewhere), though we'll actually check that end-to-end below
			// at least for the mock tracer.
			require.False(t, s.IsVerbose())
			require.True(t, s.i.hasVerboseSink())
			// Put something in the span.
			s.Record("hello")

			const testBaggageKey = "test-baggage"
			const testBaggageVal = "test-val"
			s.SetBaggageItem(testBaggageKey, testBaggageVal)
			// Also add a baggage item that is exclusive to the shadow span.
			// This wouldn't typically happen in practice, but it serves as
			// a regression test for #62702. Losing the Span context directly
			// is hard to verify via baggage items since the top-level baggage
			// is transported separately and re-inserted into the shadow context
			// on the remote side, i.e. the test-baggage item above shows up
			// regardless of whether #62702 is fixed. But if we're losing the
			// shadowCtx, the only-in-shadow item does get lost as well, so if
			// it does not then we know for sure that the shadowContext was
			// propagated properly.
			s.i.ot.shadowSpan.SetBaggageItem("only-in-shadow", "foo")

			carrier := metadataCarrier{metadata.MD{}}
			if err := tr.InjectMetaInto(s.Meta(), carrier); err != nil {
				t.Fatal(err)
			}

			// ExtractMetaFrom also extracts the embedded shadow context.
			wireSpanMeta, err := tr.ExtractMetaFrom(carrier)
			if err != nil {
				t.Fatal(err)
			}

			s2 := tr.StartSpan("child", WithParentAndManualCollection(wireSpanMeta))
			defer s2.Finish()
			s2Ctx := s2.i.ot.shadowSpan.Context()

			// Verify that the baggage is correct in both the tracer context and in the
			// shadow's context.
			shadowBaggage := make(map[string]string)
			s2Ctx.ForeachBaggageItem(func(k, v string) bool {
				shadowBaggage[k] = v
				return true
			})
			require.Equal(t, map[string]string{
				testBaggageKey: testBaggageVal,
			}, s2.Meta().Baggage)
			require.Equal(t, map[string]string{
				testBaggageKey:   testBaggageVal,
				"only-in-shadow": "foo",
			}, shadowBaggage)
		})
	}

}

func TestShadowTracerNilTracer(t *testing.T) {
	tr := NewTracer()
	// The lightstep tracer is nil when lightstep find that it is
	// misconfigured or can't instantiate a connection. Make sure
	// this does not lead to a crash.
	require.NotPanics(t, func() {
		tr.setShadowTracer(lightStepManager{}, nil)
	})
}

func TestTracer_RegistryMaxSize(t *testing.T) {
	tr := NewTracer()
	for i := 0; i < maxSpanRegistrySize+10; i++ {
		_ = tr.StartSpan("foo", WithForceRealSpan()) // intentionally not closed
		exp := i + 1
		if exp > maxSpanRegistrySize {
			exp = maxSpanRegistrySize
		}
		require.Len(t, tr.activeSpans.m, exp)
	}
}

// TestActiveSpanVisitorErrors confirms that the visitor of the Tracer's
// activeSpans registry gracefully exits upon receiving a sentinel error from
// `iterutil.StopIteration()`.
func TestActiveSpanVisitorErrors(t *testing.T) {
	tr := NewTracer()
	root := tr.StartSpan("root", WithForceRealSpan())
	defer root.Finish()

	child := tr.StartSpan("root.child", WithParentAndAutoCollection(root))
	defer child.Finish()

	remoteChild := tr.StartSpan("root.remotechild", WithParentAndManualCollection(child.Meta()))
	defer remoteChild.Finish()

	var numVisited int

	visitor := func(*Span) error {
		numVisited++
		return iterutil.StopIteration()
	}

	require.NoError(t, tr.VisitSpans(visitor))
	require.Equal(t, 1, numVisited)
}

// getSpanOpsWithFinished is a helper method that returns a map of spans in
// in-flight traces, keyed on operation names and with values representing
// whether the span is finished.
func getSpanOpsWithFinished(t *testing.T, tr *Tracer) map[string]bool {
	t.Helper()

	spanOpsWithFinished := make(map[string]bool)

	require.NoError(t, tr.VisitSpans(func(sp *Span) error {
		for _, rec := range sp.GetRecording() {
			spanOpsWithFinished[rec.Operation] = rec.Finished
		}
		return nil
	}))

	return spanOpsWithFinished
}

// getSortedSpanOps is a helper method that returns a sorted list of span
// operation names from in-flight traces.
func getSortedSpanOps(t *testing.T, tr *Tracer) []string {
	t.Helper()

	var spanOps []string

	require.NoError(t, tr.VisitSpans(func(sp *Span) error {
		for _, rec := range sp.GetRecording() {
			spanOps = append(spanOps, rec.Operation)
		}
		return nil
	}))

	sort.Strings(spanOps)
	return spanOps
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

	child.ImportRemoteSpans(childChildFinished.GetRecording())

	childChildFinished.Finish()
	require.Len(t, tr2.activeSpans.m, 1)

	// Even though only `root` is tracked by tr1, we also reach
	// root.child and (via ImportRemoteSpans) the remote child.
	require.Equal(t, []string{"root", "root.child", "root.child.remotechilddone"}, getSortedSpanOps(t, tr1))
	require.Len(t, getSortedSpanOps(t, tr1), 3)
	require.Equal(t, []string{"root.child.remotechild"}, getSortedSpanOps(t, tr2))

	childChild.Finish()
	child.Finish()
	root.Finish()

	// Nothing is tracked any more.
	require.Len(t, getSortedSpanOps(t, tr1), 0)
	require.Len(t, getSortedSpanOps(t, tr2), 0)
	require.Len(t, tr1.activeSpans.m, 0)
	require.Len(t, tr2.activeSpans.m, 0)
}

// TestSpanRecordingFinished verifies that Finished()ed Spans surfaced in an
// in-flight trace have recordings indicating that they have, in fact, finished.
func TestSpanRecordingFinished(t *testing.T) {
	tr1 := NewTracer()
	root := tr1.StartSpan("root", WithForceRealSpan())
	root.SetVerbose(true)

	child := tr1.StartSpan("root.child", WithParentAndAutoCollection(root))
	childChild := tr1.StartSpan("root.child.child", WithParentAndAutoCollection(child))

	tr2 := NewTracer()
	remoteChildChild := tr2.StartSpan("root.child.remotechild", WithParentAndManualCollection(child.Meta()))
	child.ImportRemoteSpans(remoteChildChild.GetRecording())

	// All spans are un-finished.
	sortedSpanOps := getSortedSpanOps(t, tr1)
	require.Equal(t, []string{"root", "root.child", "root.child.child", "root.child.remotechild"}, sortedSpanOps)
	spanOpsWithFinished := getSpanOpsWithFinished(t, tr1)
	for _, finished := range spanOpsWithFinished {
		require.False(t, finished)
	}

	childChild.Finish()
	spanOpsWithFinished = getSpanOpsWithFinished(t, tr1)

	// Only childChild should appear to have finished.
	require.False(t, spanOpsWithFinished["root"])
	require.False(t, spanOpsWithFinished["root.child"])
	require.True(t, spanOpsWithFinished["root.child.child"])
	require.False(t, spanOpsWithFinished["root.child.remotechild"])

	spanOpsWithFinished = getSpanOpsWithFinished(t, tr1)

	// Only childChild should appear to have finished.
	require.False(t, spanOpsWithFinished["root"])
	require.False(t, spanOpsWithFinished["root.child"])
	require.True(t, spanOpsWithFinished["root.child.child"])
	require.False(t, spanOpsWithFinished["root.child.remotechild"])

	remoteChildChild.SetOperationName("root.child.remotechild-reimport")
	remoteChildChild.Finish()
	// NB: importing a span twice is essentially a bad idea. It's ok in
	// this test though.
	child.ImportRemoteSpans(remoteChildChild.GetRecording())
	child.Finish()
	spanOpsWithFinished = getSpanOpsWithFinished(t, tr1)

	// Only child, childChild, and remoteChildChild should appear to have finished.
	require.False(t, spanOpsWithFinished["root"])
	require.True(t, spanOpsWithFinished["root.child"])
	require.True(t, spanOpsWithFinished["root.child.child"])
	// The original remotechild import is still unfinished, as it was imported from
	// unfinished span.
	require.False(t, spanOpsWithFinished["root.child.remotechild"])
	// The re-imported remotechild (after it had finished) is finished, though.
	require.True(t, spanOpsWithFinished["root.child.remotechild-reimport"])

	root.Finish()
	// Nothing is tracked anymore.
	spanOpsWithFinished = getSpanOpsWithFinished(t, tr1)
	require.Len(t, spanOpsWithFinished, 0)
}

func TestTracer_TracingVerbosityIndependentSemanticsIsActive(t *testing.T) {
	// Verify that GetRecording() returns nil for non-verbose spans if we're in
	// mixed-version mode.
	tr := NewTracer()
	tr.TracingVerbosityIndependentSemanticsIsActive = func() bool { return false }
	sp := tr.StartSpan("root", WithForceRealSpan())
	defer sp.Finish()
	sp.SetVerbose(true)
	sp.Record("foo")
	require.NotNil(t, sp.GetRecording())
	sp.SetVerbose(false)
	require.Nil(t, sp.GetRecording())
	tr.TracingVerbosityIndependentSemanticsIsActive = func() bool { return true }
	require.NotNil(t, sp.GetRecording())
}

func TestNoopSpanFinish(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("noop")
	require.Equal(t, tr.noopSpan, sp)
	require.EqualValues(t, 1, tr.noopSpan.numFinishCalled)
	sp.Finish()
	require.EqualValues(t, 1, tr.noopSpan.numFinishCalled)
}
