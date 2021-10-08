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
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otelsdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
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
	require.False(t, sp.IsNoop())
	sp = tr.StartSpan("foo", WithParentAndAutoCollection(tr.noopSpan))
	require.False(t, sp.IsVerbose()) // parent was not verbose
	require.False(t, sp.IsNoop())
}

func TestTracerRecording(t *testing.T) {
	tr := NewTracer()

	noop1 := tr.StartSpan("noop")
	if !noop1.IsNoop() {
		t.Error("expected noop Span")
	}
	noop1.Record("hello")

	// Noop span returns empty recording.
	require.Equal(t, Recording(nil), noop1.GetRecording())

	noop2 := tr.StartSpan("noop2", WithParentAndManualCollection(noop1.Meta()))
	if !noop2.IsNoop() {
		t.Error("expected noop child Span")
	}
	noop2.Finish()
	noop1.Finish()

	s1 := tr.StartSpan("a", WithForceRealSpan())
	if s1.IsNoop() {
		t.Error("WithForceRealSpan (but not recording) Span should not be noop")
	}
	if s1.IsVerbose() {
		t.Error("WithForceRealSpan Span should not be verbose")
	}

	// Initial recording of this fresh (real) span.
	if err := CheckRecordedSpans(s1.GetRecording(), ``); err != nil {
		t.Fatal(err)
	}

	s1.SetVerbose(true)
	if err := CheckRecordedSpans(s1.GetRecording(), `
		span: a
			tags: _unfinished=1 _verbose=1
	`); err != nil {
		t.Fatal(err)
	}
	s1.SetVerbose(false)

	// Real parent --> real child.
	real3 := tr.StartSpan("noop3", WithParentAndManualCollection(s1.Meta()))
	if real3.IsNoop() {
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

	if err := CheckRecordedSpans(s1.GetRecording(), `
		span: a
			tags: _unfinished=1 _verbose=1
			event: x=2
			span: b
				tags: _unfinished=1 _verbose=1
				event: x=3
	`); err != nil {
		t.Fatal(err)
	}

	if err := CheckRecordedSpans(s2.GetRecording(), `
		span: b
			tags: _unfinished=1 _verbose=1
			event: x=3
	`); err != nil {
		t.Fatal(err)
	}

	s3 := tr.StartSpan("c", WithParentAndAutoCollection(s2))
	s3.Recordf("x=%d", 4)
	s3.SetTag("tag", attribute.StringValue("val"))

	s2.Finish()

	if err := CheckRecordedSpans(s1.GetRecording(), `
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
	if err := CheckRecordedSpans(s1.GetRecording(), `
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
	if err := CheckRecordedSpans(s1.GetRecording(), `
		span: a
	`); err != nil {
		t.Fatal(err)
	}

	// The child Span, now finished, will drop future recordings.
	s3.Recordf("x=%d", 5)
	if err := CheckRecordedSpans(s3.GetRecording(), `
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

	if err := CheckRecordedSpans(sp1.GetRecording(), `
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
	if err := CheckRecordedSpans(sp1.GetRecording(), `
		span: parent
			tags: _verbose=1
	`); err != nil {
		t.Fatal(err)
	}
	if err := CheckRecordedSpans(sp2.GetRecording(), `
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
	if err := CheckRecordedSpans(sp1.GetRecording(), `
		span: parent
			tags: _verbose=1
			span: child
				tags: _verbose=1 key=val
	`); err != nil {
		t.Fatal(err)
	}
}

func TestSterileSpan(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTestingKnobs(TracerTestingKnobs{ForceRealSpans: true}))

	// Check that a children of sterile spans are roots.
	sp1 := tr.StartSpan("parent", WithSterile())
	// Make the span verbose so that we can use its recording below to assert that
	// there were no children.
	sp1.SetVerbose(true)
	sp2 := tr.StartSpan("child", WithParentAndAutoCollection(sp1))
	require.Zero(t, sp2.i.crdb.parentSpanID)

	require.True(t, sp1.Meta().sterile)
	require.False(t, sp2.Meta().sterile)
	sp3 := tr.StartSpan("child", WithParentAndManualCollection(sp1.Meta()))
	require.Zero(t, sp3.i.crdb.parentSpanID)

	sp2.Finish()
	sp3.Finish()
	require.NoError(t, CheckRecordedSpans(sp1.GetRecording(), `
		span: parent
			tags: _unfinished=1 _verbose=1
	`))

	// Check that the meta of a sterile span doesn't get injected into carriers.
	carrier := metadataCarrier{metadata.MD{}}
	require.NoError(t, tr.InjectMetaInto(sp1.Meta(), carrier))
	require.Len(t, carrier.MD, 0)
}

func TestTracerInjectExtract(t *testing.T) {
	tr := NewTracer()
	tr2 := NewTracer()

	// Verify that noop spans become noop spans on the remote side.

	noop1 := tr.StartSpan("noop")
	if !noop1.IsNoop() {
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
	if !noop2.IsNoop() {
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
	if err := CheckRecordedSpans(rec, `
		span: remote op
			tags: _verbose=1
			event: x=1
	`); err != nil {
		t.Fatal(err)
	}

	if err := CheckRecordedSpans(s1.GetRecording(), `
		span: a
			tags: _unfinished=1 _verbose=1
	`); err != nil {
		t.Fatal(err)
	}

	s1.ImportRemoteSpans(rec)
	s1.Finish()

	if err := CheckRecordedSpans(s1.GetRecording(), `
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

func TestOtelTracer(t *testing.T) {
	tr := NewTracer()
	sr := tracetest.NewSpanRecorder()
	otelTr := otelsdk.NewTracerProvider(
		otelsdk.WithSpanProcessor(sr),
		otelsdk.WithSampler(otelsdk.AlwaysSample()),
	).Tracer("test")
	tr.SetOpenTelemetryTracer(otelTr)
	s := tr.StartSpan("test")
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

	carrier := metadataCarrier{metadata.MD{}}
	if err := tr.InjectMetaInto(s.Meta(), carrier); err != nil {
		t.Fatal(err)
	}

	// ExtractMetaFrom also extracts the embedded OpenTelemetry context.
	wireSpanMeta, err := tr.ExtractMetaFrom(carrier)
	if err != nil {
		t.Fatal(err)
	}

	s2 := tr.StartSpan("child", WithParentAndManualCollection(wireSpanMeta))

	exp := map[string]string{
		testBaggageKey: testBaggageVal,
	}
	require.Equal(t, exp, s2.Meta().Baggage)
	// Note: we don't propagate baggage to the otel spans very well - we don't use
	// the otel baggage features. We do, however, set attributes on the shadow
	// spans when we can, which happens to be sufficient for this test.

	rs := sr.Started()
	require.Len(t, rs, 2)
	require.Len(t, rs[0].Events(), 1)
	require.Equal(t, "hello", rs[0].Events()[0].Name)
	require.Len(t, rs[0].Attributes(), 1)
	require.Equal(t,
		attribute.KeyValue{Key: testBaggageKey, Value: attribute.StringValue(testBaggageVal)},
		rs[0].Attributes()[0])
	require.Len(t, rs[1].Attributes(), 1)
	require.Equal(t,
		attribute.KeyValue{Key: testBaggageKey, Value: attribute.StringValue(testBaggageVal)},
		rs[1].Attributes()[0])
	require.Equal(t, rs[0].SpanContext().TraceID(), rs[1].Parent().TraceID())
	require.Equal(t, rs[0].SpanContext().SpanID(), rs[1].Parent().SpanID())
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

func TestNoopSpanFinish(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("noop")
	require.Equal(t, tr.noopSpan, sp)
	require.EqualValues(t, 1, tr.noopSpan.numFinishCalled)
	sp.Finish()
	require.EqualValues(t, 1, tr.noopSpan.numFinishCalled)
}

// Test that a span constructed with a no-op span behaves like a root span - it
// is present in the active spans registry.
func TestSpanWithNoopParentIsInActiveSpans(t *testing.T) {
	tr := NewTracer()
	noop := tr.StartSpan("noop")
	require.True(t, noop.IsNoop())
	root := tr.StartSpan("foo", WithParentAndAutoCollection(noop), WithForceRealSpan())
	require.Len(t, tr.activeSpans.m, 1)
	visitor := func(sp *Span) error {
		require.Equal(t, root, sp)
		return nil
	}
	require.NoError(t, tr.VisitSpans(visitor))
}

func TestConcurrentChildAndRecording(t *testing.T) {
	tr := NewTracer()
	rootSp := tr.StartSpan("root", WithForceRealSpan())
	rootSp.SetVerbose(true)
	var wg sync.WaitGroup
	const n = 1000
	wg.Add(2 * n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			sp := tr.StartSpan(
				"child",
				WithParentAndAutoCollection(rootSp),      // links sp to rootSp
				WithSpanKind(oteltrace.SpanKindConsumer)) // causes a tag to be set
			sp.Finish()
		}()
		go func() {
			defer wg.Done()
			_ = rootSp.GetRecording()
		}()
	}
	wg.Wait()
}
