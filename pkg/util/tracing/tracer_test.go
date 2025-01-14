// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/types"
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
	var nilSpan *Span
	tr := NewTracer()
	tr._useNetTrace = 1
	require.True(t, tr.AlwaysTrace())
	nilMeta := nilSpan.Meta()
	require.True(t, nilMeta.Empty())
	sp := tr.StartSpan("foo", WithRemoteParentFromSpanMeta(nilMeta))
	require.False(t, sp.IsVerbose()) // parent was not verbose, so neither is sp
	require.NotNil(t, sp)
	sp.Finish()
	sp = tr.StartSpan("foo", WithParent(nil))
	require.False(t, sp.IsVerbose()) // parent was not verbose
	require.NotNil(t, sp)
	sp.Finish()
}

func TestTracingOffRecording(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeOnDemand))
	// Noop spans are nil.
	noop1 := tr.StartSpan("noop")
	require.Nil(t, noop1)

	// Recording using a nil span is a no-op.
	noop1.Record("hello")

	// Starting a child span of a nil span is a no-op.
	noop2 := tr.StartSpan("noop2", WithParent(noop1), WithDetachedRecording())
	require.Nil(t, noop2)

	// Noop span returns Empty recording.
	require.Nil(t, noop1.GetRecording(tracingpb.RecordingVerbose))
}

func TestTracerRecording(t *testing.T) {
	ctx := context.Background()
	tr := NewTracerWithOpt(ctx, WithTracingMode(TracingModeActiveSpansRegistry))

	// Check that a span that was not configured to record returns nil for its
	// recording.
	sNonRecording := tr.StartSpan("not recording")
	require.Equal(t, tracingpb.RecordingOff, sNonRecording.RecordingType())
	require.Nil(t, sNonRecording.GetConfiguredRecording())
	require.Nil(t, sNonRecording.GetRecording(tracingpb.RecordingVerbose))
	require.Nil(t, sNonRecording.GetRecording(tracingpb.RecordingStructured))
	require.Nil(t, sNonRecording.FinishAndGetConfiguredRecording())

	s1 := tr.StartSpan("a", WithRecording(tracingpb.RecordingStructured))
	if s1 == nil {
		t.Error("recording Span should not be nil")
	}
	if s1.IsVerbose() {
		t.Error("WithRecording(RecordingStructured) should not be verbose")
	}

	// Initial recording of this fresh (real) span.
	rec := s1.GetRecording(tracingpb.RecordingStructured)
	require.Len(t, rec, 1)
	require.Nil(t, rec[0].Logs)
	require.Nil(t, rec[0].TagGroups)
	require.Empty(t, rec[0].ChildrenMetadata)
	require.Empty(t, rec[0].StructuredRecords)

	s1.RecordStructured(&types.Int32Value{Value: 5})
	checkRecording(t, s1.GetRecording(tracingpb.RecordingStructured), `
		=== operation:a
		structured:‹{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":5}›`)

	s1.SetRecordingType(tracingpb.RecordingVerbose)
	if err := CheckRecordedSpans(s1.GetRecording(tracingpb.RecordingVerbose), `
		span: a
			tags: _unfinished=1 _verbose=1`); err != nil {
		t.Fatal(err)
	}
	s1.SetRecordingType(tracingpb.RecordingOff)

	// Real parent --> real child.
	real3 := tr.StartSpan("noop3", WithRemoteParentFromSpanMeta(s1.Meta()))
	if real3 == nil {
		t.Error("expected real child Span")
	}
	real3.Finish()

	s1.Recordf("x=%d", 1)
	s1.SetRecordingType(tracingpb.RecordingVerbose)
	s1.Recordf("x=%d", 2)
	s2 := tr.StartSpan("b", WithParent(s1))
	if !s2.IsVerbose() {
		t.Error("recording Span should be verbose")
	}
	s2.Recordf("x=%d", 3)

	if err := CheckRecordedSpans(s1.GetRecording(tracingpb.RecordingVerbose), `
		span: a
			tags: _unfinished=1 _verbose=1
			event: x=2
			span: b
				tags: _unfinished=1 _verbose=1
				event: x=3`); err != nil {
		t.Fatal(err)
	}

	if err := CheckRecordedSpans(s2.GetRecording(tracingpb.RecordingVerbose), `
		span: b
			tags: _unfinished=1 _verbose=1
			event: x=3
	`); err != nil {
		t.Fatal(err)
	}

	s3 := tr.StartSpan("c", WithParent(s2))
	s3.Recordf("x=%d", 4)
	s3.SetTag("tag", attribute.StringValue("val"))

	s2.Finish()

	if err := CheckRecordedSpans(s1.GetRecording(tracingpb.RecordingVerbose), `
		span: a
			tags: _unfinished=1 _verbose=1
			event: x=2
			span: b
				tags: _verbose=1
				event: x=3
				span: c
					tags: _unfinished=1 _verbose=1 tag=val
					event: x=4`); err != nil {
		t.Fatal(err)
	}
	// We Finish() s3, but note that the recording shows it as _unfinished. That's
	// because s2's recording was snapshotted at the time s2 was finished, above.
	s3.Finish()
	if err := CheckRecordedSpans(s1.GetRecording(tracingpb.RecordingVerbose), `
		span: a
			tags: _unfinished=1 _verbose=1
			event: x=2
			span: b
				tags: _verbose=1
				event: x=3
				span: c
					tags: _unfinished=1 _verbose=1 tag=val
					event: x=4`); err != nil {
		t.Fatal(err)
	}
	s1.Finish()

	s4 := tr.StartSpan("a", WithRecording(tracingpb.RecordingStructured))
	s4.SetRecordingType(tracingpb.RecordingOff)
	s4.Recordf("x=%d", 100)
	require.Nil(t, s4.GetRecording(tracingpb.RecordingStructured))
	s4.Finish()
}

func TestStartChildSpan(t *testing.T) {
	tr := NewTracer()
	sp1 := tr.StartSpan("parent", WithRecording(tracingpb.RecordingVerbose))
	sp2 := tr.StartSpan("child", WithParent(sp1))
	sp2.Finish()

	if err := CheckRecordedSpans(sp1.FinishAndGetRecording(tracingpb.RecordingVerbose), `
		span: parent
			tags: _verbose=1
			span: child
				tags: _verbose=1`); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", WithRecording(tracingpb.RecordingVerbose))
	sp2 = tr.StartSpan("child", WithParent(sp1), WithDetachedRecording())
	if err := CheckRecordedSpans(sp2.FinishAndGetRecording(tracingpb.RecordingVerbose), `
		span: child
			tags: _verbose=1`); err != nil {
		t.Fatal(err)
	}
	if err := CheckRecordedSpans(sp1.FinishAndGetRecording(tracingpb.RecordingVerbose), `
		span: parent
			tags: _verbose=1`); err != nil {
		t.Fatal(err)
	}

	sp1 = tr.StartSpan("parent", WithRecording(tracingpb.RecordingVerbose))
	sp2 = tr.StartSpan("child", WithParent(sp1),
		WithLogTags(logtags.SingleTagBuffer("key", "val")))
	sp2.Finish()
	if err := CheckRecordedSpans(sp1.FinishAndGetRecording(tracingpb.RecordingVerbose), `
		span: parent
			tags: _verbose=1
			span: child
				tags: _verbose=1 key=val`); err != nil {
		t.Fatal(err)
	}
}

func TestSterileSpan(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))

	// Check that a children of sterile spans are roots.
	// Make the span verbose so that we can use its recording below to assert that
	// there were no children.
	sp1 := tr.StartSpan("parent", WithSterile(), WithRecording(tracingpb.RecordingVerbose))
	defer sp1.Finish()
	sp2 := tr.StartSpan("child", WithParent(sp1))
	require.Zero(t, sp2.i.crdb.parentSpanID)

	require.True(t, sp1.Meta().sterile)
	require.False(t, sp2.Meta().sterile)
	sp3 := tr.StartSpan("child", WithParent(sp1), WithDetachedRecording())
	require.Zero(t, sp3.i.crdb.parentSpanID)

	sp2.Finish()
	sp3.Finish()
	require.NoError(t, CheckRecordedSpans(sp1.GetRecording(tracingpb.RecordingVerbose), `
		span: parent
			tags: _unfinished=1 _verbose=1`))

	// Check that the meta of a sterile span doesn't get injected into carriers.
	carrier := MetadataCarrier{metadata.MD{}}
	tr.InjectMetaInto(sp1.Meta(), carrier)
	require.Len(t, carrier.MD, 0)
}

func TestTracer_ForceVerboseOperationName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name             string
		opName           string
		opNameRegexp     string
		spanOpts         []SpanOption
		expRecordingType tracingpb.RecordingType
	}
	for _, tc := range []testCase{
		{
			name:             "span maintains OFF recording type when opName regex unset",
			opName:           "opName",
			expRecordingType: tracingpb.RecordingOff,
		},
		{
			name:             "span set to VERBOSE recording type when opName regex matches",
			opName:           "opName",
			opNameRegexp:     "someOtherName|opName",
			expRecordingType: tracingpb.RecordingVerbose,
		},
		{
			name:         "span set to VERBOSE when regex matches despite SpanOption specifying other recording",
			opName:       "opName",
			opNameRegexp: "someOtherName|opName",
			spanOpts: []SpanOption{
				WithRecording(tracingpb.RecordingStructured),
			},
			expRecordingType: tracingpb.RecordingVerbose,
		},
		{
			name:         "span set to VERBOSE when regex matches despite noop span parent",
			opName:       "opName",
			opNameRegexp: "someOtherName|opName",
			spanOpts: []SpanOption{
				WithRemoteParentFromSpanMeta(noopSpanMeta),
			},
			expRecordingType: tracingpb.RecordingVerbose,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))
			if tc.opNameRegexp != "" {
				require.NoError(t, tr.setVerboseOpNameRegexp(tc.opNameRegexp))
			}
			sp := tr.StartSpan(tc.opName, tc.spanOpts...)
			require.Equal(t, tc.expRecordingType, sp.RecordingType())
		})
	}
}

func TestTracerInjectExtractNoop(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeOnDemand))
	tr2 := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeOnDemand))

	// Verify that noop (nil) spans become noop spans on the remote side.

	noop1 := tr.StartSpan("noop")
	require.Nilf(t, noop1, "expected nil Span: %+v", noop1)
	carrier := MetadataCarrier{metadata.MD{}}
	tr.InjectMetaInto(noop1.Meta(), carrier)
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
	noop2 := tr2.StartSpan("remote op", WithRemoteParentFromSpanMeta(wireSpanMeta))
	require.Nilf(t, noop2, "expected nil Span: %+v", noop2)
	noop1.Finish()
	noop2.Finish()
}

func TestTracerInjectExtract(t *testing.T) {
	tr := NewTracer()
	tr2 := NewTracer()

	// Verify that verbose tracing is propagated and triggers verbosity on the
	// remote side.

	s1 := tr.StartSpan("a", WithRecording(tracingpb.RecordingVerbose))

	carrier := MetadataCarrier{metadata.MD{}}
	tr.InjectMetaInto(s1.Meta(), carrier)

	wireSpanMeta, err := tr2.ExtractMetaFrom(carrier)
	if err != nil {
		t.Fatal(err)
	}
	s2 := tr2.StartSpan("remote op", WithRemoteParentFromSpanMeta(wireSpanMeta))

	// Compare TraceIDs
	trace1 := s1.Meta().traceID
	trace2 := s2.Meta().traceID
	if trace1 != trace2 {
		t.Errorf("traceID doesn't match: parent %d child %d", trace1, trace2)
	}
	s2.Recordf("x=%d", 1)

	// Verify that recording was started automatically.
	rec := s2.FinishAndGetRecording(tracingpb.RecordingVerbose)
	if err := CheckRecordedSpans(rec, `
		span: remote op
			tags: _verbose=1
			event: x=1
	`); err != nil {
		t.Fatal(err)
	}

	if err := CheckRecordedSpans(s1.GetRecording(tracingpb.RecordingVerbose), `
		span: a
			tags: _unfinished=1 _verbose=1
	`); err != nil {
		t.Fatal(err)
	}

	s1.ImportRemoteRecording(rec)
	if err := CheckRecordedSpans(s1.FinishAndGetRecording(tracingpb.RecordingVerbose), `
		span: a
			tags: _verbose=1
			span: remote op
				tags: _verbose=1
				event: x=1`); err != nil {
		t.Fatal(err)
	}
}

func TestTracer_PropagateNonRecordingRealSpanAcrossRPCBoundaries(t *testing.T) {
	// Verify that when a span is put on the wire on one end, and is checked
	// against the span inclusion functions both on the client and server, a real
	// span results in a real span.
	tr1 := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))
	sp1 := tr1.StartSpan("tr1.root")
	defer sp1.Finish()
	carrier := MetadataCarrier{MD: metadata.MD{}}
	require.True(t, SpanInclusionFuncForClient(sp1))
	tr1.InjectMetaInto(sp1.Meta(), carrier)
	require.Equal(t, 3, carrier.Len(), "%+v", carrier) // trace id, span id, recording mode

	tr2 := NewTracer()
	meta, err := tr2.ExtractMetaFrom(carrier)
	require.NoError(t, err)
	require.True(t, SpanInclusionFuncForServer(tr2, meta))
	sp2 := tr2.StartSpan("tr2.child", WithRemoteParentFromSpanMeta(meta))
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

	carrier := MetadataCarrier{metadata.MD{}}
	tr.InjectMetaInto(s.Meta(), carrier)

	// ExtractMetaFrom also extracts the embedded OpenTelemetry context.
	wireSpanMeta, err := tr.ExtractMetaFrom(carrier)
	if err != nil {
		t.Fatal(err)
	}

	sp2 := tr.StartSpan("child", WithRemoteParentFromSpanMeta(wireSpanMeta))
	sp2.SetLazyTag("lazy expanding tag", testExpandingTag{})
	sp2.SetLazyTag("lazy tag", testStringerLazyTag{})

	rs := sr.Started()
	require.Len(t, rs, 2)
	require.Len(t, rs[0].Events(), 1)
	require.Equal(t, "hello", rs[0].Events()[0].Name)
	require.Equal(t, rs[0].SpanContext().TraceID(), rs[1].Parent().TraceID())
	require.Equal(t, rs[0].SpanContext().SpanID(), rs[1].Parent().SpanID())

	sp2.Finish()
	rs2 := sr.Ended()

	expectedAttributes := []attribute.KeyValue{
		{
			Key:   attribute.Key("lazy expanding tag-exp1"),
			Value: attribute.StringValue("1"),
		},
		{
			Key:   attribute.Key("lazy expanding tag-exp2"),
			Value: attribute.StringValue("2"),
		},
		{
			Key:   attribute.Key("lazy tag"),
			Value: attribute.StringValue("lazy stringer"),
		},
	}
	actualAttributes := rs2[0].Attributes()

	require.Len(t, rs2, 1)
	require.Len(t, actualAttributes, 3)
	require.Equal(t, expectedAttributes, actualAttributes)
}

func TestTracer_RegistryMaxSize(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))
	spans := make([]*Span, 0, maxSpanRegistrySize+10)
	for i := 0; i < maxSpanRegistrySize+10; i++ {
		sp := tr.StartSpan("foo")
		spans = append(spans, sp)
		exp := i + 1
		if exp > maxSpanRegistrySize {
			exp = maxSpanRegistrySize
		}
		require.Len(t, tr.activeSpansRegistry.mu.m, exp)
	}
	for _, sp := range spans {
		sp.Finish()
	}
}

// TestActiveSpanVisitorErrors confirms that the visitor of the Tracer's
// activeSpans registry gracefully exits upon receiving a sentinel error from
// `iterutil.StopIteration()`.
func TestActiveSpanVisitorErrors(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))
	root := tr.StartSpan("root")
	defer root.Finish()

	child := tr.StartSpan("root.child", WithParent(root))
	defer child.Finish()

	remoteChild := tr.StartSpan("root.remotechild", WithParent(child), WithDetachedRecording())
	defer remoteChild.Finish()

	var numVisited int

	visitor := func(RegistrySpan) error {
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

	require.NoError(t, tr.VisitSpans(func(sp RegistrySpan) error {
		rec := sp.GetFullRecording(tracingpb.RecordingVerbose)
		for _, rec := range rec.Flatten() {
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

	require.NoError(t, tr.VisitSpans(func(sp RegistrySpan) error {
		rec := sp.GetFullRecording(tracingpb.RecordingVerbose)
		for _, rec := range rec.Flatten() {
			spanOps = append(spanOps, rec.Operation)
		}
		return nil
	}))

	sort.Strings(spanOps)
	return spanOps
}

// TestTracer_VisitSpans verifies that in-flight Spans are tracked by the
// Tracer, and that Finish'ed Spans are not.
func TestTracer_VisitSpans(t *testing.T) {
	tr1 := NewTracer()
	tr2 := NewTracer()

	root := tr1.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))
	child := tr1.StartSpan("root.child", WithParent(root))
	require.Len(t, tr1.activeSpansRegistry.mu.m, 1)

	childChild := tr2.StartSpan("root.child.remotechild", WithRemoteParentFromSpanMeta(child.Meta()))
	childChildFinished := tr2.StartSpan("root.child.remotechilddone", WithRemoteParentFromSpanMeta(child.Meta()))
	require.Len(t, tr2.activeSpansRegistry.mu.m, 2)
	child.ImportRemoteRecording(childChildFinished.FinishAndGetRecording(tracingpb.RecordingVerbose))
	require.Len(t, tr2.activeSpansRegistry.mu.m, 1)

	// All spans are part of the recording (root.child.remotechilddone was
	// manually imported).
	require.Equal(t, []string{"root", "root.child", "root.child.remotechilddone"}, getSortedSpanOps(t, tr1))
	require.Len(t, getSortedSpanOps(t, tr1), 3)
	require.ElementsMatch(t, []string{"root.child.remotechild"}, getSortedSpanOps(t, tr2))

	childChild.Finish()
	child.Finish()
	root.Finish()

	// Nothing is tracked any more.
	require.Len(t, getSortedSpanOps(t, tr1), 0)
	require.Len(t, getSortedSpanOps(t, tr2), 0)
	require.Len(t, tr1.activeSpansRegistry.mu.m, 0)
	require.Len(t, tr2.activeSpansRegistry.mu.m, 0)
}

// TestSpanRecordingFinished verifies that Finished()ed Spans surfaced in an
// in-flight trace have recordings indicating that they have, in fact, finished.
func TestSpanRecordingFinished(t *testing.T) {
	tr1 := NewTracer()
	root := tr1.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))

	child := tr1.StartSpan("root.child", WithParent(root))
	childChild := tr1.StartSpan("root.child.child", WithParent(child))

	tr2 := NewTracer()
	childTraceInfo := child.Meta().ToProto()
	remoteChildChild := tr2.StartSpan("root.child.remotechild", WithRemoteParentFromTraceInfo(childTraceInfo))
	child.ImportRemoteRecording(remoteChildChild.GetRecording(tracingpb.RecordingVerbose))
	remoteChildChild.Finish()

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

	child.Finish()
	spanOpsWithFinished = getSpanOpsWithFinished(t, tr1)

	// Only child and childChild should appear to have finished.
	require.False(t, spanOpsWithFinished["root"])
	require.True(t, spanOpsWithFinished["root.child"])
	require.True(t, spanOpsWithFinished["root.child.child"])
	// The original remotechild import is still unfinished, as it was imported from
	// unfinished span.
	require.False(t, spanOpsWithFinished["root.child.remotechild"])

	root.Finish()
	// Nothing is tracked anymore.
	spanOpsWithFinished = getSpanOpsWithFinished(t, tr1)
	require.Len(t, spanOpsWithFinished, 0)
}

// Test that the noop (nil) span can be used after finish.
func TestNoopSpanFinish(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeOnDemand))
	var sp *Span
	sp1 := tr.StartSpan("noop")
	sp2 := tr.StartSpan("noop")
	require.Equal(t, sp1, sp)
	require.Equal(t, sp2, sp)
	sp1.Finish()
	sp2.Record("dummy")
	sp2.Finish()
}

// Test that a span constructed with a nil span behaves like a root span - it
// is present in the active spans registry.
func TestSpanWithNoopParentIsInActiveSpans(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeOnDemand))
	noop := tr.StartSpan("noop")
	require.Nil(t, noop)
	root := tr.StartSpan("foo", WithParent(noop), WithForceRealSpan())
	defer root.Finish()
	require.Len(t, tr.activeSpansRegistry.mu.m, 1)
	visitor := func(sp RegistrySpan) error {
		require.Equal(t, root.i.crdb, sp)
		return nil
	}
	require.NoError(t, tr.VisitSpans(visitor))
}

func TestConcurrentChildAndRecording(t *testing.T) {
	tr := NewTracer()
	rootSp := tr.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))
	defer rootSp.Finish()
	var wg sync.WaitGroup
	const n = 1000
	wg.Add(2 * n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			sp := tr.StartSpan(
				"child",
				WithParent(rootSp),                       // links sp to rootSp
				WithSpanKind(oteltrace.SpanKindConsumer)) // causes a tag to be set
			sp.Finish()
		}()
		go func() {
			defer wg.Done()
			_ = rootSp.GetRecording(tracingpb.RecordingVerbose)
		}()
	}
	wg.Wait()
}

func TestFinishedSpanInRecording(t *testing.T) {
	tr := NewTracer()
	s1 := tr.StartSpan("a", WithRecording(tracingpb.RecordingVerbose))
	s2 := tr.StartSpan("b", WithParent(s1))
	s3 := tr.StartSpan("c", WithParent(s2))

	// Check that s2 is included in the recording both before and after it's
	// finished.
	require.NoError(t, CheckRecordedSpans(s1.GetRecording(tracingpb.RecordingVerbose), `
span: a
    tags: _unfinished=1 _verbose=1
    span: b
        tags: _unfinished=1 _verbose=1
        span: c
            tags: _unfinished=1 _verbose=1
`))
	s3.Finish()
	require.NoError(t, CheckRecordedSpans(s1.GetRecording(tracingpb.RecordingVerbose), `
span: a
    tags: _unfinished=1 _verbose=1
    span: b
        tags: _unfinished=1 _verbose=1
        span: c
            tags: _verbose=1
`))
	s2.Finish()
	require.NoError(t, CheckRecordedSpans(s1.GetRecording(tracingpb.RecordingVerbose), `
span: a
    tags: _unfinished=1 _verbose=1
    span: b
        tags: _verbose=1
        span: c
            tags: _verbose=1
`))
	s1.Finish()

	// Now the same thing, but finish s2 first.
	s1 = tr.StartSpan("a", WithRecording(tracingpb.RecordingVerbose))
	s2 = tr.StartSpan("b", WithParent(s1))
	s3 = tr.StartSpan("c", WithParent(s2))

	s2.Finish()
	require.NoError(t, CheckRecordedSpans(s1.FinishAndGetRecording(tracingpb.RecordingVerbose), `
span: a
    tags: _verbose=1
    span: b
        tags: _verbose=1
        span: c
            tags: _unfinished=1 _verbose=1
`))
	s3.Finish()
}

// Test that, when a parent span finishes, children that are still open become
// roots and are inserted into the registry.
func TestRegistryOrphanSpansBecomeRoots(t *testing.T) {
	ctx := context.Background()
	tr := NewTracerWithOpt(ctx, WithTracingMode(TracingModeActiveSpansRegistry))
	// s1 must be recording because, otherwise, the child spans are not linked to
	// it.
	s1 := tr.StartSpan("parent", WithRecording(tracingpb.RecordingStructured))
	s2 := tr.StartSpan("child1", WithParent(s1))
	s3 := tr.StartSpan("child2", WithParent(s1))
	require.Equal(t, []*crdbSpan{s1.i.crdb}, tr.activeSpansRegistry.testingAll())
	s1.Finish()
	require.ElementsMatch(t, []*crdbSpan{s2.i.crdb, s3.i.crdb}, tr.activeSpansRegistry.testingAll())
	s2.Finish()
	s3.Finish()
	require.Len(t, tr.activeSpansRegistry.testingAll(), 0)
}

func TestContextWithRecordingSpan(t *testing.T) {
	tr := NewTracer()
	_, getRecAndFinish := ContextWithRecordingSpan(context.Background(), tr, "test")
	// Test that the callback can be called multiple times.
	rec1 := getRecAndFinish()
	require.NoError(t, CheckRecordedSpans(rec1, `
span: test
	tags: _verbose=1
`))
	rec2 := getRecAndFinish()
	require.Equal(t, rec1, rec2)
}

// Check that it is illegal to create a child with a different Tracer than the parent.
// Note that if the parent is a sterile span, it'll be replaced with a nil span,
// meaning its tracer won't be available at the time we are creatiing the span, so
// this assertion doesn't apply for sterile spans. Note also we intend to remove
// the concept of a sterile span shortly.
func TestChildNeedsSameTracerAsParent(t *testing.T) {
	t.Run("", func(t *testing.T) {
		tr1 := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeOnDemand))
		tr2 := NewTracer()

		parent := tr1.StartSpan("parent", WithForceRealSpan())
		defer parent.Finish()
		require.Panics(t, func() {
			tr2.StartSpan("child", WithParent(parent))
		})
	})
}

// TestSpanReuse checks that spans are reused through the Tracer's pool, instead
// of being allocated every time. This is a basic test, because the sync.Pool is
// generally not deterministic. See TestSpanPooling for a more comprehensive
// one.
func TestSpanReuse(t *testing.T) {
	skip.UnderRace(t, "sync.Pool seems to be emptied very frequently under race, making the test unreliable")
	skip.UnderDeadlock(t, "span reuse triggers false-positives in the deadlock detector")
	ctx := context.Background()
	tr := NewTracerWithOpt(ctx,
		// Ask the tracer to always reuse spans, overriding the testing's
		// metamorphic default and mimicking production.
		WithSpanReusePercent(100),
		WithTracingMode(TracingModeActiveSpansRegistry),
		WithTestingKnobs(TracerTestingKnobs{
			MaintainAllocationCounters: true,
		}))
	s1 := tr.StartSpan("root")
	s2 := tr.StartSpan("child", WithParent(s1))
	s3 := tr.StartSpan("child2", WithParent(s2))
	s1.Finish()
	s2.Finish()
	s3.Finish()
	created, alloc := tr.TestingGetStatsAndReset()
	require.Equal(t, 3, created)
	require.Equal(t, 3, alloc)

	// Due to the vagaries of sync.Pool (interaction with GC), the reuse is not
	// perfectly reliable. We try several times, and declare success if we ever
	// get full reuse.
	for i := 0; i < 10; i++ {
		tr.TestingGetStatsAndReset()
		s1 = tr.StartSpan("root")
		s2 = tr.StartSpan("child", WithParent(s1))
		s3 = tr.StartSpan("child2", WithParent(s2))
		s2.Finish()
		s3.Finish()
		s1.Finish()
		created, alloc = tr.TestingGetStatsAndReset()
		require.Equal(t, created, 3)
		if alloc == 0 {
			// Test succeeded.
			return
		}
	}
	t.Fatal("spans do not seem to be reused reliably")
}

// Test that parents and children can finish in any order (even concurrently)
// and that the recording is always sane in such cases.
func TestSpanFinishRaces(t *testing.T) {
	ctx := context.Background()
	tr := NewTracerWithOpt(ctx,
		WithSpanReusePercent(100),
		WithTracingMode(TracingModeActiveSpansRegistry),
		WithTestingKnobs(TracerTestingKnobs{
			ReleaseSpanToPool: func(sp *Span) bool {
				// Asynchronously overwrite sp, to catch races.
				go func() {
					*sp = Span{}
				}()
				// Tell the tracer to not pool this span. We've hijacked it above.
				return false
			}}),
	)

	const numSpans = 4

	for ti := 0; ti < 1000; ti++ {
		sps := make([]*Span, numSpans)
		for i := 0; i < numSpans; i++ {
			var opt SpanOption
			if i > 0 {
				opt = WithParent(sps[i-1])
			}
			sps[i] = tr.StartSpan(fmt.Sprint(i), WithRecording(tracingpb.RecordingVerbose), opt)
			sps[i].Recordf("msg %d", i)
		}

		finishOrder := rand.Perm(numSpans)
		var rec tracingpb.Recording
		g := sync.WaitGroup{}
		g.Add(len(finishOrder))
		for _, idx := range finishOrder {
			go func(idx int) {
				if idx != 0 {
					sps[idx].Finish()
				} else {
					rec = sps[0].FinishAndGetRecording(tracingpb.RecordingVerbose)
				}
				g.Done()
			}(idx)
		}
		g.Wait()
		require.Len(t, rec, numSpans)
		for i, s := range rec {
			require.Len(t, s.Logs, 1)
			require.Equal(t, fmt.Sprintf("msg %d", i), s.Logs[0].Message.StripMarkers())
		}
	}
}

// Test that updates to the EnableActiveSpansRegistry affect span creation.
func TestTracerClusterSettings(t *testing.T) {
	ctx := context.Background()
	sv := settings.Values{}
	EnableActiveSpansRegistry.Override(ctx, &sv, true)

	tr := NewTracerWithOpt(ctx, WithClusterSettings(&sv))
	sp := tr.StartSpan("test")
	require.NotNil(t, sp)
	sp.Finish()

	EnableActiveSpansRegistry.Override(ctx, &sv, false)
	sp = tr.StartSpan("test")
	require.Nil(t, sp)
	sp.Finish()

	EnableActiveSpansRegistry.Override(ctx, &sv, true)
	sp = tr.StartSpan("test")
	require.NotNil(t, sp)
	sp.Finish()
}

func TestTracerSnapshots(t *testing.T) {
	tr := NewTracer()

	s1 := tr.SaveSnapshot()
	require.Equal(t, SnapshotID(1), s1.ID)
	_ = tr.SaveSnapshot()
	s3 := tr.SaveSnapshot()
	require.Equal(t, SnapshotID(3), s3.ID)
	require.Equal(t, 3, len(tr.GetSnapshots()))

	b1 := tr.SaveAutomaticSnapshot()
	require.Equal(t, SnapshotID(1), b1.ID)
	b2 := tr.SaveAutomaticSnapshot()
	require.Equal(t, SnapshotID(2), b2.ID)
	require.Equal(t, 3, len(tr.GetSnapshots()))
	require.Equal(t, 2, len(tr.GetAutomaticSnapshots()))

	for _, i := range []SnapshotID{1, 2, 3} {
		_, err := tr.GetSnapshot(i)
		require.NoError(t, err)
	}
	for _, i := range []SnapshotID{1, 2} {
		_, err := tr.GetAutomaticSnapshot(i)
		require.NoError(t, err)
	}
}

func TestTracerSnapshotLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tr := NewTracer()
	ctx := context.Background()

	sv := settings.Values{}
	periodicSnapshotInterval.Override(ctx, &sv, 0)

	setting, done, testKnob := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(done)
	defer close(testKnob)

	go tr.runPeriodicSnapshotsLoop(&sv, setting, done, testKnob)

	// Verify our snapshot loop is running by sending rate-change notifications
	// and then verify it is not snapshotting since it is disabled.
	setting <- struct{}{}
	setting <- struct{}{}
	require.Empty(t, tr.GetAutomaticSnapshots())

	periodicSnapshotInterval.Override(ctx, &sv, time.Microsecond)
	for sent := false; !sent; {
		select {
		case setting <- struct{}{}:
		case testKnob <- struct{}{}:
			sent = true
		}
	}

	snaps := tr.GetAutomaticSnapshots()
	require.NotEmpty(t, snaps)
}

// helper that blocks long enough to appear in the stack.
func blockingFunc1(ch chan<- struct{}) {
	ch <- struct{}{} // allow snapshot to start.
	ch <- struct{}{} // wait for snapshot to be done.
}

// helper that blocks long enough to appear in the stack.
func blockingFunc2(ch chan<- struct{}) {
	ch <- struct{}{}
	ch <- struct{}{}
}

// helper that blocks long enough to appear in the stack.
func blockingFunc3(ch chan<- struct{}) {
	ch <- struct{}{}
	ch <- struct{}{}
}

func blockingCaller(ch chan<- struct{}) {
	blockingFunc2(ch)
}

// TestTracerStackHistory tests MaybeRecordStackHistory.
func TestTracerStackHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tr := NewTracer()

	for _, verbose := range []bool{true, false} {
		sp := tr.StartSpan("test", WithRecording(tracingpb.RecordingStructured))
		if verbose {
			sp = tr.StartSpan("test", WithRecording(tracingpb.RecordingVerbose))
		}
		ch := make(chan struct{})
		defer close(ch)
		go func() {
			for range ch {
				tr.SaveAutomaticSnapshot()
				<-ch // read again to unpark func.
			}
		}()

		blockingFunc1(ch)
		started := timeutil.Now()
		blockingFunc2(ch)
		blockingFunc3(ch)
		blockingCaller(ch)

		sp.MaybeRecordStackHistory(started)

		rec := sp.FinishAndGetConfiguredRecording()[0]
		require.Len(t, rec.StructuredRecords, 3)
		stacks := make([]string, 3)
		for i, rec := range rec.StructuredRecords {
			var stack tracingpb.CapturedStack
			require.NoError(t, types.UnmarshalAny(rec.Payload, &stack))
			stacks[i] = stack.Stack
		}
		require.Contains(t, stacks[0], "tracing.blockingFunc2")
		require.Contains(t, stacks[1], "tracing.blockingFunc3")
		require.Contains(t, stacks[2], "tracing.blockingCaller")

		if verbose {
			require.Len(t, rec.Logs, 3)
			for i := range rec.Logs {
				require.NotContains(t, rec.Logs[i].Message, "tracing.blockingFunc1")
			}
			require.Contains(t, rec.Logs[0].Message, "tracing.blockingFunc2")
			require.Contains(t, rec.Logs[1].Message, "tracing.blockingFunc3")
			require.Contains(t, rec.Logs[2].Message, "tracing.blockingCaller")
		}
	}
}
