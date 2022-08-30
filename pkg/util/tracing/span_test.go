// Copyright 2020 The Cockroach Authors.
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
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otelsdk "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/metadata"
)

func TestStartSpan(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeOnDemand))
	sp := tr.StartSpan("test")
	defer sp.Finish()
	require.Equal(t, "noop", sp.OperationName())

	sp2 := tr.StartSpan("test", WithRecording(tracingpb.RecordingStructured))
	defer sp2.Finish()
	require.Equal(t, "test", sp2.OperationName())
}

func TestRecordingString(t *testing.T) {
	tr := NewTracer()
	tr2 := NewTracer()

	root := tr.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))
	root.Record("root 1")
	{
		// Hackily fix the timing on the first log message, so that we can check it later.
		r := root.i.crdb.mu.recording.logs.GetFirst().(*tracingpb.LogRecord)
		r.Time = root.i.crdb.startTime.Add(time.Millisecond)
		root.i.crdb.mu.recording.logs.RemoveFirst()
		root.i.crdb.mu.recording.logs.AddFirst(r)
	}

	// Sleep a bit so that everything that comes afterwards has higher timestamps
	// than the one we just assigned. Otherwise the sorting will be screwed up.
	time.Sleep(10 * time.Millisecond)

	carrier := MetadataCarrier{MD: metadata.MD{}}
	tr.InjectMetaInto(root.Meta(), carrier)

	wireSpanMeta, err := tr2.ExtractMetaFrom(carrier)
	require.NoError(t, err)

	remoteChild := tr2.StartSpan("remote child", WithRemoteParentFromSpanMeta(wireSpanMeta), WithDetachedRecording())
	root.Record("root 2")
	remoteChild.Record("remote child 1")

	remoteRec := remoteChild.FinishAndGetRecording(tracingpb.RecordingVerbose)
	root.ImportRemoteRecording(remoteRec)

	root.Record("root 3")

	ch2 := tr.StartSpan("local child", WithParent(root))
	root.Record("root 4")
	ch2.Record("local child 1")
	ch2.Finish()

	root.Record("root 5")

	rec := root.FinishAndGetRecording(tracingpb.RecordingVerbose)
	// Sanity check that the recording looks like we want. Note that this is not
	// its String() representation; this just lists all the spans in order.
	require.NoError(t, CheckRecordedSpans(rec, `
		span: root
			tags: _verbose=1
			event: root 1
			event: root 2
			event: root 3
			event: root 4
			event: root 5
			span: remote child
				tags: _verbose=1
				event: remote child 1
			span: local child
				tags: _verbose=1
				event: local child 1
		`))

	require.NoError(t, CheckRecording(rec, `
		=== operation:root _verbose:1
		[remote child]
		[local child]
		event:root 1
			=== operation:remote child _verbose:1
			event:remote child 1
		event:root 2
		event:root 3
			=== operation:local child _verbose:1
			event:local child 1
		event:root 4
		event:root 5
		`))
	// Check the timing info on the first two lines.
	lines := strings.Split(rec.String(), "\n")
	l, err := parseLine(lines[0])
	require.NoError(t, err)
	require.Equal(t, traceLine{
		timeSinceTraceStart: "0.000ms",
		timeSincePrev:       "0.000ms",
		text:                "=== operation:root _verbose:1",
	}, l)
	l, err = parseLine(lines[3])
	require.Equal(t, traceLine{
		timeSinceTraceStart: "1.000ms",
		timeSincePrev:       "1.000ms",
		text:                "event:root 1",
	}, l)
	require.NoError(t, err)
}

type traceLine struct {
	timeSinceTraceStart, timeSincePrev string
	text                               string
}

func parseLine(s string) (traceLine, error) {
	// Parse lines like:
	//      0.007ms      0.007ms    event:root 1
	re := regexp.MustCompile(`\s*(.*s)\s*(.*s)\s{4}(.*)`)
	match := re.FindStringSubmatch(s)
	if match == nil {
		return traceLine{}, errors.Newf("line doesn't match: %s", s)
	}
	return traceLine{
		timeSinceTraceStart: match[1],
		timeSincePrev:       match[2],
		text:                match[3],
	}, nil
}

func TestRecordingInRecording(t *testing.T) {
	tr := NewTracer()

	root := tr.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))
	child := tr.StartSpan("child", WithParent(root), WithRecording(tracingpb.RecordingVerbose))
	// The remote grandchild is also recording, however since it's remote the spans
	// have to be imported into the parent manually (this would usually happen via
	// code at the RPC boundaries).
	grandChild := tr.StartSpan("grandchild", WithParent(child), WithDetachedRecording())
	child.ImportRemoteRecording(grandChild.FinishAndGetRecording(tracingpb.RecordingVerbose))
	childRec := child.FinishAndGetRecording(tracingpb.RecordingVerbose)
	require.NoError(t, CheckRecordedSpans(childRec, `
		span: child
			tags: _verbose=1
			span: grandchild
				tags: _verbose=1
		`))

	rootRec := root.FinishAndGetRecording(tracingpb.RecordingVerbose)
	require.NoError(t, CheckRecordedSpans(rootRec, `
		span: root
			tags: _verbose=1
			span: child
				tags: _verbose=1
				span: grandchild
					tags: _verbose=1
		`))

	require.NoError(t, CheckRecording(childRec, `
		=== operation:child _verbose:1
		[grandchild]
			=== operation:grandchild _verbose:1
		`))
}

// Verify that GetRecording propagates the structured events even when the
// receiving Span isn't verbose during import.
func TestImportRemoteRecording(t *testing.T) {
	for _, verbose := range []bool{false, true} {
		t.Run(fmt.Sprintf("%s=%t", "verbose-child=", verbose), func(t *testing.T) {
			tr := NewTracerWithOpt(context.Background())
			var opt SpanOption
			if verbose {
				opt = WithRecording(tracingpb.RecordingVerbose)
			} else {
				opt = WithRecording(tracingpb.RecordingStructured)
			}
			sp := tr.StartSpan("root", opt)
			ch := tr.StartSpan("child", WithParent(sp), WithDetachedRecording())
			ch.RecordStructured(&types.Int32Value{Value: 4})
			ch.Record("foo")
			sp.ImportRemoteRecording(ch.FinishAndGetRecording(tracingpb.RecordingVerbose))

			if verbose {
				require.NoError(t, CheckRecording(sp.FinishAndGetRecording(tracingpb.RecordingVerbose), `
				=== operation:root _verbose:1
				[child]
					=== operation:child _verbose:1
					event:&Int32Value{Value:4,XXX_unrecognized:[],}
					event:foo
					structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":4}
	`))
			} else {
				require.NoError(t, CheckRecording(sp.FinishAndGetRecording(tracingpb.RecordingStructured), `
				=== operation:root
				[child]
				structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":4}
	`))
			}
		})
	}
}

func TestImportRemoteRecordingMaintainsRightByteSize(t *testing.T) {
	tr1 := NewTracer()

	child := tr1.StartSpan("child", WithRecording(tracingpb.RecordingStructured))
	child.RecordStructured(&types.Int32Value{Value: 42})
	child.RecordStructured(&types.StringValue{Value: "test"})

	root := tr1.StartSpan("root", WithRecording(tracingpb.RecordingStructured))
	root.ImportRemoteRecording(child.GetRecording(tracingpb.RecordingStructured))
	c := root.i.crdb
	c.mu.Lock()
	buf := c.mu.recording.structured
	sz := 0
	for i := 0; i < buf.Len(); i++ {
		sz += buf.Get(i).(memorySizable).MemorySize()
	}
	c.mu.Unlock()
	require.NotZero(t, buf.bytesSize)
	require.Equal(t, buf.bytesSize, int64(sz))
}

func TestSpanRecordStructured(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingStructured))
	defer sp.Finish()

	sp.RecordStructured(&types.Int32Value{Value: 4})
	rec := sp.GetRecording(tracingpb.RecordingStructured)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].StructuredRecords, 1)
	item := rec[0].StructuredRecords[0]
	var d1 types.DynamicAny
	require.NoError(t, types.UnmarshalAny(item.Payload, &d1))
	require.IsType(t, (*types.Int32Value)(nil), d1.Message)

	require.NoError(t, CheckRecordedSpans(rec, `
		span: root
		`))
	require.NoError(t, CheckRecording(rec, `
		=== operation:root
        structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":4}
	`))
}

// TestSpanRecordStructuredLimit tests recording behavior when the size of
// structured data recorded into the span exceeds the configured limit.
func TestSpanRecordStructuredLimit(t *testing.T) {
	now := timeutil.Now()
	clock := timeutil.NewManualTime(now)
	tr := NewTracerWithOpt(context.Background(), WithTestingKnobs(TracerTestingKnobs{Clock: clock}))

	sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingStructured))
	defer sp.Finish()

	pad := func(i int) string { return fmt.Sprintf("%06d", i) }
	payload := func(i int) Structured { return &types.StringValue{Value: pad(i)} }
	anyPayload, err := types.MarshalAny(payload(42))
	require.NoError(t, err)
	structuredRecord := &tracingpb.StructuredRecord{
		Time:    now,
		Payload: anyPayload,
	}

	numStructuredRecordings := maxStructuredBytesPerSpan / structuredRecord.MemorySize()
	const extra = 10
	for i := 1; i <= numStructuredRecordings+extra; i++ {
		sp.RecordStructured(payload(i))
	}

	sp.SetRecordingType(tracingpb.RecordingVerbose)
	rec := sp.GetRecording(tracingpb.RecordingVerbose)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].StructuredRecords, numStructuredRecordings)
	val, ok := rec[0].FindTagGroup(tracingpb.AnonymousTagGroupName).FindTag("_dropped")
	require.True(t, ok)
	require.Equal(t, "1", val)

	first := rec[0].StructuredRecords[0]
	last := rec[0].StructuredRecords[len(rec[0].StructuredRecords)-1]
	var d1 types.DynamicAny
	require.NoError(t, types.UnmarshalAny(first.Payload, &d1))
	require.IsType(t, (*types.StringValue)(nil), d1.Message)

	var res string
	require.NoError(t, types.StdStringUnmarshal(&res, first.Payload.Value))
	require.Equal(t, pad(extra+1), res)

	var d2 types.DynamicAny
	require.NoError(t, types.UnmarshalAny(last.Payload, &d2))
	require.IsType(t, (*types.StringValue)(nil), d2.Message)
	require.NoError(t, types.StdStringUnmarshal(&res, last.Payload.Value))
	require.Equal(t, pad(numStructuredRecordings+extra), res)
}

// TestSpanRecordLimit tests recording behavior when the amount of data logged
// into the span exceeds the configured limit.
func TestSpanRecordLimit(t *testing.T) {
	// Logs include the timestamp, and we want to fix them so they're not
	// variably sized (needed for the test below).
	clock := &timeutil.ManualTime{}
	tr := NewTracerWithOpt(context.Background(), WithTestingKnobs(TracerTestingKnobs{Clock: clock}))

	msg := func(i int) string { return fmt.Sprintf("msg: %10d", i) }

	// Determine the size of a log record by actually recording once.
	logSize := func() int {
		sp := tr.StartSpan("dummy", WithRecording(tracingpb.RecordingVerbose))
		defer sp.Finish()
		sp.Recordf("%s", msg(42))
		return sp.GetRecording(tracingpb.RecordingVerbose)[0].Logs[0].MemorySize()
	}()

	sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()

	numLogs := maxLogBytesPerSpan / logSize
	const extra = 10
	for i := 1; i <= numLogs+extra; i++ {
		sp.Recordf("%s", msg(i))
	}

	rec := sp.GetRecording(tracingpb.RecordingVerbose)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].Logs, numLogs)
	val, ok := rec[0].FindTagGroup(tracingpb.AnonymousTagGroupName).FindTag("_dropped")
	require.True(t, ok)
	require.Equal(t, val, "1")

	first := rec[0].Logs[0]
	last := rec[0].Logs[len(rec[0].Logs)-1]

	require.Equal(t, first.Msg().StripMarkers(), msg(extra+1))
	require.Equal(t, last.Msg().StripMarkers(), msg(numLogs+extra))
}

func TestChildSpanRegisteredWithRecordingParent(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingStructured))
	defer sp.Finish()
	ch := tr.StartSpan("child", WithParent(sp))
	defer ch.Finish()
	children := sp.i.crdb.mu.openChildren
	require.Len(t, children, 1)
	require.Equal(t, ch.i.crdb, children[0].spanRef.i.crdb)
	ch.RecordStructured(&types.Int32Value{Value: 5})
	// Check that the child's structured event is in the recording.
	rec := sp.GetRecording(tracingpb.RecordingStructured)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].StructuredRecords, 1)
	require.Len(t, rec[0].ChildrenMetadata, 1)
}

// TestRecordingMaxSpans verifies that recordings don't grow over the limit.
func TestRecordingMaxSpans(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()
	extraChildren := 10
	numChildren := maxRecordedSpansPerTrace + extraChildren
	for i := 0; i < numChildren; i++ {
		child := tr.StartSpan(fmt.Sprintf("child %d", i), WithParent(sp))
		exp := i + 2
		// maxRecordedSpansPerTrace technically limits the number of child spans, so
		// we add one for the root.
		over := false
		if exp > maxRecordedSpansPerTrace+1 {
			exp = maxRecordedSpansPerTrace + 1
			over = true
		}
		if over {
			// Structured events from children that are not included in the recording
			// are still collected (subject to the structured recording bytes limit).
			child.RecordStructured(&types.Int32Value{Value: int32(i)})
		}
		child.Finish()
		require.Len(t, sp.GetRecording(tracingpb.RecordingVerbose), exp)
	}
	rec := sp.GetRecording(tracingpb.RecordingVerbose)
	root := rec[0]
	require.Len(t, root.StructuredRecords, extraChildren)
}

// TestRecordingDowngradesToStructuredIfTooBig finishes a span that has reached
// the maximum number of recorded spans and asserts that its structured
// recordings are correctly added to the parent.
func TestRecordingDowngradesToStructuredIfTooBig(t *testing.T) {
	now := timeutil.Now()
	clock := timeutil.NewManualTime(now)
	tr := NewTracerWithOpt(context.Background(), WithTestingKnobs(TracerTestingKnobs{Clock: clock}))

	s1 := tr.StartSpan("p", WithRecording(tracingpb.RecordingVerbose))
	s2 := tr.StartSpan("c", WithParent(s1))
	extraChildren := 10
	numChildren := maxRecordedSpansPerTrace + extraChildren
	payload := &types.Int32Value{Value: int32(1)}
	for i := 0; i < numChildren; i++ {
		child := tr.StartSpan(fmt.Sprintf("cc%d", i), WithParent(s2))
		child.RecordStructured(payload)
		child.Finish()
	}

	// We expect recordings from sp and up to the maximum number of spans and
	// structured records from all spans over the max.
	rec := s2.FinishAndGetConfiguredRecording()
	require.Len(t, rec, maxRecordedSpansPerTrace+1)
	require.Len(t, rec[0].StructuredRecords, extraChildren)

	pl, err := types.MarshalAny(payload)
	require.NoError(t, err)
	structuredRecordSize := (&tracingpb.StructuredRecord{Time: now, Payload: pl}).MemorySize()
	maxNumStructuredRecordings := maxStructuredBytesPerSpan / structuredRecordSize
	if maxNumStructuredRecordings > numChildren {
		maxNumStructuredRecordings = numChildren
	}

	// Since s2's child count exceeded the maximum, we don't expect to see any of
	// its span recordings in s1. But, we should only see as many of s2's
	// structured recordings as possible.
	rec2 := s1.FinishAndGetConfiguredRecording()
	require.Len(t, rec2, 1)
	require.Len(t, rec2[0].StructuredRecords, maxNumStructuredRecordings)
}

// Test that a RecordingStructured parent does not panic when asked to ingest a
// remote verbose recording. Ingesting a recording of different type is unusual,
// since children are created with the parent's recording mode, but it can
// happen if the child's recording mode was changed dynamically.
func TestRemoteSpanWithDifferentRecordingMode(t *testing.T) {
	tr := NewTracer()
	s1 := tr.StartSpan("p", WithRecording(tracingpb.RecordingStructured))
	s2 := tr.StartSpan("c", WithRemoteParentFromSpanMeta(s1.Meta()), WithRecording(tracingpb.RecordingVerbose))
	s3 := tr.StartSpan("cc", WithParent(s2), WithRecording(tracingpb.RecordingVerbose))
	s3.Finish()
	r := s2.FinishAndGetConfiguredRecording()
	require.NotPanics(t, func() { s1.ImportRemoteRecording(r) })
	r2 := s1.FinishAndGetConfiguredRecording()
	require.Len(t, r2, 1)
}

type explodyNetTr struct {
	trace.Trace
}

func (tr *explodyNetTr) Finish() {
	if tr.Trace == nil {
		panic("(*trace.Trace).Finish called twice")
	}
	tr.Trace.Finish()
	tr.Trace = nil
}

// TestSpan_UseAfterFinish finishes a Span multiple times and
// calls all of its methods multiple times as well. This is
// to check that `Span.detectUseAfterFinish` is called in the right places,
// and serves as a regression test for issues such as:
//
// https://github.com/cockroachdb/cockroach/issues/58489#issuecomment-781263005
func TestSpan_UseAfterFinish(t *testing.T) {
	// First, test with a Tracer configured to NOT panic on use-after-Finish.
	t.Run("production settings", func(t *testing.T) {
		tr := NewTracerWithOpt(context.Background(),
			WithTracingMode(TracingModeActiveSpansRegistry),
			// Mimic production settings, otherwise we crash on span-use-after-Finish in
			// tests.
			WithUseAfterFinishOpt(false /* panicOnUseAfterFinish */, false /* debugUseAfterFinish */))
		require.False(t, tr.PanicOnUseAfterFinish())
		tr._useNetTrace = 1
		sp := tr.StartSpan("foo")
		require.NotNil(t, sp.i.netTr)
		// Set up netTr to reliably explode if Finish'ed twice. We
		// expect `sp.Finish` to not let it come to that.
		sp.i.netTr = &explodyNetTr{Trace: sp.i.netTr}
		sp.Finish()
		require.True(t, sp.detectUseAfterFinish())
		sp.Finish()
		require.EqualValues(t, 1, atomic.LoadInt32(&sp.finished))

		netTrT := reflect.TypeOf(sp)
		for i := 0; i < netTrT.NumMethod(); i++ {
			f := netTrT.Method(i)
			t.Run(f.Name, func(t *testing.T) {
				// The receiver is the first argument.
				args := []reflect.Value{reflect.ValueOf(sp)}
				for i := 1; i < f.Type.NumIn(); i++ {
					// Zeroes for the rest. It would be nice to do something
					// like `quick.Check` here (or even just call quick.Check!)
					// but that's for another day. It should be doable!
					args = append(args, reflect.Zero(f.Type.In(i)))
				}
				// NB: on an impl of Span that calls through to `trace.Trace.Finish`, and
				// on my machine, and at the time of writing, `tr.Finish` would reliably
				// deadlock on exactly the 10th call. This motivates the choice of 20
				// below.
				for i := 0; i < 20; i++ {
					t.Run("invoke", func(t *testing.T) {
						if i == 9 {
							f.Func.Call(args)
						} else {
							f.Func.Call(args)
						}
					})
				}
			})
		}

		// Check that creating a child from a finished parent doesn't crash. The
		// "child" is expected to really be a root.
		var parentID tracingpb.SpanID
		require.NotPanics(t, func() {
			child := tr.StartSpan("child", WithParent(sp))
			parentID = child.i.crdb.parentSpanID
			child.Finish()
		})
		require.Zero(t, parentID)
	})

	// Second, test with a Tracer configured to panic on use-after-Finish.
	t.Run("crash settings", func(t *testing.T) {
		tr := NewTracerWithOpt(context.Background(),
			WithTracingMode(TracingModeActiveSpansRegistry),
			WithUseAfterFinishOpt(true /* panicOnUseAfterFinish */, true /* debugUseAfterFinish */))
		require.True(t, tr.PanicOnUseAfterFinish())
		sp := tr.StartSpan("foo")
		sp.Finish()
		require.Panics(t, func() {
			sp.Record("boom")
		})
		require.Panics(t, func() {
			sp.GetRecording(tracingpb.RecordingStructured)
		})
		require.Panics(t, func() {
			sp.Finish()
		})
		// Check that creating a child from a finished parent crashes.
		require.Panics(t, func() {
			tr.StartSpan("child", WithParent(sp))
		})
	})
}

type countingStringer int32

func (i *countingStringer) String() string {
	*i++ // not for concurrent use
	return fmt.Sprint(*i)
}

type testExpandingTag struct{}

var _ LazyTag = testExpandingTag{}

func (t testExpandingTag) Render() []attribute.KeyValue {
	return []attribute.KeyValue{
		{
			Key:   "exp1",
			Value: attribute.IntValue(1),
		},
		{
			Key:   "exp2",
			Value: attribute.IntValue(2),
		},
	}
}

type testStringerLazyTag struct{}

func (t testStringerLazyTag) String() string {
	return "lazy stringer"
}

func TestSpanTags(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()
	sp.SetTag("tag", attribute.IntValue(42))
	sp.SetLazyTag("lazy expanding tag", testExpandingTag{})
	sp.SetLazyTag("lazy tag", testStringerLazyTag{})

	tag, ok := sp.GetLazyTag("lazy expanding tag")
	require.True(t, ok)
	require.IsType(t, testExpandingTag{}, tag)

	rec := sp.GetRecording(tracingpb.RecordingVerbose)

	anonTagGroup := rec[0].FindTagGroup(tracingpb.AnonymousTagGroupName)
	_, ok = anonTagGroup.FindTag("tag")
	require.True(t, ok)

	val, ok := anonTagGroup.FindTag("lazy tag")
	require.True(t, ok)
	require.Equal(t, "lazy stringer", val)

	lazyExpandingTagGroup := rec[0].FindTagGroup("lazy expanding tag")
	require.NotNil(t, lazyExpandingTagGroup)

	val, ok = lazyExpandingTagGroup.FindTag("exp1")
	require.True(t, ok)
	require.Equal(t, "1", val)

	val, ok = lazyExpandingTagGroup.FindTag("exp2")
	require.True(t, ok)
	require.Equal(t, "2", val)
}

// TestSpanTagsInRecordings verifies that tags added before a recording started
// are part of the recording.
func TestSpanTagsInRecordings(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))
	var counter countingStringer
	logTags := logtags.SingleTagBuffer("foo", "tagbar")
	logTags = logTags.Add("foo1", &counter)
	sp := tr.StartSpan("root",
		WithLogTags(logTags),
	)
	defer sp.Finish()

	require.False(t, sp.IsVerbose())
	sp.SetTag("foo2", attribute.StringValue("bar2"))
	sp.Record("dummy recording")
	rec := sp.GetRecording(tracingpb.RecordingStructured)
	require.Nil(t, rec)
	// We didn't stringify the log tag.
	require.Zero(t, int(counter))

	sp.SetRecordingType(tracingpb.RecordingVerbose)
	rec = sp.GetRecording(tracingpb.RecordingVerbose)
	require.Len(t, rec, 1)

	require.Len(t, rec[0].TagGroups, 1)
	anonTagGroup := rec[0].FindTagGroup(tracingpb.AnonymousTagGroupName)
	require.Len(t, anonTagGroup.Tags, 5) // foo:tagbar foo1:1 foor2:bar2 _unfinished:1 _verbose:1

	_, ok := anonTagGroup.FindTag("foo")
	require.True(t, ok)

	_, ok = anonTagGroup.FindTag("foo2")
	require.True(t, ok)

	require.Equal(t, 1, int(counter))

	// Verify that subsequent tags are also captured.
	sp.SetTag("foo3", attribute.StringValue("bar3"))
	rec = sp.GetRecording(tracingpb.RecordingVerbose)
	require.Len(t, rec, 1)

	require.Len(t, rec[0].TagGroups, 1)
	anonTagGroup = rec[0].FindTagGroup(tracingpb.AnonymousTagGroupName)
	require.Len(t, anonTagGroup.Tags, 6)

	_, ok = anonTagGroup.FindTag("foo3")
	require.True(t, ok)

	require.Equal(t, 2, int(counter))
}

// Check that recordings have the "_verbose" marker tag only while the
// respective span is recording verbosely.
func TestVerboseTag(t *testing.T) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))
	sp := tr.StartSpan("root")
	defer sp.Finish()

	sp.SetRecordingType(tracingpb.RecordingStructured)
	rec := sp.GetRecording(tracingpb.RecordingVerbose)
	anonTagGroup := rec[0].FindTagGroup(tracingpb.AnonymousTagGroupName)
	ok := anonTagGroup != nil
	if ok {
		_, ok = anonTagGroup.FindTag("_verbose")
	}
	require.False(t, ok)

	// The tag is present while the span is recording verbosely.
	sp.SetRecordingType(tracingpb.RecordingVerbose)
	rec = sp.GetRecording(tracingpb.RecordingVerbose)

	_, ok = rec[0].FindTagGroup(tracingpb.AnonymousTagGroupName).FindTag("_verbose")
	require.True(t, ok)

	// After we stop recording, the tag goes away.
	sp.SetRecordingType(tracingpb.RecordingStructured)
	rec = sp.GetRecording(tracingpb.RecordingVerbose)
	anonTagGroup = rec[0].FindTagGroup(tracingpb.AnonymousTagGroupName)
	ok = anonTagGroup != nil
	if ok {
		_, ok = anonTagGroup.FindTag("_verbose")
	}
	require.False(t, ok)
}

func TestStructureRecording(t *testing.T) {
	for _, finishCh1 := range []bool{true, false} {
		t.Run(fmt.Sprintf("finish1=%t", finishCh1), func(t *testing.T) {
			for _, finishCh2 := range []bool{true, false} {
				t.Run(fmt.Sprintf("finish2=%t", finishCh2), func(t *testing.T) {
					tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))
					sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingStructured))
					ch1 := tr.StartSpan("child", WithParent(sp))
					ch2 := tr.StartSpan("grandchild", WithParent(ch1))
					for i := int32(0); i < 5; i++ {
						sp.RecordStructured(&types.Int32Value{Value: i})
						ch1.RecordStructured(&types.Int32Value{Value: i})
						ch2.RecordStructured(&types.Int32Value{Value: i})
					}
					if finishCh2 {
						ch2.Finish()
					}
					if finishCh1 {
						ch1.Finish()
					}
					rec := sp.GetRecording(tracingpb.RecordingStructured)
					require.Len(t, rec, 1)
					require.Len(t, rec[0].StructuredRecords, 15)

					sp.Finish()
					if !finishCh1 {
						ch1.Finish()
					}
					if !finishCh2 {
						ch2.Finish()
					}
				})
			}
		})
	}
}

// TestVerboseRecordingFinishedChildrenDurations tests that the VerboseRecording
// of a span includes the `operation : duration` mapping of all children
// (finished and open) in that recording.
func TestVerboseRecordingFinishedChildrenDurations(t *testing.T) {
	checkChildrenMetadata := func(
		actualOperations map[string]tracingpb.OperationMetadata,
		expectedOperations map[string]tracingpb.OperationMetadata) {
		t.Helper()
		require.Len(t, actualOperations, len(expectedOperations))
		for opName, actualMetadata := range actualOperations {
			expectedMetadata, ok := expectedOperations[opName]
			require.True(t, ok)
			require.Greater(t, actualMetadata.Duration, time.Duration(0))
			require.Equal(t, expectedMetadata.Count, actualMetadata.Count)
			require.Equal(t, expectedMetadata.ContainsUnfinished, actualMetadata.ContainsUnfinished)
		}
	}
	tr := NewTracer()
	root := tr.StartSpan("root", WithRecording(tracingpb.RecordingVerbose))
	child := tr.StartSpan("child", WithParent(root))
	gc := tr.StartSpan("grandchild", WithParent(child))
	child2 := tr.StartSpan("child2", WithParent(root))

	// grandchild has no children, and so the recording should contain no
	// metadata.
	rec := gc.FinishAndGetConfiguredRecording()
	require.Len(t, rec, 1)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{})

	rec = child.GetConfiguredRecording()
	require.Len(t, rec, 2)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{
		"grandchild": {Count: 1},
	})

	// Now, let's Finish() child2, but leave child as an open child of root.
	child2.Finish()

	// Root should have entries for Finish()ed `child2`, open child `child`, and
	// its Finish()ed `grandchild`.
	rec = root.GetConfiguredRecording()
	require.Len(t, rec, 4)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{
		"child2":     {Count: 1},
		"child":      {Count: 1, ContainsUnfinished: true},
		"grandchild": {Count: 1},
	})

	// Create another span with the same opName.
	childWithSameOpName := tr.StartSpan("child", WithParent(root))
	childWithSameOpName.Finish()
	rec = root.GetConfiguredRecording()
	require.Len(t, rec, 5)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{
		"child2":     {Count: 1},
		"child":      {Count: 2, ContainsUnfinished: true},
		"grandchild": {Count: 1},
	})

	// Finish child, and re-check root's recording.
	child.Finish()
	rec = root.FinishAndGetConfiguredRecording()
	require.Len(t, rec, 5)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{
		"child2":     {Count: 1},
		"child":      {Count: 2},
		"grandchild": {Count: 1},
	})
}

// TestStructuredRecordingFinishedChildrenDurations tests that the
// StructuredRecording of a span includes the `operation : duration` mapping of
// all children (finished and open) in that recording.
func TestStructuredRecordingFinishedChildrenDurations(t *testing.T) {
	checkChildrenMetadata := func(
		actualOperations map[string]tracingpb.OperationMetadata,
		expectedOperations map[string]tracingpb.OperationMetadata) {
		t.Helper()
		require.Len(t, actualOperations, len(expectedOperations))
		for opName, actualMetadata := range actualOperations {
			expectedMetadata, ok := expectedOperations[opName]
			require.True(t, ok)
			require.Greater(t, actualMetadata.Duration, time.Duration(0))
			require.Equal(t, expectedMetadata.Count, actualMetadata.Count)
			require.Equal(t, expectedMetadata.ContainsUnfinished, actualMetadata.ContainsUnfinished)
		}
	}
	tr := NewTracer()
	root := tr.StartSpan("root", WithRecording(tracingpb.RecordingStructured))
	child := tr.StartSpan("child", WithParent(root))
	gc := tr.StartSpan("grandchild", WithParent(child))
	child2 := tr.StartSpan("child2", WithParent(root))

	// grandchild has no children, and so the recording should contain no
	// metadata.
	rec := gc.FinishAndGetConfiguredRecording()
	require.Len(t, rec, 1)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{})

	// child has no StructuredEvents, but it should return information about its
	// children.
	rec = child.GetConfiguredRecording()
	require.Len(t, rec, 1)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{
		"grandchild": {Count: 1},
	})

	// Now, let's Finish() child2, but leave child as an open child of root.
	child2.Finish()

	// Root should have entries for Finish()ed `child2`, open child `child`, and
	// its Finish()ed `grandchild`.
	rec = root.GetConfiguredRecording()
	require.Len(t, rec, 1)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{
		"child2":     {Count: 1},
		"child":      {Count: 1, ContainsUnfinished: true},
		"grandchild": {Count: 1},
	})

	// Create another span with the same opName.
	childWithSameOpName := tr.StartSpan("child", WithParent(root))
	childWithSameOpName.Finish()
	rec = root.GetConfiguredRecording()
	require.Len(t, rec, 1)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{
		"child2":     {Count: 1},
		"child":      {Count: 2, ContainsUnfinished: true},
		"grandchild": {Count: 1},
	})

	// Finish child, and re-check root's recording.
	child.Finish()
	rec = root.FinishAndGetConfiguredRecording()
	require.Len(t, rec, 1)
	checkChildrenMetadata(rec[0].ChildrenMetadata, map[string]tracingpb.OperationMetadata{
		"child2":     {Count: 1},
		"child":      {Count: 2},
		"grandchild": {Count: 1},
	})
}

// Test that a child span that's still open at the time when
// parent.FinishAndGetRecording() is called is included in the parent's
// recording.
func TestOpenChildIncludedRecording(t *testing.T) {
	tr := NewTracerWithOpt(context.Background())
	parent := tr.StartSpan("parent", WithRecording(tracingpb.RecordingVerbose))
	child := tr.StartSpan("child", WithParent(parent))
	rec := parent.FinishAndGetRecording(tracingpb.RecordingVerbose)
	require.NoError(t, CheckRecording(rec, `
		=== operation:parent _verbose:1
		[child]
			=== operation:child _unfinished:1 _verbose:1
	`))
	child.Finish()
}

func TestWithRemoteParentFromTraceInfo(t *testing.T) {
	traceID := tracingpb.TraceID(1)
	parentSpanID := tracingpb.SpanID(2)
	otelTraceID := [16]byte{1, 2, 3, 4, 5}
	otelSpanID := [8]byte{6, 7, 8, 9, 10}
	ti := tracingpb.TraceInfo{
		TraceID:       traceID,
		ParentSpanID:  parentSpanID,
		RecordingMode: tracingpb.RecordingMode_STRUCTURED,
		Otel: &tracingpb.TraceInfo_OtelInfo{
			TraceID: otelTraceID[:],
			SpanID:  otelSpanID[:],
		},
	}

	tr := NewTracer()
	tr.SetOpenTelemetryTracer(otelsdk.NewTracerProvider().Tracer("test"))

	sp := tr.StartSpan("test", WithRemoteParentFromTraceInfo(&ti))
	defer sp.Finish()

	require.Equal(t, traceID, sp.TraceID())
	require.Equal(t, parentSpanID, sp.i.crdb.parentSpanID)
	require.Equal(t, tracingpb.RecordingStructured, sp.RecordingType())
	require.NotNil(t, sp.i.otelSpan)
	otelCtx := sp.i.otelSpan.SpanContext()
	require.Equal(t, oteltrace.TraceID(otelTraceID), otelCtx.TraceID())
}

type mockEventListener struct {
	eventsSeen int
	notifyImpl func()
}

func (f *mockEventListener) Notify(_ Structured) {
	f.eventsSeen++
	if f.notifyImpl != nil {
		f.notifyImpl()
	}
}

var _ EventListener = &mockEventListener{}

func TestEventListener(t *testing.T) {
	tr := NewTracer()
	rootEventListener := &mockEventListener{}
	sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingStructured),
		WithEventListeners(rootEventListener))

	// Record a few Structured events.
	sp.RecordStructured(&types.Int32Value{Value: 4})
	sp.RecordStructured(&types.Int32Value{Value: 5})
	require.Equal(t, 2, rootEventListener.eventsSeen)

	// Register another event listener on only the child span.
	childEventListener := &mockEventListener{}
	childSp := tr.StartSpan("child", WithParent(sp),
		WithEventListeners(childEventListener))

	childSp.RecordStructured(&types.Int32Value{Value: 6})
	childSp.RecordStructured(&types.Int32Value{Value: 7})
	require.Equal(t, 4, rootEventListener.eventsSeen)
	require.Equal(t, 2, childEventListener.eventsSeen)

	// Record an event on the root span, and make sure we don't see it on the
	// listener registered with the child span.
	sp.RecordStructured(&types.Int32Value{Value: 8})
	require.Equal(t, 5, rootEventListener.eventsSeen)
	require.Equal(t, 2, childEventListener.eventsSeen)

	// Finish the child span, and ensure the Structured events aren't re-seen by
	// the listener when the child deposits them with the parent.
	childSp.Finish()
	require.Equal(t, 5, rootEventListener.eventsSeen)

	// Create a remote child, and the root listener should not be inherited.
	remoteSp := tr.StartSpan("remote-child", WithRemoteParentFromSpanMeta(sp.Meta()))
	remoteSp.RecordStructured(&types.Int32Value{Value: 9})
	require.Equal(t, 5, rootEventListener.eventsSeen)

	// But, when we import the recording in the root span, the root listener
	// should see these events.
	sp.ImportRemoteRecording(remoteSp.FinishAndGetConfiguredRecording())
	require.Equal(t, 6, rootEventListener.eventsSeen)

	// Create another child.
	childSp2 := tr.StartSpan("child2", WithParent(sp))
	childSp2.RecordStructured(&types.Int32Value{Value: 10})
	require.Equal(t, 7, rootEventListener.eventsSeen)

	// Now Finish() the parent before the child and ensure that the root event
	// listener does not see events from the child once the parent has been
	// Finish()ed.
	sp.Finish()
	childSp2.RecordStructured(&types.Int32Value{Value: 11})
	require.Equal(t, 7, rootEventListener.eventsSeen)
	childSp2.Finish()
}

func TestEventListenerNotifiedWithoutHoldingSpanMutex(t *testing.T) {
	tr := NewTracer()
	rootEventListener := &mockEventListener{}
	sp := tr.StartSpan("root", WithRecording(tracingpb.RecordingStructured),
		WithEventListeners(rootEventListener))
	defer sp.Finish()

	// Set the EventListeners Notify() method to acquire the span's mutex.
	rootEventListener.notifyImpl = func() {
		sp.i.crdb.mu.Lock()
		defer sp.i.crdb.mu.Unlock()
	}

	// Record a StructuredEvent, if Notify() were called holding the span's mutex
	// this would deadlock.
	sp.RecordStructured(&types.Int32Value{Value: 5})
	require.Equal(t, 1, rootEventListener.eventsSeen)
}

// TestFinishedChildrenMetadata tests that on Finish() the parent span's
// `childrenMetadata` map captures all the children in the recording rooted at
// the finished span.
func TestFinishedChildrenMetadata(t *testing.T) {
	checkChildrenMetadata := func(
		sp *Span, expectedOperations map[string]tracingpb.OperationMetadata) {
		t.Helper()
		c := sp.i.crdb
		c.mu.Lock()
		defer c.mu.Unlock()
		require.Len(t, c.mu.recording.childrenMetadata, len(expectedOperations))
		for opName, actualMetadata := range c.mu.recording.childrenMetadata {
			expectedMetadata, ok := expectedOperations[opName]
			require.True(t, ok)
			require.Greater(t, actualMetadata.Duration, time.Duration(0))
			require.Equal(t, expectedMetadata.Count, actualMetadata.Count)
			require.Equal(t, expectedMetadata.ContainsUnfinished, actualMetadata.ContainsUnfinished)
		}
	}

	fn := func(recordingMode tracingpb.RecordingType) {
		tr := NewTracer()
		root := tr.StartSpan("root", WithRecording(recordingMode))
		child := tr.StartSpan("child", WithParent(root))

		gc1 := tr.StartSpan("grandchild1", WithParent(child))
		ggc1 := tr.StartSpan("greatgrandchild1", WithParent(gc1))

		gc2 := tr.StartSpan("grandchild2", WithParent(child))
		ggc2 := tr.StartSpan("greatgrandchild2", WithParent(gc2))

		// First let's Finish() the lowest children. This should mean that their
		// parents have entries for their duration.
		ggc1.Finish()
		checkChildrenMetadata(gc1, map[string]tracingpb.OperationMetadata{
			"greatgrandchild1": {Count: 1},
		})

		ggc2.Finish()
		checkChildrenMetadata(gc2, map[string]tracingpb.OperationMetadata{
			"greatgrandchild2": {Count: 1},
		})

		// Finish() one of the grand children.
		gc1.Finish()
		checkChildrenMetadata(child, map[string]tracingpb.OperationMetadata{
			"grandchild1":      {Count: 1},
			"greatgrandchild1": {Count: 1},
		})

		// Now Finish() `child` since it has both finished and open children at this
		// point. We expect to see metadata of all children (finished + open) rooted
		// in childs' recording to be copied into root.
		child.Finish()
		checkChildrenMetadata(root, map[string]tracingpb.OperationMetadata{
			"child":            {Count: 1},
			"grandchild1":      {Count: 1},
			"greatgrandchild1": {Count: 1},
			"grandchild2":      {Count: 1, ContainsUnfinished: true},
			"greatgrandchild2": {Count: 1},
		})

		// gc2 should now be a root span in the registry since it was open when
		// `child` was finished. Finishing it should have no effect on roots'
		// recorded metadata.
		gc2.Finish()
		checkChildrenMetadata(root, map[string]tracingpb.OperationMetadata{
			"child":            {Count: 1},
			"grandchild1":      {Count: 1},
			"greatgrandchild1": {Count: 1},
			"grandchild2":      {Count: 1, ContainsUnfinished: true},
			"greatgrandchild2": {Count: 1},
		})
		root.Finish()
	}

	t.Run("verbose-recording", func(t *testing.T) {
		fn(tracingpb.RecordingVerbose)
	})

	t.Run("structured-recording", func(t *testing.T) {
		fn(tracingpb.RecordingStructured)
	})
}
