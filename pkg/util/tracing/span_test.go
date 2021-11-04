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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/metadata"
)

func TestRecordingString(t *testing.T) {
	tr := NewTracer()
	tr2 := NewTracer()

	root := tr.StartSpan("root", WithForceRealSpan())
	root.SetVerbose(true)
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

	carrier := metadataCarrier{MD: metadata.MD{}}
	tr.InjectMetaInto(root.Meta(), carrier)

	wireSpanMeta, err := tr2.ExtractMetaFrom(carrier)
	require.NoError(t, err)

	remoteChild := tr2.StartSpan("remote child", WithParentAndManualCollection(wireSpanMeta))
	root.Record("root 2")
	remoteChild.Record("remote child 1")
	remoteChild.Finish()

	remoteRec := remoteChild.GetRecording(RecordingVerbose)
	root.ImportRemoteSpans(remoteRec)

	root.Record("root 3")

	ch2 := tr.StartSpan("local child", WithParentAndAutoCollection(root))
	root.Record("root 4")
	ch2.Record("local child 1")
	ch2.Finish()

	root.Record("root 5")
	root.Finish()

	rec := root.GetRecording(RecordingVerbose)
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
	l, err = parseLine(lines[1])
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

	root := tr.StartSpan("root", WithForceRealSpan())
	root.SetVerbose(true)
	child := tr.StartSpan("child", WithParentAndAutoCollection(root), WithForceRealSpan())
	child.SetVerbose(true)
	// The remote grandchild is also recording, however since it's remote the spans
	// have to be imported into the parent manually (this would usually happen via
	// code at the RPC boundaries).
	grandChild := tr.StartSpan("grandchild", WithParentAndManualCollection(child.Meta()))
	grandChild.Finish()
	child.ImportRemoteSpans(grandChild.GetRecording(RecordingVerbose))
	child.Finish()
	root.Finish()

	rootRec := root.GetRecording(RecordingVerbose)
	require.NoError(t, CheckRecordedSpans(rootRec, `
		span: root
			tags: _verbose=1
			span: child
				tags: _verbose=1
				span: grandchild
					tags: _verbose=1
		`))

	childRec := child.GetRecording(RecordingVerbose)
	require.NoError(t, CheckRecordedSpans(childRec, `
		span: child
			tags: _verbose=1
			span: grandchild
				tags: _verbose=1
		`))

	require.NoError(t, CheckRecording(childRec, `
		=== operation:child _verbose:1
			=== operation:grandchild _verbose:1
		`))
}

// Verify that GetRecording propagates the structured events even when the
// receiving Span isn't verbose during import.
func TestImportRemoteSpans(t *testing.T) {
	for _, verbose := range []bool{false, true} {
		t.Run(fmt.Sprintf("%s=%t", "verbose-child=", verbose), func(t *testing.T) {
			tr := NewTracerWithOpt(context.Background(), WithTestingKnobs(TracerTestingKnobs{ForceRealSpans: true}))
			sp := tr.StartSpan("root")
			ch := tr.StartSpan("child", WithParentAndManualCollection(sp.Meta()))
			ch.RecordStructured(&types.Int32Value{Value: 4})
			if verbose {
				sp.SetVerbose(true)
				ch.SetVerbose(true)
			}
			ch.Record("foo")
			ch.SetVerbose(false)
			ch.Finish()
			sp.ImportRemoteSpans(ch.GetRecording(RecordingVerbose))
			sp.Finish()

			if verbose {
				require.NoError(t, CheckRecording(sp.GetRecording(RecordingVerbose), `
				=== operation:root _verbose:1
					=== operation:child _verbose:1
					event:foo
					structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":4}
	`))
			} else {
				require.NoError(t, CheckRecording(sp.GetRecording(RecordingStructured), `
				=== operation:root
				structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":4}
	`))
			}
		})
	}
}

func TestSpanRecordStructured(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("root", WithForceRealSpan())
	defer sp.Finish()

	sp.RecordStructured(&types.Int32Value{Value: 4})
	rec := sp.GetRecording(RecordingStructured)
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

	sp := tr.StartSpan("root", WithForceRealSpan())
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

	sp.SetVerbose(true)
	rec := sp.GetRecording(RecordingVerbose)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].StructuredRecords, numStructuredRecordings)
	require.Equal(t, "1", rec[0].Tags["_dropped"])

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

	sp := tr.StartSpan("root", WithForceRealSpan())
	defer sp.Finish()
	sp.SetVerbose(true)

	msg := func(i int) string { return fmt.Sprintf("msg: %10d", i) }

	// Determine the size of a log record by actually recording once.
	sp.Recordf("%s", msg(42))
	logSize := sp.GetRecording(RecordingVerbose)[0].Logs[0].MemorySize()
	sp.ResetRecording()

	numLogs := maxLogBytesPerSpan / logSize
	const extra = 10
	for i := 1; i <= numLogs+extra; i++ {
		sp.Recordf("%s", msg(i))
	}

	rec := sp.GetRecording(RecordingVerbose)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].Logs, numLogs)
	require.Equal(t, rec[0].Tags["_dropped"], "1")

	first := rec[0].Logs[0]
	last := rec[0].Logs[len(rec[0].Logs)-1]

	require.Equal(t, first.Msg().StripMarkers(), msg(extra+1))
	require.Equal(t, last.Msg().StripMarkers(), msg(numLogs+extra))
}

// testStructuredImpl is a testing implementation of Structured event.
type testStructuredImpl struct {
	*types.Int32Value
}

var _ Structured = &testStructuredImpl{}

func (t *testStructuredImpl) String() string {
	return fmt.Sprintf("structured=%d", t.Value)
}

func newTestStructured(i int) *testStructuredImpl {
	return &testStructuredImpl{
		&types.Int32Value{Value: int32(i)},
	}
}

// TestSpanReset checks that resetting a span clears out existing recordings.
func TestSpanReset(t *testing.T) {
	// Logs include the timestamp, and we want to fix them so they're not
	// variably sized (needed for the test below).
	clock := &timeutil.ManualTime{}
	tr := NewTracerWithOpt(context.Background(), WithTestingKnobs(TracerTestingKnobs{Clock: clock}))

	sp := tr.StartSpan("root", WithForceRealSpan())
	defer sp.Finish()
	sp.SetVerbose(true)

	for i := 1; i <= 10; i++ {
		if i%2 == 0 {
			sp.RecordStructured(newTestStructured(i))
		} else {
			sp.Recordf("%d", i)
		}
	}

	require.NoError(t, CheckRecordedSpans(sp.GetRecording(RecordingVerbose), `
		span: root
			tags: _unfinished=1 _verbose=1
			event: 1
			event: structured=2
			event: 3
			event: structured=4
			event: 5
			event: structured=6
			event: 7
			event: structured=8
			event: 9
			event: structured=10
		`))
	require.NoError(t, CheckRecording(sp.GetRecording(RecordingVerbose), `
		=== operation:root _unfinished:1 _verbose:1
		event:1
		event:structured=2
		event:3
		event:structured=4
		event:5
		event:structured=6
		event:7
		event:structured=8
		event:9
		event:structured=10
		structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":2}
		structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":4}
		structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":6}
		structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":8}
		structured:{"@type":"type.googleapis.com/google.protobuf.Int32Value","value":10}
	`))

	sp.ResetRecording()

	require.NoError(t, CheckRecordedSpans(sp.GetRecording(RecordingVerbose), `
		span: root
			tags: _unfinished=1 _verbose=1
		`))
	require.NoError(t, CheckRecording(sp.GetRecording(RecordingVerbose), `
		=== operation:root _unfinished:1 _verbose:1
	`))

	msg := func(i int) string { return fmt.Sprintf("msg: %010d", i) }
	sp.Record(msg(42))
	logSize := sp.GetRecording(RecordingVerbose)[0].Logs[0].MemorySize()
	numLogs := maxLogBytesPerSpan / logSize
	const extra = 10

	for i := 1; i <= numLogs+extra; i++ {
		sp.Record(msg(i))
	}

	require.Equal(t, sp.GetRecording(RecordingVerbose)[0].Tags["_dropped"], "1")
	sp.ResetRecording()
	_, found := sp.GetRecording(RecordingVerbose)[0].Tags["_dropped"]
	require.False(t, found)
}

func TestNonVerboseChildSpanRegisteredWithParent(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("root", WithForceRealSpan())
	defer sp.Finish()
	ch := tr.StartSpan("child", WithParentAndAutoCollection(sp))
	defer ch.Finish()
	children := sp.i.crdb.mu.recording.openChildren
	require.Len(t, children, 1)
	require.Equal(t, ch.i.crdb, children[0])
	ch.RecordStructured(&types.Int32Value{Value: 5})
	// Check that the child's structured event is in the recording.
	rec := sp.GetRecording(RecordingStructured)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].StructuredRecords, 1)
}

// TestSpanMaxChildren verifies that a Span can
// track at most maxChildrenPerSpan direct children.
func TestSpanMaxChildren(t *testing.T) {
	tr := NewTracer()
	sp := tr.StartSpan("root", WithForceRealSpan())
	defer sp.Finish()
	for i := 0; i < maxChildrenPerSpan+123; i++ {
		tr.StartSpan(fmt.Sprintf("child %d", i), WithParentAndAutoCollection(sp), WithForceRealSpan())
		exp := i + 1
		if exp > maxChildrenPerSpan {
			exp = maxChildrenPerSpan
		}
		require.Len(t, sp.i.crdb.mu.recording.openChildren, exp)
	}
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
// to check that `Span.done` is called in the right places,
// and serves as a regression test for issues such as:
//
// https://github.com/cockroachdb/cockroach/issues/58489#issuecomment-781263005
func TestSpan_UseAfterFinish(t *testing.T) {
	tr := NewTracer()
	tr._useNetTrace = 1
	sp := tr.StartSpan("foo", WithForceRealSpan())
	require.NotNil(t, sp.i.netTr)
	// Set up netTr to reliably explode if Finish'ed twice. We
	// expect `sp.Finish` to not let it come to that.
	sp.i.netTr = &explodyNetTr{Trace: sp.i.netTr}
	sp.Finish()
	require.True(t, sp.done())
	sp.Finish()
	require.EqualValues(t, 2, sp.numFinishCalled)

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
}

type countingStringer int32

func (i *countingStringer) String() string {
	*i++ // not for concurrent use
	return fmt.Sprint(*i)
}

// TestSpanTagsInRecordings verifies that tags added before a recording started
// are part of the recording.
func TestSpanTagsInRecordings(t *testing.T) {
	tr := NewTracer()
	var counter countingStringer
	logTags := logtags.SingleTagBuffer("foo", "tagbar")
	logTags = logTags.Add("foo1", &counter)
	sp := tr.StartSpan("root",
		WithForceRealSpan(),
		WithLogTags(logTags),
	)
	defer sp.Finish()

	require.False(t, sp.IsVerbose())
	sp.SetTag("foo2", attribute.StringValue("bar2"))
	sp.Record("dummy recording")
	rec := sp.GetRecording(RecordingStructured)
	require.Nil(t, rec)
	// We didn't stringify the log tag.
	require.Zero(t, int(counter))

	sp.SetVerbose(true)
	rec = sp.GetRecording(RecordingVerbose)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].Tags, 5) // _unfinished:1 _verbose:1 foo:tagbar foo1:1 foor2:bar2
	_, ok := rec[0].Tags["foo"]
	require.True(t, ok)
	_, ok = rec[0].Tags["foo2"]
	require.True(t, ok)
	require.Equal(t, 1, int(counter))

	// Verify that subsequent tags are also captured.
	sp.SetTag("foo3", attribute.StringValue("bar3"))
	rec = sp.GetRecording(RecordingVerbose)
	require.Len(t, rec, 1)
	require.Len(t, rec[0].Tags, 6)
	_, ok = rec[0].Tags["foo3"]
	require.True(t, ok)
	require.Equal(t, 2, int(counter))
}

func TestStructureRecording(t *testing.T) {
	for _, finishCh1 := range []bool{true, false} {
		t.Run(fmt.Sprintf("finish1=%t", finishCh1), func(t *testing.T) {
			for _, finishCh2 := range []bool{true, false} {
				t.Run(fmt.Sprintf("finish2=%t", finishCh2), func(t *testing.T) {
					tr := NewTracerWithOpt(context.Background(), WithTestingKnobs(TracerTestingKnobs{ForceRealSpans: true}))
					sp := tr.StartSpan("root")
					ch1 := tr.StartSpan("child", WithParentAndAutoCollection(sp))
					ch2 := tr.StartSpan("grandchild", WithParentAndAutoCollection(ch1))
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
					rec := sp.GetRecording(RecordingStructured)
					require.Len(t, rec, 1)
					require.Len(t, rec[0].StructuredRecords, 15)
				})
			}
		})
	}
}
