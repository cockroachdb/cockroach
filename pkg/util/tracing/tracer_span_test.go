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
	"math/rand"
	"regexp"
	"strconv"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMaxChildSpansPerSpan verifies that the maxChildSpansPerSpan limit is
// enforced for both local and remote spans.
func TestMaxChildSpansPerSpan(t *testing.T) {
	// Randomize the maximum number of children. Whatever its value, it should not
	// be exceeded.
	const maxMaxChildSpans = 1000
	initTracer := func() (_ *Tracer, maxChildSpans int64, _ *settings.Values) {
		maxChildSpans = int64(rand.Intn(maxMaxChildSpans))
		var values settings.Values
		values.Init(nil)
		maxChildSpansPerSpan.Override(&values, maxChildSpans)

		tr := NewTracer()
		tr.Configure(&values)
		return tr, maxChildSpans, &values
	}
	logToSpanAndFinish := func(sp opentracing.Span, msg string) {
		sp.LogFields(otlog.String(LogMessageField, msg))
		sp.Finish()
	}
	addLocalChild := func(ctx context.Context, i int) {
		childStr := strconv.Itoa(i)
		_, child := ChildSpan(ctx, childStr)
		logToSpanAndFinish(child, childStr)
	}
	mkRemoteSpan := func(tr *Tracer, i int, children int) []RecordedSpan {
		childStr := strconv.Itoa(i)
		sp := tr.StartSpan(childStr, Recordable)
		StartRecording(sp, SnowballRecording)
		ctx := opentracing.ContextWithSpan(context.Background(), sp)
		for i := 0; i < children; i++ {
			addLocalChild(ctx, i)
		}
		logToSpanAndFinish(sp, childStr)
		return GetRecording(sp)
	}

	// Ensure that no limit is enforced when there is a 0 limit.
	t.Run("unlimited", func(t *testing.T) {
		tr, _, settings := initTracer()
		maxChildSpansPerSpan.Override(settings, 0)
		root := tr.StartSpan("root", Recordable)
		StartRecording(root, SnowballRecording)
		ctx := opentracing.ContextWithSpan(context.Background(), root)
		const aLargeNumber = 1 << 15
		for i := 0; i < aLargeNumber; /* a very large number */ i++ {
			addLocalChild(ctx, i)
		}
		root.Finish()
		recording := GetRecording(root)
		require.Len(t, recording, int(aLargeNumber+1))
	})

	// Ensure that the limit is enforced on local children.
	t.Run("child spans", func(t *testing.T) {
		tr, maxChildSpans, _ := initTracer()
		root := tr.StartSpan("root", Recordable)
		StartRecording(root, SnowballRecording)
		ctx := opentracing.ContextWithSpan(context.Background(), root)
		for i := 0; i < int(maxChildSpans*2); i++ {
			addLocalChild(ctx, i)
		}
		root.Finish()
		recording := GetRecording(root)
		require.Len(t, recording, int(maxChildSpans+1))
	})

	// Ensure that the limit is enforced when adding remote spans one at a time.
	t.Run("remote spans", func(t *testing.T) {
		tr, maxChildSpans, _ := initTracer()
		root := tr.StartSpan("root", Recordable)
		StartRecording(root, SnowballRecording)
		for i := 0; i < int(maxChildSpans*2); i++ {
			require.NoError(t, ImportRemoteSpans(root, mkRemoteSpan(tr, i, 0 /* children */)))
		}
		root.Finish()
		recording := GetRecording(root)
		require.Len(t, recording, int(maxChildSpans+1))
	})

	// Ensure that when the setting increases, new spans get recorded (we may
	// eventually want to change this).
	t.Run("increase setting", func(t *testing.T) {
		tr, maxChildren, settings := initTracer()
		root := tr.StartSpan("root", Recordable)
		StartRecording(root, SnowballRecording)
		ctx := opentracing.ContextWithSpan(context.Background(), root)
		// Only the first 3rd of these should get recorded.
		for i := 0; i < int(maxChildren*3); i++ {
			addLocalChild(ctx, i)
		}

		maxChildSpansPerSpan.Override(settings, 2*maxChildren)
		// The next 3rd of these should get recorded.
		for i := 0; i < int(maxChildren*3); i++ {
			addLocalChild(ctx, i)
		}
		root.Finish()
		recording := GetRecording(root)
		require.Len(t, recording, int(2*maxChildren+1))
	})

	// Ensure that when the setting increases, new spans get recorded (we may
	// eventually want to change this).
	t.Run("decrease setting", func(t *testing.T) {
		tr, maxChildren, settings := initTracer()
		root := tr.StartSpan("root", Recordable)
		StartRecording(root, SnowballRecording)
		ctx := opentracing.ContextWithSpan(context.Background(), root)
		// Only the first 3rd of these should get recorded.
		for i := 0; i < int(maxChildren/2); i++ {
			addLocalChild(ctx, i)
		}

		maxChildSpansPerSpan.Override(settings, maxChildren/2)
		// None of these should get recorded.
		for i := 0; i < int(maxChildren*3); i++ {
			addLocalChild(ctx, i)
		}
		root.Finish()
		recording := GetRecording(root)
		require.Len(t, recording, int(maxChildren/2+1))
	})

	// Ensure that the limit is enforced when mixing adding local children and
	// larger remote spans concurrently.
	t.Run("concurrent random mix", func(t *testing.T) {
		tr, maxChildSpans, _ := initTracer()
		root := tr.StartSpan("root", Recordable)
		StartRecording(root, SnowballRecording)
		ctx := opentracing.ContextWithSpan(context.Background(), root)
		var wg sync.WaitGroup
		for written := 0; written < int(maxChildSpans*2); {
			r := rand.Float64()
			switch {
			case r < .5: // add local child
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					addLocalChild(ctx, i)
				}(written)
				written++
			default: // add some remote spans
				children := rand.Intn(int(maxChildSpans) / 2)
				wg.Add(1)
				go func(i, children int) {
					defer wg.Done()
					assert.NoError(t, ImportRemoteSpans(root, mkRemoteSpan(tr, i, children)))
				}(written, children)
				written += 1 + children
			}
		}
		wg.Wait()
		root.Finish()
		recording := GetRecording(root)
		require.Len(t, recording, int(maxChildSpans+1))
	})
}

func TestRecordingString(t *testing.T) {
	tr := NewTracer()
	tr2 := NewTracer()

	root := tr.StartSpan("root", Recordable)
	StartRecording(root, SnowballRecording)
	root.LogFields(otlog.String(LogMessageField, "root 1"))

	carrier := make(opentracing.HTTPHeadersCarrier)
	err := tr.Inject(root.Context(), opentracing.HTTPHeaders, carrier)
	require.NoError(t, err)
	wireContext, err := tr2.Extract(opentracing.HTTPHeaders, carrier)
	remoteChild := tr2.StartSpan("remote child", opentracing.FollowsFrom(wireContext))
	root.LogFields(otlog.String(LogMessageField, "root 2"))
	remoteChild.LogFields(otlog.String(LogMessageField, "remote child 1"))
	require.NoError(t, err)
	remoteChild.Finish()
	remoteRec := GetRecording(remoteChild)
	err = ImportRemoteSpans(root, remoteRec)
	require.NoError(t, err)
	root.Finish()

	root.LogFields(otlog.String(LogMessageField, "root 3"))

	ch2 := StartChildSpan("local child", root, nil /* logTags */, false /* separateRecording */)
	root.LogFields(otlog.String(LogMessageField, "root 4"))
	ch2.LogFields(otlog.String(LogMessageField, "local child 1"))
	ch2.Finish()

	root.LogFields(otlog.String(LogMessageField, "root 5"))
	root.Finish()

	rec := GetRecording(root)
	// Sanity check that the recording looks like we want. Note that this is not
	// its String() representation; this just list all the spans in order.
	err = TestingCheckRecordedSpans(rec, `
span root:
	tags: sb=1
	event: root 1
	event: root 2
	event: root 3
	event: root 4
	event: root 5
span remote child:
	tags: sb=1
	event: remote child 1
span local child:
	event: local child 1
`)
	require.NoError(t, err)

	exp := `=== operation:root sb:1
event:root 1
    === operation:remote child sb:1
    event:remote child 1
event:root 2
event:root 3
    === operation:local child
    event:local child 1
event:root 4
event:root 5
`
	require.Equal(t, exp, recToStrippedString(rec))
}

func recToStrippedString(r Recording) string {
	s := r.String()
	// Strip the timing info, converting rows like:
	//      0.007ms      0.007ms    event:root 1
	// into:
	//    event:root 1
	re := regexp.MustCompile(`.*s.*s\s\s\s\s`)
	stripped := string(re.ReplaceAll([]byte(s), nil))
	return stripped
}

func TestRecordingInRecording(t *testing.T) {
	tr := NewTracer()

	root := tr.StartSpan("root", Recordable)
	StartRecording(root, SnowballRecording)
	child := tr.StartSpan("child", opentracing.ChildOf(root.Context()), Recordable)
	StartRecording(child, SnowballRecording)
	grandChild := tr.StartSpan("grandchild", opentracing.ChildOf(child.Context()))
	grandChild.Finish()
	child.Finish()
	root.Finish()

	rootRec := GetRecording(root)
	require.NoError(t, TestingCheckRecordedSpans(rootRec, `
span root:
	tags: sb=1
span child:
	tags: sb=1
span grandchild:
	tags: sb=1
`))

	childRec := GetRecording(child)
	require.NoError(t, TestingCheckRecordedSpans(childRec, `
span child:
	tags: sb=1
span grandchild:
	tags: sb=1
`))

	exp := `=== operation:child sb:1
    === operation:grandchild sb:1
`
	require.Equal(t, exp, recToStrippedString(childRec))
}
