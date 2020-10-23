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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/require"
)

func TestRecordingString(t *testing.T) {
	tr := NewTracer()
	tr2 := NewTracer()

	root := tr.StartSpan("root", Recordable)
	rootSp := root.(*Span)
	StartRecording(root, SnowballRecording)
	root.LogFields(otlog.String(tracingpb.LogMessageField, "root 1"))
	// Hackily fix the timing on the first log message, so that we can check it later.
	rootSp.crdb.mu.recording.recordedLogs[0].Timestamp = rootSp.crdb.startTime.Add(time.Millisecond)
	// Sleep a bit so that everything that comes afterwards has higher timestamps
	// than the one we just assigned. Otherwise the sorting will be screwed up.
	time.Sleep(10 * time.Millisecond)

	carrier := make(opentracing.HTTPHeadersCarrier)
	err := tr.Inject(root.Context(), opentracing.HTTPHeaders, carrier)
	require.NoError(t, err)
	wireContext, err := tr2.Extract(opentracing.HTTPHeaders, carrier)
	remoteChild := tr2.StartSpan("remote child", opentracing.FollowsFrom(wireContext))
	root.LogFields(otlog.String(tracingpb.LogMessageField, "root 2"))
	remoteChild.LogFields(otlog.String(tracingpb.LogMessageField, "remote child 1"))
	require.NoError(t, err)
	remoteChild.Finish()
	remoteRec := GetRecording(remoteChild)
	err = ImportRemoteSpans(root, remoteRec)
	require.NoError(t, err)
	root.Finish()

	root.LogFields(otlog.String(tracingpb.LogMessageField, "root 3"))

	ch2 := tr.StartChildSpan("local child", root.(*Span).SpanContext(), nil /* logTags */, false /* recordable */, false /* separateRecording */)
	root.LogFields(otlog.String(tracingpb.LogMessageField, "root 4"))
	ch2.LogFields(otlog.String(tracingpb.LogMessageField, "local child 1"))
	ch2.Finish()

	root.LogFields(otlog.String(tracingpb.LogMessageField, "root 5"))
	root.Finish()

	rec := GetRecording(root)
	// Sanity check that the recording looks like we want. Note that this is not
	// its String() representation; this just list all the spans in order.
	err = TestingCheckRecordedSpans(rec, `
Span root:
	tags: sb=1
	event: root 1
	event: root 2
	event: root 3
	event: root 4
	event: root 5
Span remote child:
	tags: sb=1
	event: remote child 1
Span local child:
	tags: sb=1
	event: local child 1
`)
	require.NoError(t, err)

	exp := `=== operation:root sb:1
event:root 1
    === operation:remote child sb:1
    event:remote child 1
event:root 2
event:root 3
    === operation:local child sb:1
    event:local child 1
event:root 4
event:root 5
`
	require.Equal(t, exp, recToStrippedString(rec))

	// Check the timing info on the first two lines.
	lines := strings.Split(rec.String(), "\n")
	l, err := parseLine(lines[0])
	require.NoError(t, err)
	require.Equal(t, traceLine{
		timeSinceTraceStart: "0.000ms",
		timeSincePrev:       "0.000ms",
		text:                "=== operation:root sb:1",
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

func recToStrippedString(r Recording) string {
	s := r.String()
	// Strip the timing info, converting rows like:
	//      0.007ms      0.007ms    event:root 1
	// into:
	//    event:root 1
	re := regexp.MustCompile(`.*s.*s\s{4}`)
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
Span root:
	tags: sb=1
Span child:
	tags: sb=1
Span grandchild:
	tags: sb=1
`))

	childRec := GetRecording(child)
	require.NoError(t, TestingCheckRecordedSpans(childRec, `
Span child:
	tags: sb=1
Span grandchild:
	tags: sb=1
`))

	exp := `=== operation:child sb:1
    === operation:grandchild sb:1
`
	require.Equal(t, exp, recToStrippedString(childRec))
}
