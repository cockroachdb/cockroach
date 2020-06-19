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
	"testing"

	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/require"
)

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

	recStr := rec.String()
	// Strip the timing info, converting rows like:
	//      0.007ms      0.007ms    event:root 1
	// into:
	//    event:root 1
	re := regexp.MustCompile(`.*s.*s\s\s\s\s`)
	stripped := string(re.ReplaceAll([]byte(recStr), nil))

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
	require.Equal(t, exp, stripped)
}
