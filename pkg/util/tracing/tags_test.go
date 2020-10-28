// Copyright 2018 The Cockroach Authors.
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
	"github.com/stretchr/testify/require"
)

func TestLogTags(t *testing.T) {
	tr := NewTracer()
	shadowTracer := mockTracer{}
	tr.setShadowTracer(&mockTracerManager{}, &shadowTracer)

	l := logtags.SingleTagBuffer("tag1", "val1")
	l = l.Add("tag2", "val2")
	sp1 := tr.StartSpan("foo", WithForceRealSpan(), WithLogTags(l))
	sp1.StartRecording(SingleNodeRecording)
	sp1.Finish()
	require.NoError(t, TestingCheckRecordedSpans(sp1.GetRecording(), `
		Span foo:
		  tags: tag1=val1 tag2=val2
	`))
	require.NoError(t, shadowTracer.expectSingleSpanWithTags("tag1", "tag2"))
	shadowTracer.clear()

	RegisterTagRemapping("tag1", "one")
	RegisterTagRemapping("tag2", "two")

	sp2 := tr.StartSpan("bar", WithForceRealSpan(), WithLogTags(l))
	sp2.StartRecording(SingleNodeRecording)
	sp2.Finish()
	require.NoError(t, TestingCheckRecordedSpans(sp2.GetRecording(), `
		Span bar:
			tags: one=val1 two=val2
	`))
	require.NoError(t, shadowTracer.expectSingleSpanWithTags("one", "two"))
	shadowTracer.clear()

	sp3 := tr.StartSpan("baz", WithLogTags(l), WithForceRealSpan())
	sp3.StartRecording(SingleNodeRecording)
	sp3.Finish()
	require.NoError(t, TestingCheckRecordedSpans(sp3.GetRecording(), `
		Span baz:
			tags: one=val1 two=val2
	`))
	require.NoError(t, shadowTracer.expectSingleSpanWithTags("one", "two"))
}
