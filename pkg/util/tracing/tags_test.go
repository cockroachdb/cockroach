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
)

func TestLogTags(t *testing.T) {
	tr := NewTracer()

	l := logtags.SingleTagBuffer("tag1", "val1")
	l = l.Add("tag2", "val2")
	sp1 := tr.StartSpan("foo", Recordable, LogTags(l))
	StartRecording(sp1, SingleNodeRecording)

	if err := TestingCheckRecordedSpans(GetRecording(sp1), `
		span foo:
		  tags: tag1=val1 tag2=val2
	`); err != nil {
		t.Fatal(err)
	}

	RegisterTagRemapping("tag1", "one")
	RegisterTagRemapping("tag2", "two")

	sp2 := tr.StartSpan("bar", Recordable, LogTags(l))
	StartRecording(sp2, SingleNodeRecording)

	if err := TestingCheckRecordedSpans(GetRecording(sp2), `
		span bar:
			tags: one=val1 two=val2
	`); err != nil {
		t.Fatal(err)
	}
}
