// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tracing

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
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
