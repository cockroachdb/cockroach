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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/logtags"
	"github.com/opentracing/opentracing-go"
)

// BenchmarkTracer_StartSpanCtx primarily helps keep
// tab on the allocation counts for starting a Span.
//
// This benchmark explicitly excludes construction of
// the SpanOptions, which require allocations as well.
func BenchmarkTracer_StartSpanCtx(b *testing.B) {
	ctx := context.WithValue(context.Background(), "foo", "bar")

	tr := NewTracer()
	sv := settings.Values{}
	tracingMode.Override(&sv, int64(modeBackground))
	tr.Configure(&sv)

	staticLogTags := logtags.Buffer{}
	staticLogTags.Add("foo", "bar")

	staticTag := opentracing.Tag{
		Key:   "statictag",
		Value: "staticvalue",
	}
	b.ResetTimer()

	parSp := tr.StartSpan("one-off", WithForceRealSpan())
	defer parSp.Finish()

	for _, tc := range []struct {
		name string
		opts []SpanOption
	}{
		{"none", nil},
		{"logtag", []SpanOption{WithLogTags(&staticLogTags)}},
		{"tag", []SpanOption{WithTags(staticTag)}},
		{"autoparent", []SpanOption{WithParentAndAutoCollection(parSp)}},
		{"manualparent", []SpanOption{WithParentAndManualCollection(parSp.Meta())}},
	} {
		b.Run(fmt.Sprintf("opts=%s", tc.name), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				newCtx, sp := tr.StartSpanCtx(ctx, "benching", tc.opts...)
				_ = newCtx
				_ = sp
			}
		})
	}

}
