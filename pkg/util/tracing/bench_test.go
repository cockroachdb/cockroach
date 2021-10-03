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
)

// BenchmarkTracer_StartSpanCtx primarily helps keep
// tab on the allocation counts for starting a Span.
//
// This benchmark explicitly excludes construction of
// the SpanOptions, which require allocations as well.
func BenchmarkTracer_StartSpanCtx(b *testing.B) {
	ctx := context.Background()

	tr := NewTracer()
	sv := settings.Values{}
	tr.Configure(ctx, &sv)

	staticLogTags := logtags.Buffer{}
	staticLogTags.Add("foo", "bar")

	b.ResetTimer()

	parSp := tr.StartSpan("one-off", WithForceRealSpan())
	defer parSp.Finish()

	for _, tc := range []struct {
		name string
		opts []SpanOption
	}{
		{"none", nil},
		{"real", []SpanOption{
			WithForceRealSpan(),
		}},
		{"real,logtag", []SpanOption{
			WithForceRealSpan(), WithLogTags(&staticLogTags),
		}},
		{"real,autoparent", []SpanOption{
			WithForceRealSpan(), WithParentAndAutoCollection(parSp),
		}},
		{"real,manualparent", []SpanOption{
			WithForceRealSpan(), WithParentAndManualCollection(parSp.Meta()),
		}},
	} {
		b.Run(fmt.Sprintf("opts=%s", tc.name), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				newCtx, sp := tr.StartSpanCtx(ctx, "benching", tc.opts...)
				_ = newCtx
				sp.Finish() // clean up
			}
		})
	}

}

// BenchmarkSpan_GetRecording microbenchmarks GetRecording.
func BenchmarkSpan_GetRecording(b *testing.B) {
	ctx := context.Background()
	var sv settings.Values
	tr := NewTracer()
	tr.Configure(ctx, &sv)

	sp := tr.StartSpan("foo", WithForceRealSpan())

	run := func(b *testing.B, sp *Span) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = sp.GetRecording()
		}
	}

	b.ResetTimer()
	b.Run("root-only", func(b *testing.B) {
		run(b, sp)
	})

	child := tr.StartSpan("bar", WithParentAndAutoCollection(sp), WithForceRealSpan())
	b.Run("child-only", func(b *testing.B) {
		run(b, child)
	})

	b.Run("root-child", func(b *testing.B) {
		run(b, sp)
	})
}
