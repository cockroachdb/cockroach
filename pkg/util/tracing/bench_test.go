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

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/logtags"
	"github.com/gogo/protobuf/types"
)

// BenchmarkTracer_StartSpanCtx primarily helps keep
// tab on the allocation counts for starting a Span.
//
// This benchmark explicitly excludes construction of
// the SpanOptions, which require allocations as well.
func BenchmarkTracer_StartSpanCtx(b *testing.B) {
	skip.UnderDeadlock(b, "span reuse triggers false-positives in the deadlock detector")
	ctx := context.Background()

	staticLogTags := logtags.Buffer{}
	staticLogTags.Add("foo", "bar")

	for _, tc := range []struct {
		name        string
		defaultMode TracingMode
		parent      bool
		opts        []SpanOption
	}{
		{"none", TracingModeOnDemand, false, nil},
		{"real", TracingModeActiveSpansRegistry, false, nil},
		{"real,logtag", TracingModeActiveSpansRegistry, false, []SpanOption{WithLogTags(&staticLogTags)}},
		{"real,autoparent", TracingModeActiveSpansRegistry, true, nil},
		{"real,manualparent", TracingModeActiveSpansRegistry, true, []SpanOption{WithDetachedRecording()}},
	} {
		b.Run(fmt.Sprintf("opts=%s", tc.name), func(b *testing.B) {
			tr := NewTracerWithOpt(ctx,
				WithTracingMode(TracingModeActiveSpansRegistry),
				WithSpanReusePercent(100))
			b.ResetTimer()

			var parent *Span
			var numOpts = len(tc.opts)
			if tc.parent {
				parent = tr.StartSpan("one-off")
				defer parent.Finish()
				numOpts++
			}
			opts := make([]SpanOption, numOpts)
			copy(opts, tc.opts)

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if parent != nil {
					// The WithParent option needs to be re-created every time; it cannot be reused.
					opts[len(opts)-1] = WithParent(parent)
				}
				newCtx, sp := tr.StartSpanCtx(ctx, "benching", opts...)
				_ = newCtx
				sp.Finish() // clean up
			}
		})
	}

}

// BenchmarkSpan_GetRecording microbenchmarks GetRecording.
func BenchmarkSpan_GetRecording(b *testing.B) {
	ctx := context.Background()
	tr := NewTracerWithOpt(ctx, WithTracingMode(TracingModeActiveSpansRegistry))

	sp := tr.StartSpan("foo")

	run := func(b *testing.B, sp *Span) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = sp.GetRecording(RecordingStructured)
		}
	}

	b.ResetTimer()
	b.Run("root-only", func(b *testing.B) {
		run(b, sp)
	})

	child := tr.StartSpan("bar", WithParent(sp))
	b.Run("child-only", func(b *testing.B) {
		run(b, child)
	})

	b.Run("root-child", func(b *testing.B) {
		run(b, sp)
	})
}

func BenchmarkRecordingWithStructuredEvent(b *testing.B) {
	skip.UnderDeadlock(b, "span reuse triggers false-positives in the deadlock detector")
	tr := NewTracerWithOpt(context.Background(),
		WithTracingMode(TracingModeActiveSpansRegistry),
		WithSpanReusePercent(100))

	ev := &types.Int32Value{Value: 5}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		root := tr.StartSpan("foo", WithRecording(RecordingStructured))
		root.RecordStructured(ev)
		child := tr.StartSpan("bar", WithParent(root))
		child.RecordStructured(ev)
		child.Finish()
		_ = root.FinishAndGetRecording(RecordingStructured)
	}
}

// BenchmarkSpanCreation creates traces with a couple of spans in them.
func BenchmarkSpanCreation(b *testing.B) {
	skip.UnderDeadlock(b, "span reuse triggers false-positives in the deadlock detector")
	tr := NewTracerWithOpt(context.Background(),
		WithTracingMode(TracingModeActiveSpansRegistry),
		WithSpanReusePercent(100))
	const numChildren = 5
	childNames := make([]string, numChildren)
	for i := 0; i < numChildren; i++ {
		childNames[i] = fmt.Sprintf("child%d", i)
	}

	for _, detachedChild := range []bool{false, true} {
		b.Run(fmt.Sprintf("detached-child=%t", detachedChild), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				b.ReportAllocs()
				sps := make([]*Span, 0, 10)
				for pb.Next() {
					sps = sps[:0]
					ctx, sp := tr.StartSpanCtx(context.Background(), "root")
					sps = append(sps, sp)
					for j := 0; j < numChildren; j++ {
						var sp *Span
						if !detachedChild {
							ctx, sp = EnsureChildSpan(ctx, tr, childNames[j])
						} else {
							ctx, sp = EnsureForkSpan(ctx, tr, childNames[j])
						}
						sps = append(sps, sp)
					}
					for j := len(sps) - 1; j >= 0; j-- {
						sps[j].Finish()
					}
				}
			})
		})
	}
}
