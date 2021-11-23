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
	"github.com/gogo/protobuf/types"
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
			WithForceRealSpan(), WithParent(parSp),
		}},
		{"real,manualparent", []SpanOption{
			WithForceRealSpan(), WithParent(parSp), WithDetachedRecording(),
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
			_ = sp.GetRecording(RecordingStructured)
		}
	}

	b.ResetTimer()
	b.Run("root-only", func(b *testing.B) {
		run(b, sp)
	})

	child := tr.StartSpan("bar", WithParent(sp), WithForceRealSpan())
	b.Run("child-only", func(b *testing.B) {
		run(b, child)
	})

	b.Run("root-child", func(b *testing.B) {
		run(b, sp)
	})
}

func BenchmarkRecordingWithStructuredEvent(b *testing.B) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))

	ev := &types.Int32Value{Value: 5}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		root := tr.StartSpan("foo")
		root.RecordStructured(ev)
		child := tr.StartSpan("bar", WithParent(root))
		child.RecordStructured(ev)
		child.Finish()
		_ = root.GetRecording(RecordingStructured)
	}
}

// BenchmarkSpanCreation creates traces with a couple of spans in them.
func BenchmarkSpanCreation(b *testing.B) {
	tr := NewTracerWithOpt(context.Background(), WithTracingMode(TracingModeActiveSpansRegistry))
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
