// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
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

	staticLogTags := &logtags.Buffer{}
	staticLogTags = staticLogTags.Add("foo", "bar")
	mockListener := &mockEventListener{}

	for _, tc := range []struct {
		name              string
		defaultMode       TracingMode
		parent            bool
		withEventListener bool
		opts              []SpanOption
		regexpFilter      string
	}{
		{name: "real", defaultMode: TracingModeActiveSpansRegistry},
		{name: "real,logtag", defaultMode: TracingModeActiveSpansRegistry,
			opts: []SpanOption{WithLogTags(staticLogTags)}},
		{name: "real,autoparent", defaultMode: TracingModeActiveSpansRegistry, parent: true},
		{name: "real,manualparent", defaultMode: TracingModeActiveSpansRegistry, parent: true,
			opts: []SpanOption{WithDetachedRecording()}},
		{name: "real,autoparent,withEventListener", defaultMode: TracingModeActiveSpansRegistry,
			parent: true, withEventListener: true},
		{name: "real,manualparent,withEventListener", defaultMode: TracingModeActiveSpansRegistry, parent: true,
			withEventListener: true, opts: []SpanOption{WithDetachedRecording()}},
		{name: "real,regexp", defaultMode: TracingModeActiveSpansRegistry, regexpFilter: "op1|op2|op3|^op[a-zA-Z]+"},
	} {
		b.Run(fmt.Sprintf("opts=%s", tc.name), func(b *testing.B) {
			// Note: testutils.RunTrueAndFalse is not used here because it
			// would create a dependency cycle.
			for _, parallel := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s=%v", "parallel", parallel), func(b *testing.B) {
					tr := NewTracerWithOpt(ctx,
						WithTracingMode(tc.defaultMode),
						WithSpanReusePercent(100))
					b.ResetTimer()

					if tc.regexpFilter != "" {
						err := tr.setVerboseOpNameRegexp("op1|op2|op3|^op[a-zA-Z]+")
						if err != nil {
							b.Fatalf("failed to set verbose regexp: %v", err)
						}
					}

					var parent *Span
					var numOpts = len(tc.opts)
					if tc.parent {
						if tc.withEventListener {
							parent = tr.StartSpan("one-off", WithEventListeners(mockListener))
						} else {
							parent = tr.StartSpan("one-off")
						}
						defer parent.Finish()
						numOpts++
					}
					opts := make([]SpanOption, numOpts)
					copy(opts, tc.opts)

					b.ReportAllocs()
					if parallel {
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if parent != nil {
									// The WithParent option needs to be re-created every time; it cannot be reused.
									opts[len(opts)-1] = WithParent(parent)
								}
								newCtx, sp := tr.StartSpanCtx(ctx, "benching", opts...)
								_ = newCtx
								sp.Finish() // clean up
							}
						})
					} else {
						for i := 0; i < b.N; i++ {
							if parent != nil {
								// The WithParent option needs to be re-created every time; it cannot be reused.
								opts[len(opts)-1] = WithParent(parent)
							}
							newCtx, sp := tr.StartSpanCtx(ctx, "benching", opts...)
							_ = newCtx
							sp.Finish() // clean up
						}
					}
				})
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
			_ = sp.GetRecording(tracingpb.RecordingStructured)
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
	ev := &types.Int32Value{Value: 5}
	mockListener := &mockEventListener{}

	for _, tc := range []struct {
		name              string
		withEventListener bool
	}{
		{name: "with-event-listener", withEventListener: true},
		{name: "without-event-listener"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			tr := NewTracerWithOpt(context.Background(),
				WithTracingMode(TracingModeActiveSpansRegistry),
				WithSpanReusePercent(100))

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var root *Span
				if tc.withEventListener {
					root = tr.StartSpan("foo", WithRecording(tracingpb.RecordingStructured),
						WithEventListeners(mockListener))
				} else {
					root = tr.StartSpan("foo", WithRecording(tracingpb.RecordingStructured))
				}

				root.RecordStructured(ev)

				// The child span will also inherit the root span's event listener.
				child := tr.StartSpan("bar", WithParent(root))
				child.RecordStructured(ev)
				child.Finish()
				_ = root.FinishAndGetRecording(tracingpb.RecordingStructured)
			}
		})
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
