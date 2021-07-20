// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

func TestTrace(t *testing.T) {
	for _, tc := range []struct {
		name  string
		init  func(context.Context) (context.Context, *tracing.Span)
		check func(*testing.T, context.Context, *tracing.Span)
	}{
		{
			name: "verbose",
			init: func(ctx context.Context) (context.Context, *tracing.Span) {
				tracer := tracing.NewTracer()
				sp := tracer.StartSpan("s", tracing.WithForceRealSpan())
				sp.SetVerbose(true)
				ctxWithSpan := tracing.ContextWithSpan(ctx, sp)
				return ctxWithSpan, sp
			},
			check: func(t *testing.T, _ context.Context, sp *tracing.Span) {
				if err := tracing.TestingCheckRecordedSpans(sp.GetRecording(), `
		span: s
			tags: _verbose=1
			event: test1
			event: test2
			event: testerr
			event: log
	`); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "zipkin",
			init: func(ctx context.Context) (context.Context, *tracing.Span) {
				tr := tracing.NewTracer()
				st := cluster.MakeTestingClusterSettings()
				tracing.ZipkinCollector.Override(ctx, &st.SV, "127.0.0.1:9000000")
				tr.Configure(ctx, &st.SV)
				return tr.StartSpanCtx(context.Background(), "foo")
			},
			check: func(t *testing.T, ctx context.Context, sp *tracing.Span) {
				// This isn't quite a real end-to-end-check, but it is good enough
				// to give us confidence that we're really passing log events to
				// the span, and the tracing package in turn has tests that verify
				// that a span so configured will actually log them to the external
				// trace.
				require.True(t, sp.Tracer().HasExternalSink())
				require.True(t, log.HasSpanOrEvent(ctx))
				require.True(t, log.ExpensiveLogEnabled(ctx, 0 /* level */))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			// Events to context without a trace should be no-ops.
			log.Event(ctx, "should-not-show-up")

			ctxWithSpan, sp := tc.init(ctx)
			log.Event(ctxWithSpan, "test1")
			log.VEvent(ctxWithSpan, log.NoLogV(), "test2")
			log.VErrEvent(ctxWithSpan, log.NoLogV(), "testerr")
			log.Info(ctxWithSpan, "log")

			// Events to parent context should still be no-ops.
			log.Event(ctx, "should-not-show-up")

			sp.Finish()
			tc.check(t, ctxWithSpan, sp)
		})
	}
}

func TestTraceWithTags(t *testing.T) {
	ctx := context.Background()
	ctx = logtags.AddTag(ctx, "tag", 1)

	tracer := tracing.NewTracer()
	sp := tracer.StartSpan("s", tracing.WithForceRealSpan())
	ctxWithSpan := tracing.ContextWithSpan(ctx, sp)
	sp.SetVerbose(true)

	log.Event(ctxWithSpan, "test1")
	log.VEvent(ctxWithSpan, log.NoLogV(), "test2")
	log.VErrEvent(ctxWithSpan, log.NoLogV(), "testerr")
	log.Info(ctxWithSpan, "log")

	sp.Finish()
	if err := tracing.TestingCheckRecordedSpans(sp.GetRecording(), `
		span: s
			tags: _verbose=1
			event: [tag=1] test1
			event: [tag=1] test2
			event: [tag=1] testerr
			event: [tag=1] log
	`); err != nil {
		t.Fatal(err)
	}
}
