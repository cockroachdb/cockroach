// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

func TestTrace(t *testing.T) {
	ctx := context.Background()

	// Events to context without a trace should be no-ops.
	Event(ctx, "should-not-show-up")

	tracer := tracing.NewTracer()
	sp := tracer.StartSpan("s", tracing.WithRecording(tracingpb.RecordingVerbose))
	ctxWithTrace := tracing.ContextWithSpan(ctx, sp)
	// Events should only go to the trace.
	Event(ctxWithTrace, "test3")
	VEventf(ctxWithTrace, NoLogV(), "test4")
	VErrEventf(ctxWithTrace, NoLogV(), "%s", "test5err")

	rec := sp.FinishAndGetRecording(tracingpb.RecordingVerbose)
	if err := tracing.CheckRecordedSpans(rec, `
		span: s
			tags: _verbose=1
			event: test3
			event: test4
			event: test5err
	`); err != nil {
		t.Fatal(err)
	}
}
