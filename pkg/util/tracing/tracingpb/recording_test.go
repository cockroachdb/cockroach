// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingpb_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/redact"
)

func TestRecordingString(t *testing.T) {
	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, "recording_test"))

	for _, test := range []struct {
		name           string
		fn             func() tracingpb.Recording
		skipFullFormat bool
	}{
		{
			name: "empty",
			fn: func() tracingpb.Recording {
				return tracingpb.Recording{}
			},
		},
		{
			name: "single_event_synthetic",
			fn: func() tracingpb.Recording {
				now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
				return tracingpb.Recording{
					{
						TraceID:   tracingpb.TraceID(1),
						SpanID:    tracingpb.SpanID(1),
						Operation: "test-operation",
						StartTime: now,
						Duration:  time.Second,
						Logs: []tracingpb.LogRecord{
							{
								Time:    now.Add(100 * time.Millisecond),
								Message: redact.Sprint("test event message"),
							},
						},
					},
				}
			},
		},
		{
			name:           "single_event_real_span",
			skipFullFormat: true,
			fn: func() tracingpb.Recording {
				tr := tracing.NewTracer()
				tr.SetRedactable(true)
				ctx, getAndFinish := tracing.ContextWithRecordingSpan(context.Background(), tr, "test-op")
				log.Eventf(ctx, "the answer to the universe and everything is %v", "43")
				return getAndFinish()
			},
		},
		{
			name: "with_child",
			fn: func() tracingpb.Recording {
				rec := makeWithChildFixture()
				baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
				rec[1].Logs = []tracingpb.LogRecord{
					{
						Time:    baseTime.Add(200 * time.Millisecond),
						Message: redact.Sprint("child event"),
					},
				}
				return rec
			},
		},
		{
			name: "with_child_no_logs",
			fn: func() tracingpb.Recording {
				return makeWithChildFixture()
			},
		},
	} {
		// Test full format (String)
		if !test.skipFullFormat {
			t.Run(test.name, w.Run(t, test.name, func(t *testing.T) string {
				recording := test.fn()
				return recording.String()
			}))
		}

		// Test minimal format (SafeFormatMinimal)
		// Note: Some test cases panic due to bugs in recording.go where spans without
		// TagGroups or child spans with no logs cause index out of range panics.
		t.Run(test.name+"_minimal", w.Run(t, test.name+"_minimal", func(t *testing.T) string {
			recording := test.fn()
			var sb redact.StringBuilder
			recording.SafeFormatMinimal(&sb)
			return string(sb.RedactableString())
		}))
	}
}

func makeWithChildFixture() tracingpb.Recording {
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	parentID := tracingpb.SpanID(1)
	childID := tracingpb.SpanID(2)
	traceID := tracingpb.TraceID(1)
	return tracingpb.Recording{
		{
			TraceID:   traceID,
			SpanID:    parentID,
			Operation: "parent-operation",
			StartTime: baseTime,
			Duration:  time.Second,
		},
		{
			TraceID:      traceID,
			SpanID:       childID,
			ParentSpanID: parentID,
			Operation:    "child-operation",
			StartTime:    baseTime.Add(100 * time.Millisecond),
			Duration:     500 * time.Millisecond,
		},
	}
}
