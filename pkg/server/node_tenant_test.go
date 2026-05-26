// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

// Check that redactRecording strips sensitive details from recordings.
//
// See kvccl.TestTenantTracesAreRedacted for an end-to-end test of tenant trace
// redaction.
func TestRedactRecording(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		msgNotSensitive = "msg-tenant-shown"
		msgSensitive    = "msg-tenant-hidden"
		tagNotSensitive = "tag-tenant-shown"
		tagSensitive    = "tag-tenant-hidden"
	)

	mkRec := func() tracingpb.Recording {
		t.Helper()
		tags := (&logtags.Buffer{}).
			Add("tag_sensitive", tagSensitive).
			Add("tag_not_sensitive", redact.Safe(tagNotSensitive))
		ctx := logtags.WithTags(context.Background(), tags)
		tracer := tracing.NewTracer()
		tracer.SetRedactable(true)
		ctx, sp := tracer.StartSpanCtx(ctx, "foo", tracing.WithRecording(tracingpb.RecordingVerbose))
		log.Eventf(ctx, "%s %s", msgSensitive, redact.Safe(msgNotSensitive))
		sp.SetTag("all_span_tags_are_stripped", attribute.StringValue("because_no_redactability"))
		rec := sp.FinishAndGetRecording(tracingpb.RecordingVerbose)
		require.Len(t, rec, 1)
		return rec
	}

	rec := mkRec()
	redactRecording(rec)
	require.Zero(t, rec[0].TagGroups)
	require.Len(t, rec[0].Logs, 1)
	msg := rec[0].Logs[0].Msg().StripMarkers()
	t.Log(msg)
	require.NotContains(t, msg, msgSensitive)
	require.NotContains(t, msg, tagSensitive)
	require.Contains(t, msg, msgNotSensitive)
	require.Contains(t, msg, tagNotSensitive)
}

// Guard against a new sensitive field being added to RecordedSpan. If you're
// here to see why this test failed to compile, ensure that the change you're
// making to RecordedSpan does not include new sensitive data that may leak from
// the KV layer to tenants. If it does, update redactRecording() appropriately.
func TestNewSpanFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type calcifiedRecordedSpan struct {
		TraceID                    tracingpb.TraceID
		SpanID                     tracingpb.SpanID
		ParentSpanID               tracingpb.SpanID
		Operation                  string
		TagGroups                  []tracingpb.TagGroup
		StartTime                  time.Time
		Duration                   time.Duration
		Logs                       []tracingpb.LogRecord
		Verbose                    bool
		RecordingMode              tracingpb.RecordingMode
		GoroutineID                uint64
		Finished                   bool
		StructuredRecords          []tracingpb.StructuredRecord
		StructuredRecordsSizeBytes int64
		ChildrenMetadata           map[string]tracingpb.OperationMetadata
	}
	_ = (*calcifiedRecordedSpan)((*tracingpb.RecordedSpan)(nil))
}
