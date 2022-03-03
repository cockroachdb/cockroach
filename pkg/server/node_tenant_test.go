// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

// TestMaybeRedactRecording verifies that redactRecordingForTenant strips
// sensitive details for recordings consumed by tenants.
//
// See kvccl.TestTenantTracesAreRedacted for an end-to-end test of this.
func TestRedactRecordingForTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		msgNotSensitive = "msg-tenant-shown"
		msgSensitive    = "msg-tenant-hidden"
		tagNotSensitive = "tag-tenant-shown"
		tagSensitive    = "tag-tenant-hidden"
	)

	mkRec := func() tracing.Recording {
		t.Helper()
		tags := (&logtags.Buffer{}).
			Add("tag_sensitive", tagSensitive).
			Add("tag_not_sensitive", redact.Safe(tagNotSensitive))
		ctx := logtags.WithTags(context.Background(), tags)
		tracer := tracing.NewTracer()
		tracer.SetRedactable(true)
		ctx, sp := tracer.StartSpanCtx(ctx, "foo", tracing.WithRecording(tracing.RecordingVerbose))
		log.Eventf(ctx, "%s %s", msgSensitive, redact.Safe(msgNotSensitive))
		sp.SetTag("all_span_tags_are_stripped", attribute.StringValue("because_no_redactability"))
		rec := sp.FinishAndGetRecording(tracing.RecordingVerbose)
		require.Len(t, rec, 1)
		return rec
	}

	t.Run("regular-tenant", func(t *testing.T) {
		rec := mkRec()
		require.NoError(t, redactRecordingForTenant(roachpb.MakeTenantID(100), rec))
		require.Zero(t, rec[0].Tags)
		require.Len(t, rec[0].Logs, 1)
		msg := rec[0].Logs[0].Msg().StripMarkers()
		t.Log(msg)
		require.NotContains(t, msg, msgSensitive)
		require.NotContains(t, msg, tagSensitive)
		require.Contains(t, msg, msgNotSensitive)
		require.Contains(t, msg, tagNotSensitive)
	})

	t.Run("system-tenant", func(t *testing.T) {
		rec := mkRec()
		require.NoError(t, redactRecordingForTenant(roachpb.SystemTenantID, rec))
		require.Equal(t, map[string]string{
			"_verbose":                   "1",
			"all_span_tags_are_stripped": "because_no_redactability",
			"tag_not_sensitive":          tagNotSensitive,
			"tag_sensitive":              tagSensitive,
		}, rec[0].Tags)
		require.Len(t, rec[0].Logs, 1)
		msg := rec[0].Logs[0].Msg().StripMarkers()
		t.Log(msg)
		require.Contains(t, msg, msgSensitive)
		require.Contains(t, msg, tagSensitive)
		require.Contains(t, msg, msgNotSensitive)
		require.Contains(t, msg, tagNotSensitive)
	})

	t.Run("no-unhandled-fields", func(t *testing.T) {
		// Guard against a new sensitive field being added to RecordedSpan. If
		// you're here to see why this test failed to compile, ensure that the
		// change you're making to RecordedSpan does not include new sensitive data
		// that may leak from the KV layer to tenants. If it does, update
		// redactRecordingForTenant appropriately.
		type calcifiedRecordedSpan struct {
			TraceID           tracingpb.TraceID
			SpanID            tracingpb.SpanID
			ParentSpanID      tracingpb.SpanID
			Operation         string
			Tags              map[string]string
			StartTime         time.Time
			Duration          time.Duration
			RedactableLogs    bool
			Logs              []tracingpb.LogRecord
			Verbose           bool
			GoroutineID       uint64
			Finished          bool
			StructuredRecords []tracingpb.StructuredRecord
		}
		_ = (*calcifiedRecordedSpan)((*tracingpb.RecordedSpan)(nil))
	})
}
