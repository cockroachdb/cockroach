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
	ptypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// TestMaybeRedactRecording verifies that maybeRedactRecording strips
// sensitive details for recordings consumed by tenants.
//
// See kvccl.TestTenantTracesAreRedacted for an end-to-end test of this.
func TestMaybeRedactRecording(t *testing.T) {
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
			Add("tag_not_sensitive", log.Safe(tagNotSensitive))
		ctx := logtags.WithTags(context.Background(), tags)
		ctx, sp := tracing.NewTracer().StartSpanCtx(ctx, "foo", tracing.WithForceRealSpan())
		sp.SetVerbose(true)

		log.Eventf(ctx, "%s %s", msgSensitive, log.Safe(msgNotSensitive))
		sp.SetTag("all_span_tags_are_stripped", "because_no_redactability")
		sp.Finish()
		rec := sp.GetRecording()
		require.Len(t, rec, 1)
		return rec
	}

	t.Run("regular-tenant", func(t *testing.T) {
		rec := mkRec()
		maybeRedactRecording(roachpb.MakeTenantID(100), rec)
		require.Zero(t, rec[0].Tags)
		require.Len(t, rec[0].Logs, 1)
		msg := rec[0].Logs[0].Fields[0].Value
		t.Log(msg)
		require.NotContains(t, msg, msgSensitive)
		require.NotContains(t, msg, tagSensitive)
		require.Contains(t, msg, msgNotSensitive)
		require.Contains(t, msg, tagNotSensitive)
	})

	t.Run("system-tenant", func(t *testing.T) {
		rec := mkRec()
		maybeRedactRecording(roachpb.SystemTenantID, rec)
		require.Equal(t, map[string]string{
			"_verbose":                   "1",
			"all_span_tags_are_stripped": "because_no_redactability",
			"tag_not_sensitive":          tagNotSensitive,
			"tag_sensitive":              tagSensitive,
		}, rec[0].Tags)
		require.Len(t, rec[0].Logs, 1)
		msg := rec[0].Logs[0].Fields[0].Value
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
		// maybeRedactRecording appropriately.
		type calcifiedRecordedSpan struct {
			TraceID                      uint64
			SpanID                       uint64
			ParentSpanID                 uint64
			Operation                    string
			Baggage                      map[string]string
			Tags                         map[string]string
			StartTime                    time.Time
			Duration                     time.Duration
			RedactableLogs               bool
			Logs                         []tracingpb.LogRecord
			DeprecatedInternalStructured []*ptypes.Any
			GoroutineID                  uint64
			Finished                     bool
			StructuredRecords            []tracingpb.StructuredRecord
		}
		_ = (*calcifiedRecordedSpan)((*tracingpb.RecordedSpan)(nil))
	})
}
