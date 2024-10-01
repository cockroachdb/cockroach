// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// StructuredEvent emits a structured event log of severity sev to the channel the provided
// event belongs to.
func StructuredEvent(ctx context.Context, sev logpb.Severity, event logpb.EventPayload) {
	// Note: we use depth 0 intentionally here, so that structured
	// events can be reliably detected (their source filename will
	// always be log/event_log.go).
	StructuredEventDepth(ctx, sev, 0, event)
}

// StructuredEventDepth emits a structured event log of severity sev and depth to the channel the provided
// event belongs to.
func StructuredEventDepth(
	ctx context.Context, sev logpb.Severity, depth int, event logpb.EventPayload,
) {
	// Populate the missing common fields.
	common := event.CommonDetails()
	if common.Timestamp == 0 {
		common.Timestamp = timeutil.Now().UnixNano()
	}
	if len(common.EventType) == 0 {
		common.EventType = logpb.GetEventTypeName(event)
	}

	entry := makeStructuredEntry(ctx,
		sev,
		event.LoggingChannel(),
		depth,
		event)

	if sp := getSpan(ctx); sp != nil {
		// Prevent `entry` from moving to the heap when this branch is not taken.
		heapEntry := entry
		eventInternal(sp, entry.sev >= severity.ERROR, &heapEntry)
	}

	logger := logging.getLogger(entry.ch)
	logger.outputLogEntry(entry)
}
