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

// StructuredEvent emits a structured event to the debug log.
func StructuredEvent(ctx context.Context, event logpb.EventPayload) {
	// Populate the missing common fields.
	common := event.CommonDetails()
	if common.Timestamp == 0 {
		common.Timestamp = timeutil.Now().UnixNano()
	}
	if len(common.EventType) == 0 {
		common.EventType = logpb.GetEventTypeName(event)
	}

	entry := makeStructuredEntry(ctx,
		severity.INFO,
		event.LoggingChannel(),
		// Note: we use depth 0 intentionally here, so that structured
		// events can be reliably detected (their source filename will
		// always be log/event_log.go).
		0, /* depth */
		event)

	if sp := getSpan(ctx); sp != nil {
		// Prevent `entry` from moving to the heap when this branch is not taken.
		heapEntry := entry
		eventInternal(sp, entry.sev >= severity.ERROR, &heapEntry)
	}

	logger := logging.getLogger(entry.ch)
	logger.outputLogEntry(entry)
}
