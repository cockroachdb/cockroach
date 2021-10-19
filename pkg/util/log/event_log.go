// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// StructuredEvent emits a structured event to the debug log.
func StructuredEvent(ctx context.Context, event eventpb.EventPayload) {
	// Populate the missing common fields.
	common := event.CommonDetails()
	if common.Timestamp == 0 {
		common.Timestamp = timeutil.Now().UnixNano()
	}
	if len(common.EventType) == 0 {
		common.EventType = eventpb.GetEventTypeName(event)
	}

	entry := makeStructuredEntry(ctx,
		severity.INFO,
		event.LoggingChannel(),
		// Note: we use depth 0 intentionally here, so that structured
		// events can be reliably detected (their source filename will
		// always be log/event_log.go).
		0, /* depth */
		event)

	if sp, el, ok := getSpanOrEventLog(ctx); ok {
		eventInternal(sp, el, (entry.sev >= severity.ERROR), entry.convertToLegacy())
	}

	logger := logging.getLogger(entry.ch)
	logger.outputLogEntry(entry)
}
