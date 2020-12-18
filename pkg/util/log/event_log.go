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
	"encoding/json"

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
	// TODO(knz): Avoid marking all the JSON payload as redactable. Do
	// redaction per-field.
	b, err := json.Marshal(event)
	if err != nil {
		Fatalf(ctx, "unexpected JSON encoding error: %+v", err)
	}

	// TODO(knz): Avoid escaping the JSON format when emitting the payload
	// to an external sink.

	// Note: we use depth 0 intentionally here, so that structured
	// events can be reliably detected (their source filename will
	// always be log/event_log.go).
	// TODO(knz): Consider another way to mark structured events.
	logfDepth(ctx, 0, severity.INFO, event.LoggingChannel(), "Structured event: %s", string(b))
}
