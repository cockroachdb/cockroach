// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/redact"
)

// EventType represents the type of an event emitted via Structured.
type EventType string

const (
	STATEMENT_STATS EventType = "stmt_stats"
)

// StructuredMeta is a metadata object that accompanies each event emitted via Structured.
type StructuredMeta struct {
	EventType EventType `json:"event_type"`
}

// structured is a wrapper around StructuredMeta and an arbitrary event.
// It's used to easily combine both into a single JSON object at Marshal time.
type structured struct {
	Metadata StructuredMeta `json:"metadata"`
	Event    any            `json:"event"`
}

// Structured emits a structured JSON payload to the STRUCTURED_EVENTS channel.
// TODO(abarganier): Redaction is not considered here yet. Enable redaction via struct tags.
// TODO(abarganier): StructuredEvent() is a similar API. We should consider how to reconcile or perhaps
// combine the two.
func Structured(ctx context.Context, meta StructuredMeta, payload any) {
	if meta.EventType == "" {
		Warningf(ctx, "event type not provided in structured event meta: %v", meta)
		return
	}
	m := structured{
		Metadata: meta,
		Event:    payload,
	}
	// TODO(abarganier): Implement redaction in the JSON serialization step. Ideally, done using struct tags
	// and code generation, similar to what we do for TELEMETRY events.
	bytes, err := json.Marshal(m)
	if err != nil {
		Warningf(ctx, "failed to marshal structured event to JSON with meta: %v", meta)
		return
	}

	entry := makeEntry(ctx, severity.INFO, logpb.Channel_STRUCTURED_EVENTS, 0 /*depth*/)
	entry.structured = true
	entry.payload = makeRedactablePayload(ctx, redact.RedactableString(bytes))

	logger := logging.getLogger(entry.ch)
	logger.outputLogEntry(entry)
}
