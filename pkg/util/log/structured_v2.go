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
	"bytes"
	"context"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// Registered structured event types below.
var (
	STATEMENT_STATS = StructuredLogMeta{
		EventType: "stmt_stats",
		Version:   "0.1",
	}
)

// StructuredLogMeta is a metadata object that accompanies each event emitted via Structured.
// Any changes to the structure of an event's payload should correspond with a version bump.
type StructuredLogMeta struct {
	EventType EventType `json:"event_type"`
	Version   string    `json:"version"`
}

var _ redact.SafeFormatter = (*StructuredLogMeta)(nil)

// SafeFormat implements the redact.SafeFormatter interface.
func (s StructuredLogMeta) SafeFormat(sp interfaces.SafePrinter, _ rune) {
	sp.Printf(`{"event_type": %q, "version": %q}`, s.EventType, s.Version)
}

func (s StructuredLogMeta) String() string {
	return redact.StringWithoutMarkers(s)
}

// EventType identifies the specific type of structured log event.
type EventType string

var (
	trimPrefix = []byte("{")
	trimSuffix = []byte("}")
)

// StructuredPayload is a wrapper around StructuredMeta and an arbitrary event.
// It's used to easily combine both into a single JSON object at Marshal time.
type StructuredPayload struct {
	Metadata StructuredLogMeta `json:"metadata"`
	Payload  any               `json:"payload"`
}

// Structured emits a structured JSON payload to the STRUCTURED_EVENTS channel, along with the included metadata.
// TODO(abarganier): Redaction is not considered here yet. Enable redaction via struct tags.
// TODO(abarganier): StructuredEvent() is a similar API. We should consider how to reconcile or perhaps
// combine the two.
func Structured(ctx context.Context, meta StructuredLogMeta, payload any) {
	if meta.EventType == "" || meta.Version == "" {
		panic(errors.AssertionFailedf("structured event metadata '%s' is missing one or more key fields", meta))
	}
	m := StructuredPayload{
		Metadata: meta,
		Payload:  payload,
	}
	// TODO(abarganier): Implement redaction in the JSON serialization step. Ideally, done using struct tags
	// and code generation, similar to what we do for TELEMETRY events.
	payloadBytes, err := json.Marshal(m)
	if err != nil {
		Warningf(ctx, "failed to marshal structured event to JSON with meta: %v", meta)
		return
	}
	// TODO(abarganier): the log formatting code today already wraps JSON payloads in `{...}`, since originally,
	// the code generation used by the TELEMETRY channel to serialize events to JSON omitted the curly-braces
	// from the payload. We will eventually do the same when we get code gen working for our own structured events,
	// but for now, this is a hack to prevent needless nesting of our payload.
	payloadBytes = bytes.TrimPrefix(payloadBytes, trimPrefix)
	payloadBytes = bytes.TrimSuffix(payloadBytes, trimSuffix)

	entry := makeEntry(ctx, severity.INFO, logpb.Channel_STRUCTURED_EVENTS, 0 /*depth*/)
	entry.structured = true
	// TODO(abarganier): Once redaction is in place, we shouldn't need to cast to RedactableString here.
	entry.payload = makeRedactablePayload(ctx, redact.RedactableString(payloadBytes))

	outputLogEntry(ctx, entry)
	logging.processStructured(ctx, meta.EventType, payload)
}

// InfoE logs a structured event at severity.INFO before sending
// it to be processed by a StructuredLogProcessor
func InfoE(ctx context.Context, event logpb.EventPayload) {
	logStructuredEvent(ctx, severity.INFO, event, 1)
}

// WarnE logs a structured event at severity.WARNING before sending
// it to be processed by a StructuredLogProcessor
func WarnE(ctx context.Context, event logpb.EventPayload) {
	logStructuredEvent(ctx, severity.WARNING, event, 1)
}

// ErrorE logs a structured event at severity.ERROR before sending
// it to be processed by a StructuredLogProcessor
func ErrorE(ctx context.Context, event logpb.EventPayload) {
	logStructuredEvent(ctx, severity.ERROR, event, 1)
}

// FatalE logs a structured event at severity.FATAL before sending
// it to be processed by a StructuredLogProcessor
func FatalE(ctx context.Context, event logpb.EventPayload) {
	logStructuredEvent(ctx, severity.FATAL, event, 1)
}

// structuredEvent logs a logpb.EventPayload as a structured entry at a provided
// logpb.Severity. Once it is logged, it is sent to be processed by a StructuredLogProcessor.
func logStructuredEvent(
	ctx context.Context, sev logpb.Severity, event logpb.EventPayload, depth int,
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
		depth+1,
		event)

	outputLogEntry(ctx, entry)
	eventType := EventType(event.CommonDetails().EventType)
	logging.processStructured(ctx, eventType, event)
}

func outputLogEntry(ctx context.Context, entry logEntry) {

	if sp := getSpan(ctx); sp != nil {
		// Prevent `entry` from moving to the heap when this branch is not taken.
		heapEntry := entry
		eventInternal(sp, entry.sev >= severity.ERROR, &heapEntry)
	}

	logger := logging.getLogger(entry.ch)
	logger.outputLogEntry(entry)
}
