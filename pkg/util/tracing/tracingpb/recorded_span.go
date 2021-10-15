// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracingpb

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/redact"
	types "github.com/gogo/protobuf/types"
)

// TraceID is a probabilistically-unique id, shared by all spans in a trace.
type TraceID uint64

// SpanID is a probabilistically-unique span id.
type SpanID uint64

// LogMessageField is the field name used for the log message in a LogRecord.
const LogMessageField = "event"

func (s *RecordedSpan) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("=== %s (id: %d parent: %d)\n", s.Operation, s.SpanID, s.ParentSpanID))
	for _, ev := range s.Logs {
		sb.WriteString(fmt.Sprintf("%s %s\n", ev.Time.UTC().Format(time.RFC3339Nano), ev.Msg(s.RedactableLogs)))
	}
	return sb.String()
}

// Structured visits the data passed to RecordStructured for the Span from which
// the RecordedSpan was created.
func (s *RecordedSpan) Structured(visit func(*types.Any, time.Time)) {
	for _, sr := range s.StructuredRecords {
		visit(sr.Payload, sr.Time)
	}
}

// Msg extracts the message of the LogRecord, which is either in an "event" or
// "error" field.
func (l LogRecord) Msg(redactable bool) string {
	if l.Message != "" {
		return l.Message.StripMarkers(redactable)
	}

	// Compatibility with 21.2: look at l.DeprecatedFields.
	for _, f := range l.DeprecatedFields {
		key := f.Key
		if key == LogMessageField {
			return f.Value.StripMarkers(redactable)
		}
		if key == "error" {
			return fmt.Sprint("error: %s", f.Value)
		}
	}
	return ""
}

// MemorySize implements the sizable interface.
func (l *LogRecord) MemorySize() int {
	return 3*8 + // 3 words for time.Time
		2*8 + // 2 words for StringHeader
		len(l.Message)
}

// MemorySize implements the sizable interface.
func (r *StructuredRecord) MemorySize() int {
	return 3*8 + // 3 words for time.Time
		1*8 + // 1 words for *Any
		r.Payload.Size() // TODO(andrei): this is the encoded size, not the mem size
}

// MaybeRedactableString is a string that doesn't know if it is a redactable one
// or not. By default, it should be treated as not being one.
//
// This is a helper for trace log messages, where the fact that a given message
// is redactable is stored in the surrounding recording.
type MaybeRedactableString string

// StripMarkers strips the markers if this is a redactable string,
// otherwise returns the string unmodified.
func (m MaybeRedactableString) StripMarkers(redactable bool) string {
	if redactable {
		return redact.RedactableString(m).StripMarkers()
	}
	return string(m)
}
