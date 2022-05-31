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

// Recording represents a group of RecordedSpans rooted at a fixed root span, as
// returned by GetRecording. Spans are sorted by StartTime.
type Recording []RecordedSpan

// Less implements sort.Interface.
func (r Recording) Less(i, j int) bool {
	return r[i].StartTime.Before(r[j].StartTime)
}

// Swap implements sort.Interface.
func (r Recording) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// Len implements sort.Interface.
func (r Recording) Len() int {
	return len(r)
}

// LogMessageField is the field name used for the log message in a LogRecord.
const LogMessageField = "event"

func (s *RecordedSpan) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("=== %s (id: %d parent: %d)\n", s.Operation, s.SpanID, s.ParentSpanID))
	for _, ev := range s.Logs {
		sb.WriteString(fmt.Sprintf("%s %s\n", ev.Time.UTC().Format(time.RFC3339Nano), ev.Msg()))
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

func (s *RecordedSpan) FindTagGroup(name string) *TagGroup {
	for _, tagGroup := range s.TagGroups {
		if tagGroup.GetName() == name {
			return tagGroup
		}
	}
	return nil
}

// Msg extracts the message of the LogRecord, which is either in an "event" or
// "error" field.
func (l LogRecord) Msg() redact.RedactableString {
	if l.Message != "" {
		return l.Message
	}

	// Compatibility with 21.2: look at l.DeprecatedFields.
	for _, f := range l.DeprecatedFields {
		key := f.Key
		if key == LogMessageField {
			return f.Value
		}
		if key == "error" {
			return redact.Sprintf("error: %s", f.Value)
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

func (tg *TagGroup) GetName() string {
	if len(tg.Tags) == 1 {
		return tg.Tags[0].Key
	}
	return tg.Name
}

func (tg *TagGroup) FindTag(key string) *string {
	for _, tag := range tg.Tags {
		if tag.Key == key {
			return &tag.Value
		}
	}
	return nil
}
