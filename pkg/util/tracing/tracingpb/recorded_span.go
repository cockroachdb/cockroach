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

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/redact"
	types "github.com/gogo/protobuf/types"
)

const (
	// AnonymousTagGroupName is the name of the anonymous tag group.
	AnonymousTagGroupName = ""
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

// FindTagGroup returns the tag group matching the supplied name, or nil if
// the name is not found.
func (s *RecordedSpan) FindTagGroup(name string) *TagGroup {
	for i := range s.TagGroups {
		tagGroup := &s.TagGroups[i]
		if tagGroup.Name == name {
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

// FindTag returns the value matching the supplied key, or nil if the key is
// not found.
func (tg *TagGroup) FindTag(key string) (string, bool) {
	for _, tag := range tg.Tags {
		if tag.Key == key {
			return tag.Value, true
		}
	}
	return "", false
}

// Combine returns the sum of m and other.
func (m OperationMetadata) Combine(other OperationMetadata) OperationMetadata {
	m.Count += other.Count
	m.ContainsUnfinished = m.ContainsUnfinished || other.ContainsUnfinished
	m.Duration += other.Duration
	return m
}

var _ redact.SafeFormatter = OperationMetadata{}

func (m OperationMetadata) String() string {
	return redact.StringWithoutMarkers(m)
}

// SafeFormat implements redact.SafeFormatter.
func (m OperationMetadata) SafeFormat(s redact.SafePrinter, _ rune) {
	s.Printf("{count: %d, duration %s", m.Count, humanizeutil.Duration(m.Duration))
	if m.ContainsUnfinished {
		s.Printf(", unfinished")
	}
	s.Print("}")
}

func (c CapturedStack) String() string {
	age := c.Age.Seconds()
	if c.Stack == "" && c.SharedSuffix > 0 {
		return fmt.Sprintf("stack as of %.1fs ago had not changed from previous stack", age)
	}
	if c.SharedLines > 0 {
		return fmt.Sprintf("stack as of %.1fs ago: %s\n ...+%d lines matching previous stack", age, c.Stack, c.SharedLines)
	}
	return fmt.Sprintf("stack as of %.1fs ago: %s", age, c.Stack)
}
