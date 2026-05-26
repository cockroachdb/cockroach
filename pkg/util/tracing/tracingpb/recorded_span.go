// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingpb

import (
	"fmt"
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

func (TraceID) SafeValue() {}

// SpanID is a probabilistically-unique span id.
type SpanID uint64

func (SpanID) SafeValue() {}

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

// String implements fmt.Stringer.
func (s *RecordedSpan) String() string {
	return redact.Sprint(s).StripMarkers()
}

// SafeFormat implements redact.SafeFormatter.
func (s *RecordedSpan) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("=== %s (id: %d parent: %d)\n", redact.Safe(s.Operation), s.SpanID, s.ParentSpanID)
	for _, ev := range s.Logs {
		w.Printf("%s %s\n", redact.Safe(ev.Time.UTC().Format(time.RFC3339Nano)), ev.Msg())
	}
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

// EnsureTagGroup returns a reference to the tag group with the given name,
// creating it if it doesn't exist.
func (s *RecordedSpan) EnsureTagGroup(name string) *TagGroup {
	if tg := s.FindTagGroup(name); tg != nil {
		return tg
	}
	s.TagGroups = append(s.TagGroups, TagGroup{Name: name})
	return &s.TagGroups[len(s.TagGroups)-1]
}

// AddStructuredRecord adds r to s' structured logs and returns r.MemorySize().
//
// Note that the limit on the size of a span's structured records is not
// enforced here. If it's needed, the caller has to do it.
func (s *RecordedSpan) AddStructuredRecord(r StructuredRecord) int64 {
	size := int64(r.MemorySize())
	s.StructuredRecords = append(s.StructuredRecords, r)
	s.StructuredRecordsSizeBytes += size
	return size
}

// TrimStructured potentially drops structured log records in order to keep the
// structured record size <= maxSize. The prefix of the records that sum up to
// <= maxSize is kept.
func (s *RecordedSpan) TrimStructured(maxSize int64) int64 {
	if s.StructuredRecordsSizeBytes <= maxSize {
		return 0
	}
	size := int64(0)
	for i := range s.StructuredRecords {
		recordSize := int64(s.StructuredRecords[i].MemorySize())
		if size+recordSize > maxSize {
			// Zero-out the slice elements that are about to be trimmed, so they can
			// be GC'ed.
			for j := i; j < len(s.StructuredRecords); j++ {
				s.StructuredRecords[j] = StructuredRecord{}
			}
			// Trim all records from i onwards.
			s.StructuredRecords = s.StructuredRecords[:i]
			break
		}
	}
	oldSize := s.StructuredRecordsSizeBytes
	s.StructuredRecordsSizeBytes = size
	return oldSize - size
}

// AddTag adds a tag to the group. If a tag with the given key already exists,
// its value is updated.
func (tg *TagGroup) AddTag(k, v string) {
	for i := range tg.Tags {
		tag := &tg.Tags[i]
		if tag.Key == k {
			tag.Value = v
			return
		}
	}
	tg.Tags = append(tg.Tags, Tag{Key: k, Value: v})
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
	s.SafeRune('}')
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
