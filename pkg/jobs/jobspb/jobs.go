// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobspb

import (
	"iter"
	"maps"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

// JobID is the ID of a job.
type JobID = catpb.JobID

// InvalidJobID is the zero value for JobID corresponding to no job.
const InvalidJobID = catpb.InvalidJobID

// ToText implements the ProtobinExecutionDetailFile interface.
func (t *TraceData) ToText() []byte {
	rec := tracingpb.Recording(t.CollectedSpans)
	return []byte(rec.String())
}

// RestoreFrontierEntries is a slice of RestoreFrontierEntries.
type RestoreFrontierEntries []RestoreProgress_FrontierEntry

func (fes RestoreFrontierEntries) Equal(fes2 RestoreFrontierEntries) bool {
	if len(fes) != len(fes2) {
		return false
	}
	for i := range fes {
		if !fes[i].Span.Equal(fes2[i].Span) {
			return false
		}
		if !fes[i].Timestamp.Equal(fes2[i].Timestamp) {
			return false
		}
	}
	return true
}

// ResolvedSpanEntries is a slice of ResolvedSpanEntries.
// TODO(msbutler): use generics and combine with above.
type ResolvedSpanEntries []ResolvedSpan

func (rse ResolvedSpanEntries) Equal(rse2 ResolvedSpanEntries) bool {
	if len(rse) != len(rse2) {
		return false
	}
	for i := range rse {
		if !rse[i].Span.Equal(rse2[i].Span) {
			return false
		}
		if !rse[i].Timestamp.Equal(rse2[i].Timestamp) {
			return false
		}
	}
	return true
}

// SafeValue implements the redact.SafeValue interface.
func (ResolvedSpan_BoundaryType) SafeValue() {}

// NewTimestampSpansMap takes a go timestamp-to-spans go map and converts
// it into a new TimestampSpansMap.
func NewTimestampSpansMap(m map[hlc.Timestamp]roachpb.Spans) *TimestampSpansMap {
	if len(m) == 0 {
		return nil
	}
	tsm := &TimestampSpansMap{
		Entries: make([]TimestampSpansMap_Entry, 0, len(m)),
	}
	for ts, spans := range m {
		tsm.Entries = append(tsm.Entries, TimestampSpansMap_Entry{
			Timestamp: ts,
			Spans:     spans,
		})
	}
	return tsm
}

// All returns an iterator over the entries in the map.
func (tsm *TimestampSpansMap) All() iter.Seq2[hlc.Timestamp, roachpb.Spans] {
	return func(yield func(hlc.Timestamp, roachpb.Spans) bool) {
		if tsm.IsEmpty() {
			return
		}
		for _, entry := range tsm.Entries {
			if !yield(entry.Timestamp, entry.Spans) {
				return
			}
		}
	}
}

// MinTimestamp returns the min timestamp in the map.
// Returns the empty timestamp if map is empty.
func (tsm *TimestampSpansMap) MinTimestamp() hlc.Timestamp {
	return iterutil.MinFunc(iterutil.Keys(tsm.All()), hlc.Timestamp.Compare)
}

// SpanCount returns the number of spans in the map.
func (tsm *TimestampSpansMap) SpanCount() (count int) {
	for _, sp := range tsm.All() {
		count += len(sp)
	}
	return count
}

// Equal returns whether two maps contain the same entries.
func (tsm *TimestampSpansMap) Equal(other *TimestampSpansMap) bool {
	return maps.EqualFunc(
		maps.Collect(tsm.All()),
		maps.Collect(other.All()),
		func(s roachpb.Spans, t roachpb.Spans) bool {
			return slices.EqualFunc(s, t, roachpb.Span.Equal)
		})
}

// IsEmpty returns whether the map is empty.
func (tsm *TimestampSpansMap) IsEmpty() bool {
	return tsm == nil || len(tsm.Entries) == 0
}

// IsEmpty returns whether the checkpoint is empty.
func (m *ChangefeedProgress_Checkpoint) IsEmpty() bool {
	return m == nil || (len(m.Spans) == 0 && m.Timestamp.IsEmpty())
}
