// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobspb

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// TimestampSpansGoMap is a go map from timestamps to spans.
type TimestampSpansGoMap map[hlc.Timestamp]roachpb.Spans

// MinTimestamp returns the min timestamp in the map.
// Returns the empty timestamp if map is empty.
func (m TimestampSpansGoMap) MinTimestamp() hlc.Timestamp {
	if len(m) == 0 {
		return hlc.Timestamp{}
	}
	minTS := hlc.MaxTimestamp
	for ts := range m {
		if ts.Less(minTS) {
			minTS = ts
		}
	}
	return minTS
}

// SpanCount returns the number of spans in the map.
func (m TimestampSpansGoMap) SpanCount() (count int) {
	for _, sp := range m {
		count += len(sp)
	}
	return count
}

// NewTimestampSpansMap takes a go timestamp-to-spans map and converts
// it into a new TimestampSpansMap.
func NewTimestampSpansMap(m TimestampSpansGoMap) *TimestampSpansMap {
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

// ToGoMap converts a TimestampSpansMap into a go map.
func (tsm *TimestampSpansMap) ToGoMap() TimestampSpansGoMap {
	if tsm == nil {
		return nil
	}
	m := make(TimestampSpansGoMap, len(tsm.Entries))
	for _, entry := range tsm.Entries {
		m[entry.Timestamp] = entry.Spans
	}
	return m
}

// IsEmpty returns whether the checkpoint is empty.
func (tsm *TimestampSpansMap) IsEmpty() bool {
	return tsm == nil || len(tsm.Entries) == 0
}

// IsEmpty returns whether the checkpoint is empty.
func (m *ChangefeedProgress_Checkpoint) IsEmpty() bool {
	return m == nil || (len(m.Spans) == 0 && m.Timestamp.IsEmpty())
}
