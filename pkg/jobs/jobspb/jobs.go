// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobspb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
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
