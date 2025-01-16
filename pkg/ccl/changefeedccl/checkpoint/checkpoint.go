// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package checkpoint contains code responsible for handling changefeed
// checkpoints.
package checkpoint

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SpanIter is an iterator over a collection of spans.
type SpanIter func(forEachSpan span.Operation)

// Make creates a checkpoint with as many spans that should be checkpointed (are
// above the highwater mark) as can fit in maxBytes, along with the earliest
// timestamp of the checkpointed spans. A SpanGroup is used to merge adjacent
// spans above the high-water mark.
func Make(
	frontier hlc.Timestamp, forEachSpan SpanIter, maxBytes int64, metrics *Metrics,
) jobspb.ChangefeedProgress_Checkpoint {
	start := timeutil.Now()

	// Collect leading spans into a SpanGroup to merge adjacent spans and store
	// the lowest timestamp found.
	var checkpointSpanGroup roachpb.SpanGroup
	checkpointTS := hlc.MaxTimestamp
	forEachSpan(func(s roachpb.Span, ts hlc.Timestamp) span.OpResult {
		if frontier.Less(ts) {
			checkpointSpanGroup.Add(s)
			if ts.Less(checkpointTS) {
				checkpointTS = ts
			}
		}
		return span.ContinueMatch
	})
	if checkpointSpanGroup.Len() == 0 {
		return jobspb.ChangefeedProgress_Checkpoint{}
	}

	// Ensure we only return up to maxBytes spans.
	var checkpointSpans []roachpb.Span
	var used int64
	for _, span := range checkpointSpanGroup.Slice() {
		used += int64(len(span.Key)) + int64(len(span.EndKey))
		if used > maxBytes {
			break
		}
		checkpointSpans = append(checkpointSpans, span)
	}

	cp := jobspb.ChangefeedProgress_Checkpoint{
		Spans:     checkpointSpans,
		Timestamp: checkpointTS,
	}

	if metrics != nil {
		metrics.CreateNanos.RecordValue(int64(timeutil.Since(start)))
		metrics.TotalBytes.RecordValue(int64(cp.Size()))
		metrics.SpanCount.RecordValue(int64(len(cp.Spans)))
	}

	return cp
}
