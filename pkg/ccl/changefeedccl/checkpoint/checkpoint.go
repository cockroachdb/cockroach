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
) jobspb. //lint:ignore SA1019 deprecated usage
		ChangefeedProgress_Checkpoint {
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
		//lint:ignore SA1019 deprecated usage
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

	//lint:ignore SA1019 deprecated usage
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

// SpanForwarder is an interface for forwarding spans to a changefeed.
// Implemented by span.Frontier.
type SpanForwarder interface {
	// Forward advances the timestamp for a span. Any part of the span that doesn't
	// overlap the tracked span set will be ignored. True is returned if the
	// frontier advanced as a result.
	Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error)

	// Frontier returns the minimum timestamp being tracked.
	Frontier() hlc.Timestamp
}

// Restore restores the checkpointed spans progress to the given SpanForwarder.
// If checkpoint is nil, it uses the oldCheckpointSpans and oldCheckpointTs to
// restore changefeed progress. Otherwise, it uses the given checkpoint. Returns
// error if something unexpected happens.
func Restore(
	sf SpanForwarder,
	oldCheckpointSpans []roachpb.Span,
	oldCheckpointTs hlc.Timestamp,
	checkpoint *jobspb.TimestampSpansMap,
) error {
	currHighWater := sf.Frontier()
	if checkpoint == nil {
		for _, checkpointedSp := range oldCheckpointSpans {
			if currHighWater.Less(oldCheckpointTs) {
				// (TODO:#140509) In ALTER CHANGEFEED and some testing scenarios,
				// changefeed only saves the checkpointed spans but not checkpointed
				// timestamps. In these cases, we want to avoid forwarding the spans to
				// its checkpointed timestamps (which will be 0). So we make sure not to
				// regress here.
				if _, err := sf.Forward(checkpointedSp, oldCheckpointTs); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for _, entry := range checkpoint.Entries {
		checkpointedTs := entry.Timestamp
		for _, checkpointedSp := range entry.Spans {
			if currHighWater.Less(checkpointedTs) {
				// Theoretically, this should not be possible since changefeed only
				// checkpoint spans that are above the high-water mark. But in some
				// testing scenarios, changefeed only saves checkpointed spans but not
				// checkpointed timestamps to indicate that they should be filtered out
				// during backfills. So we make sure not to regress here.
				if _, err := sf.Forward(checkpointedSp, checkpointedTs); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
