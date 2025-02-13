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
	"github.com/cockroachdb/errors"
)

// SpanIter is an iterator over a collection of spans.
type SpanIter func(forEachSpan span.Operation)

// Make creates a checkpoint with as many spans that should be checkpointed (are
// above the highwater mark) as can fit in maxBytes, along with the earliest
// timestamp of the checkpointed spans. Adjacent spans at the same timestamp
// are merged together to reduce checkpoint size.
func Make(
	overallResolved hlc.Timestamp, forEachSpan SpanIter, maxBytes int64, metrics *Metrics,
) *jobspb.TimestampSpansMap {
	start := timeutil.Now()

	spanGroupMap := make(map[hlc.Timestamp]*roachpb.SpanGroup)
	forEachSpan(func(s roachpb.Span, ts hlc.Timestamp) span.OpResult {
		if ts.After(overallResolved) {
			if spanGroupMap[ts] == nil {
				spanGroupMap[ts] = new(roachpb.SpanGroup)
			}
			spanGroupMap[ts].Add(s)
		}
		return span.ContinueMatch
	})
	if len(spanGroupMap) == 0 {
		return nil
	}

	checkpointSpansMap := make(map[hlc.Timestamp]roachpb.Spans)
	var totalSpanKeyBytes int64
	for ts, spanGroup := range spanGroupMap {
		for _, sp := range spanGroup.Slice() {
			spanKeyBytes := int64(len(sp.Key)) + int64(len(sp.EndKey))
			if totalSpanKeyBytes+spanKeyBytes > maxBytes {
				break
			}
			checkpointSpansMap[ts] = append(checkpointSpansMap[ts], sp)
			totalSpanKeyBytes += spanKeyBytes
		}
	}
	cp := jobspb.NewTimestampSpansMap(checkpointSpansMap)
	if cp == nil {
		return nil
	}

	if metrics != nil {
		metrics.CreateNanos.RecordValue(int64(timeutil.Since(start)))
		metrics.TotalBytes.RecordValue(int64(cp.Size()))
		metrics.SpanCount.RecordValue(int64(cp.SpanCount()))
	}

	return cp
}

// SpanForwarder is an interface for forwarding spans to a changefeed.
type SpanForwarder interface {
	Forward(span roachpb.Span, ts hlc.Timestamp) (bool, error)
}

// Restore restores the saved progress from a checkpoint to the given SpanForwarder.
func Restore(sf SpanForwarder, checkpoint *jobspb.TimestampSpansMap) error {
	for ts, spans := range checkpoint.All() {
		if ts.IsEmpty() {
			return errors.New("checkpoint timestamp is empty")
		}
		for _, sp := range spans {
			if _, err := sf.Forward(sp, ts); err != nil {
				return err
			}
		}
	}
	return nil
}

// ConvertFromLegacyCheckpoint converts a checkpoint from the legacy format
// into the current format.
func ConvertFromLegacyCheckpoint(
	//lint:ignore SA1019 deprecated usage
	checkpoint *jobspb.ChangefeedProgress_Checkpoint,
	statementTime hlc.Timestamp,
	initialHighWater hlc.Timestamp,
) *jobspb.TimestampSpansMap {
	if checkpoint.IsEmpty() {
		return nil
	}

	checkpointTS := checkpoint.Timestamp

	// Checkpoint records from 21.2 were used only for backfills and did not store
	// the timestamp, since in a backfill it must either be the StatementTime for
	// an initial backfill, or right after the high-water for schema backfills.
	if checkpointTS.IsEmpty() {
		if initialHighWater.IsEmpty() {
			checkpointTS = statementTime
		} else {
			checkpointTS = initialHighWater.Next()
		}
	}

	return jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
		checkpointTS: checkpoint.Spans,
	})
}

// ConvertToLegacyCheckpoint converts a checkpoint from the current format
// into the legacy format.
func ConvertToLegacyCheckpoint(
	checkpoint *jobspb.TimestampSpansMap,
) *jobspb. //lint:ignore SA1019 deprecated usage
		ChangefeedProgress_Checkpoint {
	if checkpoint.IsEmpty() {
		return nil
	}

	// Collect leading spans into a SpanGroup to merge adjacent spans.
	var checkpointSpanGroup roachpb.SpanGroup
	for _, spans := range checkpoint.All() {
		checkpointSpanGroup.Add(spans...)
	}
	if checkpointSpanGroup.Len() == 0 {
		return nil
	}

	//lint:ignore SA1019 deprecated usage
	return &jobspb.ChangefeedProgress_Checkpoint{
		Spans:     checkpointSpanGroup.Slice(),
		Timestamp: checkpoint.MinTimestamp(),
	}
}
