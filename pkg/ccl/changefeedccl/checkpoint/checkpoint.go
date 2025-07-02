// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package checkpoint contains code responsible for handling changefeed
// checkpoints.
package checkpoint

import (
	"iter"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Make creates a checkpoint with as many spans that should be checkpointed (are
// above the highwater mark) as can fit in maxBytes, along with the earliest
// timestamp of the checkpointed spans. Adjacent spans at the same timestamp
// are merged together to reduce checkpoint size.
func Make(
	overallResolved hlc.Timestamp,
	spans iter.Seq2[roachpb.Span, hlc.Timestamp],
	maxBytes int64,
	metrics *Metrics,
) *jobspb.TimestampSpansMap {
	start := timeutil.Now()

	spanGroupMap := make(map[hlc.Timestamp]*roachpb.SpanGroup)
	for s, ts := range spans {
		if ts.After(overallResolved) {
			if spanGroupMap[ts] == nil {
				spanGroupMap[ts] = new(roachpb.SpanGroup)
			}
			spanGroupMap[ts].Add(s)
		}
	}
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
		metrics.TimestampCount.RecordValue(int64(cp.TimestampCount()))
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
