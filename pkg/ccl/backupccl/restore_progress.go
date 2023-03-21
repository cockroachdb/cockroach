// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	spanUtils "github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// restoreCheckpointMaxBytes controls the maximum number of key bytes that will be added
// to the checkpoint record. The default is set using the same reasoning as
// changefeed.frontier_checkpoint_max_bytes.
var restoreCheckpointMaxBytes = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"restore.frontier_checkpoint_max_bytes",
	"controls the maximum size of the restore checkpoint frontier as a the sum of the (span,"+
		"timestamp) tuples",
	1<<20, // 1 MiB
)

// completedSpanTime indicates the timestamp that the progress frontier will
// mark completed spans with.
var completedSpanTime = hlc.MaxTimestamp

type progressTracker struct {
	// nextRequiredSpanKey maps a required span endkey to the subsequent requiredSpan's startKey.
	nextRequiredSpanKey map[string]roachpb.Key

	maxBytes int64

	mu struct {
		// fields that may get updated while read are put in the lock.
		syncutil.Mutex

		checkpointFrontier *spanUtils.Frontier

		// res tracks the amount of data that has been ingested.
		res roachpb.RowCount

		// Note that the fields below are used for the deprecated high watermark progress
		// tracker.
		// highWaterMark represents the index into the requestsCompleted map.
		highWaterMark int64
		ceiling       int64

		// As part of job progress tracking, inFlightImportSpans tracks all the
		// spans that have been generated are being processed by the processors in
		// distRestore. requestsCompleleted tracks the spans from
		// inFlightImportSpans that have completed its processing. Once all spans up
		// to index N have been processed (and appear in requestsCompleted), then
		// any spans with index < N will be removed from both inFlightImportSpans
		// and requestsCompleted maps.
		inFlightImportSpans map[int64]roachpb.Span
		requestsCompleted   map[int64]bool
	}
	useFrontier        bool
	inFlightSpanFeeder chan execinfrapb.RestoreSpanEntry

	// endTime is the restore as of timestamp. This can be empty, and an empty timestamp
	// indicates a restore of the latest revision.
	endTime hlc.Timestamp
}

func makeProgressTracker(
	requiredSpans roachpb.Spans,
	persistedSpans []jobspb.RestoreProgress_FrontierEntry,
	useFrontier bool,
	maxBytes int64,
	endTime hlc.Timestamp,
) (*progressTracker, error) {

	var (
		checkpointFrontier  *spanUtils.Frontier
		err                 error
		nextRequiredSpanKey map[string]roachpb.Key
		inFlightSpanFeeder  chan execinfrapb.RestoreSpanEntry
	)
	if useFrontier {
		checkpointFrontier, err = loadCheckpointFrontier(requiredSpans, persistedSpans)
		if err != nil {
			return nil, err
		}
		nextRequiredSpanKey = make(map[string]roachpb.Key)
		for i := 0; i < len(requiredSpans)-1; i++ {
			nextRequiredSpanKey[requiredSpans[i].EndKey.String()] = requiredSpans[i+1].Key
		}

	} else {
		inFlightSpanFeeder = make(chan execinfrapb.RestoreSpanEntry, 1000)
	}

	pt := &progressTracker{}
	pt.mu.checkpointFrontier = checkpointFrontier
	pt.mu.highWaterMark = -1
	pt.mu.ceiling = 0
	pt.mu.inFlightImportSpans = make(map[int64]roachpb.Span)
	pt.mu.requestsCompleted = make(map[int64]bool)
	pt.nextRequiredSpanKey = nextRequiredSpanKey
	pt.maxBytes = maxBytes
	pt.useFrontier = useFrontier
	pt.inFlightSpanFeeder = inFlightSpanFeeder
	pt.endTime = endTime
	return pt, nil
}

func loadCheckpointFrontier(
	requiredSpans roachpb.Spans, persistedSpans []jobspb.RestoreProgress_FrontierEntry,
) (*spanUtils.Frontier, error) {
	numRequiredSpans := len(requiredSpans) - 1
	contiguousSpan := roachpb.Span{
		Key:    requiredSpans[0].Key,
		EndKey: requiredSpans[numRequiredSpans].EndKey,
	}
	checkpointFrontier, err := spanUtils.MakeFrontier(contiguousSpan)
	if err != nil {
		return nil, err
	}
	for _, sp := range persistedSpans {
		_, err = checkpointFrontier.Forward(sp.Span, sp.Timestamp)
		if err != nil {
			return nil, err
		}
	}
	return checkpointFrontier, err
}

// persistFrontier converts a span frontier into a list of (span, timestamp)
// tuples that can persist to disk. If the user passes a nonzero maxBytes, the
// first N spans in the frontier that remain below the maxBytes memory limit
// will return.
func persistFrontier(
	frontier *spanUtils.Frontier, maxBytes int64,
) []jobspb.RestoreProgress_FrontierEntry {
	var used int64
	completedSpansSlice := make([]jobspb.RestoreProgress_FrontierEntry, 0)
	frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done spanUtils.OpResult) {
		if ts.Equal(completedSpanTime) {

			// Persist the first N spans in the frontier that remain below the memory limit.
			used += int64(len(sp.Key) + len(sp.EndKey) + ts.Size())
			if maxBytes != 0 && used > maxBytes {
				return spanUtils.StopMatch
			}
			// TODO (msbutler): we may want to persist spans that have been
			// restored up to a certain system time, if on resume, we build
			// facilities in the generative split and scatter processor to
			// create a restore span entry with files from a minimum
			// timestamp.
			completedSpansSlice = append(completedSpansSlice,
				jobspb.RestoreProgress_FrontierEntry{Span: sp, Timestamp: ts})
		}
		return spanUtils.ContinueMatch
	})
	return completedSpansSlice
}

func (pt *progressTracker) updateJobCallback(
	progressedCtx context.Context, progressDetails jobspb.ProgressDetails,
) {
	switch d := progressDetails.(type) {
	case *jobspb.Progress_Restore:
		pt.mu.Lock()
		if pt.useFrontier {
			// TODO (msbutler): this requires iterating over every span in the frontier,
			// and rewriting every completed required span to disk.
			// We may want to be more intelligent about this.
			d.Restore.Checkpoint = persistFrontier(pt.mu.checkpointFrontier, pt.maxBytes)
		} else {
			if pt.mu.highWaterMark >= 0 {
				d.Restore.HighWater = pt.mu.inFlightImportSpans[pt.mu.highWaterMark].Key
			}
		}
		pt.mu.Unlock()
	default:
		log.Errorf(progressedCtx, "job payload had unexpected type %T", d)
	}
}

// ingestUpdate updates the progressTracker after a progress update returns from
// the distributed processors. ingestUpdate returns true if the update indicates
// the completion of a span, false otherwise.
func (pt *progressTracker) ingestUpdate(
	ctx context.Context, rawProgress *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) (bool, error) {
	var progDetails backuppb.RestoreProgress
	if err := pbtypes.UnmarshalAny(&rawProgress.ProgressDetails, &progDetails); err != nil {
		log.Errorf(ctx, "unable to unmarshal restore progress details: %+v", err)
	}
	pt.mu.Lock()
	defer pt.mu.Unlock()

	// After the change to bound the memory used by SST iterator, we can now
	// get a progress entry for a partially completed span. Do not mark the
	// span as done when receiving these entries.
	//
	// NB: progDetails with empty CompleteUpTo timestamps always denote the
	// completion of a span, as either we've received details from a
	// processor that does not memory monitor, or the restore's endTime was
	// empty.
	if !progDetails.CompleteUpTo.IsEmpty() && !progDetails.CompleteUpTo.Equal(pt.endTime) {
		return false, nil
	}

	pt.mu.res.Add(progDetails.Summary)
	if pt.useFrontier {
		updateSpan := progDetails.DataSpan.Clone()
		// If the completedSpan has the same end key as a requiredSpan_i, forward
		// the frontier for the span [completedSpan_startKey,
		// requiredSpan_i+1_startKey]. This trick ensures the span frontier will
		// contain a single entry when the restore completes. Recall that requiredSpans are
		// disjoint, and a spanFrontier never merges disjoint spans. So, without
		// this trick, the spanFrontier will have O(requiredSpans) entries when the
		// restore completes. This trick ensures all spans persisted to the frontier are adjacent,
		// and consequently, will eventually merge.
		//
		// Here's a visual example:
		//  - this restore has two required spans: [a,d) and [e,h).
		//  - the restore span entry [c,d) just completed, implying the frontier logically looks like:
		//
		//	tC|             x---o
		//	t0|
		//	  keys--a---b---c---d---e---f---g---h->
		//
		// r-spans: |---span1---|   |---span2---|
		//
		// - since [c,d)'s endkey equals the required span (a,d]'s endkey,
		//   also update the gap between required span 1 and 2 in the frontier:
		//
		//	tC|             x-------o
		//	t0|
		//	  keys--a---b---c---d---e---f---g---h->
		//
		// r-spans: |---span1---|   |---span2---|
		//
		// - this will ensure that when all subspans in required spans 1 and 2 complete,
		//   the checkpoint frontier has one span:
		//
		//	tC|     x---------------------------o
		//	t0|
		//	  keys--a---b---c---d---e---f---g---h->
		//
		// r-spans: |---span1---|   |---span2---|
		if newEndKey, ok := pt.nextRequiredSpanKey[updateSpan.EndKey.String()]; ok {
			updateSpan.EndKey = newEndKey
		}
		if _, err := pt.mu.checkpointFrontier.Forward(updateSpan, completedSpanTime); err != nil {
			return false, err
		}
	} else {
		idx := progDetails.ProgressIdx

		if idx >= pt.mu.ceiling {
			for i := pt.mu.ceiling; i <= idx; i++ {
				importSpan, ok := <-pt.inFlightSpanFeeder
				if !ok {
					// The channel has been closed, there is nothing left to do.
					log.Infof(ctx, "exiting restore checkpoint loop as the import span channel has been closed")
					return true, nil
				}
				pt.mu.inFlightImportSpans[i] = importSpan.Span
			}
			pt.mu.ceiling = idx + 1
		}

		if sp, ok := pt.mu.inFlightImportSpans[idx]; ok {
			// Assert that we're actually marking the correct span done. See #23977.
			if !sp.Key.Equal(progDetails.DataSpan.Key) {
				return false, errors.Newf("request %d for span %v does not match import span for same idx: %v",
					idx, progDetails.DataSpan, sp,
				)
			}
			pt.mu.requestsCompleted[idx] = true
			prevHighWater := pt.mu.highWaterMark
			for j := pt.mu.highWaterMark + 1; j < pt.mu.ceiling && pt.mu.requestsCompleted[j]; j++ {
				pt.mu.highWaterMark = j
			}
			for j := prevHighWater; j < pt.mu.highWaterMark; j++ {
				delete(pt.mu.requestsCompleted, j)
				delete(pt.mu.inFlightImportSpans, j)
			}
		}
	}
	return true, nil
}
