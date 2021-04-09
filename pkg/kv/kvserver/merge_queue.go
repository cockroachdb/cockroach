// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	// mergeQueuePurgatoryCheckInterval is the interval at which replicas in
	// purgatory make merge attempts. Since merges are relatively untested, the
	// reasons that a range may fail to merge are unknown, so the merge queue has
	// a large purgatory interval.
	mergeQueuePurgatoryCheckInterval = 1 * time.Minute

	// The current implementation of merges requires rebalancing replicas on the
	// right-hand range so that they are collocated with those on the left-hand
	// range. This is expensive, so limit to one merge at a time.
	mergeQueueConcurrency = 1
)

// MergeQueueInterval is a setting that controls how often the merge queue waits
// between processing replicas.
var MergeQueueInterval = settings.RegisterDurationSetting(
	"kv.range_merge.queue_interval",
	"how long the merge queue waits between processing replicas",
	time.Second,
	settings.NonNegativeDuration,
)

// mergeQueue manages a queue of ranges slated to be merged with their right-
// hand neighbor.
//
// A range will only be queued if it is beneath the minimum size threshold. Once
// queued, the size of the right-hand neighbor will additionally be checked;
// merges can only proceed if a) the right-hand neighbor is beneath the minimum
// size threshold, and b) the merged range would not need to be immediately
// split, e.g. because the new range would exceed the maximum size threshold.
//
// Note that the merge queue is not capable of initiating all possible merges.
// Consider the example below:
//
//      /Table/51/1    /Table/51/2    /Table/52
//         32MB            0MB           32MB
//
// The range beginning at /Table/51/2 is empty and would, ideally, be merged
// away. The range to its left, /Table/51/1, will not propose a merge because it
// is over the minimum size threshold. And /Table/51/2 will not propose a merge
// because the next range, /Table/52, is a new table and thus the split is
// mandatory.
//
// There are several ways to solve this. /Table/51/2 could look both left and
// right to find a merge partner, but discovering ones left neighbor is rather
// difficult and involves scanning the meta ranges. /Table/51/1 could propose a
// merge even though it's over the minimum size threshold, but this would result
// in a lot more RangeStats requests--essentially every range would send a
// RangeStats request on every scanner cycle.
//
// The current approach seems to be a nice balance of finding nearly all
// mergeable ranges without sending many RPCs. It has the additional nice
// property of not sending any RPCs to meta ranges until a merge is actually
// initiated.
type mergeQueue struct {
	*baseQueue
	db       *kv.DB
	purgChan <-chan time.Time
}

func newMergeQueue(store *Store, db *kv.DB, gossip *gossip.Gossip) *mergeQueue {
	mq := &mergeQueue{
		db:       db,
		purgChan: time.NewTicker(mergeQueuePurgatoryCheckInterval).C,
	}
	mq.baseQueue = newBaseQueue(
		"merge", mq, store, gossip,
		queueConfig{
			maxSize:        defaultQueueMaxSize,
			maxConcurrency: mergeQueueConcurrency,
			// TODO(ajwerner): Sometimes the merge queue needs to send multiple
			// snapshots, but the timeout function here is configured based on the
			// duration required to send a single snapshot. That being said, this
			// timeout provides leeway for snapshots to be 10x slower than the
			// specified rate and still respects the queue processing minimum timeout.
			// While using the below function is certainly better than just using the
			// default timeout, it would be better to have a function which takes into
			// account how many snapshots processing will need to send. That might be
			// hard to determine ahead of time. An alternative would be to calculate
			// the timeout with a function that additionally considers the replication
			// factor.
			processTimeoutFunc:   makeRateLimitedTimeoutFunc(rebalanceSnapshotRate),
			needsLease:           true,
			needsSystemConfig:    true,
			acceptsUnsplitRanges: false,
			successes:            store.metrics.MergeQueueSuccesses,
			failures:             store.metrics.MergeQueueFailures,
			pending:              store.metrics.MergeQueuePending,
			processingNanos:      store.metrics.MergeQueueProcessingNanos,
			purgatory:            store.metrics.MergeQueuePurgatory,
		},
	)
	return mq
}

func (mq *mergeQueue) enabled() bool {
	st := mq.store.ClusterSettings()
	return kvserverbase.MergeQueueEnabled.Get(&st.SV)
}

func (mq *mergeQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, sysCfg *config.SystemConfig,
) (shouldQ bool, priority float64) {
	if !mq.enabled() {
		return false, 0
	}

	desc := repl.Desc()

	if desc.EndKey.Equal(roachpb.RKeyMax) {
		// The last range has no right-hand neighbor to merge with.
		return false, 0
	}

	if sysCfg.NeedsSplit(ctx, desc.StartKey, desc.EndKey.Next()) {
		// This range would need to be split if it extended just one key further.
		// There is thus no possible right-hand neighbor that it could be merged
		// with.
		return false, 0
	}

	sizeRatio := float64(repl.GetMVCCStats().Total()) / float64(repl.GetMinBytes())
	if math.IsNaN(sizeRatio) || sizeRatio >= 1 {
		// This range is above the minimum size threshold. It does not need to be
		// merged.
		return false, 0
	}

	// Invert sizeRatio to compute the priority so that smaller ranges are merged
	// before larger ranges.
	priority = 1 - sizeRatio
	return true, priority
}

// rangeMergePurgatoryError wraps an error that occurs during merging to
// indicate that the error should send the range to purgatory.
type rangeMergePurgatoryError struct{ error }

func (rangeMergePurgatoryError) purgatoryErrorMarker() {}

var _ purgatoryError = rangeMergePurgatoryError{}

func (mq *mergeQueue) requestRangeStats(
	ctx context.Context, key roachpb.Key,
) (*roachpb.RangeDescriptor, enginepb.MVCCStats, float64, error) {

	var ba roachpb.BatchRequest
	ba.Add(&roachpb.RangeStatsRequest{
		RequestHeader: roachpb.RequestHeader{Key: key},
	})

	br, pErr := mq.db.NonTransactionalSender().Send(ctx, ba)
	if pErr != nil {
		return nil, enginepb.MVCCStats{}, 0, pErr.GoError()
	}
	res := br.Responses[0].GetInner().(*roachpb.RangeStatsResponse)
	return &res.RangeInfo.Desc, res.MVCCStats, res.QueriesPerSecond, nil
}

func (mq *mergeQueue) process(
	ctx context.Context, lhsRepl *Replica, sysCfg *config.SystemConfig,
) (processed bool, err error) {
	if !mq.enabled() {
		log.VEventf(ctx, 2, "skipping merge: queue has been disabled")
		return false, nil
	}

	lhsStats := lhsRepl.GetMVCCStats()
	minBytes := lhsRepl.GetMinBytes()
	if lhsStats.Total() >= minBytes {
		log.VEventf(ctx, 2, "skipping merge: LHS meets minimum size threshold %d with %d bytes",
			minBytes, lhsStats.Total())
		return false, nil
	}

	lhsDesc := lhsRepl.Desc()
	lhsQPS := lhsRepl.GetSplitQPS()
	rhsDesc, rhsStats, rhsQPS, err := mq.requestRangeStats(ctx, lhsDesc.EndKey.AsRawKey())
	if err != nil {
		return false, err
	}
	if rhsStats.Total() >= minBytes {
		log.VEventf(ctx, 2, "skipping merge: RHS meets minimum size threshold %d with %d bytes",
			minBytes, lhsStats.Total())
		return false, nil
	}

	// Range was manually split and not expired, so skip merging.
	now := mq.store.Clock().NowAsClockTimestamp()
	if now.ToTimestamp().Less(rhsDesc.GetStickyBit()) {
		log.VEventf(ctx, 2, "skipping merge: ranges were manually split and sticky bit was not expired")
		// TODO(jeffreyxiao): Consider returning a purgatory error to avoid
		// repeatedly processing ranges that cannot be merged.
		return false, nil
	}

	mergedDesc := &roachpb.RangeDescriptor{
		StartKey: lhsDesc.StartKey,
		EndKey:   rhsDesc.EndKey,
	}
	mergedStats := lhsStats
	mergedStats.Add(rhsStats)

	var mergedQPS float64
	if lhsRepl.SplitByLoadEnabled() {
		mergedQPS = lhsQPS + rhsQPS
	}

	// Check if the merged range would need to be split, if so, skip merge.
	// Use a lower threshold for load based splitting so we don't find ourselves
	// in a situation where we keep merging ranges that would be split soon after
	// by a small increase in load.
	conservativeLoadBasedSplitThreshold := 0.5 * lhsRepl.SplitByLoadQPSThreshold()
	shouldSplit, _ := shouldSplitRange(ctx, mergedDesc, mergedStats,
		lhsRepl.GetMaxBytes(), lhsRepl.shouldBackpressureWrites(), sysCfg)
	if shouldSplit || mergedQPS >= conservativeLoadBasedSplitThreshold {
		log.VEventf(ctx, 2,
			"skipping merge to avoid thrashing: merged range %s may split "+
				"(estimated size, estimated QPS: %d, %v)",
			mergedDesc, mergedStats.Total(), mergedQPS)
		return false, nil
	}

	{
		store := lhsRepl.store
		// AdminMerge errors if there is a learner or joint config on either
		// side and AdminRelocateRange removes any on the range it operates on.
		// For the sake of obviousness, just fix this all upfront.
		var err error
		lhsDesc, err = maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, store, lhsDesc)
		if err != nil {
			log.VEventf(ctx, 2, `%v`, err)
			return false, err
		}

		rhsDesc, err = maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, store, rhsDesc)
		if err != nil {
			log.VEventf(ctx, 2, `%v`, err)
			return false, err
		}
	}
	leftRepls, rightRepls := lhsDesc.Replicas().Descriptors(), rhsDesc.Replicas().Descriptors()

	// Defensive sanity check that the ranges involved only have either VOTER_FULL
	// and NON_VOTER replicas.
	for i := range leftRepls {
		if typ := leftRepls[i].GetType(); !(typ == roachpb.VOTER_FULL || typ == roachpb.NON_VOTER) {
			return false,
				errors.AssertionFailedf(
					`cannot merge because lhs is either in a joint state or has learner replicas: %v`,
					leftRepls,
				)
		}
	}
	for i := range rightRepls {
		if typ := rightRepls[i].GetType(); !(typ == roachpb.VOTER_FULL || typ == roachpb.NON_VOTER) {
			return false,
				errors.AssertionFailedf(
					`cannot merge because rhs is either in a joint state or has learner replicas: %v`,
					rightRepls,
				)
		}
	}

	// Range merges require that the set of stores that contain a replica for the
	// RHS range be equal to the set of stores that contain a replica for the LHS
	// range. The LHS and RHS ranges' leaseholders do not need to be co-located
	// and types of the replicas (voting or non-voting) do not matter.
	if !replicasCollocated(leftRepls, rightRepls) {
		// TODO(aayush): We enable merges to proceed even when LHS and/or RHS are in
		// violation of their constraints (by adding or removing replicas on the RHS
		// as needed). We could instead choose to check constraints conformance of
		// these ranges and only try to collocate them if they're not in violation,
		// which would help us make better guarantees about not transiently
		// violating constraints during a merge.
		voterTargets := lhsDesc.Replicas().Voters().ReplicationTargets()
		nonVoterTargets := lhsDesc.Replicas().NonVoters().ReplicationTargets()

		// AdminRelocateRange moves the lease to the first target in the list, so
		// sort the existing leaseholder there to leave it unchanged.
		lease, _ := lhsRepl.GetLease()
		for i := range voterTargets {
			if t := voterTargets[i]; t.NodeID == lease.Replica.NodeID && t.StoreID == lease.Replica.StoreID {
				if i > 0 {
					voterTargets[0], voterTargets[i] = voterTargets[i], voterTargets[0]
				}
				break
			}
		}
		// The merge queue will only merge ranges that have the same zone config
		// (see check inside mergeQueue.shouldQueue).
		if err := mq.store.DB().AdminRelocateRange(
			ctx, rhsDesc.StartKey, voterTargets, nonVoterTargets,
		); err != nil {
			return false, err
		}
	}

	log.VEventf(ctx, 2, "merging to produce range: %s-%s", mergedDesc.StartKey, mergedDesc.EndKey)
	reason := fmt.Sprintf("lhs+rhs has (size=%s+%s=%s qps=%.2f+%.2f=%.2fqps) below threshold (size=%s, qps=%.2f)",
		humanizeutil.IBytes(lhsStats.Total()),
		humanizeutil.IBytes(rhsStats.Total()),
		humanizeutil.IBytes(mergedStats.Total()),
		lhsQPS,
		rhsQPS,
		mergedQPS,
		humanizeutil.IBytes(minBytes),
		conservativeLoadBasedSplitThreshold,
	)
	_, pErr := lhsRepl.AdminMerge(ctx, roachpb.AdminMergeRequest{
		RequestHeader: roachpb.RequestHeader{Key: lhsRepl.Desc().StartKey.AsRawKey()},
	}, reason)
	if err := pErr.GoError(); errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
		// ConditionFailedErrors are an expected outcome for range merge
		// attempts because merges can race with other descriptor modifications.
		// On seeing a ConditionFailedError, don't return an error and enqueue
		// this replica again in case it still needs to be merged.
		log.Infof(ctx, "merge saw concurrent descriptor modification; maybe retrying")
		mq.MaybeAddAsync(ctx, lhsRepl, now)
		return false, nil
	} else if err != nil {
		// While range merges are unstable, be extra cautious and mark every error
		// as purgatory-worthy.
		//
		// TODO(aayush): Merges are indeed stable now, we can be smarter here about
		// which errors should be marked as purgatory-worthy.
		log.Warningf(ctx, "%v", err)
		return false, rangeMergePurgatoryError{err}
	}
	if testingAggressiveConsistencyChecks {
		if _, err := mq.store.consistencyQueue.process(ctx, lhsRepl, sysCfg); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}
	return true, nil
}

func (mq *mergeQueue) timer(time.Duration) time.Duration {
	return MergeQueueInterval.Get(&mq.store.ClusterSettings().SV)
}

func (mq *mergeQueue) purgatoryChan() <-chan time.Time {
	return mq.purgChan
}
