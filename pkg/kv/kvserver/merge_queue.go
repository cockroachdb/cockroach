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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	settings.SystemOnly,
	"kv.range_merge.queue_interval",
	"how long the merge queue waits between processing replicas",
	5*time.Second,
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
//	/Table/51/1    /Table/51/2    /Table/52
//	   32MB            0MB           32MB
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

var _ queueImpl = &mergeQueue{}

func newMergeQueue(store *Store, db *kv.DB) *mergeQueue {
	mq := &mergeQueue{
		db:       db,
		purgChan: time.NewTicker(mergeQueuePurgatoryCheckInterval).C,
	}
	mq.baseQueue = newBaseQueue(
		"merge", mq, store,
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
			needsSpanConfigs:     true,
			acceptsUnsplitRanges: false,
			successes:            store.metrics.MergeQueueSuccesses,
			failures:             store.metrics.MergeQueueFailures,
			storeFailures:        store.metrics.StoreFailures,
			pending:              store.metrics.MergeQueuePending,
			processingNanos:      store.metrics.MergeQueueProcessingNanos,
			purgatory:            store.metrics.MergeQueuePurgatory,
			disabledConfig:       kvserverbase.MergeQueueEnabled,
		},
	)
	return mq
}

func (mq *mergeQueue) shouldQueue(
	ctx context.Context, now hlc.ClockTimestamp, repl *Replica, conf *roachpb.SpanConfig,
) (shouldQueue bool, priority float64) {
	desc := repl.Desc()

	if desc.EndKey.Equal(roachpb.RKeyMax) {
		// The last range has no right-hand neighbor to merge with.
		return false, 0
	}
	confReader, err := repl.store.GetConfReader(ctx)
	if err != nil {
		log.Infof(ctx, "unable to load the span config (err=%v) for range %d", err, desc.RangeID)
		return false, 0
	}

	needsSplit, err := confReader.NeedsSplit(ctx, desc.StartKey, desc.EndKey.Next())
	if err != nil {
		log.Warningf(
			ctx,
			"could not compute if extending range would result in a split (err=%v); skipping merge for range %s",
			err,
			desc.RangeID,
		)
		return false, 0
	}
	if needsSplit {
		// This range would need to be split if it extended just one key further.
		// There is thus no possible right-hand neighbor that it could be merged
		// with.
		return false, 0
	}

	sizeRatio := float64(repl.GetMVCCStats().Total()) / float64(conf.RangeMinBytes)
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

var _ errors.SafeFormatter = decommissionPurgatoryError{}

func (e rangeMergePurgatoryError) SafeFormatError(p errors.Printer) (next error) {
	p.Print(e.error)
	return nil
}

func (rangeMergePurgatoryError) PurgatoryErrorMarker() {}

var _ PurgatoryError = rangeMergePurgatoryError{}

func (mq *mergeQueue) requestRangeStats(
	ctx context.Context, key roachpb.Key,
) (
	desc *roachpb.RangeDescriptor,
	stats enginepb.MVCCStats,
	lbSnap split.LoadSplitSnapshot,
	err error,
) {

	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.RangeStatsRequest{
		RequestHeader: kvpb.RequestHeader{Key: key},
	})

	br, pErr := mq.db.NonTransactionalSender().Send(ctx, ba)
	if pErr != nil {
		return nil, enginepb.MVCCStats{}, lbSnap, pErr.GoError()
	}
	res := br.Responses[0].GetInner().(*kvpb.RangeStatsResponse)

	desc = &res.RangeInfo.Desc
	stats = res.MVCCStats

	// The load based splitter will only track the max of at most one statistic
	// at a time for load based splitting. This is either CPU or QPS. However we
	// don't enforce that only one max stat is returned. We set QPS after CPU,
	// possibly overwriting if both are set. A default value of 0 could be
	// returned in the response if the rhs node is on a pre 23.1 version. To
	// avoid merging the range due to low load (0 CPU), always overwrite the
	// value if MaxQPS is set. If neither are >= 0, OK won't be set and the
	// objective will be default value, QPS.
	if res.MaxCPUPerSecond >= 0 {
		lbSnap = split.LoadSplitSnapshot{
			SplitObjective: split.SplitCPU,
			Max:            res.MaxCPUPerSecond,
			Ok:             true,
		}
	}
	if res.MaxQueriesPerSecond >= 0 {
		lbSnap = split.LoadSplitSnapshot{
			SplitObjective: split.SplitQPS,
			Max:            res.MaxQueriesPerSecond,
			Ok:             true,
		}
	}
	return desc, stats, lbSnap, nil
}

func (mq *mergeQueue) process(
	ctx context.Context, lhsRepl *Replica, conf *roachpb.SpanConfig,
) (processed bool, err error) {

	confReader, err := lhsRepl.store.GetConfReader(ctx)
	if err != nil {
		return false, errors.Wrapf(err, "unable to load conf reader")
	}

	lhsDesc := lhsRepl.Desc()
	lhsStats := lhsRepl.GetMVCCStats()
	minBytes := conf.RangeMinBytes
	if lhsStats.Total() >= minBytes {
		log.VEventf(ctx, 2, "skipping merge: LHS meets minimum size threshold %d with %d bytes",
			minBytes, lhsStats.Total())
		return false, nil
	}

	rhsDesc, rhsStats, rhsLoadSplitSnap, err := mq.requestRangeStats(ctx, lhsDesc.EndKey.AsRawKey())
	if err != nil {
		return false, err
	}
	if rhsStats.Total() >= minBytes {
		log.VEventf(ctx, 2, "skipping merge: RHS meets minimum size threshold %d with %d bytes",
			minBytes, rhsStats.Total())
		return false, nil
	}

	// Range was manually split and not expired, so skip merging.
	now := mq.store.Clock().NowAsClockTimestamp()
	if now.ToTimestamp().Less(rhsDesc.StickyBit) {
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

	lhsLoadSplitSnap := lhsRepl.loadBasedSplitter.Snapshot(ctx, mq.store.Clock().PhysicalTime())
	var loadMergeReason redact.RedactableString
	if lhsRepl.SplitByLoadEnabled() {
		var canMergeLoad bool
		if canMergeLoad, loadMergeReason = canMergeRangeLoad(
			ctx, lhsLoadSplitSnap, rhsLoadSplitSnap,
		); !canMergeLoad {
			log.VEventf(ctx, 2, "skipping merge to avoid thrashing: merged range %s may split %s",
				mergedDesc, loadMergeReason)
			return false, nil
		}
	}

	shouldSplit, _ := shouldSplitRange(ctx, mergedDesc, mergedStats,
		conf.RangeMaxBytes, lhsRepl.shouldBackpressureWrites(conf), confReader)
	if shouldSplit {
		log.VEventf(ctx, 2,
			"skipping merge to avoid thrashing: merged range %s may split "+
				"(estimated size: %d)",
			mergedDesc, mergedStats.Total())
		return false, nil
	}

	{
		// AdminMerge errors if there is a learner or joint config on either
		// side and AdminRelocateRange removes any on the range it operates on.
		// For the sake of obviousness, just fix this all upfront. The merge is
		// performed by the LHS leaseholder, so it can easily do this for LHS.
		// We deal with the RHS, whose leaseholder may be remote, further down.
		var err error
		// TODO(aayush): Separately track metrics for how many learners were removed
		// by the mergeQueue here.
		lhsDesc, _, err = lhsRepl.maybeLeaveAtomicChangeReplicasAndRemoveLearners(ctx, lhsDesc)
		if err != nil {
			log.VEventf(ctx, 2, `%v`, err)
			return false, err
		}
	}
	leftRepls, rightRepls := lhsDesc.Replicas().Descriptors(), rhsDesc.Replicas().Descriptors()

	// Defensive sanity check that the ranges involved only have either VOTER_FULL
	// and NON_VOTER replicas.
	for i := range leftRepls {
		if typ := leftRepls[i].Type; !(typ == roachpb.VOTER_FULL || typ == roachpb.NON_VOTER) {
			return false,
				errors.AssertionFailedf(
					`cannot merge because lhs is either in a joint state or has learner replicas: %v`,
					leftRepls,
				)
		}
	}

	// Range merges require that the set of stores that contain a replica for the
	// RHS range be equal to the set of stores that contain a replica for the LHS
	// range. The LHS and RHS ranges' leaseholders do not need to be co-located
	// and types of the replicas (voting or non-voting) do not matter. Even if
	// replicas are collocated, the RHS might still be in a joint config, and
	// calling AdminRelocateRange will fix this.
	if !replicasCollocated(leftRepls, rightRepls) ||
		rhsDesc.Replicas().InAtomicReplicationChange() {
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
		//
		// TODO(aayush): Remove this logic to move lease to the front for 22.2,
		// since 22.1 nodes support the new `transferLeaseToFirstVoter` parameter
		// for `AdminRelocateRange`.
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
			ctx,
			rhsDesc.StartKey,
			voterTargets,
			nonVoterTargets,
			false, /* transferLeaseToFirstVoter */
		); err != nil {
			return false, err
		}

		// Refresh RHS descriptor.
		rhsDesc, _, _, err = mq.requestRangeStats(ctx, lhsDesc.EndKey.AsRawKey())
		if err != nil {
			return false, err
		}
		rightRepls = rhsDesc.Replicas().Descriptors()
	}
	for i := range rightRepls {
		if typ := rightRepls[i].Type; !(typ == roachpb.VOTER_FULL || typ == roachpb.NON_VOTER) {
			log.Infof(ctx, "RHS Type: %s", typ)
			return false,
				errors.AssertionFailedf(
					`cannot merge because rhs is either in a joint state or has learner replicas: %v`,
					rightRepls,
				)
		}
	}

	log.VEventf(ctx, 2, "merging to produce range: %s-%s", mergedDesc.StartKey, mergedDesc.EndKey)
	reason := redact.Sprintf("lhs+rhs size (%s+%s=%s) below threshold (%s) %s",
		humanizeutil.IBytes(lhsStats.Total()),
		humanizeutil.IBytes(rhsStats.Total()),
		humanizeutil.IBytes(mergedStats.Total()),
		humanizeutil.IBytes(minBytes),
		loadMergeReason,
	)
	_, pErr := lhsRepl.AdminMerge(ctx, kvpb.AdminMergeRequest{
		RequestHeader: kvpb.RequestHeader{Key: lhsRepl.Desc().StartKey.AsRawKey()},
	}, reason)
	if err := pErr.GoError(); errors.HasType(err, (*kvpb.ConditionFailedError)(nil)) {
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
		if _, err := mq.store.consistencyQueue.process(ctx, lhsRepl, conf); err != nil {
			log.Warningf(ctx, "%v", err)
		}
	}

	// Adjust the splitter to account for the additional load from the RHS. We
	// could just Reset the splitter, but then we'd need to wait out a full
	// measurement period (default of 5m) before merging this range again.
	if mergedLoadSplitStat := lhsLoadSplitSnap.Max + rhsLoadSplitSnap.Max; mergedLoadSplitStat != 0 {
		lhsRepl.loadBasedSplitter.RecordMax(mq.store.Clock().PhysicalTime(), mergedLoadSplitStat)
	}
	return true, nil
}

func (*mergeQueue) postProcessScheduled(
	ctx context.Context, replica replicaInQueue, conf *roachpb.SpanConfig, priority float64,
) {
}

func (mq *mergeQueue) timer(time.Duration) time.Duration {
	return MergeQueueInterval.Get(&mq.store.ClusterSettings().SV)
}

func (mq *mergeQueue) purgatoryChan() <-chan time.Time {
	return mq.purgChan
}

func (mq *mergeQueue) updateChan() <-chan time.Time {
	return nil
}

func canMergeRangeLoad(
	ctx context.Context, lhs, rhs split.LoadSplitSnapshot,
) (can bool, reason redact.RedactableString) {
	// When load is a consideration for splits and, by extension, merges, the
	// mergeQueue is fairly conservative. In an effort to avoid thrashing and to
	// avoid overreacting to temporary fluctuations in load, the mergeQueue will
	// only consider a merge when the combined load across the RHS and LHS
	// ranges is below half the threshold required to split a range due to load.
	// Furthermore, to ensure that transient drops in load do not trigger range
	// merges, the mergeQueue will only consider a merge when it deems the
	// maximum qps measurement from both sides to be sufficiently stable and
	// reliable, meaning that it was a maximum measurement over some extended
	// period of time.
	if !lhs.Ok {
		return false, "LHS load measurement not yet reliable"
	}
	if !rhs.Ok {
		return false, "RHS load measurement not yet reliable"
	}

	// When the lhs and rhs split stats are of different types, or do not match
	// the current split objective they cannot merge together. This could occur
	// just after changing the split objective to a different value, where
	// there is a mismatch.
	if lhs.SplitObjective != rhs.SplitObjective {
		return false, redact.Sprintf("LHS load measurement is a different type (%s) than the RHS (%s)",
			lhs.SplitObjective,
			rhs.SplitObjective,
		)
	}

	obj := lhs.SplitObjective
	// Check if the merged range would need to be split, if so, skip merge.
	// Use a lower threshold for load based splitting so we don't find ourselves
	// in a situation where we keep merging ranges that would be split soon after
	// by a small increase in load.
	merged := lhs.Max + rhs.Max
	conservativeLoadBasedSplitThreshold := 0.5 * lhs.Threshold

	if merged >= conservativeLoadBasedSplitThreshold {
		return false, redact.Sprintf("lhs+rhs %s (%s+%s=%s) above threshold (%s)",
			obj,
			obj.Format(lhs.Max),
			obj.Format(rhs.Max),
			obj.Format(merged),
			obj.Format(conservativeLoadBasedSplitThreshold),
		)
	}

	return true, redact.Sprintf("lhs+rhs %s (%s+%s=%s) below threshold (%s)",
		obj,
		obj.Format(lhs.Max),
		obj.Format(rhs.Max),
		obj.Format(merged),
		obj.Format(conservativeLoadBasedSplitThreshold),
	)
}
