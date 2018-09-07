// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// mergeQueuePurgatoryCheckInterval is the interval at which replicas in
	// purgatory make merge attempts. Since merges are relatively untested, the
	// reasons that a range may fail to merge are unknown, so the merge queue has
	// a large purgatory interval.
	mergeQueuePurgatoryCheckInterval = 1 * time.Minute

	// The current implementation of merges requires rewriting the right-hand data
	// onto the left-hand range, even when the ranges are collocated. This is
	// expensive, so limit to one merge at a time.
	mergeQueueConcurrency = 1
)

// MergeQueueEnabled is a setting that controls whether the merge queue is
// enabled.
var MergeQueueEnabled = settings.RegisterBoolSetting(
	"kv.range_merge.queue_enabled",
	"whether the automatic merge queue is enabled",
	false,
)

// MergeQueueInterval is a setting that controls how often the merge queue waits
// between processing replicas.
var MergeQueueInterval = func() *settings.DurationSetting {
	s := settings.RegisterNonNegativeDurationSetting(
		"kv.range_merge.queue_interval",
		"how long the merge queue waits between processing replicas",
		time.Second,
	)
	s.Hide()
	return s
}()

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
	db       *client.DB
	purgChan <-chan time.Time
}

func newMergeQueue(store *Store, db *client.DB, gossip *gossip.Gossip) *mergeQueue {
	mq := &mergeQueue{
		db:       db,
		purgChan: time.NewTicker(mergeQueuePurgatoryCheckInterval).C,
	}
	mq.baseQueue = newBaseQueue(
		"merge", mq, store, gossip,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			maxConcurrency:       mergeQueueConcurrency,
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
	return st.Version.IsMinSupported(cluster.VersionRangeMerges) && MergeQueueEnabled.Get(&st.SV)
}

func (mq *mergeQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, sysCfg config.SystemConfig,
) (shouldQ bool, priority float64) {
	if !mq.enabled() {
		return false, 0
	}

	desc := repl.Desc()

	if desc.EndKey.Equal(roachpb.RKeyMax) {
		// The last range has no right-hand neighbor to merge with.
		return false, 0
	}

	if sysCfg.NeedsSplit(desc.StartKey, desc.EndKey.Next()) {
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
) (roachpb.RangeDescriptor, enginepb.MVCCStats, error) {
	res, pErr := client.SendWrappedWith(ctx, mq.db.NonTransactionalSender(), roachpb.Header{
		ReturnRangeInfo: true,
	}, &roachpb.RangeStatsRequest{
		RequestHeader: roachpb.RequestHeader{Key: key},
	})
	if pErr != nil {
		return roachpb.RangeDescriptor{}, enginepb.MVCCStats{}, pErr.GoError()
	}
	rangeInfos := res.Header().RangeInfos
	if len(rangeInfos) != 1 {
		return roachpb.RangeDescriptor{}, enginepb.MVCCStats{}, fmt.Errorf(
			"mergeQueue.requestRangeStats: response had %d range infos but exactly one was expected",
			len(rangeInfos))
	}
	return rangeInfos[0].Desc, res.(*roachpb.RangeStatsResponse).MVCCStats, nil
}

func (mq *mergeQueue) process(
	ctx context.Context, lhsRepl *Replica, sysCfg config.SystemConfig,
) error {
	if !mq.enabled() {
		log.VEventf(ctx, 2, "skipping merge: queue has been disabled")
		return nil
	}

	lhsStats := lhsRepl.GetMVCCStats()
	minBytes := lhsRepl.GetMinBytes()
	if lhsStats.Total() >= minBytes {
		log.VEventf(ctx, 2, "skipping merge: LHS meets minimum size threshold %d with %d bytes",
			minBytes, lhsStats.Total())
		return nil
	}

	lhsDesc := lhsRepl.Desc()
	rhsDesc, rhsStats, err := mq.requestRangeStats(ctx, lhsDesc.EndKey.AsRawKey())
	if err != nil {
		return err
	}
	if rhsStats.Total() >= minBytes {
		log.VEventf(ctx, 2, "skipping merge: RHS meets minimum size threshold %d with %d bytes",
			minBytes, lhsStats.Total())
		return nil
	}

	mergedDesc := &roachpb.RangeDescriptor{
		StartKey: lhsDesc.StartKey,
		EndKey:   rhsDesc.EndKey,
	}
	mergedStats := lhsStats
	mergedStats.Add(rhsStats)
	if ok, _ := shouldSplitRange(mergedDesc, mergedStats, lhsRepl.GetMaxBytes(), sysCfg); ok {
		log.VEventf(ctx, 2, "skipping merge: merged range %s would need to be split (estimated size: %d)",
			mergedDesc, mergedStats.Total())
		return nil
	}

	if !replicaSetsEqual(lhsDesc.Replicas, rhsDesc.Replicas) {
		var targets []roachpb.ReplicationTarget
		for _, lhsReplDesc := range lhsDesc.Replicas {
			targets = append(targets, roachpb.ReplicationTarget{
				NodeID: lhsReplDesc.NodeID, StoreID: lhsReplDesc.StoreID,
			})
		}
		lease, _ := lhsRepl.GetLease()
		for i := range targets {
			if targets[i].NodeID == lease.Replica.NodeID && targets[i].StoreID == lease.Replica.StoreID {
				if i > 0 {
					targets[0], targets[i] = targets[i], targets[0]
				}
				break
			}
		}
		// TODO(benesch): RelocateRange needs to be made more robust. It cannot
		// currently handle certain edge cases, like multiple stores on one node. It
		// also adds all new replicas before removing any old replicas, rather than
		// performing interleaved adds/removes, resulting in a moment where the
		// number of replicas is potentially double the configured replication
		// factor.
		if err := RelocateRange(ctx, mq.db, rhsDesc, targets); err != nil {
			return err
		}
	}

	_, pErr := lhsRepl.AdminMerge(ctx, roachpb.AdminMergeRequest{})
	switch err := pErr.GoError(); err.(type) {
	case nil:
	case *roachpb.ConditionFailedError:
		// ConditionFailedErrors are an expected outcome for range merge
		// attempts because merges can race with other descriptor modifications.
		// On seeing a ConditionFailedError, don't return an error and enqueue
		// this replica again in case it still needs to be merged.
		log.Infof(ctx, "merge saw concurrent descriptor modification; maybe retrying")
		mq.MaybeAdd(lhsRepl, mq.store.Clock().Now())
	default:
		// While range merges are unstable, be extra cautious and mark every error
		// as purgatory-worthy.
		return rangeMergePurgatoryError{err}
	}
	if testingAggressiveConsistencyChecks {
		if err := mq.store.consistencyQueue.process(ctx, lhsRepl, sysCfg); err != nil {
			log.Warning(ctx, err)
		}
	}
	return nil
}

func (mq *mergeQueue) timer(time.Duration) time.Duration {
	return MergeQueueInterval.Get(&mq.store.ClusterSettings().SV)
}

func (mq *mergeQueue) purgatoryChan() <-chan time.Time {
	return mq.purgChan
}
