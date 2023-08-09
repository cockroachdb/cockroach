// Copyright 2014 The Cockroach Authors.
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
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/gc"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// mvccGCQueueDefaultTimerDuration is the default duration between MVCC GCs
	// of queued replicas.
	mvccGCQueueDefaultTimerDuration = 1 * time.Second
	// mvccHiPriGCQueueDefaultTimerDuration is default duration between MVCC GCs
	// of queued replicas when replicas contain only garbage removed by range
	// tombstone completely.
	mvccHiPriGCQueueDefaultTimerDuration = 0 * time.Second
	// mvccGCQueueTimeout is the timeout for a single MVCC GC run.
	mvccGCQueueTimeout = 10 * time.Minute
	// mvccGCQueueIntentBatchTimeout is the timeout for resolving a single batch
	// of intents. It is used to ensure progress in the face of unavailable ranges
	// (since intent resolution may touch other ranges), but can prevent progress
	// for ranged intent resolution if it exceeds the timeout.
	mvccGCQueueIntentBatchTimeout = 2 * time.Minute

	// mvccGCQueueCooldownDuration is duration to wait between MVCC GC attempts of
	// the same rage when triggered by a low score threshold. This cooldown time
	// is reduced proportionally to score and becomes 0 when score reaches a
	// mvccGCKeyScoreNoCooldownThreshold score.
	mvccGCQueueCooldownDuration = 2 * time.Hour
	// mvccGCQueueIntentCooldownDuration is the duration to wait between MVCC GC
	// attempts of the same range when triggered solely by intents. This is to
	// prevent continually spinning on intents that belong to active transactions,
	// which can't be cleaned up.
	mvccGCQueueIntentCooldownDuration = 2 * time.Hour
	// intentAgeNormalization is the average age of outstanding intents
	// which amount to a score of "1" added to total replica priority.
	intentAgeNormalization = 8 * time.Hour

	// Thresholds used to decide whether to queue for MVCC GC based on keys and
	// intents.
	mvccGCKeyScoreThreshold           = 1
	mvccGCKeyScoreNoCooldownThreshold = 2
	mvccGCIntentScoreThreshold        = 1
	mvccGCDropRangeKeyScoreThreshold  = 1

	probablyLargeAbortSpanSysCountThreshold = 10000
	largeAbortSpanBytesThreshold            = 16 * (1 << 20) // 16mb

	// deleteRangePriority is used to indicate replicas that needs to be processed
	// out of band quickly to enable storage compaction optimization because all
	// data in them was removed.
	deleteRangePriority = math.MaxFloat64
	// gcHintScannerTimeout determines how long hint scanner can hold mvcc gc queue
	// from progressing. We don't want to scanner to fail if timeout is too short
	// as it may miss some high priority replicas and increase compaction load,
	// but at the same time if something goes wrong and we can't obtain read locks
	// on replica, we can tolerate so much time. 5 minutes shouldn't delay GC
	// excessively and we wouldn't retry scan at least until we process all
	// found replicas and do a normal scanner cycle that uses
	// server.defaultScanInterval rescan period.
	gcHintScannerTimeout = 5 * time.Minute
)

// mvccGCQueueInterval is a setting that controls how long the mvcc GC queue
// waits between processing replicas.
var mvccGCQueueInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.mvcc_gc.queue_interval",
	"how long the mvcc gc queue waits between processing replicas",
	mvccGCQueueDefaultTimerDuration,
	settings.NonNegativeDuration,
)

// mvccGCQueueHighPriInterval
var mvccGCQueueHighPriInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.mvcc_gc.queue_high_priority_interval",
	"how long the mvcc gc queue waits between processing high priority replicas (e.g. after table drops)",
	mvccHiPriGCQueueDefaultTimerDuration,
	settings.NonNegativeDuration,
)

func largeAbortSpan(ms enginepb.MVCCStats) bool {
	// Checks if the size of the abort span exceeds the given threshold.
	// The abort span is not supposed to become that large, but it does
	// happen and causes stability fallout, usually due to a combination of
	// shortcomings:
	//
	// 1. there's no trigger for GC based on abort span size alone (before
	//    this code block here was written)
	// 2. transaction aborts tended to create unnecessary abort span entries,
	//    fixed (and 19.2-backported) in:
	//    https://github.com/cockroachdb/cockroach/pull/42765
	// 3. aborting transactions in a busy loop:
	//    https://github.com/cockroachdb/cockroach/issues/38088
	//    (and we suspect this also happens in user apps occasionally)
	// 4. large snapshots would never complete due to the queue time limits
	//    (addressed in https://github.com/cockroachdb/cockroach/pull/44952).

	// New versions (20.2+) of Cockroach accurately track the size of the abort
	// span (after a migration period of a few days, assuming default consistency
	// checker intervals). For mixed-version 20.1/20.2 clusters, we also include
	// a heuristic based on SysBytes (which always reflects the abort span). This
	// heuristic can be removed in 21.1.
	definitelyLargeAbortSpan := ms.AbortSpanBytes >= largeAbortSpanBytesThreshold
	probablyLargeAbortSpan := ms.SysBytes >= largeAbortSpanBytesThreshold && ms.SysCount >= probablyLargeAbortSpanSysCountThreshold
	return definitelyLargeAbortSpan || probablyLargeAbortSpan
}

// mvccGCQueue manages a queue of replicas slated to be scanned in their
// entirety using the MVCC versions iterator. The mvcc gc queue manages
// the following tasks:
//
//   - GC of version data via TTL expiration (and more complex schemes
//     as implemented going forward).
//   - Resolve extant write intents (pushing their transactions).
//   - GC of old transaction and AbortSpan entries. This should include
//     most committed and aborted entries almost immediately and, after a
//     threshold on inactivity, all others.
//
// The shouldQueue function combines the need for the above tasks into a
// single priority. If any task is overdue, shouldQueue returns true.
type mvccGCQueue struct {
	*baseQueue

	// Set to true when GC finds range that has a hint indicating that range is
	// completely cleared.
	lastRangeWasHighPriority bool
	// leaseholderCheckInterceptor is a leasholder check used by high priority replica scanner
	// its only purpose is to allow test function injection.
	leaseholderCheckInterceptor func(ctx context.Context, replica *Replica, now hlc.ClockTimestamp) bool
}

var _ queueImpl = &mvccGCQueue{}

// newMVCCGCQueue returns a new instance of mvccGCQueue.
func newMVCCGCQueue(store *Store) *mvccGCQueue {
	mgcq := &mvccGCQueue{
		leaseholderCheckInterceptor: store.TestingKnobs().MVCCGCQueueLeaseCheckInterceptor,
	}
	mgcq.baseQueue = newBaseQueue(
		"mvccGC", mgcq, store,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSpanConfigs:     true,
			acceptsUnsplitRanges: false,
			processTimeoutFunc: func(st *cluster.Settings, _ replicaInQueue) time.Duration {
				timeout := mvccGCQueueTimeout
				if d := queueGuaranteedProcessingTimeBudget.Get(&st.SV); d > timeout {
					timeout = d
				}
				return timeout
			},
			successes:       store.metrics.MVCCGCQueueSuccesses,
			failures:        store.metrics.MVCCGCQueueFailures,
			pending:         store.metrics.MVCCGCQueuePending,
			processingNanos: store.metrics.MVCCGCQueueProcessingNanos,
			disabledConfig:  kvserverbase.MVCCGCQueueEnabled,
		},
	)
	return mgcq
}

// mvccGCQueueScore holds details about the score returned by
// makeMVCCGCQueueScoreImpl for testing and logging. The fields in this struct
// are documented in makeMVCCGCQueueScoreImpl.
type mvccGCQueueScore struct {
	TTL                 time.Duration
	LastGC              time.Duration
	DeadFraction        float64
	ValuesScalableScore float64
	IntentScore         float64
	FuzzFactor          float64
	FinalScore          float64
	ShouldQueue         bool

	GCBytes                  int64
	GCByteAge                int64
	ExpMinGCByteAgeReduction int64
}

func (r mvccGCQueueScore) String() string {
	if (r == mvccGCQueueScore{}) {
		return "(empty)"
	}
	if r.ExpMinGCByteAgeReduction < 0 {
		r.ExpMinGCByteAgeReduction = 0
	}
	lastGC := "never"
	if r.LastGC != 0 {
		lastGC = fmt.Sprintf("%s ago", r.LastGC)
	}
	return fmt.Sprintf("queue=%t with %.2f/fuzz(%.2f)=%.2f=valScaleScore(%.2f)*deadFrac(%.2f)+intentScore(%.2f)\n"+
		"likely last GC: %s, %s non-live, curr. age %s*s, min exp. reduction: %s*s",
		r.ShouldQueue, r.FinalScore, r.FuzzFactor, r.FinalScore/r.FuzzFactor, r.ValuesScalableScore,
		r.DeadFraction, r.IntentScore, lastGC, humanizeutil.IBytes(r.GCBytes),
		humanizeutil.IBytes(r.GCByteAge), humanizeutil.IBytes(r.ExpMinGCByteAgeReduction))
}

// shouldQueue determines whether a replica should be queued for garbage
// collection, and if so, at what priority. Returns true for shouldQ
// in the event that the cumulative ages of GC'able bytes or extant
// intents exceed thresholds.
func (mgcq *mvccGCQueue) shouldQueue(
	ctx context.Context, _ hlc.ClockTimestamp, repl *Replica,
) (bool, float64) {
	// Consult the protected timestamp state to determine whether we can GC and
	// the timestamp which can be used to calculate the score.
	conf, err := repl.SpanConfig()
	if err != nil {
		log.VErrEventf(ctx, 2, "skipping gc, can't read SpanConfig: %v", err)
		return false, 0
	}
	canGC, _, gcTimestamp, oldThreshold, newThreshold, err := repl.checkProtectedTimestampsForGC(ctx, conf.TTL())
	if err != nil {
		log.VErrEventf(ctx, 2, "failed to check protected timestamp for gc: %v", err)
		return false, 0
	}
	if !canGC {
		return false, 0
	}
	canAdvanceGCThreshold := !newThreshold.Equal(oldThreshold)
	lastGC, err := repl.getQueueLastProcessed(ctx, mgcq.name)
	if err != nil {
		log.VErrEventf(ctx, 2, "failed to fetch last processed time: %v", err)
		return false, 0
	}

	r := makeMVCCGCQueueScore(ctx, repl, gcTimestamp, lastGC, conf.TTL(), canAdvanceGCThreshold)
	return r.ShouldQueue, r.FinalScore
}

func makeMVCCGCQueueScore(
	ctx context.Context,
	repl *Replica,
	now hlc.Timestamp,
	lastGC hlc.Timestamp,
	gcTTL time.Duration,
	canAdvanceGCThreshold bool,
) mvccGCQueueScore {
	repl.mu.Lock()
	ms := *repl.mu.state.Stats
	repl.mu.Unlock()

	if repl.store.cfg.TestingKnobs.DisableLastProcessedCheck {
		lastGC = hlc.Timestamp{}
	}

	// Use desc.RangeID for fuzzing the final score, so that different ranges
	// have slightly different priorities and even symmetrical workloads don't
	// trigger GC at the same time.
	r := makeMVCCGCQueueScoreImpl(
		ctx, int64(repl.RangeID), now, ms, gcTTL, lastGC, canAdvanceGCThreshold,
		repl.GetGCHint(), gc.TxnCleanupThreshold.Get(&repl.ClusterSettings().SV),
	)
	return r
}

// makeMVCCGCQueueScoreImpl is used to compute when to trigger the MVCC GC
// Queue. It's important that we don't queue a replica before a relevant amount
// of data is actually deletable, or the queue might run in a tight loop. To
// this end, we use a base score with the right interplay between GCByteAge and
// TTL and additionally weigh it so that GC is delayed when a large proportion
// of the data in the replica is live. Additionally, returned scores are
// slightly perturbed to avoid groups of replicas becoming eligible for GC at
// the same time repeatedly. We also use gcQueueIntentCooldownTimer to avoid
// spinning when GCing solely based on intents, since we may not be able to GC
// them.
//
// More details below.
//
// When a key of size `B` is deleted at timestamp `T` or superseded by a newer
// version, it henceforth is accounted for in the range's `GCBytesAge`. At time
// `S`, its contribution to age will be `B*seconds(S-T)`. The aggregate
// `GCBytesAge` of all deleted versions in the cluster is what the GC queue at
// the time of writing bases its `shouldQueue` method on.
//
// If a replica is queued to have its old values garbage collected, its contents
// are scanned. However, the values which are deleted follow a criterion that
// isn't immediately connected to `GCBytesAge`: We (basically) delete everything
// that's older than the Replica's `TTLSeconds`.
//
// Thus, it's not obvious that garbage collection has the effect of reducing the
// metric that we use to consider the replica for the next GC cycle, and it
// seems that we messed it up.
//
// The previous metric used for queueing: `GCBytesAge/(1<<20 * ttl)` does not
// have the right scaling. For example, consider that a value of size `1mb` is
// overwritten with a newer version. After `ttl` seconds, it contributes `1mb`
// to `GCBytesAge`, and so the replica has a score of `1`, i.e. (roughly) the
// range becomes interesting to the GC queue. When GC runs, it will delete value
// that are `ttl` old, which our value is. But a Replica is ~64mb, so picture
// that you have 64mb of key-value data all at the same timestamp, and they
// become superseded. Already after `ttl/64`, the metric becomes 1, but they
// keys won't be GC'able for another (63*ttl)/64. Thus, GC will run "all the
// time" long before it can actually have an effect.
//
// The metric with correct scaling must thus take into account the size of the
// range. What size exactly? Any data that isn't live (i.e. isn't readable by a
// scan from the far future). That's `KeyBytes + ms.ValBytes - ms.LiveBytes`,
// which is also known as `GCBytes` in the code. Hence, the better metric is
// `GCBytesAge/(ttl*GCBytes)`.
//
// Using this metric guarantees that after truncation, `GCBytesAge` is at most
// `ttl*GCBytes` (where `GCBytes` has been updated), i.e. the new metric is at
// most 1.
//
// To visualize this, picture a rectangular frame of width `ttl` and height
// `GCBytes` (i.e. the horizontal dimension is time, the vertical one bytes),
// where the right boundary of the frame corresponds to age zero. Each non-live
// key is a domino aligned with the right side of the frame, its height equal to
// its size, and its width given by the duration (in seconds) it's been
// non-live.
//
// The combined surface of the dominos is then `GCBytesAge`, and the claim is
// that if the total sum of domino heights (i.e. sizes) is `GCBytes`, and the
// surface is larger than `ttl*GCBytes` by some positive `X`, then after
// removing the dominos that cross the line `x=-ttl` (i.e. `ttl` to the left
// from the right side of the frame), at least a surface area of `X` has been
// removed.
//
//	   x=-ttl                 GCBytes=1+4
//	     |           3 (age)
//	     |          +-------+
//	     |          | keep  | 1 (bytes)
//	     |          +-------+
//	+-----------------------+
//	|                       |
//	|        remove         | 3 (bytes)
//	|                       |
//	+-----------------------+
//	     |   7 (age)
//
// # This is true because
//
// deletable area  = total area       - nondeletable area
//
//	 = X + ttl*GCBytes  - nondeletable area
//	>= X + ttl*GCBytes  - ttl*(bytes in nondeletable area)
//	 = X + ttl*(GCBytes - bytes in nondeletable area)
//	>= X.
//
// Or, in other words, you can only hope to put `ttl*GCBytes` of area in the
// "safe" rectangle. Once you've done that, everything else you put is going to
// be deleted.
//
// This means that running GC will always result in a `GCBytesAge` of `<=
// ttl*GCBytes`, and that a decent trigger for GC is a multiple of
// `ttl*GCBytes`.
func makeMVCCGCQueueScoreImpl(
	ctx context.Context,
	fuzzSeed int64,
	now hlc.Timestamp,
	ms enginepb.MVCCStats,
	gcTTL time.Duration,
	lastGC hlc.Timestamp,
	canAdvanceGCThreshold bool,
	hint roachpb.GCHint,
	txnCleanupThreshold time.Duration,
) mvccGCQueueScore {
	ms.Forward(now.WallTime)
	var r mvccGCQueueScore

	if !lastGC.IsEmpty() {
		r.LastGC = time.Duration(now.WallTime - lastGC.WallTime)
	}

	r.TTL = gcTTL

	// Treat a zero TTL as a one-second TTL, which avoids a priority of infinity
	// and otherwise behaves indistinguishable given that we can't possibly hope
	// to GC values faster than that.
	if r.TTL <= time.Second {
		r.TTL = time.Second
	}

	r.GCByteAge = ms.GCByteAge(now.WallTime)
	r.GCBytes = ms.GCBytes()

	// If we GC'ed now, we can expect to delete at least this much GCByteAge.
	// GCByteAge - TTL*GCBytes = ExpMinGCByteAgeReduction & algebra.
	//
	// Note that for ranges with ContainsEstimates > 0, the value here may not
	// reflect reality, and may even be nonsensical (though that's unlikely).
	r.ExpMinGCByteAgeReduction = r.GCByteAge - r.GCBytes*int64(r.TTL.Seconds())

	// DeadFraction is close to 1 when most values are dead, and close to zero
	// when most of the replica is live. For example, for a replica with no
	// superseded values, this should be (almost) zero. For one just hit
	// completely by a DeleteRange, it should be (almost) one.
	//
	// The algebra below is complicated by the fact that ranges may contain
	// stats that aren't exact (ContainsEstimates > 0).
	clamp := func(n int64) float64 {
		if n < 0 {
			return 0.0
		}
		return float64(n)
	}
	r.DeadFraction = math.Max(1-clamp(ms.LiveBytes)/(1+clamp(ms.Total())), 0)

	// The "raw" GC score is the total GC'able bytes age normalized by (non-live
	// size * the replica's TTL in seconds). This is a scale-invariant factor by
	// (at least) which GCByteAge reduces when deleting values older than the
	// TTL. The risk of an inaccurate GCBytes in the presence of estimated stats
	// is neglected as GCByteAge and GCBytes undercount in the same way and
	// estimation only happens for timeseries writes.
	denominator := r.TTL.Seconds() * (1.0 + clamp(r.GCBytes)) // +1 avoids NaN
	r.ValuesScalableScore = clamp(r.GCByteAge) / denominator
	// However, it doesn't take into account the size of the live data, which
	// also needs to be scanned in order to GC. We don't want to run this costly
	// scan unless we get a corresponding expected reduction in GCByteAge, so we
	// weighs by fraction of non-live data below.

	// Intent score. This computes the average age of outstanding intents and
	// normalizes. Note that at the time of writing this criterion hasn't
	// undergone a reality check yet.
	r.IntentScore = ms.AvgIntentAge(now.WallTime) / float64(intentAgeNormalization.Nanoseconds()/1e9)

	// Randomly skew the score down a bit to cause decoherence of replicas with
	// similar load. Note that we'll only ever reduce the score, never increase
	// it (for increasing it could lead to a fruitless run).
	r.FuzzFactor = 0.95 + 0.05*rand.New(rand.NewSource(fuzzSeed)).Float64()

	// Compute priority.
	valScore := r.DeadFraction * r.ValuesScalableScore
	r.FinalScore = r.FuzzFactor * (valScore + r.IntentScore)

	// Check GC queueing eligibility using cooldown discounted by score.
	isGCScoreMet := func(score float64, minThreshold, maxThreshold float64, cooldown time.Duration) bool {
		if minThreshold > maxThreshold {
			if util.RaceEnabled {
				log.Fatalf(ctx,
					"invalid cooldown score thresholds. min (%f) must be less or equal to max (%f)",
					minThreshold, maxThreshold)
			}
			// Swap thresholds for non test builds. This should never happen in practice.
			minThreshold, maxThreshold = maxThreshold, minThreshold
		}
		// Cool down rate is how much we want to cool down after previous gc based
		// on the score. If score is at min threshold we would wait for cooldown
		// time, it is proportionally reduced as score reaches maxThreshold and is
		// zero at maxThreshold which means no cooldown is necessary.
		coolDownRate := 1 - (score-minThreshold)/(maxThreshold-minThreshold)
		adjustedCoolDown := time.Duration(int64(float64(cooldown.Nanoseconds()) * coolDownRate))
		if score > minThreshold && (r.LastGC == 0 || r.LastGC >= adjustedCoolDown) {
			return true
		}
		return false
	}

	// First determine whether we should queue based on MVCC score alone.
	r.ShouldQueue = canAdvanceGCThreshold && isGCScoreMet(r.FuzzFactor*valScore, mvccGCKeyScoreThreshold,
		mvccGCKeyScoreNoCooldownThreshold, mvccGCQueueCooldownDuration)

	// Next, determine whether we should queue based on intent score. For
	// intents, we also enforce a cooldown time since we may not actually
	// be able to clean up any intents (for active transactions).
	if !r.ShouldQueue && r.FuzzFactor*r.IntentScore > mvccGCIntentScoreThreshold &&
		(r.LastGC == 0 || r.LastGC >= mvccGCQueueIntentCooldownDuration) {
		r.ShouldQueue = true
	}

	// Finally, queue if we find large abort spans for abandoned transactions.
	if largeAbortSpan(ms) && !r.ShouldQueue &&
		(r.LastGC == 0 || r.LastGC > txnCleanupThreshold) {
		r.ShouldQueue = true
		r.FinalScore++
	}

	maybeRangeDel := suspectedFullRangeDeletion(ms)
	hasActiveGCHint := gcHintedRangeDelete(hint, gcTTL, now)

	if hasActiveGCHint && (maybeRangeDel || ms.ContainsEstimates > 0) {
		// We have GC hint allowing us to collect range and we either satisfy
		// heuristic that indicate no live data or we have estimates and we assume
		// hint is correct.
		r.ShouldQueue = canAdvanceGCThreshold
		r.FinalScore = deleteRangePriority
	}

	if !r.ShouldQueue && maybeRangeDel {
		// If we don't have GC hint, but range del heuristic is satisfied, then
		// check with lowered score threshold as all the data is deleted.
		r.ShouldQueue = canAdvanceGCThreshold && r.FuzzFactor*valScore > mvccGCDropRangeKeyScoreThreshold
	}

	return r
}

func gcHintedRangeDelete(hint roachpb.GCHint, ttl time.Duration, now hlc.Timestamp) bool {
	deleteTimestamp := hint.LatestRangeDeleteTimestamp
	if deleteTimestamp.IsEmpty() {
		return false
	}
	return deleteTimestamp.Add(ttl.Nanoseconds(), 0).Less(now)
}

// suspectedFullRangeDeletion checks for ranges where there's no live data and
// range tombstones are present. This is an indication that range is likely
// removed by bulk operations and its garbage collection should be done faster
// than if it has to wait for double ttl.
func suspectedFullRangeDeletion(ms enginepb.MVCCStats) bool {
	if ms.LiveCount > 0 || ms.IntentCount > 0 {
		return false
	}
	return ms.RangeKeyCount > 0
}

type replicaGCer struct {
	repl                *Replica
	count               int32 // update atomically
	admissionController kvadmission.Controller
	storeID             roachpb.StoreID
}

var _ gc.GCer = &replicaGCer{}

func (r *replicaGCer) template() kvpb.GCRequest {
	desc := r.repl.Desc()
	var template kvpb.GCRequest
	template.Key = desc.StartKey.AsRawKey()
	template.EndKey = desc.EndKey.AsRawKey()

	return template
}

func (r *replicaGCer) send(ctx context.Context, req kvpb.GCRequest) error {
	n := atomic.AddInt32(&r.count, 1)
	log.Eventf(ctx, "sending batch %d (%d keys)", n, len(req.Keys))

	ba := &kvpb.BatchRequest{}
	// Technically not needed since we're talking directly to the Replica.
	ba.RangeID = r.repl.Desc().RangeID
	ba.Timestamp = r.repl.Clock().Now()
	ba.Add(&req)
	// Since we are talking directly to the replica, we need to explicitly do
	// admission control here, as we are bypassing server.Node.
	var admissionHandle kvadmission.Handle
	if r.admissionController != nil {
		pri := admissionpb.WorkPriority(gc.AdmissionPriority.Get(&r.repl.ClusterSettings().SV))
		ba.AdmissionHeader = kvpb.AdmissionHeader{
			// TODO(irfansharif): GC could be expected to be BulkNormalPri, so
			// that it does not impact user-facing traffic when resources (e.g.
			// CPU, write capacity of the store) are scarce. However long delays
			// in GC can slow down user-facing traffic due to more versions in
			// the store, and can increase write amplification of the store
			// since there is more live data. Ideally, we should adjust this
			// priority based on how far behind we are with respect to GC-ing
			// data in this range. Keeping it static at NormalPri proved
			// disruptive when a large volume of MVCC GC work is suddenly
			// accrued (if an old protected timestamp record was just released
			// for ex. following a long paused backup job being
			// completed/canceled, or just an old, long running backup job
			// finishing). For now, use a cluster setting that defaults to
			// BulkNormalPri.
			//
			// After we implement dynamic priority adjustment, it's not clear
			// whether we need additional pacing mechanisms to provide better
			// latency isolation similar to ongoing work for backups (since MVCC
			// GC work is CPU intensive): #82955. It's also worth noting that we
			// might be able to do most MVCC GC work as part of regular
			// compactions (#42514) -- the CPU use by the MVCC GC queue during
			// keyspace might still be worth explicitly accounting/limiting, but
			// it'll be lessened overall.
			Priority:                 int32(pri),
			CreateTime:               timeutil.Now().UnixNano(),
			Source:                   kvpb.AdmissionHeader_ROOT_KV,
			NoMemoryReservedAtSource: true,
		}
		ba.Replica.StoreID = r.storeID
		var err error
		admissionHandle, err = r.admissionController.AdmitKVWork(ctx, roachpb.SystemTenantID, ba)
		if err != nil {
			return err
		}
	}
	_, writeBytes, pErr := r.repl.SendWithWriteBytes(ctx, ba)
	defer writeBytes.Release()
	if r.admissionController != nil {
		r.admissionController.AdmittedKVWorkDone(admissionHandle, writeBytes)
	}
	if pErr != nil {
		log.VErrEventf(ctx, 2, "%v", pErr.String())
		return pErr.GoError()
	}
	return nil
}

func (r *replicaGCer) SetGCThreshold(ctx context.Context, thresh gc.Threshold) error {
	req := r.template()
	req.Threshold = thresh.Key
	return r.send(ctx, req)
}

func (r *replicaGCer) GC(
	ctx context.Context,
	keys []kvpb.GCRequest_GCKey,
	rangeKeys []kvpb.GCRequest_GCRangeKey,
	clearRange *kvpb.GCRequest_GCClearRange,
) error {
	if len(keys) == 0 && len(rangeKeys) == 0 && clearRange == nil {
		return nil
	}
	req := r.template()
	req.Keys = keys
	req.RangeKeys = rangeKeys
	req.ClearRange = clearRange
	return r.send(ctx, req)
}

// process first determines whether the replica can run MVCC GC given its view
// of the protected timestamp subsystem and its current state. This check also
// determines the most recent time which can be used for the purposes of
// updating the GC threshold and running GC.
//
// If it is safe to GC, process iterates through all keys in a replica's range,
// calling the garbage collector for each key and associated set of
// values. GC'd keys are batched into GC calls. Extant intents are resolved if
// intents are older than intentAgeThreshold. The transaction and AbortSpan
// records are also scanned and old entries evicted. During normal operation,
// both of these records are cleaned up when their respective transaction
// finishes, so the amount of work done here is expected to be small.
//
// Some care needs to be taken to avoid cyclic recreation of entries during GC:
// * a Push initiated due to an intent may recreate a transaction entry
// * resolving an intent may write a new AbortSpan entry
// * obtaining the transaction for a AbortSpan entry requires a Push
//
// The following order is taken below:
//  1. collect all intents with sufficiently old txn record
//  2. collect these intents' transactions
//  3. scan the transaction table, collecting abandoned or completed txns
//  4. push all of these transactions (possibly recreating entries)
//  5. resolve all intents (unless the txn is not yet finalized), which
//     will recreate AbortSpan entries (but with the txn timestamp; i.e.
//     likely GC'able)
//  6. scan the AbortSpan table for old entries
//  7. push these transactions (again, recreating txn entries).
//  8. send a GCRequest.
func (mgcq *mvccGCQueue) process(ctx context.Context, repl *Replica) (processed bool, err error) {
	// Record the CPU time processing the request for this replica. This is
	// recorded regardless of errors that are encountered.
	defer repl.MeasureReqCPUNanos(grunning.Time())

	// Lookup the descriptor and GC policy for the zone containing this key range.
	desc := repl.Desc()
	conf, err := repl.SpanConfig()
	if err != nil {
		return false, err
	}

	// Consult the protected timestamp state to determine whether we can GC and
	// the timestamp which can be used to calculate the score and updated GC
	// threshold.
	canGC, cacheTimestamp, gcTimestamp, oldThreshold, newThreshold, err := repl.checkProtectedTimestampsForGC(ctx, conf.TTL())
	if err != nil {
		return false, err
	}
	if !canGC {
		return false, nil
	}
	canAdvanceGCThreshold := !newThreshold.Equal(oldThreshold)
	// We don't recheck ShouldQueue here, since the range may have been enqueued
	// manually e.g. via the admin server.
	lastGC, err := repl.getQueueLastProcessed(ctx, mgcq.name)
	if err != nil {
		lastGC = hlc.Timestamp{}
		log.VErrEventf(ctx, 2, "failed to fetch last processed time: %v", err)
	}
	r := makeMVCCGCQueueScore(ctx, repl, gcTimestamp, lastGC, conf.TTL(), canAdvanceGCThreshold)
	log.VEventf(ctx, 2, "processing replica %s with score %s", repl.String(), r)
	// Synchronize the new GC threshold decision with concurrent
	// AdminVerifyProtectedTimestamp requests.
	if err := repl.markPendingGC(cacheTimestamp, newThreshold); err != nil {
		log.VEventf(ctx, 1, "not gc'ing replica %v due to pending protection: %v", repl, err)
		return false, nil
	}
	// Update the last processed timestamp.
	if err := repl.setQueueLastProcessed(ctx, mgcq.name, repl.store.Clock().Now()); err != nil {
		log.VErrEventf(ctx, 2, "failed to update last processed time: %v", err)
	}

	snap := repl.store.TODOEngine().NewSnapshot()
	defer snap.Close()

	intentAgeThreshold := gc.IntentAgeThreshold.Get(&repl.store.ClusterSettings().SV)
	maxIntentsPerCleanupBatch := gc.MaxIntentsPerCleanupBatch.Get(&repl.store.ClusterSettings().SV)
	maxIntentKeyBytesPerCleanupBatch := gc.MaxIntentKeyBytesPerCleanupBatch.Get(&repl.store.ClusterSettings().SV)
	txnCleanupThreshold := gc.TxnCleanupThreshold.Get(&repl.store.ClusterSettings().SV)
	var clearRangeMinKeys int64 = 0
	if repl.store.ClusterSettings().Version.IsActive(ctx, clusterversion.V23_1) {
		clearRangeMinKeys = gc.ClearRangeMinKeys.Get(&repl.store.ClusterSettings().SV)
	}

	info, err := gc.Run(ctx, desc, snap, gcTimestamp, newThreshold,
		gc.RunOptions{
			IntentAgeThreshold:                     intentAgeThreshold,
			MaxIntentsPerIntentCleanupBatch:        maxIntentsPerCleanupBatch,
			MaxIntentKeyBytesPerIntentCleanupBatch: maxIntentKeyBytesPerCleanupBatch,
			TxnCleanupThreshold:                    txnCleanupThreshold,
			MaxTxnsPerIntentCleanupBatch:           intentresolver.MaxTxnsPerIntentCleanupBatch,
			IntentCleanupBatchTimeout:              mvccGCQueueIntentBatchTimeout,
			ClearRangeMinKeys:                      clearRangeMinKeys,
		},
		conf.TTL(),
		&replicaGCer{
			repl:                repl,
			admissionController: mgcq.store.cfg.KVAdmissionController,
			storeID:             mgcq.store.StoreID(),
		},
		func(ctx context.Context, intents []roachpb.Intent) error {
			intentCount, err := repl.store.intentResolver.
				CleanupIntents(ctx, intents, gcTimestamp, kvpb.PUSH_TOUCH)
			if err == nil {
				mgcq.store.metrics.GCResolveSuccess.Inc(int64(intentCount))
			} else {
				mgcq.store.metrics.GCResolveFailed.Inc(int64(intentCount))
			}
			return err
		},
		func(ctx context.Context, txn *roachpb.Transaction) error {
			err := repl.store.intentResolver.
				CleanupTxnIntentsOnGCAsync(ctx, repl.RangeID, txn, gcTimestamp,
					func(pushed, succeeded bool) {
						if pushed {
							mgcq.store.metrics.GCPushTxn.Inc(1)
						}
						if succeeded {
							mgcq.store.metrics.GCResolveSuccess.Inc(int64(len(txn.LockSpans)))
						} else {
							mgcq.store.metrics.GCTxnIntentsResolveFailed.Inc(int64(len(txn.LockSpans)))
						}
					})
			if errors.Is(err, stop.ErrThrottled) {
				log.Eventf(ctx, "processing txn %s: %s; skipping for future GC", txn.ID.Short(), err)
				return nil
			}
			return err
		})
	if err != nil {
		return false, err
	}

	scoreAfter := makeMVCCGCQueueScore(
		ctx, repl, repl.store.Clock().Now(), lastGC, conf.TTL(), canAdvanceGCThreshold)
	log.VEventf(ctx, 2, "MVCC stats after GC: %+v", repl.GetMVCCStats())
	log.VEventf(ctx, 2, "GC score after GC: %s", scoreAfter)
	updateStoreMetricsWithGCInfo(mgcq.store.metrics, info)
	// If the score after running through the queue indicates that this
	// replica should be re-queued for GC it most likely means that there
	// is something wrong with the stats. One such known issue is
	// https://github.com/cockroachdb/cockroach/issues/82920. To fix this we
	// recompute stats, it's an expensive operation but it's better to recompute
	// them then to spin the GC queue.
	// Note: the score is not recomputed as if the GC queue was going to run again,
	// because we are reusing the old lastGC and canAdvanceGCThreshold. This helps
	// avoid issues with e.g. cooldown timers and focuses the recomputation on the
	// difference in stats after GC.

	if scoreAfter.ShouldQueue {
		// The scores are very long, so splitting into multiple lines manually for
		// readability.
		log.Infof(ctx, "GC still needed following GC, recomputing MVCC stats")
		log.Infof(ctx, "old score %s", r)
		log.Infof(ctx, "new score %s", scoreAfter)
		req := kvpb.RecomputeStatsRequest{
			RequestHeader: kvpb.RequestHeader{Key: desc.StartKey.AsRawKey()},
		}
		var b kv.Batch
		b.AddRawRequest(&req)
		err := repl.store.db.Run(ctx, &b)
		if err != nil {
			log.Errorf(ctx, "failed to recompute stats with error=%s", err)
		}
	}

	return true, nil
}

func updateStoreMetricsWithGCInfo(metrics *StoreMetrics, info gc.Info) {
	metrics.GCNumKeysAffected.Inc(int64(info.NumKeysAffected))
	metrics.GCNumRangeKeysAffected.Inc(int64(info.NumRangeKeysAffected))
	metrics.GCIntentsConsidered.Inc(int64(info.IntentsConsidered))
	metrics.GCIntentTxns.Inc(int64(info.IntentTxns))
	metrics.GCTransactionSpanScanned.Inc(int64(info.TransactionSpanTotal))
	metrics.GCTransactionSpanGCAborted.Inc(int64(info.TransactionSpanGCAborted))
	metrics.GCTransactionSpanGCCommitted.Inc(int64(info.TransactionSpanGCCommitted))
	metrics.GCTransactionSpanGCStaging.Inc(int64(info.TransactionSpanGCStaging))
	metrics.GCTransactionSpanGCPending.Inc(int64(info.TransactionSpanGCPending))
	metrics.GCAbortSpanScanned.Inc(int64(info.AbortSpanTotal))
	metrics.GCAbortSpanConsidered.Inc(int64(info.AbortSpanConsidered))
	metrics.GCAbortSpanGCNum.Inc(int64(info.AbortSpanGCNum))
	metrics.GCPushTxn.Inc(int64(info.PushTxn))
	metrics.GCResolveTotal.Inc(int64(info.ResolveTotal))
	metrics.GCUsedClearRange.Inc(int64(info.ClearRangeSpanOperations))
	metrics.GCFailedClearRange.Inc(int64(info.ClearRangeSpanFailures))
}

func (mgcq *mvccGCQueue) postProcessScheduled(
	ctx context.Context, processedReplica replicaInQueue, priority float64,
) {
	if priority < deleteRangePriority {
		mgcq.lastRangeWasHighPriority = false
		return
	}

	if !mgcq.lastRangeWasHighPriority {
		// We are most likely processing first range that has a GC hint notifying
		// that multiple range deletions happen.
		if err := timeutil.RunWithTimeout(ctx, "gc-check-hinted-hi-pri-replicas", gcHintScannerTimeout, func(ctx context.Context) error {
			mgcq.scanReplicasForHiPriGCHints(ctx, processedReplica.GetRangeID())
			return ctx.Err()
		}); err != nil {
			log.Infof(ctx, "failed to start mvcc gc scan for range delete hints, error: %s", err)
		}
		// Set flag indicating that we are already collecting high priority to avoid
		// rescanning and re-enqueueing ranges multiple times.
		mgcq.lastRangeWasHighPriority = true
	}
}

// scanReplicasForHiPriGCHints scans replicas in random order which makes
// single bad replica that could cause scan to time out be less disruptive
// in case we need to retry.
func (mgcq *mvccGCQueue) scanReplicasForHiPriGCHints(
	ctx context.Context, triggerRange roachpb.RangeID,
) {
	var foundReplicas int
	clockNow := mgcq.store.Clock().NowAsClockTimestamp()
	now := clockNow.ToTimestamp()
	v := newStoreReplicaVisitor(mgcq.store)
	v.Visit(func(replica *Replica) bool {
		if replica.GetRangeID() == triggerRange {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		gCHint := replica.GetGCHint()
		if !gCHint.LatestRangeDeleteTimestamp.IsEmpty() {
			desc := replica.Desc()
			spanConfig, err := replica.SpanConfig()
			if err != nil {
				log.Infof(ctx, "skip replica since no span config found ")
				return true
			}
			gcThreshold := now.Add(-int64(spanConfig.GCPolicy.TTLSeconds)*1e9,
				0)
			if gCHint.LatestRangeDeleteTimestamp.Less(gcThreshold) {
				// If replica has a hint we also need to check if this replica is a
				// leaseholder as mvcc gc queue only works with leaseholder replicas.
				var isLeaseHolder bool
				if mgcq.leaseholderCheckInterceptor != nil {
					isLeaseHolder = mgcq.leaseholderCheckInterceptor(ctx, replica, clockNow)
				} else {
					l := replica.LeaseStatusAt(ctx, clockNow)
					isLeaseHolder = l.IsValid() && l.OwnedBy(replica.StoreID())
				}
				if !isLeaseHolder {
					return true
				}
				added, _ := mgcq.addInternal(ctx, desc, replica.ReplicaID(), deleteRangePriority)
				if added {
					mgcq.store.metrics.GCEnqueueHighPriority.Inc(1)
					foundReplicas++
				}
			}
		}
		return true
	})
	log.Infof(ctx, "mvcc gc scan for range delete hints found %d replicas", foundReplicas)
}

// timer returns a constant duration to space out GC processing
// for successive queued replicas.
func (mgcq *mvccGCQueue) timer(_ time.Duration) time.Duration {
	if mgcq.lastRangeWasHighPriority {
		return mvccGCQueueHighPriInterval.Get(&mgcq.store.ClusterSettings().SV)
	}
	return mvccGCQueueInterval.Get(&mgcq.store.ClusterSettings().SV)
}

// purgatoryChan returns nil.
func (*mvccGCQueue) purgatoryChan() <-chan time.Time {
	return nil
}

func (*mvccGCQueue) updateChan() <-chan time.Time {
	return nil
}
