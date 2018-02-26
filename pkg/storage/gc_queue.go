// Copyright 2014 The Cockroach Authors.
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
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	// gcQueueTimerDuration is the duration between GCs of queued replicas.
	gcQueueTimerDuration = 1 * time.Second
	// intentAgeNormalization is the average age of outstanding intents
	// which amount to a score of "1" added to total replica priority.
	intentAgeNormalization = 24 * time.Hour // 1 day
	// intentAgeThreshold is the threshold after which an extant intent
	// will be resolved.
	intentAgeThreshold = 2 * time.Hour // 2 hour

	// txnCleanupThreshold is the threshold after which a transaction is
	// considered abandoned and fit for removal, as measured by the
	// maximum of its last heartbeat and timestamp. Abort spans for the
	// transaction are cleaned up at the same time.
	//
	// TODO(tschottdorf): need to enforce at all times that this is much
	// larger than the heartbeat interval used by the coordinator.
	txnCleanupThreshold = time.Hour

	// Thresholds used to decide whether to queue for GC based
	// on keys and intents.
	gcKeyScoreThreshold    = 2
	gcIntentScoreThreshold = 10

	// gcKeyVersionChunkBytes is the threshold size for splitting
	// GCRequests into multiple batches.
	gcKeyVersionChunkBytes = base.ChunkRaftCommandThresholdBytes
)

// gcQueue manages a queue of replicas slated to be scanned in their
// entirety using the MVCC versions iterator. The gc queue manages the
// following tasks:
//
//  - GC of version data via TTL expiration (and more complex schemes
//    as implemented going forward).
//  - Resolve extant write intents (pushing their transactions).
//  - GC of old transaction and AbortSpan entries. This should include
//    most committed and aborted entries almost immediately and, after a
//    threshold on inactivity, all others.
//
// The shouldQueue function combines the need for the above tasks into a
// single priority. If any task is overdue, shouldQueue returns true.
type gcQueue struct {
	*baseQueue
}

// newGCQueue returns a new instance of gcQueue.
func newGCQueue(store *Store, gossip *gossip.Gossip) *gcQueue {
	gcq := &gcQueue{}
	gcq.baseQueue = newBaseQueue(
		"gc", gcq, store, gossip,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSystemConfig:    true,
			acceptsUnsplitRanges: false,
			successes:            store.metrics.GCQueueSuccesses,
			failures:             store.metrics.GCQueueFailures,
			pending:              store.metrics.GCQueuePending,
			processingNanos:      store.metrics.GCQueueProcessingNanos,
		},
	)
	return gcq
}

// A gcFunc sends the chunks of GCRequests in the order in which they
// appear. It may but does not have to return on the first error.
type gcFunc func(context.Context, [][]roachpb.GCRequest_GCKey, *GCInfo) error

// A cleanupIntentsFunc synchronously resolves the supplied intents
// (which may be PENDING, in which case they are first pushed) while
// taking care of proper batching.
type cleanupIntentsFunc func(context.Context, []roachpb.Intent) error

// A cleanupTxnIntentsFunc asynchronously cleans up intents from a
// transaction record, pushing the transaction first if it is
// PENDING. Once all intents are resolved successfully, removes the
// transaction record.
type cleanupTxnIntentsAsyncFunc func(context.Context, *roachpb.Transaction, []roachpb.Intent) error

// gcQueueScore holds details about the score returned by makeGCQueueScoreImpl for
// testing and logging. The fields in this struct are documented in
// makeGCQueueScoreImpl.
type gcQueueScore struct {
	TTL                 time.Duration
	LikelyLastGC        time.Duration
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

func (r gcQueueScore) String() string {
	if (r == gcQueueScore{}) {
		return "(empty)"
	}
	if r.ExpMinGCByteAgeReduction < 0 {
		r.ExpMinGCByteAgeReduction = 0
	}
	likelyLastGC := "never"
	if r.LikelyLastGC != 0 {
		likelyLastGC = fmt.Sprintf("%s ago", r.LikelyLastGC)
	}
	return fmt.Sprintf("queue=%t with %.2f/fuzz(%.2f)=%.2f=valScaleScore(%.2f)*deadFrac(%.2f)+intentScore(%.2f)\n"+
		"likely last GC: %s, %s non-live, curr. age %s*s, min exp. reduction: %s*s",
		r.ShouldQueue, r.FinalScore, r.FuzzFactor, r.FinalScore/r.FuzzFactor, r.ValuesScalableScore,
		r.DeadFraction, r.IntentScore, likelyLastGC, humanizeutil.IBytes(r.GCBytes),
		humanizeutil.IBytes(r.GCByteAge), humanizeutil.IBytes(r.ExpMinGCByteAgeReduction))
}

// shouldQueue determines whether a replica should be queued for garbage
// collection, and if so, at what priority. Returns true for shouldQ
// in the event that the cumulative ages of GC'able bytes or extant
// intents exceed thresholds.
func (gcq *gcQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, sysCfg config.SystemConfig,
) (bool, float64) {
	r := makeGCQueueScore(ctx, repl, now, sysCfg)
	return r.ShouldQueue, r.FinalScore
}

func makeGCQueueScore(
	ctx context.Context, repl *Replica, now hlc.Timestamp, sysCfg config.SystemConfig,
) gcQueueScore {
	repl.mu.Lock()
	ms := *repl.mu.state.Stats
	gcThreshold := *repl.mu.state.GCThreshold
	repl.mu.Unlock()

	desc := repl.Desc()
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		log.Errorf(ctx, "could not find zone config for range %s: %s", repl, err)
		return gcQueueScore{}
	}
	// Use desc.RangeID for fuzzing the final score, so that different ranges
	// have slightly different priorities and even symmetrical workloads don't
	// trigger GC at the same time.
	r := makeGCQueueScoreImpl(
		ctx, int64(desc.RangeID), now, ms, zone.GC.TTLSeconds,
	)
	if (gcThreshold != hlc.Timestamp{}) {
		r.LikelyLastGC = time.Duration(now.WallTime - gcThreshold.Add(r.TTL.Nanoseconds(), 0).WallTime)
	}
	return r
}

// makeGCQueueScoreImpl is used to compute when to trigger the GC Queue. It's
// important that we don't queue a replica before a relevant amount of data is
// actually deletable, or the queue might run in a tight loop. To this end, we
// use a base score with the right interplay between GCByteAge and TTL and
// additionally weigh it so that GC is delayed when a large proportion of the
// data in the replica is live. Additionally, returned scores are slightly
// perturbed to avoid groups of replicas becoming eligible for GC at the same
// time repeatedly.
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
// key is a domino aligned with the right side of the frame, its width equal to
// its size, and its height given by the duration (in seconds) it's been
// non-live.
//
// The combined surface of the dominos is then `GCBytesAge`, and the claim is
// that if the total sum of domino heights (i.e. sizes) is `GCBytes`, and the
// surface is larger than `ttl*GCBytes` by some positive `X`, then after
// removing the dominos that cross the line `x=-ttl` (i.e. `ttl` to the left
// from the right side of the frame), at least a surface area of `X` has been
// removed.
//
//               x=-ttl                 GCBytes=1+4
//                 |           3 (age)
//                 |          +-------+
//                 |          | keep  | 1 (bytes)
//                 |          +-------+
//            +-----------------------+
//            |                       |
//            |        remove         | 3 (bytes)
//            |                       |
//            +-----------------------+
//                 |   7 (age)
//
// This is true because
//
// deletable area  = total area       - nondeletable area
//                 = X + ttl*GCBytes  - nondeletable area
//                >= X + ttl*GCBytes  - ttl*(bytes in nondeletable area)
//                 = X + ttl*(GCBytes - bytes in nondeletable area)
//                >= X.
//
// Or, in other words, you can only hope to put `ttl*GCBytes` of area in the
// "safe" rectangle. Once you've done that, everything else you put is going to
// be deleted.
//
// This means that running GC will always result in a `GCBytesAge` of `<=
// ttl*GCBytes`, and that a decent trigger for GC is a multiple of
// `ttl*GCBytes`.
func makeGCQueueScoreImpl(
	ctx context.Context, fuzzSeed int64, now hlc.Timestamp, ms enginepb.MVCCStats, ttlSeconds int32,
) gcQueueScore {
	ms.Forward(now.WallTime)
	var r gcQueueScore
	r.TTL = time.Duration(ttlSeconds) * time.Second

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
	// Note that for ranges with ContainsEstimates=true, the value here may not
	// reflect reality, and may even be nonsensical (though that's unlikely).
	r.ExpMinGCByteAgeReduction = r.GCByteAge - r.GCBytes*int64(r.TTL.Seconds())

	// DeadFraction is close to 1 when most values are dead, and close to zero
	// when most of the replica is live. For example, for a replica with no
	// superseded values, this should be (almost) zero. For one just hit
	// completely by a DeleteRange, it should be (almost) one.
	//
	// The algebra below is complicated by the fact that ranges may contain
	// stats that aren't exact (ContainsEstimates=true).
	clamp := func(n int64) float64 {
		if n < 0 {
			return 0.0
		}
		return float64(n)
	}
	r.DeadFraction = math.Max(1-clamp(ms.LiveBytes)/(1+clamp(ms.ValBytes)+clamp(ms.KeyBytes)), 0)

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
	r.IntentScore = ms.AvgIntentAge(now.WallTime) / float64(intentAgeNormalization.Nanoseconds()/1E9)

	// Randomly skew the score down a bit to cause decoherence of replicas with
	// similar load. Note that we'll only ever reduce the score, never increase
	// it (for increasing it could lead to a fruitless run).
	r.FuzzFactor = 0.95 + 0.05*rand.New(rand.NewSource(fuzzSeed)).Float64()

	// Compute priority.
	valScore := r.DeadFraction * r.ValuesScalableScore
	r.ShouldQueue = r.FuzzFactor*valScore > gcKeyScoreThreshold || r.FuzzFactor*r.IntentScore > gcIntentScoreThreshold
	r.FinalScore = r.FuzzFactor * (valScore + r.IntentScore)

	return r
}

// processLocalKeyRange scans the local range key entries, consisting of
// transaction records, queue last processed timestamps, and range descriptors.
//
// - Transaction entries:
//   - For expired transactions , schedule the intents for
//     asynchronous resolution. The actual transaction spans are not
//     returned for GC in this pass, but are separately GC'ed after
//     successful resolution of all intents. The exception is if there
//     are no intents on the txn record, in which case it's returned for
//     immediate GC.
//
// - Queue last processed times: cleanup any entries which don't match
//   this range's start key. This can happen on range merges.
func processLocalKeyRange(
	ctx context.Context,
	snap engine.Reader,
	desc *roachpb.RangeDescriptor,
	cutoff hlc.Timestamp,
	infoMu *lockableGCInfo,
	cleanupTxnIntentsAsyncFn cleanupTxnIntentsAsyncFunc,
) ([]roachpb.GCRequest_GCKey, error) {
	infoMu.Lock()
	defer infoMu.Unlock()

	var gcKeys []roachpb.GCRequest_GCKey

	handleTxnIntents := func(key roachpb.Key, txn *roachpb.Transaction) error {
		// If the transaction needs to be pushed or there are intents to
		// resolve, invoke the cleanup function.
		if txn.Status == roachpb.PENDING || len(txn.Intents) > 0 {
			return cleanupTxnIntentsAsyncFn(ctx, txn, roachpb.AsIntents(txn.Intents, txn))
		}
		gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: key}) // zero timestamp
		return nil
	}

	handleOneTransaction := func(kv roachpb.KeyValue) error {
		var txn roachpb.Transaction
		if err := kv.Value.GetProto(&txn); err != nil {
			return err
		}
		infoMu.TransactionSpanTotal++
		if !txn.LastActive().Less(cutoff) {
			return nil
		}

		// The transaction record should be considered for removal.
		switch txn.Status {
		case roachpb.PENDING:
			infoMu.TransactionSpanGCPending++
		case roachpb.ABORTED:
			infoMu.TransactionSpanGCAborted++
		case roachpb.COMMITTED:
			infoMu.TransactionSpanGCCommitted++
		default:
			panic(fmt.Sprintf("invalid transaction state: %s", txn))
		}
		return handleTxnIntents(kv.Key, &txn)
	}

	handleOneQueueLastProcessed := func(kv roachpb.KeyValue, rangeKey roachpb.RKey) error {
		if !rangeKey.Equal(desc.StartKey) {
			// Garbage collect the last processed timestamp if it doesn't match start key.
			gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: kv.Key}) // zero timestamp
		}
		return nil
	}

	handleOne := func(kv roachpb.KeyValue) error {
		rangeKey, suffix, _, err := keys.DecodeRangeKey(kv.Key)
		if err != nil {
			return err
		}
		if suffix.Equal(keys.LocalTransactionSuffix.AsRawKey()) {
			if err := handleOneTransaction(kv); err != nil {
				return err
			}
		} else if suffix.Equal(keys.LocalQueueLastProcessedSuffix.AsRawKey()) {
			if err := handleOneQueueLastProcessed(kv, roachpb.RKey(rangeKey)); err != nil {
				return err
			}
		}
		return nil
	}

	startKey := keys.MakeRangeKeyPrefix(desc.StartKey)
	endKey := keys.MakeRangeKeyPrefix(desc.EndKey)

	_, err := engine.MVCCIterate(ctx, snap, startKey, endKey,
		hlc.Timestamp{}, true /* consistent */, false /* tombstones */, nil, /* txn */
		false /* reverse */, func(kv roachpb.KeyValue) (bool, error) {
			return false, handleOne(kv)
		})
	return gcKeys, err
}

// processAbortSpan iterates through the local AbortSpan entries
// and collects entries which indicate that a client which was running
// this transaction must have realized that it has been aborted (due to
// heartbeating having failed). The parameter minAge is typically a
// multiple of the heartbeat timeout used by the coordinator.
//
// TODO(tschottdorf): this could be done in Replica.GC itself, but it's
// handy to have it here for stats (though less performant due to sending
// all of the keys over the wire).
func processAbortSpan(
	ctx context.Context,
	snap engine.Reader,
	rangeID roachpb.RangeID,
	threshold hlc.Timestamp,
	infoMu *lockableGCInfo,
) []roachpb.GCRequest_GCKey {
	var gcKeys []roachpb.GCRequest_GCKey
	abortSpan := abortspan.New(rangeID)
	infoMu.Lock()
	defer infoMu.Unlock()
	abortSpan.Iterate(ctx, snap, func(key []byte, v roachpb.AbortSpanEntry) {
		infoMu.AbortSpanTotal++
		if v.Timestamp.Less(threshold) {
			infoMu.AbortSpanGCNum++
			gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: key})
		}
	})
	return gcKeys
}

// process iterates through all keys in a replica's range, calling the garbage
// collector for each key and associated set of values. GC'd keys are batched
// into GC calls. Extant intents are resolved if intents are older than
// intentAgeThreshold. The transaction and AbortSpan records are also
// scanned and old entries evicted. During normal operation, both of these
// records are cleaned up when their respective transaction finishes, so the
// amount of work done here is expected to be small.
//
// Some care needs to be taken to avoid cyclic recreation of entries during GC:
// * a Push initiated due to an intent may recreate a transaction entry
// * resolving an intent may write a new AbortSpan entry
// * obtaining the transaction for a AbortSpan entry requires a Push
//
// The following order is taken below:
// 1) collect all intents with sufficiently old txn record
// 2) collect these intents' transactions
// 3) scan the transaction table, collecting abandoned or completed txns
// 4) push all of these transactions (possibly recreating entries)
// 5) resolve all intents (unless the txn is still PENDING), which will recreate
//    AbortSpan entries (but with the txn timestamp; i.e. likely GC'able)
// 6) scan the AbortSpan table for old entries
// 7) push these transactions (again, recreating txn entries).
// 8) send a GCRequest.
func (gcq *gcQueue) process(ctx context.Context, repl *Replica, sysCfg config.SystemConfig) error {
	now := repl.store.Clock().Now()
	r := makeGCQueueScore(ctx, repl, now, sysCfg)
	if !r.ShouldQueue {
		log.Eventf(ctx, "skipping replica; low score %s", r)
		return nil
	}

	log.Eventf(ctx, "processing replica with score %s", r)
	return gcq.processImpl(ctx, repl, sysCfg, now)
}

// chunkGCRequest chunks the supplied gcKeys (which are consumed by this method) into
// multiple batches which must be executed in order by the caller.
func chunkGCRequest(
	desc *roachpb.RangeDescriptor, info *GCInfo, gcKeys [][]roachpb.GCRequest_GCKey,
) []roachpb.GCRequest {
	var template roachpb.GCRequest
	template.Key = desc.StartKey.AsRawKey()
	template.EndKey = desc.EndKey.AsRawKey()
	ret := make([]roachpb.GCRequest, 0, len(gcKeys)+1)

	gc1 := template
	gc1.Threshold = info.Threshold
	gc1.TxnSpanGCThreshold = info.TxnSpanGCThreshold
	// The first request is intentionally kept very small since it
	// updates the Range-wide GCThresholds, and thus must block all
	// reads and writes while it is being applied.

	ret = append(ret, gc1)

	for _, keys := range gcKeys {
		gc := template
		gc.Keys = keys
		ret = append(ret, gc)
	}

	return ret
}

func (gcq *gcQueue) processImpl(
	ctx context.Context, repl *Replica, sysCfg config.SystemConfig, now hlc.Timestamp,
) error {
	snap := repl.store.Engine().NewSnapshot()
	desc := repl.Desc()
	defer snap.Close()

	// Lookup the GC policy for the zone containing this key range.
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return errors.Errorf("could not find zone config for range %s: %s", repl, err)
	}

	info, err := RunGC(ctx, desc, snap, now, zone.GC,
		func(ctx context.Context, gcKeys [][]roachpb.GCRequest_GCKey, info *GCInfo) error {
			// Chunk the keys into multiple GC requests to interleave more
			// gracefully with other Raft traffic.
			batches := chunkGCRequest(desc, info, gcKeys)

			for i, gcArgs := range batches {
				var ba roachpb.BatchRequest

				// Technically not needed since we're talking directly to the Range.
				ba.RangeID = desc.RangeID
				ba.Timestamp = now

				// TODO(tschottdorf): This is one of these instances in which we want
				// to be more careful that the request ends up on the correct Replica,
				// and we might have to worry about mixing range-local and global keys
				// in a batch which might end up spanning Ranges by the time it executes.
				ba.Add(&gcArgs)
				log.Eventf(ctx, "sending batch %d of %d", i+1, len(batches))
				if _, pErr := repl.Send(ctx, ba); pErr != nil {
					log.VErrEvent(ctx, 2, pErr.String())
					return pErr.GoError()
				}
			}
			return nil
		},
		func(ctx context.Context, intents []roachpb.Intent) error {
			intentCount, err := repl.store.intentResolver.cleanupIntents(ctx, intents, now, roachpb.PUSH_ABORT)
			if err == nil {
				gcq.store.metrics.GCResolveSuccess.Inc(int64(intentCount))
			}
			return err
		},
		func(ctx context.Context, txn *roachpb.Transaction, intents []roachpb.Intent) error {
			err := repl.store.intentResolver.cleanupTxnIntentsOnGCAsync(ctx, txn, intents, now)
			if errors.Cause(err) == stop.ErrThrottled {
				log.Eventf(ctx, "processing txn %s: %s; skipping for future GC", txn.ID.Short(), err)
				return nil
			}
			return err
		})
	if err != nil {
		return err
	}

	log.Eventf(ctx, "MVCC stats after GC: %+v", repl.GetMVCCStats())
	log.Eventf(ctx, "GC score after GC: %s", makeGCQueueScore(ctx, repl, repl.store.Clock().Now(), sysCfg))
	info.updateMetrics(gcq.store.metrics)

	return nil
}

// GCInfo contains statistics and insights from a GC run.
type GCInfo struct {
	// Now is the timestamp used for age computations.
	Now hlc.Timestamp
	// Policy is the policy used for this garbage collection cycle.
	Policy config.GCPolicy
	// Stats about the userspace key-values considered, namely the number of
	// keys with GC'able data, the number of "old" intents and the number of
	// associated distinct transactions.
	NumKeysAffected, IntentsConsidered, IntentTxns int
	// TransactionSpanTotal is the total number of entries in the transaction span.
	TransactionSpanTotal int
	// Summary of transactions which were found GCable (assuming that
	// potentially necessary intent resolutions did not fail).
	TransactionSpanGCAborted, TransactionSpanGCCommitted, TransactionSpanGCPending int
	// TxnSpanGCThreshold is the cutoff for transaction span GC. Transactions
	// with a smaller LastActive() were considered for GC.
	TxnSpanGCThreshold hlc.Timestamp
	// AbortSpanTotal is the total number of transactions present in the AbortSpan.
	AbortSpanTotal int
	// AbortSpanConsidered is the number of AbortSpan entries old enough to be
	// considered for removal. An "entry" corresponds to one transaction;
	// more than one key-value pair may be associated with it.
	AbortSpanConsidered int
	// AbortSpanGCNum is the number of AbortSpan entries fit for removal (due
	// to their transactions having terminated).
	AbortSpanGCNum int
	// PushTxn is the total number of pushes attempted in this cycle.
	PushTxn int
	// ResolveTotal is the total number of attempted intent resolutions in
	// this cycle.
	ResolveTotal int
	// Threshold is the computed expiration timestamp. Equal to `Now - Policy`.
	Threshold hlc.Timestamp
	// AffectedVersionsKeyBytes is the number of (fully encoded) bytes deleted from keys in the storage engine.
	// Note that this does not account for compression that the storage engine uses to store data on disk. Real
	// space savings tends to be smaller due to this compression, and space may be released only at a later point
	// in time.
	AffectedVersionsKeyBytes int64
	// AffectedVersionsValBytes is the number of (fully encoded) bytes deleted from values in the storage engine.
	// See AffectedVersionsKeyBytes for caveats.
	AffectedVersionsValBytes int64
}

func (info *GCInfo) updateMetrics(metrics *StoreMetrics) {
	metrics.GCNumKeysAffected.Inc(int64(info.NumKeysAffected))
	metrics.GCIntentsConsidered.Inc(int64(info.IntentsConsidered))
	metrics.GCIntentTxns.Inc(int64(info.IntentTxns))
	metrics.GCTransactionSpanScanned.Inc(int64(info.TransactionSpanTotal))
	metrics.GCTransactionSpanGCAborted.Inc(int64(info.TransactionSpanGCAborted))
	metrics.GCTransactionSpanGCCommitted.Inc(int64(info.TransactionSpanGCCommitted))
	metrics.GCTransactionSpanGCPending.Inc(int64(info.TransactionSpanGCPending))
	metrics.GCAbortSpanScanned.Inc(int64(info.AbortSpanTotal))
	metrics.GCAbortSpanConsidered.Inc(int64(info.AbortSpanConsidered))
	metrics.GCAbortSpanGCNum.Inc(int64(info.AbortSpanGCNum))
	metrics.GCPushTxn.Inc(int64(info.PushTxn))
	metrics.GCResolveTotal.Inc(int64(info.ResolveTotal))
}

type lockableGCInfo struct {
	syncutil.Mutex
	GCInfo
}

// RunGC runs garbage collection for the specified descriptor on the
// provided Engine (which is not mutated). It uses the provided gcFn
// to run garbage collection once on all implicated spans,
// cleanupIntentsFn to resolve intents synchronously, and
// cleanupTxnIntentsAsyncFn to asynchronously cleanup intents and
// associated transaction record on success.
func RunGC(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap engine.Reader,
	now hlc.Timestamp,
	policy config.GCPolicy,
	gcFn gcFunc,
	cleanupIntentsFn cleanupIntentsFunc,
	cleanupTxnIntentsAsyncFn cleanupTxnIntentsAsyncFunc,
) (GCInfo, error) {

	iter := rditer.NewReplicaDataIterator(desc, snap, true /* replicatedOnly */)
	defer iter.Close()

	var infoMu = lockableGCInfo{}
	infoMu.Policy = policy
	infoMu.Now = now

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now.Add(-intentAgeThreshold.Nanoseconds(), 0)
	txnExp := now.Add(-txnCleanupThreshold.Nanoseconds(), 0)

	gc := engine.MakeGarbageCollector(now, policy)
	infoMu.Threshold = gc.Threshold
	infoMu.TxnSpanGCThreshold = txnExp

	var gcKeys [][]roachpb.GCRequest_GCKey
	var batchGCKeys []roachpb.GCRequest_GCKey
	var batchGCKeysBytes int64
	var expBaseKey roachpb.Key
	var keys []engine.MVCCKey
	var vals [][]byte
	var keyBytes int64
	var valBytes int64

	// Maps from txn ID to txn and intent key slice.
	txnMap := map[uuid.UUID]*roachpb.Transaction{}
	intentSpanMap := map[uuid.UUID][]roachpb.Span{}

	// processKeysAndValues is invoked with each key and its set of
	// values. Intents older than the intent age threshold are sent for
	// resolution and values after the MVCC metadata, and possible
	// intent, are sent for garbage collection.
	processKeysAndValues := func() {
		// If there's more than a single value for the key, possibly send for GC.
		if len(keys) > 1 {
			meta := &enginepb.MVCCMetadata{}
			if err := protoutil.Unmarshal(vals[0], meta); err != nil {
				log.Errorf(ctx, "unable to unmarshal MVCC metadata for key %q: %s", keys[0], err)
			} else {
				// In the event that there's an active intent, send for
				// intent resolution if older than the threshold.
				startIdx := 1
				if meta.Txn != nil {
					// Keep track of intent to resolve if older than the intent
					// expiration threshold.
					if hlc.Timestamp(meta.Timestamp).Less(intentExp) {
						txnID := meta.Txn.ID
						if _, ok := txnMap[txnID]; !ok {
							txnMap[txnID] = &roachpb.Transaction{
								TxnMeta: *meta.Txn,
							}
							// IntentTxns and PushTxn will be equal here, since
							// pushes to transactions whose record lies in this
							// range (but which are not associated to a remaining
							// intent on it) happen asynchronously and are accounted
							// for separately. Thus higher up in the stack, we
							// expect PushTxn > IntentTxns.
							infoMu.IntentTxns++
							// All transactions in txnMap may be PENDING and
							// cleanupIntentsFn will push them to finalize them.
							infoMu.PushTxn++
						}
						infoMu.IntentsConsidered++
						intentSpanMap[txnID] = append(intentSpanMap[txnID], roachpb.Span{Key: expBaseKey})
					}
					// With an active intent, GC ignores MVCC metadata & intent value.
					startIdx = 2
				}
				// See if any values may be GC'd.
				if idx, gcTS := gc.Filter(keys[startIdx:], vals[startIdx:]); gcTS != (hlc.Timestamp{}) {
					// Batch keys after the total size of version keys exceeds
					// the threshold limit. This avoids sending potentially large
					// GC requests through Raft. Iterate through the keys in reverse
					// order so that GC requests can be made multiple times even on
					// a single key, with successively newer timestamps to prevent
					// any single request from exploding during GC evaluation.
					for i := len(keys) - 1; i >= startIdx+idx; i-- {
						keyBytes = int64(keys[i].EncodedSize())
						valBytes = int64(len(vals[i]))

						// Add the total size of the GC'able versions of the keys and values to GCInfo.
						infoMu.GCInfo.AffectedVersionsKeyBytes += keyBytes
						infoMu.GCInfo.AffectedVersionsValBytes += valBytes

						batchGCKeysBytes += keyBytes
						// If the current key brings the batch over the target
						// size, add the current timestamp to finish the current
						// chunk and start a new one.
						if batchGCKeysBytes >= gcKeyVersionChunkBytes {
							batchGCKeys = append(batchGCKeys, roachpb.GCRequest_GCKey{Key: expBaseKey, Timestamp: keys[i].Timestamp})
							gcKeys = append(gcKeys, batchGCKeys)
							batchGCKeys = []roachpb.GCRequest_GCKey{}
							batchGCKeysBytes = 0
						}
					}
					// Add the key to the batch at the GC timestamp, unless it was already added.
					if batchGCKeysBytes != 0 {
						batchGCKeys = append(batchGCKeys, roachpb.GCRequest_GCKey{Key: expBaseKey, Timestamp: gcTS})
					}
					infoMu.NumKeysAffected++
				}
			}
		}
	}

	// Iterate through the keys and values of this replica's range.
	log.Event(ctx, "iterating through range")
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return GCInfo{}, err
		} else if !ok {
			break
		}
		iterKey := iter.Key()
		if !iterKey.IsValue() || !iterKey.Key.Equal(expBaseKey) {
			// Moving to the next key (& values).
			processKeysAndValues()
			expBaseKey = iterKey.Key
			if !iterKey.IsValue() {
				keys = []engine.MVCCKey{iter.Key()}
				vals = [][]byte{iter.Value()}
				continue
			}
			// An implicit metadata.
			keys = []engine.MVCCKey{engine.MakeMVCCMetadataKey(iterKey.Key)}
			// A nil value for the encoded MVCCMetadata. This will unmarshal to an
			// empty MVCCMetadata which is sufficient for processKeysAndValues to
			// determine that there is no intent.
			vals = [][]byte{nil}
		}
		keys = append(keys, iter.Key())
		vals = append(vals, iter.Value())
	}
	// Handle last collected set of keys/vals.
	processKeysAndValues()
	if len(batchGCKeys) > 0 {
		gcKeys = append(gcKeys, batchGCKeys)
	}

	// Process local range key entries (txn records, queue last processed times).
	localRangeKeys, err := processLocalKeyRange(ctx, snap, desc, txnExp, &infoMu, cleanupTxnIntentsAsyncFn)
	if err != nil {
		return GCInfo{}, err
	}

	// From now on, all newly added keys are range-local.
	// TODO(tschottdorf): Might need to use two requests at some point since we
	// hard-coded the full non-local key range in the header, but that does
	// not take into account the range-local keys. It will be OK as long as
	// we send directly to the Replica, though.
	if len(localRangeKeys) > 0 {
		gcKeys = append(gcKeys, localRangeKeys)
	}

	// Clean up the AbortSpan.
	log.Event(ctx, "processing AbortSpan")
	abortSpanKeys := processAbortSpan(ctx, snap, desc.RangeID, txnExp, &infoMu)
	if len(abortSpanKeys) > 0 {
		gcKeys = append(gcKeys, abortSpanKeys)
	}

	infoMu.Lock()
	log.Eventf(ctx, "assembled GC keys, now proceeding to GC; stats so far %+v", infoMu.GCInfo)
	infoMu.Unlock()

	// Process the keys before beginning to push transactions and
	// resolve intents so that we don't lose all of the work we've done
	// thus far gathering GC'able keys.
	if err := gcFn(ctx, gcKeys, &infoMu.GCInfo); err != nil {
		return GCInfo{}, err
	}

	// Push transactions (if pending) and resolve intents.
	var intents []roachpb.Intent
	for txnID, txn := range txnMap {
		intents = append(intents, roachpb.AsIntents(intentSpanMap[txnID], txn)...)
	}
	infoMu.Lock()
	infoMu.ResolveTotal += len(intents)
	infoMu.Unlock()
	log.Eventf(ctx, "cleanup of %d intents", len(intents))
	if err := cleanupIntentsFn(ctx, intents); err != nil {
		return GCInfo{}, err
	}

	return infoMu.GCInfo, nil
}

// timer returns a constant duration to space out GC processing
// for successive queued replicas.
func (*gcQueue) timer(_ time.Duration) time.Duration {
	return gcQueueTimerDuration
}

// purgatoryChan returns nil.
func (*gcQueue) purgatoryChan() <-chan struct{} {
	return nil
}
