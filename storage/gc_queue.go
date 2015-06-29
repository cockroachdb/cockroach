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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// gcQueueMaxSize is the max size of the gc queue.
	gcQueueMaxSize = 100
	// gcQueueTimerDuration is the duration between GCs of queued ranges.
	gcQueueTimerDuration = 1 * time.Second
	// gcByteCountNormalization is the count of GC'able bytes which
	// amount to a score of "1" added to total range priority.
	gcByteCountNormalization = 1 << 20 // 1 MB
	// intentAgeNormalization is the average age of outstanding intents
	// which amount to a score of "1" added to total range priority.
	intentAgeNormalization = 24 * time.Hour // 1 day
	// intentAgeThreshold is the threshold after which an extant intent
	// will be resolved.
	intentAgeThreshold = 2 * time.Hour // 2 hour
)

// gcQueue manages a queue of ranges slated to be scanned in their
// entirety using the MVCC versions iterator. The gc queue manages the
// following tasks:
//
//  - GC of version data via TTL expiration (and more complex schemes
//    as implemented going forward).
//  - Resolve extant write intents and determine oldest non-resolvable
//    intent.
//
// The shouldQueue function combines the need for both tasks into a
// single priority. If any task is overdue, shouldQueue returns true.
type gcQueue struct {
	*baseQueue
}

// newGCQueue returns a new instance of gcQueue.
func newGCQueue() *gcQueue {
	gcq := &gcQueue{}
	gcq.baseQueue = newBaseQueue("gc", gcq, gcQueueMaxSize)
	return gcq
}

func (gcq *gcQueue) needsLeaderLease() bool {
	return true
}

// shouldQueue determines whether a range should be queued for garbage
// collection, and if so, at what priority. Returns true for shouldQ
// in the event that the cumulative ages of GC'able bytes or extant
// intents exceed thresholds.
func (gcq *gcQueue) shouldQueue(now proto.Timestamp, rng *Range) (shouldQ bool, priority float64) {
	// Lookup GC policy for this range.
	policy, err := gcq.lookupGCPolicy(rng)
	if err != nil {
		log.Errorf("GC policy: %s", err)
		return
	}

	// GC score is the total GC'able bytes age normalized by 1 MB * the range's TTL in seconds.
	gcScore := float64(rng.stats.GetGCBytesAge(now.WallTime)) / float64(policy.TTLSeconds) / float64(gcByteCountNormalization)

	// Intent score. This computes the average age of outstanding intents
	// and normalizes.
	intentScore := rng.stats.GetAvgIntentAge(now.WallTime) / float64(intentAgeNormalization.Nanoseconds()/1E9)

	// Compute priority.
	if gcScore > 1 {
		priority += gcScore
	}
	if intentScore > 1 {
		priority += intentScore
	}
	shouldQ = priority > 0
	return
}

// process iterates through all keys in a range, calling the garbage
// collector for each key and associated set of values. GC'd keys are
// batched into InternalGC calls. Extant intents are resolved if
// intents are older than intentAgeThreshold.
func (gcq *gcQueue) process(now proto.Timestamp, rng *Range) error {
	snap := rng.rm.Engine().NewSnapshot()
	iter := newRangeDataIterator(rng.Desc(), snap)
	defer iter.Close()
	defer snap.Close()

	// Lookup the GC policy for the zone containing this key range.
	policy, err := gcq.lookupGCPolicy(rng)
	if err != nil {
		return err
	}

	gcMeta := proto.NewGCMetadata(now.WallTime)
	gc := engine.NewGarbageCollector(now, policy)

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now
	intentExp.WallTime -= intentAgeThreshold.Nanoseconds()

	gcArgs := &proto.InternalGCRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp: now,
			RaftID:    rng.Desc().RaftID,
		},
	}
	var mu sync.Mutex
	var oldestIntentNanos int64 = math.MaxInt64
	var wg sync.WaitGroup
	var expBaseKey proto.Key
	var keys []proto.EncodedKey
	var vals [][]byte

	// updateOldestIntent atomically updates the oldest intent.
	updateOldestIntent := func(intentNanos int64) {
		mu.Lock()
		defer mu.Unlock()
		if intentNanos < oldestIntentNanos {
			oldestIntentNanos = intentNanos
		}
	}

	// processKeysAndValues is invoked with each key and its set of
	// values. Intents older than the intent age threshold are sent for
	// resolution and values after the MVCC metadata, and possible
	// intent, are sent for garbage collection.
	processKeysAndValues := func() {
		// If there's more than a single value for the key, possibly send for GC.
		if len(keys) > 1 {
			meta := &engine.MVCCMetadata{}
			if err := gogoproto.Unmarshal(vals[0], meta); err != nil {
				log.Errorf("unable to unmarshal MVCC metadata for key %q: %s", keys[0], err)
			} else {
				// In the event that there's an active intent, send for
				// intent resolution if older than the threshold.
				startIdx := 1
				if meta.Txn != nil {
					// Resolve intent asynchronously in a goroutine if the intent
					// is older than the intent expiration threshold.
					if meta.Timestamp.Less(intentExp) {
						wg.Add(1)
						go gcq.resolveIntent(rng, expBaseKey, meta, updateOldestIntent, &wg)
					} else {
						updateOldestIntent(meta.Timestamp.WallTime)
					}
					// With an active intent, GC ignores MVCC metadata & intent value.
					startIdx = 2
				}
				// See if any values may be GC'd.
				if gcTS := gc.Filter(keys[startIdx:], vals[startIdx:]); !gcTS.Equal(proto.ZeroTimestamp) {
					// TODO(spencer): need to split the requests up into
					// multiple requests in the event that more than X keys
					// are added to the request.
					gcArgs.Keys = append(gcArgs.Keys, proto.InternalGCRequest_GCKey{Key: expBaseKey, Timestamp: gcTS})
				}
			}
		}
	}

	// Iterate through this range's keys and values.
	for ; iter.Valid(); iter.Next() {
		baseKey, ts, isValue := engine.MVCCDecodeKey(iter.Key())
		if !isValue {
			// Moving to the next key (& values).
			processKeysAndValues()
			expBaseKey = baseKey
			keys = []proto.EncodedKey{iter.Key()}
			vals = [][]byte{iter.Value()}
		} else {
			if !baseKey.Equal(expBaseKey) {
				log.Errorf("unexpectedly found a value for %q with ts=%s; expected key %q", baseKey, ts, expBaseKey)
				continue
			}
			keys = append(keys, iter.Key())
			vals = append(vals, iter.Value())
		}
	}
	if iter.Error() != nil {
		return iter.Error()
	}
	// Handle last collected set of keys/vals.
	processKeysAndValues()

	// Set start and end keys.
	switch len(gcArgs.Keys) {
	case 0:
		return nil
	case 1:
		gcArgs.Key = gcArgs.Keys[0].Key
		gcArgs.EndKey = gcArgs.Key.Next()
	default:
		gcArgs.Key = gcArgs.Keys[0].Key
		gcArgs.EndKey = gcArgs.Keys[len(gcArgs.Keys)-1].Key
	}

	// Wait for any outstanding intent resolves and set oldest extant intent.
	wg.Wait()
	gcMeta.OldestIntentNanos = gogoproto.Int64(oldestIntentNanos)

	// Send GC request through range.
	gcArgs.GCMeta = *gcMeta
	if err := rng.AddCmd(rng.context(), proto.Call{Args: gcArgs, Reply: &proto.InternalGCResponse{}}); err != nil {
		return err
	}

	// Store current timestamp as last verification for this range, as
	// we've just successfully scanned.
	if err := rng.SetLastVerificationTimestamp(now); err != nil {
		log.Errorf("failed to set last verification timestamp for range %s: %s", rng, err)
	}

	return nil
}

// timer returns a constant duration to space out GC processing
// for successive queued ranges.
func (gcq *gcQueue) timer() time.Duration {
	return gcQueueTimerDuration
}

// resolveIntent resolves the intent at key by attempting to abort the
// transaction and resolve the intent. If the transaction cannot be
// aborted or intent cannot be resolved, the oldestIntentNanos value
// is atomically updated to the min of oldestIntentNanos and the
// intent's timestamp. The wait group is signaled on completion.
func (gcq *gcQueue) resolveIntent(rng *Range, key proto.Key, meta *engine.MVCCMetadata,
	updateOldestIntent func(int64), wg *sync.WaitGroup) {
	defer wg.Done() // signal wait group always on completion

	log.Infof("resolving intent at %q ts=%s", key, meta.Timestamp)

	// Attempt to push the transaction which created the intent.
	now := rng.rm.Clock().Now()
	pushArgs := &proto.InternalPushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp:    now,
			Key:          meta.Txn.Key,
			User:         UserRoot,
			UserPriority: gogoproto.Int32(proto.MaxPriority),
			Txn:          nil,
		},
		Now:       now,
		PusheeTxn: *meta.Txn,
		PushType:  proto.ABORT_TXN,
	}
	pushReply := &proto.InternalPushTxnResponse{}
	b := &client.Batch{}
	b.InternalAddCall(proto.Call{Args: pushArgs, Reply: pushReply})
	if err := rng.rm.DB().Run(b); err != nil {
		log.Warningf("push of txn %s failed: %s", meta.Txn, err)
		updateOldestIntent(meta.Timestamp.WallTime)
		return
	}

	// We pushed the transaction successfully, so resolve the intent.
	resolveArgs := &proto.InternalResolveIntentRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp: now,
			Key:       key,
			User:      UserRoot,
			Txn:       pushReply.PusheeTxn,
		},
	}
	if err := rng.AddCmd(rng.context(), proto.Call{Args: resolveArgs, Reply: &proto.InternalResolveIntentResponse{}}); err != nil {
		log.Warningf("resolve of key %q failed: %s", key, err)
		updateOldestIntent(meta.Timestamp.WallTime)
	}
}

// lookupGCPolicy queries the gossip prefix config map based on the
// supplied range's start key. It queries all matching config prefixes
// and then iterates from most specific to least, returning the first
// non-nil GC policy.
func (gcq *gcQueue) lookupGCPolicy(rng *Range) (proto.GCPolicy, error) {
	info, err := rng.rm.Gossip().GetInfo(gossip.KeyConfigZone)
	if err != nil {
		return proto.GCPolicy{}, util.Errorf("unable to fetch zone config from gossip: %s", err)
	}
	configMap, ok := info.(PrefixConfigMap)
	if !ok {
		return proto.GCPolicy{}, util.Errorf("gossiped info is not a prefix configuration map: %+v", info)
	}

	// Verify that the range doesn't cross over the zone config prefix.
	// This could be the case if the zone config is new and the range
	// hasn't been split yet along the new boundary.
	var gc *proto.GCPolicy
	if err = configMap.VisitPrefixesHierarchically(rng.Desc().StartKey, func(start, end proto.Key, config interface{}) (bool, error) {
		zone := config.(*proto.ZoneConfig)
		if zone.GC != nil {
			rng.RLock()
			isCovered := !end.Less(rng.Desc().EndKey)
			rng.RUnlock()
			if !isCovered {
				return false, util.Errorf("range is only partially covered by zone %s (%q-%q); must wait for range split", config, start, end)
			}
			gc = zone.GC
			return true, nil
		}
		if log.V(1) {
			log.Infof("skipping zone config %+v, because no GC policy is set", zone)
		}
		return false, nil
	}); err != nil {
		return proto.GCPolicy{}, err
	}

	// We should always match _at least_ the default GC.
	if gc == nil {
		return proto.GCPolicy{}, util.Errorf("no zone for range with start key %q", rng.Desc().StartKey)
	}
	return *gc, nil
}
