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
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/gogo/protobuf/proto"
)

const (
	// gcQueueMaxSize is the max size of the gc queue.
	gcQueueMaxSize = 100
	// gcQueueTimerDuration is the duration between GCs of queued replicas.
	gcQueueTimerDuration = 1 * time.Second
	// gcByteCountNormalization is the count of GC'able bytes which
	// amount to a score of "1" added to total replica priority.
	gcByteCountNormalization = 1 << 20 // 1 MB
	// intentAgeNormalization is the average age of outstanding intents
	// which amount to a score of "1" added to total replica priority.
	intentAgeNormalization = 24 * time.Hour // 1 day
	// intentAgeThreshold is the threshold after which an extant intent
	// will be resolved.
	intentAgeThreshold = 2 * time.Hour // 2 hour
)

// gcQueue manages a queue of replicas slated to be scanned in their
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
	baseQueue
}

// newGCQueue returns a new instance of gcQueue.
func newGCQueue(gossip *gossip.Gossip) *gcQueue {
	gcq := &gcQueue{}
	gcq.baseQueue = makeBaseQueue("gc", gcq, gossip, gcQueueMaxSize)
	return gcq
}

func (*gcQueue) needsLeaderLease() bool {
	return true
}

// acceptsUnsplitRanges is false because the proper GC
// policy cannot be determined for ranges that span zone configs.
func (*gcQueue) acceptsUnsplitRanges() bool {
	return false
}

// shouldQueue determines whether a replica should be queued for garbage
// collection, and if so, at what priority. Returns true for shouldQ
// in the event that the cumulative ages of GC'able bytes or extant
// intents exceed thresholds.
func (*gcQueue) shouldQueue(now roachpb.Timestamp, repl *Replica,
	sysCfg *config.SystemConfig) (shouldQ bool, priority float64) {

	desc := repl.Desc()
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		log.Errorf("could not find GC policy for range %s: %s", repl, err)
		return
	}
	policy := zone.GC

	// GC score is the total GC'able bytes age normalized by 1 MB * the replica's TTL in seconds.
	gcScore := float64(repl.stats.GetGCBytesAge(now.WallTime)) / float64(policy.TTLSeconds) / float64(gcByteCountNormalization)

	// Intent score. This computes the average age of outstanding intents
	// and normalizes.
	intentScore := repl.stats.GetAvgIntentAge(now.WallTime) / float64(intentAgeNormalization.Nanoseconds()/1E9)

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

// process iterates through all keys in a replica's range, calling the garbage
// collector for each key and associated set of values. GC'd keys are batched
// into GC calls. Extant intents are resolved if intents are older than
// intentAgeThreshold.
func (gcq *gcQueue) process(now roachpb.Timestamp, repl *Replica,
	sysCfg *config.SystemConfig) error {

	snap := repl.store.Engine().NewSnapshot()
	desc := repl.Desc()
	iter := newReplicaDataIterator(desc, snap)
	defer iter.Close()
	defer snap.Close()

	// Lookup the GC policy for the zone containing this key range.
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return fmt.Errorf("could not find GC policy for range %s: %s", repl, err)
	}
	policy := zone.GC

	gcMeta := roachpb.NewGCMetadata(now.WallTime)
	gc := engine.NewGarbageCollector(now, *policy)

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now
	intentExp.WallTime -= intentAgeThreshold.Nanoseconds()

	// TODO(tschottdorf): execution will use a leader-assigned local
	// timestamp to compute intent age. While this should be fine, could
	// consider adding a Now timestamp to GCRequest which would be used
	// instead.
	gcArgs := &roachpb.GCRequest{}
	var mu sync.Mutex
	var oldestIntentNanos int64 = math.MaxInt64
	var expBaseKey roachpb.Key
	var keys []roachpb.EncodedKey
	var vals [][]byte

	// Maps from txn ID to txn and intent key slice.
	txnMap := map[string]*roachpb.Transaction{}
	intentMap := map[string][]roachpb.Intent{}

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
			if err := proto.Unmarshal(vals[0], meta); err != nil {
				log.Errorf("unable to unmarshal MVCC metadata for key %q: %s", keys[0], err)
			} else {
				// In the event that there's an active intent, send for
				// intent resolution if older than the threshold.
				startIdx := 1
				if meta.Txn != nil {
					// Keep track of intent to resolve if older than the intent
					// expiration threshold.
					if meta.Timestamp.Less(intentExp) {
						id := string(meta.Txn.ID)
						txnMap[id] = meta.Txn
						intentMap[id] = append(intentMap[id], roachpb.Intent{Key: expBaseKey})
					} else {
						updateOldestIntent(meta.Txn.OrigTimestamp.WallTime)
					}
					// With an active intent, GC ignores MVCC metadata & intent value.
					startIdx = 2
				}
				// See if any values may be GC'd.
				if gcTS := gc.Filter(keys[startIdx:], vals[startIdx:]); !gcTS.Equal(roachpb.ZeroTimestamp) {
					// TODO(spencer): need to split the requests up into
					// multiple requests in the event that more than X keys
					// are added to the request.
					gcArgs.Keys = append(gcArgs.Keys, roachpb.GCRequest_GCKey{Key: expBaseKey, Timestamp: gcTS})
				}
			}
		}
	}

	// Iterate through the keys and values of this replica's range.
	for ; iter.Valid(); iter.Next() {
		baseKey, ts, isValue, err := engine.MVCCDecodeKey(iter.Key())
		if err != nil {
			log.Errorf("unable to decode MVCC key: %q: %v", iter.Key(), err)
			continue
		}
		if !isValue {
			// Moving to the next key (& values).
			processKeysAndValues()
			expBaseKey = baseKey
			keys = []roachpb.EncodedKey{iter.Key()}
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

	// Process push transactions in parallel.
	var wg sync.WaitGroup
	for _, txn := range txnMap {
		wg.Add(1)
		go gcq.pushTxn(repl, now, txn, updateOldestIntent, &wg)
	}
	wg.Wait()

	// Resolve all intents.
	var intents []roachpb.Intent
	for id, txn := range txnMap {
		if txn.Status != roachpb.PENDING {
			for _, intent := range intentMap[id] {
				intent.Txn = *txn
				intents = append(intents, intent)
			}
		}
	}

	done := true
	if len(intents) > 0 {
		done = false
		repl.resolveIntents(repl.context(), intents)
	}

	// Set start and end keys.
	if len(gcArgs.Keys) > 0 {
		done = false
		gcArgs.Key = gcArgs.Keys[0].Key
		gcArgs.EndKey = gcArgs.Keys[len(gcArgs.Keys)-1].Key.Next()
	}

	if done {
		return nil
	}

	// Send GC request through range.
	gcMeta.OldestIntentNanos = proto.Int64(oldestIntentNanos)
	gcArgs.GCMeta = *gcMeta

	var ba roachpb.BatchRequest
	ba.CmdID = ba.GetOrCreateCmdID(now.WallTime)
	// Technically not needed since we're talking directly to the Range.
	ba.RangeID = desc.RangeID
	ba.Add(gcArgs)
	if _, pErr := repl.Send(repl.context(), ba); pErr != nil {
		return pErr.GoError()
	}

	// Store current timestamp as last verification for this replica, as
	// we've just successfully scanned.
	if err := repl.SetLastVerificationTimestamp(now); err != nil {
		log.Errorf("failed to set last verification timestamp for replica %s: %s", repl, err)
	}

	return nil
}

// timer returns a constant duration to space out GC processing
// for successive queued replicas.
func (*gcQueue) timer() time.Duration {
	return gcQueueTimerDuration
}

// pushTxn attempts to abort the txn via push. If the transaction
// cannot be aborted, the oldestIntentNanos value is atomically
// updated to the min of oldestIntentNanos and the intent's
// timestamp. The wait group is signaled on completion.
func (*gcQueue) pushTxn(repl *Replica, now roachpb.Timestamp, txn *roachpb.Transaction, updateOldestIntent func(int64), wg *sync.WaitGroup) {
	defer wg.Done() // signal wait group always on completion
	if log.V(1) {
		log.Infof("pushing txn %s ts=%s", txn, txn.OrigTimestamp)
	}

	// Attempt to push the transaction which created the intent.
	pushArgs := &roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Now:       now,
		PusherTxn: roachpb.Transaction{Priority: roachpb.MaxPriority},
		PusheeTxn: *txn,
		PushType:  roachpb.ABORT_TXN,
	}
	b := &client.Batch{}
	b.InternalAddRequest(pushArgs)
	br, err := repl.store.DB().RunWithResponse(b)
	if err != nil {
		log.Warningf("push of txn %s failed: %s", txn, err)
		updateOldestIntent(txn.OrigTimestamp.WallTime)
		return
	}
	// Update the supplied txn on successful push.
	*txn = *br.Responses[0].GetInner().(*roachpb.PushTxnResponse).PusheeTxn
}
