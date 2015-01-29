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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// scanQueueMaxSize is the max size of the scan queue.
	scanQueueMaxSize = 100
	// gcByteCountNormalization is the count of GC'able bytes which
	// amount to a score of "1" added to total range priority.
	gcByteCountNormalization = 1 << 20 // 1 MB
	// intentAgeNormalization is the average age of outstanding intents
	// which amount to a score of "1" added to total range priority.
	intentAgeNormalization = 24 * time.Hour // 1 day
	// intentAgeThreshold is the threshold after which an extant intent
	// will be resolved.
	intentAgeThreshold = 2 * time.Hour // 2 hour
	// verificationInterval is the target duration for verifying on-disk
	// checksums via full scan.
	verificationInterval = 30 * 24 * time.Hour // 30 days
)

// scanQueue manages a queue of ranges slated to be scanned in their
// entirety using the MVCC versions iterator. Currently, range scans
// manage the following tasks:
//
//  - GC of version data via TTL expiration (and more complex schemes
//    as implemented going forward).
//  - Resolve extant write intents and determine oldest non-resolvable
//    intent.
//  - Periodic verification of on-disk checksums to identify bit-rot
//    in read-only data sets. See http://en.wikipedia.org/wiki/Data_degradation.
//
// The shouldQueue function combines the need for all three tasks into
// a single priority. If any task is overdue, shouldQueue returns true.
type scanQueue struct {
	*baseQueue
}

// newScanQueue returns a new instance of scanQueue.
func newScanQueue() *scanQueue {
	sq := &scanQueue{}
	sq.baseQueue = newBaseQueue("scan", sq.shouldQueue, sq.process, scanQueueMaxSize)
	return sq
}

// shouldQueue determines whether a range should be queued for
// scanning, and if so, at what priority. Returns true for shouldQ in
// the event that there are GC'able bytes, or it's been longer since
// the last scan than the intent sweep or verification
// intervals. Priority is derived from the addition of priority from
// GC'able bytes and how many multiples of intent or verification
// intervals have elapsed since the last scan.
func (sq *scanQueue) shouldQueue(now proto.Timestamp, rng *Range) (shouldQ bool, priority float64) {
	// Get last scan metadata & GC policy.
	scanMeta, err := rng.GetScanMetadata()
	if err != nil {
		log.Errorf("unable to fetch scan metadata: %s", err)
		return
	}
	policy, err := sq.lookupGCPolicy(rng)
	if err != nil {
		log.Errorf("GC policy: %s", err)
		return
	}

	// GC score is the total GC'able bytes age normalized by 1 MB * the range's TTL in seconds.
	gcScore := float64(rng.stats.GetGCBytesAge(now.WallTime)) / float64(policy.TTLSeconds) / float64(gcByteCountNormalization)

	// Intent score. This computes the average age of outstanding intents
	// and normalizes.
	intentScore := rng.stats.GetAvgIntentAge(now.WallTime) / float64(intentAgeNormalization.Nanoseconds()/1E9)

	// Verify score.
	verifyScore := float64(now.WallTime-scanMeta.LastScanNanos) / float64(verificationInterval.Nanoseconds())

	// Compute priority.
	if gcScore > 1 {
		priority += gcScore
	}
	if intentScore > 1 {
		priority += intentScore
	}
	if verifyScore > 1 {
		priority += verifyScore
	}
	shouldQ = priority > 0
	return
}

// process iterates through all keys in a range, calling the garbage
// collector for each key and associated set of values. GC'd keys are
// batched into InternalGC calls. Extant intents are resolved if
// intents are older than intentAgeThreshold. The very act of scanning
// keys verifies on-disk checksums, as each block checksum is checked
// on load.
func (sq *scanQueue) process(now proto.Timestamp, rng *Range) error {
	snap := rng.rm.Engine().NewSnapshot()
	iter := newRangeDataIterator(rng, snap)
	defer iter.Close()
	defer snap.Stop()

	// Lookup the GC policy for the zone containing this key range.
	policy, err := sq.lookupGCPolicy(rng)
	if err != nil {
		return err
	}

	scanMeta := proto.NewScanMetadata(now.WallTime)
	gc := engine.NewGarbageCollector(now, policy)

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now
	intentExp.WallTime -= intentAgeThreshold.Nanoseconds()

	gcArgs := &proto.InternalGCRequest{
		RequestHeader: proto.RequestHeader{
			Key:       rng.Desc.StartKey,
			Timestamp: now,
			RaftID:    rng.Desc.RaftID,
		},
	}
	var oldestIntentNanos int64 = math.MaxInt64
	var wg sync.WaitGroup
	var expBaseKey proto.Key
	var keys []proto.EncodedKey
	var vals [][]byte

	// processKeysAndValues is invoked with each key and its set of
	// values. Intents older than the intent age threshold are sent for
	// resolution and values after the MVCC metadata, and possible
	// intent, are sent for garbage collection.
	processKeysAndValues := func() {
		// If there's more than a single value for the key, possibly send for GC.
		if len(keys) > 1 {
			meta := &proto.MVCCMetadata{}
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
						log.Infof("base=%q, now=%s, meta ts=%s, intentExp=%s", expBaseKey, now, meta.Timestamp, intentExp)
						wg.Add(1)
						go sq.resolveIntent(rng, expBaseKey, meta, &oldestIntentNanos, &wg)
					} else if meta.Timestamp.WallTime < atomic.LoadInt64(&oldestIntentNanos) {
						atomic.StoreInt64(&oldestIntentNanos, meta.Timestamp.WallTime)
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
		log.Infof("base key: %q, ts=%s", baseKey, ts)
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
	// Handle last collected set of keys/vals.
	processKeysAndValues()

	// An error during iteration is presumed to means a checksum failure
	// while iterating over the underlying key/value data.
	if iter.Error() != nil {
		// TODO(spencer): do something other than fatal error here. We
		// want to quarantine this range, make it a non-participating raft
		// follower until it can be replaced and then destroyed.
		log.Fatalf("unhandled failure when scanning range %s; probable data corruption: %s", rng, iter.Error())
	}
	// Wait for any outstanding intent resolves and set oldest extant intent.
	wg.Wait()
	scanMeta.OldestIntentNanos = gogoproto.Int64(oldestIntentNanos)

	// Send GC request through range.
	gcArgs.ScanMeta = *scanMeta
	if err := rng.AddCmd(proto.InternalGC, gcArgs, &proto.InternalGCResponse{}, true); err != nil {
		return err
	}

	return nil
}

// resolveIntent resolves the intent at key by attempting to abort the
// transaction and resolve the intent. If the transaction cannot be
// aborted or intent cannot be resolved, the oldestIntentNanos value
// is atomically updated to the min of oldestIntentNanos and the
// intent's timestamp. The wait group is signaled on completion.
func (sq *scanQueue) resolveIntent(rng *Range, key proto.Key, meta *proto.MVCCMetadata,
	oldestIntentNanos *int64, wg *sync.WaitGroup) {
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
		PusheeTxn: *meta.Txn,
		Abort:     true,
	}
	pushReply := &proto.InternalPushTxnResponse{}
	if err := rng.rm.DB().Call(proto.InternalPushTxn, pushArgs, pushReply); err != nil {
		log.Warningf("push of txn %s failed: %s", meta.Txn, err)
		// Atomically update the oldest intent.
		if meta.Timestamp.WallTime < atomic.LoadInt64(oldestIntentNanos) {
			atomic.StoreInt64(oldestIntentNanos, meta.Timestamp.WallTime)
		}
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
	if err := rng.AddCmd(proto.InternalResolveIntent, resolveArgs, &proto.InternalResolveIntentResponse{}, true); err != nil {
		log.Warningf("resolve of key %q failed: %s", key, err)
		// Atomically update the oldest intent.
		if meta.Timestamp.WallTime < atomic.LoadInt64(oldestIntentNanos) {
			atomic.StoreInt64(oldestIntentNanos, meta.Timestamp.WallTime)
		}
	}
}

// lookupGCPolicy queries the gossip prefix config map based on the
// supplied range's start key. It queries all matching config prefixes
// and then iterates from most specific to least, returning the first
// non-nil GC policy.
func (sq *scanQueue) lookupGCPolicy(rng *Range) (proto.GCPolicy, error) {
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
	prefixConfigs := configMap.MatchesByPrefix(rng.Desc.StartKey)
	var zone *proto.ZoneConfig
	for _, prefixConfig := range prefixConfigs {
		zone = prefixConfig.Config.(*proto.ZoneConfig)
		if zone.GC != nil {
			rng.RLock()
			isCovered := !prefixConfig.Prefix.PrefixEnd().Less(rng.Desc.EndKey)
			rng.RUnlock()
			if !isCovered {
				return proto.GCPolicy{}, util.Errorf("range is only partially covered by zone %s; must wait for range split", prefixConfig)
			}
			return *zone.GC, nil
		}
		log.V(1).Infof("skipping zone config %+v, because no GC policy is set", zone)
	}

	// We should always match the default GC.
	return proto.GCPolicy{}, util.Errorf("no zone for range with start key %q", rng.Desc.StartKey)
}
