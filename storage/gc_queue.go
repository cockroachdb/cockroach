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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/uuid"
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
	// txnCleanupThreshold is the threshold after which a transaction is
	// considered abandoned and fit for removal, as measured by the maximum
	// of its last heartbeat and timestamp.
	// TODO(tschottdorf): need to enforce at all times that this is much
	// larger than the heartbeat interval used by the coordinator.
	txnCleanupThreshold = time.Hour
)

// gcQueue manages a queue of replicas slated to be scanned in their
// entirety using the MVCC versions iterator. The gc queue manages the
// following tasks:
//
//  - GC of version data via TTL expiration (and more complex schemes
//    as implemented going forward).
//  - Resolve extant write intents (pushing their transactions).
//  - GC of old transaction and sequence cache entries. This should include
//    most committed entries almost immediately and, after a threshold on
//    inactivity, all others.
//
// The shouldQueue function combines the need for the above tasks into a
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
	sysCfg config.SystemConfig) (shouldQ bool, priority float64) {
	desc := repl.Desc()
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		log.Errorf("could not find zone config for range %s: %s", repl, err)
		return
	}

	// GC score is the total GC'able bytes age normalized by 1 MB * the replica's TTL in seconds.
	gcScore := float64(repl.stats.GetGCBytesAge(now.WallTime)) / float64(zone.GC.TTLSeconds) / float64(gcByteCountNormalization)

	// Intent score. This computes the average age of outstanding intents
	// and normalizes.
	intentScore := repl.stats.GetAvgIntentAge(now.WallTime) / float64(intentAgeNormalization.Nanoseconds()/1E9)

	// Compute priority.
	if gcScore >= 1 {
		priority += gcScore
	}
	if intentScore >= 1 {
		priority += intentScore
	}
	shouldQ = priority > 0
	return
}

// process iterates through all keys in a replica's range, calling the garbage
// collector for each key and associated set of values. GC'd keys are batched
// into GC calls. Extant intents are resolved if intents are older than
// intentAgeThreshold. The transaction and sequence cache records are also
// scanned and old entries evicted. During normal operation, both of these
// records are cleaned up when their respective transaction finishes, so the
// amount of work done here is expected to be small.
//
// Some care needs to be taken to avoid cyclic recreation of entries during GC:
// * a Push initiated due to an intent may recreate a transaction entry
// * resolving an intent may write a new sequence cache entry
// * obtaining the transaction for a sequence cache entry requires a Push
//
// The following order is taken below:
// 1) collect all intents with sufficiently old txn record
// 2) collect these intents' transactions
// 3) scan the transaction table, collecting abandoned or completed txns
// 4) push all of these transactions (possibly recreating entries)
// 5) resolve all intents (unless the txn is still PENDING), which will recreate
//    sequence cache entries (but with the txn timestamp; i.e. likely gc'able)
// 6) scan the sequence table for old entries
// 7) push these transactions (again, recreating txn entries).
// 8) send a GCRequest.
func (gcq *gcQueue) process(now roachpb.Timestamp, repl *Replica,
	sysCfg config.SystemConfig) error {

	snap := repl.store.Engine().NewSnapshot()
	desc := repl.Desc()
	iter := newReplicaDataIterator(desc, snap, true /* replicatedOnly */)
	defer iter.Close()
	defer snap.Close()

	// Lookup the GC policy for the zone containing this key range.
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return util.Errorf("could not find zone config for range %s: %s", repl, err)
	}

	gc := engine.NewGarbageCollector(now, zone.GC)

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now
	intentExp.WallTime -= intentAgeThreshold.Nanoseconds()
	txnExp := now
	txnExp.WallTime -= txnCleanupThreshold.Nanoseconds()

	gcArgs := &roachpb.GCRequest{}
	// TODO(tschottdorf): This is one of these instances in which we want
	// to be more careful that the request ends up on the correct Replica,
	// and we might have to worry about mixing range-local and global keys
	// in a batch which might end up spanning Ranges by the time it executes.
	gcArgs.Key = desc.StartKey.AsRawKey()
	gcArgs.EndKey = desc.EndKey.AsRawKey()

	var expBaseKey roachpb.Key
	var keys []engine.MVCCKey
	var vals [][]byte

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
						txnID := *meta.Txn.ID
						txn := &roachpb.Transaction{
							TxnMeta: *meta.Txn,
						}
						txnMap[txnID] = txn
						intentSpanMap[txnID] = append(intentSpanMap[txnID], roachpb.Span{Key: expBaseKey})
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
	if iter.Error() != nil {
		return iter.Error()
	}
	// Handle last collected set of keys/vals.
	processKeysAndValues()

	txnKeys, err := processTransactionTable(repl, txnMap, txnExp)
	if err != nil {
		return err
	}

	// From now on, all newly added keys are range-local.
	// TODO(tschottdorf): Might need to use two requests at some point since we
	// hard-coded the full non-local key range in the header, but that does
	// not take into account the range-local keys. It will be OK as long as
	// we send directly to the Replica, though.
	gcArgs.Keys = append(gcArgs.Keys, txnKeys...)

	// Process push transactions in parallel.
	var wg sync.WaitGroup
	for _, txn := range txnMap {
		if txn.Status != roachpb.PENDING {
			continue
		}
		wg.Add(1)
		go pushTxn(repl, now, txn, roachpb.PUSH_ABORT, &wg)
	}
	wg.Wait()

	// Resolve all intents.
	var intents []roachpb.Intent
	for txnID, txn := range txnMap {
		if txn.Status != roachpb.PENDING {
			for _, intent := range intentSpanMap[txnID] {
				intents = append(intents, roachpb.Intent{Span: intent, Status: txn.Status, Txn: txn.TxnMeta})
			}
		}
	}

	if pErr := repl.resolveIntents(repl.context(), intents, true /* wait */, false /* !poison */); pErr != nil {
		return pErr.GoError()
	}

	// Deal with any leftover sequence cache keys. There shouldn't be many of
	// them.
	gcArgs.Keys = append(gcArgs.Keys, processSequenceCache(repl, now, txnExp, txnMap)...)

	var ba roachpb.BatchRequest
	// Technically not needed since we're talking directly to the Range.
	ba.RangeID = desc.RangeID
	ba.Timestamp = now
	ba.Add(gcArgs)
	if _, pErr := repl.Send(repl.context(), ba); pErr != nil {
		return pErr.GoError()
	}

	return nil
}

// processTransactionTable scans the transaction table and updates txnMap with
// those transactions which are old and either PENDING or with intents
// registered. In the first case we want to push the transaction so that it is
// aborted, and in the second case we may have to resolve the intents success-
// fully before GCing the entry. The transaction records which can be gc'ed are
// returned separately and are not added to txnMap nor intentSpanMap.
func processTransactionTable(r *Replica, txnMap map[uuid.UUID]*roachpb.Transaction, cutoff roachpb.Timestamp) ([]roachpb.GCRequest_GCKey, error) {
	snap := r.store.Engine().NewSnapshot()
	defer snap.Close()

	var gcKeys []roachpb.GCRequest_GCKey
	handleOne := func(kv roachpb.KeyValue) error {
		var txn roachpb.Transaction
		if err := kv.Value.GetProto(&txn); err != nil {
			return err
		}
		ts := txn.Timestamp
		if heartbeatTS := txn.LastHeartbeat; heartbeatTS != nil {
			ts.Forward(*heartbeatTS)
		}
		if !ts.Less(cutoff) {
			return nil
		}

		txnID := *txn.ID

		// The transaction record should be considered for removal.
		switch txn.Status {
		case roachpb.PENDING:
			// Marked as running, so we need to push it to abort it but won't
			// try to GC it in this cycle (for convenience).
			// TODO(tschottdorf): refactor so that we can GC PENDING entries
			// in the same cycle, but keeping the calls to pushTxn in a central
			// location (keeping it easy to batch them up in the future).
			txnMap[txnID] = &txn
			return nil
		case roachpb.ABORTED:
			// If we remove this transaction, it effectively still counts as
			// ABORTED (by design). So this can be GC'ed even if we can't
			// resolve the intents.
			// Note: Most aborted transaction weren't aborted by their client,
			// but instead by the coordinator - those will not have any intents
			// persisted, though they still might exist in the system.
			if err := r.resolveIntents(r.context(),
				roachpb.AsIntents(txn.Intents, &txn), true /* wait */, false /* !poison */); err != nil {
				log.Warningf("failed to resolve intents of aborted txn on gc: %s", err)
			}
		case roachpb.COMMITTED:
			// It's committed, so it doesn't need a push but we can only
			// GC it after its intents are resolved.
			if err := r.resolveIntents(r.context(),
				roachpb.AsIntents(txn.Intents, &txn), true /* wait */, false /* !poison */); err != nil {
				log.Warningf("unable to resolve intents of committed txn on gc: %s", err)
				// Returning the error here would abort the whole GC run, and
				// we don't want that. Instead, we simply don't GC this entry.
				return nil
			}
		default:
			panic(fmt.Sprintf("invalid transaction state: %s", txn))
		}
		gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: kv.Key}) // zero timestamp
		return nil
	}

	startKey := keys.TransactionKey(roachpb.KeyMin, uuid.EmptyUUID)
	endKey := keys.TransactionKey(roachpb.KeyMax, uuid.EmptyUUID)

	_, err := engine.MVCCIterate(snap, startKey, endKey, roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */, false /* !reverse */, func(kv roachpb.KeyValue) (bool, error) {
		return false, handleOne(kv)
	})
	return gcKeys, err
}

// processSequenceCache iterates through the local sequence cache entries,
// pushing the transactions (in cleanup mode) for those entries which appear
// to be old enough. In case the transaction indicates that it's terminated,
// the sequence cache keys are included in the result.
func processSequenceCache(r *Replica, now, cutoff roachpb.Timestamp, prevTxns map[uuid.UUID]*roachpb.Transaction) []roachpb.GCRequest_GCKey {
	snap := r.store.Engine().NewSnapshot()
	defer snap.Close()

	txns := make(map[uuid.UUID]*roachpb.Transaction)
	idToKeys := make(map[uuid.UUID][]roachpb.GCRequest_GCKey)
	r.sequence.Iterate(snap, func(key []byte, txnIDPtr *uuid.UUID, v roachpb.SequenceCacheEntry) {
		txnID := *txnIDPtr
		// If we've pushed this Txn previously, attempt cleanup (in case the
		// push was successful). Initiate new pushes only for newly discovered
		// "old" entries.
		if prevTxn, ok := prevTxns[txnID]; ok && prevTxn.Status != roachpb.PENDING {
			txns[txnID] = prevTxn
			idToKeys[txnID] = append(idToKeys[txnID], roachpb.GCRequest_GCKey{Key: key})
		} else if !cutoff.Less(v.Timestamp) {
			txns[txnID] = &roachpb.Transaction{
				TxnMeta: roachpb.TxnMeta{ID: txnIDPtr, Key: v.Key},
				Status:  roachpb.PENDING,
			}
			idToKeys[txnID] = append(idToKeys[txnID], roachpb.GCRequest_GCKey{Key: key})
		}
	})

	var wg sync.WaitGroup
	wg.Add(len(txns))
	// TODO(tschottdorf): a lot of these transactions will be on our local range,
	// so we should simply read those from a snapshot, and only push those which
	// are PENDING.
	for _, txn := range txns {
		// Check if the Txn is still alive. If this indicates that the Txn is
		// aborted and old enough to guarantee that any running coordinator
		// would have realized that the transaction wasn't running by means
		// of a heartbeat, then we're free to remove the sequence cache entry.
		// In the most likely case, there isn't even an entry (which will
		// be apparent by a zero timestamp and nil last heartbeat).
		go pushTxn(r, now, txn, roachpb.PUSH_TOUCH, &wg)
	}
	wg.Wait()

	var gcKeys []roachpb.GCRequest_GCKey
	for txnID, txn := range txns {
		if txn.Status == roachpb.PENDING {
			continue
		}
		ts := txn.Timestamp
		if txn.LastHeartbeat != nil {
			ts.Forward(*txn.LastHeartbeat)
		}
		if !cutoff.Less(ts) {
			// This is it, we can delete our sequence cache entries.
			gcKeys = append(gcKeys, idToKeys[txnID]...)
		}
	}
	return gcKeys
}

// timer returns a constant duration to space out GC processing
// for successive queued replicas.
func (*gcQueue) timer() time.Duration {
	return gcQueueTimerDuration
}

// purgatoryChan returns nil.
func (*gcQueue) purgatoryChan() <-chan struct{} {
	return nil
}

// pushTxn attempts to abort the txn via push. The wait group is signaled on
// completion.
func pushTxn(repl *Replica, now roachpb.Timestamp, txn *roachpb.Transaction,
	typ roachpb.PushTxnType, wg *sync.WaitGroup) {
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
		PusherTxn: roachpb.Transaction{Priority: roachpb.MaxUserPriority},
		PusheeTxn: txn.TxnMeta,
		PushType:  typ,
	}
	b := &client.Batch{}
	b.InternalAddRequest(pushArgs)
	br, err := repl.store.DB().RunWithResponse(b)
	if err != nil {
		log.Warningf("push of txn %s failed: %s", txn, err)
		return
	}
	// Update the supplied txn on successful push.
	*txn = br.Responses[0].GetInner().(*roachpb.PushTxnResponse).PusheeTxn
}
