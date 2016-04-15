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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"fmt"
	"math"
	"sync"
	"time"

	"golang.org/x/net/context"

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

	// abortCacheAgeThreshold is the duration after which abort cache entries
	// of transactions are garbage collected.
	// It's important that this is kept aligned with the (maximum) heartbeat
	// interval used by transaction coordinators throughout the cluster to make
	// sure that no coordinator can run with a transaction which has been
	// aborted and whose abort cache entry is being deleted.
	abortCacheAgeThreshold = 5 * DefaultHeartbeatInterval

	// considerThreshold is used in shouldQueue. Only an a normalized GC bytes
	// or intent byte age larger than the threshold queues the replica for GC.
	considerThreshold = 10

	// gcTaskLimit is the maximum number of concurrent goroutines
	// that will be created by GC.
	gcTaskLimit = 25
)

// gcQueue manages a queue of replicas slated to be scanned in their
// entirety using the MVCC versions iterator. The gc queue manages the
// following tasks:
//
//  - GC of version data via TTL expiration (and more complex schemes
//    as implemented going forward).
//  - Resolve extant write intents (pushing their transactions).
//  - GC of old transaction and abort cache entries. This should include
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

type pushFunc func(roachpb.Timestamp, *roachpb.Transaction, roachpb.PushTxnType)
type resolveFunc func([]roachpb.Intent, bool, bool) error

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
	if gcScore >= considerThreshold {
		priority += gcScore
	}
	if intentScore >= considerThreshold {
		priority += intentScore
	}
	shouldQ = priority > 0
	return
}

// processTransactionTable scans the transaction table and updates txnMap with
// those transactions which are old and either PENDING or with intents
// registered. In the first case we want to push the transaction so that it is
// aborted, and in the second case we may have to resolve the intents success-
// fully before GCing the entry. The transaction records which can be gc'ed are
// returned separately and are not added to txnMap nor intentSpanMap.
func processTransactionTable(
	ctx context.Context,
	snap engine.Engine,
	desc *roachpb.RangeDescriptor,
	txnMap map[uuid.UUID]*roachpb.Transaction,
	cutoff roachpb.Timestamp,
	infoMu *lockableGCInfo,
	resolveIntents resolveFunc,
) ([]roachpb.GCRequest_GCKey, error) {
	infoMu.Lock()
	defer infoMu.Unlock()

	var gcKeys []roachpb.GCRequest_GCKey
	handleOne := func(kv roachpb.KeyValue) error {
		var txn roachpb.Transaction
		if err := kv.Value.GetProto(&txn); err != nil {
			return err
		}
		infoMu.TransactionSpanTotal++
		if !txn.LastActive().Less(cutoff) {
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
			infoMu.TransactionSpanGCPending++
			txnMap[txnID] = &txn
			return nil
		case roachpb.ABORTED:
			// If we remove this transaction, it effectively still counts as
			// ABORTED (by design). So this can be GC'ed even if we can't
			// resolve the intents.
			// Note: Most aborted transaction weren't aborted by their client,
			// but instead by the coordinator - those will not have any intents
			// persisted, though they still might exist in the system.
			infoMu.TransactionSpanGCAborted++
			func() {
				infoMu.Unlock() // intentional
				defer infoMu.Lock()
				if err := resolveIntents(roachpb.AsIntents(txn.Intents, &txn),
					true /* wait */, false /* !poison */); err != nil {
					log.Warningf("failed to resolve intents of aborted txn on gc: %s", err)
				}
			}()
		case roachpb.COMMITTED:
			// It's committed, so it doesn't need a push but we can only
			// GC it after its intents are resolved.
			if err := func() error {
				infoMu.Unlock() // intentional
				defer infoMu.Lock()
				return resolveIntents(roachpb.AsIntents(txn.Intents, &txn), true /* wait */, false /* !poison */)
			}(); err != nil {
				log.Warningf("unable to resolve intents of committed txn on gc: %s", err)
				// Returning the error here would abort the whole GC run, and
				// we don't want that. Instead, we simply don't GC this entry.
				return nil
			}
			infoMu.TransactionSpanGCCommitted++
		default:
			panic(fmt.Sprintf("invalid transaction state: %s", txn))
		}
		gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: kv.Key}) // zero timestamp
		return nil
	}

	startKey := keys.TransactionKey(desc.StartKey.AsRawKey(), uuid.EmptyUUID)
	endKey := keys.TransactionKey(desc.EndKey.AsRawKey(), uuid.EmptyUUID)

	_, err := engine.MVCCIterate(ctx, snap, startKey, endKey,
		roachpb.ZeroTimestamp, true /* consistent */, nil, /* txn */
		false /* !reverse */, func(kv roachpb.KeyValue) (bool, error) {
			return false, handleOne(kv)
		})
	return gcKeys, err
}

// processAbortCache iterates through the local abort cache entries
// and collects entries which indicate that a client which was running
// this transaction must have realized that it has been aborted (due to
// heartbeating having failed). The parameter minAge is typically a
// multiple of the heartbeat timeout used by the coordinator.
//
// TODO(tschottdorf): this could be done in Replica.GC itself, but it's
// handy to have it here for stats (though less performant due to sending
// all of the keys over the wire).
func processAbortCache(
	ctx context.Context,
	snap engine.Engine,
	rangeID roachpb.RangeID,
	now roachpb.Timestamp,
	minAge time.Duration,
	infoMu *lockableGCInfo,
	pushTxn pushFunc,
) []roachpb.GCRequest_GCKey {
	var gcKeys []roachpb.GCRequest_GCKey
	abortCache := NewAbortCache(rangeID)
	infoMu.Lock()
	defer infoMu.Unlock()
	abortCache.Iterate(ctx, snap, func(key []byte, txnIDPtr *uuid.UUID, v roachpb.AbortCacheEntry) {
		infoMu.AbortSpanTotal++
		if v.Timestamp.Add(int64(minAge), 0).Less(now) {
			infoMu.AbortSpanGCNum++
			gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: key})
		}
	})
	return gcKeys
}

// process iterates through all keys in a replica's range, calling the garbage
// collector for each key and associated set of values. GC'd keys are batched
// into GC calls. Extant intents are resolved if intents are older than
// intentAgeThreshold. The transaction and abort cache records are also
// scanned and old entries evicted. During normal operation, both of these
// records are cleaned up when their respective transaction finishes, so the
// amount of work done here is expected to be small.
//
// Some care needs to be taken to avoid cyclic recreation of entries during GC:
// * a Push initiated due to an intent may recreate a transaction entry
// * resolving an intent may write a new abort cache entry
// * obtaining the transaction for a abort cache entry requires a Push
//
// The following order is taken below:
// 1) collect all intents with sufficiently old txn record
// 2) collect these intents' transactions
// 3) scan the transaction table, collecting abandoned or completed txns
// 4) push all of these transactions (possibly recreating entries)
// 5) resolve all intents (unless the txn is still PENDING), which will recreate
//    abort cache entries (but with the txn timestamp; i.e. likely gc'able)
// 6) scan the abort cache table for old entries
// 7) push these transactions (again, recreating txn entries).
// 8) send a GCRequest.
func (gcq *gcQueue) process(now roachpb.Timestamp, repl *Replica,
	sysCfg config.SystemConfig) error {
	ctx := repl.context(context.TODO())

	snap := repl.store.Engine().NewSnapshot()
	desc := repl.Desc()
	defer snap.Close()

	// Lookup the GC policy for the zone containing this key range.
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return util.Errorf("could not find zone config for range %s: %s", repl, err)
	}

	gcKeys, info, err := RunGC(ctx, desc, snap, now, zone.GC,
		func(now roachpb.Timestamp, txn *roachpb.Transaction, typ roachpb.PushTxnType) {
			pushTxn(repl, now, txn, typ)
		},
		func(intents []roachpb.Intent, poison bool, wait bool) error {
			return repl.store.intentResolver.resolveIntents(ctx, repl, intents, poison, wait)
		})

	if err != nil {
		return err
	}

	gcq.eventLog.Infof(true, "completed with stats %+v", info)

	var ba roachpb.BatchRequest
	var gcArgs roachpb.GCRequest
	// TODO(tschottdorf): This is one of these instances in which we want
	// to be more careful that the request ends up on the correct Replica,
	// and we might have to worry about mixing range-local and global keys
	// in a batch which might end up spanning Ranges by the time it executes.
	gcArgs.Key = desc.StartKey.AsRawKey()
	gcArgs.EndKey = desc.EndKey.AsRawKey()
	gcArgs.Keys = gcKeys

	// Technically not needed since we're talking directly to the Range.
	ba.RangeID = desc.RangeID
	ba.Timestamp = now
	ba.Add(&gcArgs)
	if _, pErr := repl.Send(ctx, ba); pErr != nil {
		return pErr.GoError()
	}
	return nil
}

// GCInfo contains statistics and insights from a GC run.
type GCInfo struct {
	// Now is the timestamp used for age computations.
	Now roachpb.Timestamp
	// Policy is the policy used for this garbage collection cycle.
	Policy config.GCPolicy
	// Stats about the userspace key-values considered, namely the number of
	// keys with GC'able data, the number of "old" intents and the number of
	// associated distinct transactions.
	GCKeys, IntentsConsidered, IntentTxns int
	// TransactionSpanTotal is the total number of entries in the transaction span.
	TransactionSpanTotal int
	// Summary of transactions which were found GCable (assuming that
	// potentially necessary intent resolutions did not fail).
	TransactionSpanGCAborted, TransactionSpanGCCommitted, TransactionSpanGCPending int
	// AbortSpanTotal is the total number of transactions present in the abort cache.
	AbortSpanTotal int
	// AbortSpanConsidered is the number of abort cache entries old enough to be
	// considered for removal. An "entry" corresponds to one transaction;
	// more than one key-value pair may be associated with it.
	AbortSpanConsidered int
	// AbortSpanGCNum is the number of abort cache entries fit for removal (due
	// to their transactions having terminated).
	AbortSpanGCNum int
	// PushTxn is the total number of pushes attempted in this cycle.
	PushTxn int
	// ResolveTotal is the total number of attempted intent resolutions in
	// this cycle.
	ResolveTotal int
	// ResolveErrors is the number of successful intent resolutions.
	ResolveSuccess int
}

type lockableGCInfo struct {
	sync.Mutex
	GCInfo
}

// RunGC runs garbage collection for the specified descriptor on the provided
// Engine (which is not mutated). It uses the provided functions pushTxn and
// resolveIntents to clarify the true status of and clean up after encountered
// transactions. It returns a slice of gc'able keys from the data, transaction,
// and abort spans.
func RunGC(ctx context.Context, desc *roachpb.RangeDescriptor, snap engine.Engine, now roachpb.Timestamp, policy config.GCPolicy,
	pushTxn pushFunc, resolveIntents resolveFunc) ([]roachpb.GCRequest_GCKey, GCInfo, error) {

	iter := newReplicaDataIterator(desc, snap, true /* replicatedOnly */)
	defer iter.Close()

	var infoMu = lockableGCInfo{}
	infoMu.Policy = policy
	infoMu.Now = now

	{
		realResolveIntents := resolveIntents
		resolveIntents = func(intents []roachpb.Intent, poison bool, wait bool) (err error) {
			defer func() {
				infoMu.Lock()
				infoMu.ResolveTotal += len(intents)
				if err == nil {
					infoMu.ResolveSuccess += len(intents)
				}
				infoMu.Unlock()
			}()
			return realResolveIntents(intents, poison, wait)
		}
		realPushTxn := pushTxn
		pushTxn = func(ts roachpb.Timestamp, txn *roachpb.Transaction, typ roachpb.PushTxnType) {
			infoMu.Lock()
			infoMu.PushTxn++
			infoMu.Unlock()
			realPushTxn(ts, txn, typ)
		}
	}

	gc := engine.MakeGarbageCollector(now, policy)

	var gcKeys []roachpb.GCRequest_GCKey

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now
	intentExp.WallTime -= intentAgeThreshold.Nanoseconds()
	txnExp := now
	txnExp.WallTime -= txnCleanupThreshold.Nanoseconds()

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
						infoMu.IntentsConsidered++
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
					gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: expBaseKey, Timestamp: gcTS})
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
		return nil, GCInfo{}, iter.Error()
	}
	// Handle last collected set of keys/vals.
	processKeysAndValues()

	infoMu.IntentTxns = len(txnMap)
	infoMu.GCKeys = len(gcKeys)

	txnKeys, err := processTransactionTable(ctx, snap, desc, txnMap, txnExp, &infoMu, resolveIntents)
	if err != nil {
		return nil, GCInfo{}, err
	}

	// From now on, all newly added keys are range-local.
	// TODO(tschottdorf): Might need to use two requests at some point since we
	// hard-coded the full non-local key range in the header, but that does
	// not take into account the range-local keys. It will be OK as long as
	// we send directly to the Replica, though.
	gcKeys = append(gcKeys, txnKeys...)

	// Process push transactions in parallel.
	var wg sync.WaitGroup
	sem := make(chan struct{}, gcTaskLimit)
	for _, txn := range txnMap {
		if txn.Status != roachpb.PENDING {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		// Avoid passing loop variable into closure.
		txnCopy := txn
		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()
			pushTxn(now, txnCopy, roachpb.PUSH_ABORT)
		}()
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

	if err := resolveIntents(intents, true /* wait */, false /* !poison */); err != nil {
		return nil, GCInfo{}, err
	}

	// Clean up the abort cache.
	gcKeys = append(gcKeys, processAbortCache(ctx, snap, desc.RangeID, now,
		abortCacheAgeThreshold, &infoMu, pushTxn)...)
	return gcKeys, infoMu.GCInfo, nil
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
	typ roachpb.PushTxnType) {

	// Attempt to push the transaction which created the intent.
	pushArgs := &roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Now:       now,
		PusherTxn: roachpb.Transaction{TxnMeta: roachpb.TxnMeta{Priority: math.MaxInt32}},
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
