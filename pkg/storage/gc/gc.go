// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package gc contains the logic to run scan a range for garbage and issue
// GC requests to remove that garbage.
//
// The Run function is the primary entry point and is called underneath the
// gcQueue in the storage package. It can also be run for debugging.
package gc

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	// IntentAgeThreshold is the threshold after which an extant intent
	// will be resolved.
	IntentAgeThreshold = 2 * time.Hour // 2 hour

	// KeyVersionChunkBytes is the threshold size for splitting
	// GCRequests into multiple batches.
	KeyVersionChunkBytes = base.ChunkRaftCommandThresholdBytes
)

// CalculateThreshold calculates the GC threshold given the policy and the
// current view of time.
func CalculateThreshold(now hlc.Timestamp, policy zonepb.GCPolicy) (threshold hlc.Timestamp) {
	ttlNanos := int64(policy.TTLSeconds) * time.Second.Nanoseconds()
	return now.Add(-ttlNanos, 0)
}

// TimestampForThreshold inverts CalculateThreshold. It returns the timestamp
// which should be used for now to arrive at the passed threshold.
func TimestampForThreshold(threshold hlc.Timestamp, policy zonepb.GCPolicy) (ts hlc.Timestamp) {
	ttlNanos := int64(policy.TTLSeconds) * time.Second.Nanoseconds()
	return threshold.Add(ttlNanos, 0)
}

// A GCer is an abstraction used by the GC queue to carry out chunked deletions.
type GCer interface {
	SetGCThreshold(context.Context, Threshold) error
	GC(context.Context, []roachpb.GCRequest_GCKey) error
}

// NoopGCer implements GCer by doing nothing.
type NoopGCer struct{}

var _ GCer = NoopGCer{}

// SetGCThreshold implements storage.GCer.
func (NoopGCer) SetGCThreshold(context.Context, Threshold) error { return nil }

// GC implements storage.GCer.
func (NoopGCer) GC(context.Context, []roachpb.GCRequest_GCKey) error { return nil }

// Threshold holds the key and txn span GC thresholds, respectively.
type Threshold struct {
	Key hlc.Timestamp
	Txn hlc.Timestamp
}

// Info contains statistics and insights from a GC run.
type Info struct {
	// Now is the timestamp used for age computations.
	Now hlc.Timestamp
	// Policy is the policy used for this garbage collection cycle.
	Policy zonepb.GCPolicy
	// Stats about the userspace key-values considered, namely the number of
	// keys with GC'able data, the number of "old" intents and the number of
	// associated distinct transactions.
	NumKeysAffected, IntentsConsidered, IntentTxns int
	// TransactionSpanTotal is the total number of entries in the transaction span.
	TransactionSpanTotal int
	// Summary of transactions which were found GCable (assuming that
	// potentially necessary intent resolutions did not fail).
	TransactionSpanGCAborted, TransactionSpanGCCommitted int
	TransactionSpanGCStaging, TransactionSpanGCPending   int
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

// CleanupIntentsFunc synchronously resolves the supplied intents
// (which may be PENDING, in which case they are first pushed) while
// taking care of proper batching.
type CleanupIntentsFunc func(context.Context, []roachpb.Intent) error

// CleanupTxnIntentsAsyncFunc asynchronously cleans up intents from a
// transaction record, pushing the transaction first if it is
// PENDING. Once all intents are resolved successfully, removes the
// transaction record.
type CleanupTxnIntentsAsyncFunc func(context.Context, *roachpb.Transaction, []roachpb.LockUpdate) error

// Run runs garbage collection for the specified descriptor on the
// provided Engine (which is not mutated). It uses the provided gcFn
// to run garbage collection once on all implicated spans,
// cleanupIntentsFn to resolve intents synchronously, and
// cleanupTxnIntentsAsyncFn to asynchronously cleanup intents and
// associated transaction record on success.
func Run(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap engine.Reader,
	now, newThreshold hlc.Timestamp,
	policy zonepb.GCPolicy,
	gcer GCer,
	cleanupIntentsFn CleanupIntentsFunc,
	cleanupTxnIntentsAsyncFn CleanupTxnIntentsAsyncFunc,
) (Info, error) {

	txnExp := now.Add(-storagebase.TxnCleanupThreshold.Nanoseconds(), 0)
	if err := gcer.SetGCThreshold(ctx, Threshold{
		Key: newThreshold,
		Txn: txnExp,
	}); err != nil {
		return Info{}, errors.Wrap(err, "failed to set GC thresholds")
	}

	info := Info{
		Policy:    policy,
		Now:       now,
		Threshold: newThreshold,
	}

	// Maps from txn ID to txn and intent key slice.
	txnMap := map[uuid.UUID]*roachpb.Transaction{}
	intentKeyMap := map[uuid.UUID][]roachpb.Key{}
	err := processReplicatedKeyRange(ctx, desc, snap, now, newThreshold, gcer, txnMap, intentKeyMap, &info)
	if err != nil {
		return Info{}, err
	}

	// From now on, all newly added keys are range-local.

	// Process local range key entries (txn records, queue last processed times).
	localRangeKeys, err := processLocalKeyRange(ctx, snap, desc, txnExp, &info, cleanupTxnIntentsAsyncFn)
	if err != nil {
		return Info{}, err
	}

	if err := gcer.GC(ctx, localRangeKeys); err != nil {
		return Info{}, err
	}

	// Clean up the AbortSpan.
	log.Event(ctx, "processing AbortSpan")
	abortSpanKeys := processAbortSpan(ctx, snap, desc.RangeID, txnExp, &info)
	if err := gcer.GC(ctx, abortSpanKeys); err != nil {
		return Info{}, err
	}

	log.Eventf(ctx, "GC'ed keys; stats %+v", info)

	// Push transactions (if pending) and resolve intents.
	var intents []roachpb.Intent
	for txnID, txn := range txnMap {
		intents = append(intents, roachpb.AsIntents(&txn.TxnMeta, intentKeyMap[txnID])...)
	}
	info.ResolveTotal += len(intents)
	log.Eventf(ctx, "cleanup of %d intents", len(intents))
	if err := cleanupIntentsFn(ctx, intents); err != nil {
		return Info{}, err
	}

	return info, nil
}

// processReplicatedKeyRange identifies garbage and sends GC requests to
// remove it.
//
// The logic iterates all versions of all keys in the range from oldest to
// newest. Expired intents are written into the txnMap and intentKeyMap.
func processReplicatedKeyRange(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap engine.Reader,
	now hlc.Timestamp,
	threshold hlc.Timestamp,
	gcer GCer,
	txnMap map[uuid.UUID]*roachpb.Transaction,
	intentKeyMap map[uuid.UUID][]roachpb.Key,
	info *Info,
) error {
	var alloc bufalloc.ByteAllocator
	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now.Add(-IntentAgeThreshold.Nanoseconds(), 0)
	handleIntent := func(md *engine.MVCCKeyValue) {
		meta := &enginepb.MVCCMetadata{}
		if err := protoutil.Unmarshal(md.Value, meta); err != nil {
			log.Errorf(ctx, "unable to unmarshal MVCC metadata for key %q: %+v", md.Key, err)
			return
		}
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
					info.IntentTxns++
					// All transactions in txnMap may be PENDING and
					// cleanupIntentsFn will push them to finalize them.
					info.PushTxn++
				}
				info.IntentsConsidered++
				alloc, md.Key.Key = alloc.Copy(md.Key.Key, 0)
				intentKeyMap[txnID] = append(intentKeyMap[txnID], md.Key.Key)
			}
		}
	}

	// Iterate all versions of all keys from oldest to newest. If a version is an
	// intent it will have the highest timestamp of any versions and will be
	// followed by a metadata entry. The loop will determine whether a given key
	// has garbage and, if so, will determine the timestamp of the latest version
	// which is garbage to be added to the current batch. If the current version
	// pushes the size of keys to be removed above the limit, the current key will
	// be added with that version and the batch will be sent. When the newest
	// version for a key has been reached, if haveGarbageForThisKey, we'll add the
	// current key to the batch with the gcTimestampForThisKey.
	var (
		batchGCKeys           []roachpb.GCRequest_GCKey
		batchGCKeysBytes      int64
		haveGarbageForThisKey bool
		gcTimestampForThisKey hlc.Timestamp
		sentBatchForThisKey   bool
	)
	it := makeGCIterator(desc, snap)
	defer it.close()
	for ; ; it.step() {
		s, ok := it.state()
		if !ok {
			if it.err != nil {
				return it.err
			}
			break
		}
		if s.curIsNotValue() { // Step over metadata or other system keys
			continue
		}
		if s.curIsIntent() {
			handleIntent(s.next)
			continue
		}
		isNewest := s.curIsNewest()
		if isGarbage(threshold, s.cur, s.next, isNewest) {
			keyBytes := int64(s.cur.Key.EncodedSize())
			batchGCKeysBytes += keyBytes
			haveGarbageForThisKey = true
			gcTimestampForThisKey = s.cur.Key.Timestamp
			info.AffectedVersionsKeyBytes += keyBytes
			info.AffectedVersionsValBytes += int64(len(s.cur.Value))
		}
		if affected := isNewest && (sentBatchForThisKey || haveGarbageForThisKey); affected {
			info.NumKeysAffected++
		}
		shouldSendBatch := batchGCKeysBytes >= KeyVersionChunkBytes
		if shouldSendBatch || isNewest && haveGarbageForThisKey {
			alloc, s.cur.Key.Key = alloc.Copy(s.cur.Key.Key, 0)
			batchGCKeys = append(batchGCKeys, roachpb.GCRequest_GCKey{
				Key:       s.cur.Key.Key,
				Timestamp: gcTimestampForThisKey,
			})
			haveGarbageForThisKey = false
			gcTimestampForThisKey = hlc.Timestamp{}

			// Mark that we sent a batch for this key so we know that we had garbage
			// even if it turns out that there's no more garbage for this key.
			// We want to count a key as affected once even if we paginate the
			// deletion of its versions.
			sentBatchForThisKey = shouldSendBatch && !isNewest
		}
		if shouldSendBatch {
			if err := gcer.GC(ctx, batchGCKeys); err != nil {
				// Even though we are batching the GC process, it's
				// safe to continue because we bumped the GC
				// thresholds. We may leave some inconsistent history
				// behind, but nobody can read it.
				log.Warningf(ctx, "failed to GC a batch of keys: %v", err)
			}
			batchGCKeys = nil
			batchGCKeysBytes = 0
			alloc = nil
		}
	}
	if len(batchGCKeys) > 0 {
		if err := gcer.GC(ctx, batchGCKeys); err != nil {
			return err
		}
	}
	return nil
}

// isGarbage makes a determination whether a key ('cur') is garbage. If 'next'
// is non-nil, it should be the chronologically newer version of the same key
// (or the metadata KV if cur is an intent). If isNewest is false, next must be
// non-nil. isNewest implies that this is the highest timestamp committed
// version for this key. If isNewest is true and next is non-nil, it is an
// intent. Conservatively we have to assume that the intent will get aborted,
// so we will be able to GC just the values that we could remove if there
// weren't an intent. Hence this definition of isNewest.
//
// We keep all values (including deletes) above the expiration time, plus
// the first value before or at the expiration time. This allows reads to be
// guaranteed as described above. However if this were the only rule, then if
// the most recent write was a delete, it would never be removed. Thus, when a
// deleted value is the most recent before expiration, it can be deleted.
func isGarbage(threshold hlc.Timestamp, cur, next *engine.MVCCKeyValue, isNewest bool) bool {
	// If the value is not at or below the threshold then it's not garbage.
	if belowThreshold := cur.Key.Timestamp.LessEq(threshold); !belowThreshold {
		return false
	}
	isDelete := len(cur.Value) == 0
	if isNewest && !isDelete {
		return false
	}
	// If this value is not a delete, then we need to make sure that the next
	// value is also at or below the threshold.
	// NB: This doesn't need to check whether next is nil because we know
	// isNewest is false when evaluating rhs of the or below.
	if !isDelete && next == nil {
		panic("huh")
	}
	return isDelete || next.Key.Timestamp.LessEq(threshold)
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
	info *Info,
	cleanupTxnIntentsAsyncFn CleanupTxnIntentsAsyncFunc,
) ([]roachpb.GCRequest_GCKey, error) {
	var gcKeys []roachpb.GCRequest_GCKey

	handleTxnIntents := func(key roachpb.Key, txn *roachpb.Transaction) error {
		// If the transaction needs to be pushed or there are intents to
		// resolve, invoke the cleanup function.
		if !txn.Status.IsFinalized() || len(txn.IntentSpans) > 0 {
			return cleanupTxnIntentsAsyncFn(ctx, txn, roachpb.AsLockUpdates(txn, txn.IntentSpans, lock.Replicated))
		}
		gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: key}) // zero timestamp
		return nil
	}

	handleOneTransaction := func(kv roachpb.KeyValue) error {
		var txn roachpb.Transaction
		if err := kv.Value.GetProto(&txn); err != nil {
			return err
		}
		info.TransactionSpanTotal++
		if cutoff.LessEq(txn.LastActive()) {
			return nil
		}

		// The transaction record should be considered for removal.
		switch txn.Status {
		case roachpb.PENDING:
			info.TransactionSpanGCPending++
		case roachpb.STAGING:
			info.TransactionSpanGCStaging++
		case roachpb.ABORTED:
			info.TransactionSpanGCAborted++
		case roachpb.COMMITTED:
			info.TransactionSpanGCCommitted++
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

	_, err := engine.MVCCIterate(ctx, snap, startKey, endKey, hlc.Timestamp{}, engine.MVCCScanOptions{},
		func(kv roachpb.KeyValue) (bool, error) {
			return false, handleOne(kv)
		})
	return gcKeys, err
}

// processAbortSpan iterates through the local AbortSpan entries
// and collects entries which indicate that a client which was running
// this transaction must have realized that it has been aborted (due to
// heartbeating having failed). The parameter minAge is typically a
// multiple of the heartbeat timeout used by the coordinator.
func processAbortSpan(
	ctx context.Context,
	snap engine.Reader,
	rangeID roachpb.RangeID,
	threshold hlc.Timestamp,
	info *Info,
) []roachpb.GCRequest_GCKey {
	var gcKeys []roachpb.GCRequest_GCKey
	abortSpan := abortspan.New(rangeID)
	if err := abortSpan.Iterate(ctx, snap, func(key roachpb.Key, v roachpb.AbortSpanEntry) error {
		info.AbortSpanTotal++
		if v.Timestamp.Less(threshold) {
			info.AbortSpanGCNum++
			gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{Key: key})
		}
		return nil
	}); err != nil {
		// Still return whatever we managed to collect.
		log.Warning(ctx, err)
	}
	return gcKeys
}
