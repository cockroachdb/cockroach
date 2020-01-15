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
// The Run function is the primary entrypoint and is called underneath the
// gcQueue in the storage package. It can also be run for debugging.
package gc

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
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

// A GCer is an abstraction used by the GC queue to carry out chunked deletions.
type GCer interface {
	SetGCThreshold(context.Context, GCThreshold) error
	GC(context.Context, []roachpb.GCRequest_GCKey) error
}

// NoopGCer implements GCer by doing nothing.
type NoopGCer struct{}

var _ GCer = NoopGCer{}

// SetGCThreshold implements storage.GCer.
func (NoopGCer) SetGCThreshold(context.Context, GCThreshold) error { return nil }

// GC implements storage.GCer.
func (NoopGCer) GC(context.Context, []roachpb.GCRequest_GCKey) error { return nil }

// GCThreshold holds the key and txn span GC thresholds, respectively.
type GCThreshold struct {
	Key hlc.Timestamp
	Txn hlc.Timestamp
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

// A CleanupIntentsFunc synchronously resolves the supplied intents
// (which may be PENDING, in which case they are first pushed) while
// taking care of proper batching.
type CleanupIntentsFunc func(context.Context, []roachpb.Intent) error

// A cleanupTxnIntentsFunc asynchronously cleans up intents from a
// transaction record, pushing the transaction first if it is
// PENDING. Once all intents are resolved successfully, removes the
// transaction record.
type CleanupTxnIntentsAsyncFunc func(context.Context, *roachpb.Transaction, []roachpb.Intent) error

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
	gcer GCer,
	cleanupIntentsFn CleanupIntentsFunc,
	cleanupTxnIntentsAsyncFn CleanupTxnIntentsAsyncFunc,
) (GCInfo, error) {

	iter := rditer.NewReplicaDataIterator(desc, snap,
		true /* replicatedOnly */, false /* seekEnd */)
	defer iter.Close()

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now.Add(-IntentAgeThreshold.Nanoseconds(), 0)
	txnExp := now.Add(-storagebase.TxnCleanupThreshold.Nanoseconds(), 0)

	gc := MakeGarbageCollector(now, policy)

	if err := gcer.SetGCThreshold(ctx, GCThreshold{
		Key: gc.Threshold,
		Txn: txnExp,
	}); err != nil {
		return GCInfo{}, errors.Wrap(err, "failed to set GC thresholds")
	}

	var batchGCKeys []roachpb.GCRequest_GCKey
	var batchGCKeysBytes int64
	var expBaseKey roachpb.Key
	var keys []engine.MVCCKey
	var vals [][]byte
	var keyBytes int64
	var valBytes int64
	info := GCInfo{
		Policy:    policy,
		Now:       now,
		Threshold: gc.Threshold,
	}

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
				log.Errorf(ctx, "unable to unmarshal MVCC metadata for key %q: %+v", keys[0], err)
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
							info.IntentTxns++
							// All transactions in txnMap may be PENDING and
							// cleanupIntentsFn will push them to finalize them.
							info.PushTxn++
						}
						info.IntentsConsidered++
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
						info.AffectedVersionsKeyBytes += keyBytes
						info.AffectedVersionsValBytes += valBytes

						batchGCKeysBytes += keyBytes
						// If the current key brings the batch over the target
						// size, add the current timestamp to finish the current
						// chunk and start a new one.
						if batchGCKeysBytes >= KeyVersionChunkBytes {
							batchGCKeys = append(batchGCKeys, roachpb.GCRequest_GCKey{Key: expBaseKey, Timestamp: keys[i].Timestamp})

							err := gcer.GC(ctx, batchGCKeys)

							// Succeed or fail, allow releasing the memory backing batchGCKeys.
							iter.ResetAllocator()
							batchGCKeys = nil
							batchGCKeysBytes = 0

							if err != nil {
								// Even though we are batching the GC process, it's
								// safe to continue because we bumped the GC
								// thresholds. We may leave some inconsistent history
								// behind, but nobody can read it.
								log.Warning(ctx, err)
								return
							}
						}
					}
					// Add the key to the batch at the GC timestamp, unless it was already added.
					if batchGCKeysBytes != 0 {
						batchGCKeys = append(batchGCKeys, roachpb.GCRequest_GCKey{Key: expBaseKey, Timestamp: gcTS})
					}
					info.NumKeysAffected++
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
		} else if ctx.Err() != nil {
			// Stop iterating if our context has expired.
			return GCInfo{}, err
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
		if err := gcer.GC(ctx, batchGCKeys); err != nil {
			return GCInfo{}, err
		}
	}

	// From now on, all newly added keys are range-local.

	// Process local range key entries (txn records, queue last processed times).
	localRangeKeys, err := processLocalKeyRange(ctx, snap, desc, txnExp, &info, cleanupTxnIntentsAsyncFn)
	if err != nil {
		return GCInfo{}, err
	}

	if err := gcer.GC(ctx, localRangeKeys); err != nil {
		return GCInfo{}, err
	}

	// Clean up the AbortSpan.
	log.Event(ctx, "processing AbortSpan")
	abortSpanKeys := processAbortSpan(ctx, snap, desc.RangeID, txnExp, &info)
	if err := gcer.GC(ctx, abortSpanKeys); err != nil {
		return GCInfo{}, err
	}

	log.Eventf(ctx, "GC'ed keys; stats %+v", info)

	// Push transactions (if pending) and resolve intents.
	var intents []roachpb.Intent
	for txnID, txn := range txnMap {
		intents = append(intents, roachpb.AsIntents(intentSpanMap[txnID], txn)...)
	}
	info.ResolveTotal += len(intents)
	log.Eventf(ctx, "cleanup of %d intents", len(intents))
	if err := cleanupIntentsFn(ctx, intents); err != nil {
		return GCInfo{}, err
	}

	return info, nil
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
	info *GCInfo,
	cleanupTxnIntentsAsyncFn CleanupTxnIntentsAsyncFunc,
) ([]roachpb.GCRequest_GCKey, error) {
	var gcKeys []roachpb.GCRequest_GCKey

	handleTxnIntents := func(key roachpb.Key, txn *roachpb.Transaction) error {
		// If the transaction needs to be pushed or there are intents to
		// resolve, invoke the cleanup function.
		if !txn.Status.IsFinalized() || len(txn.IntentSpans) > 0 {
			return cleanupTxnIntentsAsyncFn(ctx, txn, roachpb.AsIntents(txn.IntentSpans, txn))
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
		if !txn.LastActive().Less(cutoff) {
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
//
// TODO(tschottdorf): this could be done in Replica.GC itself, but it's
// handy to have it here for stats (though less performant due to sending
// all of the keys over the wire).
func processAbortSpan(
	ctx context.Context,
	snap engine.Reader,
	rangeID roachpb.RangeID,
	threshold hlc.Timestamp,
	info *GCInfo,
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
