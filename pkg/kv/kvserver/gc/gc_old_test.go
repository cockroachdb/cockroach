// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// runGCOld is an older implementation of Run. It is used for benchmarking and
// testing.
//
// runGCOld runs garbage collection for the specified descriptor on the
// provided Engine (which is not mutated). It uses the provided GCer
// to run garbage collection once on all implicated spans,
// cleanupIntentsFn to resolve intents synchronously, and
// cleanupTxnIntentsAsyncFn to asynchronously cleanup intents and
// associated transaction record on success.
func runGCOld(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	snap storage.Reader,
	now hlc.Timestamp,
	_ hlc.Timestamp, // exists to make signature match RunGC
	intentAgeThreshold time.Duration,
	policy zonepb.GCPolicy,
	gcer GCer,
	cleanupIntentsFn CleanupIntentsFunc,
	cleanupTxnIntentsAsyncFn CleanupTxnIntentsAsyncFunc,
) (Info, error) {

	iter := rditer.NewReplicaMVCCDataIterator(desc, snap, false /* seekEnd */)
	defer iter.Close()

	// Compute intent expiration (intent age at which we attempt to resolve).
	intentExp := now.Add(-intentAgeThreshold.Nanoseconds(), 0)
	txnExp := now.Add(-kvserverbase.TxnCleanupThreshold.Nanoseconds(), 0)

	gc := MakeGarbageCollector(now, policy)

	if err := gcer.SetGCThreshold(ctx, Threshold{
		Key: gc.Threshold,
		Txn: txnExp,
	}); err != nil {
		return Info{}, errors.Wrap(err, "failed to set GC thresholds")
	}

	var batchGCKeys []roachpb.GCRequest_GCKey
	var batchGCKeysBytes int64
	var expBaseKey roachpb.Key
	var keys []storage.MVCCKey
	var vals [][]byte
	var keyBytes int64
	var valBytes int64
	info := Info{
		Policy:    policy,
		Now:       now,
		Threshold: gc.Threshold,
	}

	// Maps from txn ID to txn and intent key slice.
	txnMap := map[uuid.UUID]*roachpb.Transaction{}
	intentKeyMap := map[uuid.UUID][]roachpb.Key{}

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
					if meta.Timestamp.ToTimestamp().Less(intentExp) {
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
						intentKeyMap[txnID] = append(intentKeyMap[txnID], expBaseKey)
					}
					// With an active intent, GC ignores MVCC metadata & intent value.
					startIdx = 2
				}
				// See if any values may be GC'd.
				if idx, gcTS := gc.Filter(keys[startIdx:], vals[startIdx:]); !gcTS.IsEmpty() {
					// Batch keys after the total size of version keys exceeds
					// the threshold limit. This avoids sending potentially large
					// GC requests through Raft. Iterate through the keys in reverse
					// order so that GC requests can be made multiple times even on
					// a single key, with successively newer timestamps to prevent
					// any single request from exploding during GC evaluation.
					for i := len(keys) - 1; i >= startIdx+idx; i-- {
						keyBytes = int64(keys[i].EncodedSize())
						valBytes = int64(len(vals[i]))

						// Add the total size of the GC'able versions of the keys and values to Info.
						info.AffectedVersionsKeyBytes += keyBytes
						info.AffectedVersionsValBytes += valBytes

						batchGCKeysBytes += keyBytes
						// If the current key brings the batch over the target
						// size, add the current timestamp to finish the current
						// chunk and start a new one.
						if batchGCKeysBytes >= KeyVersionChunkBytes {
							batchGCKeys = append(batchGCKeys, roachpb.GCRequest_GCKey{Key: expBaseKey, Timestamp: keys[i].Timestamp})

							err := gcer.GC(ctx, batchGCKeys)

							batchGCKeys = nil
							batchGCKeysBytes = 0

							if err != nil {
								// Even though we are batching the GC process, it's
								// safe to continue because we bumped the GC
								// thresholds. We may leave some inconsistent history
								// behind, but nobody can read it.
								log.Warningf(ctx, "%v", err)
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
			return Info{}, err
		} else if !ok {
			break
		} else if ctx.Err() != nil {
			// Stop iterating if our context has expired.
			return Info{}, err
		}
		iterKey := iter.Key()
		if !iterKey.IsValue() || !iterKey.Key.Equal(expBaseKey) {
			// Moving to the next key (& values).
			processKeysAndValues()
			expBaseKey = iterKey.Key
			if !iterKey.IsValue() {
				keys = []storage.MVCCKey{iter.Key()}
				vals = [][]byte{iter.Value()}
				continue
			}
			// An implicit metadata.
			keys = []storage.MVCCKey{storage.MakeMVCCMetadataKey(iterKey.Key)}
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
			return Info{}, err
		}
	}

	// From now on, all keys processed are range-local.

	// Process local range key entries (txn records, queue last processed times).
	if err := processLocalKeyRange(ctx, snap, desc, txnExp, &info, cleanupTxnIntentsAsyncFn, gcer); err != nil {
		log.Warningf(ctx, "while gc'ing local key range: %s", err)
	}

	// Clean up the AbortSpan.
	log.Event(ctx, "processing AbortSpan")
	if err := processAbortSpan(ctx, snap, desc.RangeID, txnExp, &info, gcer); err != nil {
		log.Warningf(ctx, "while gc'ing abort span: %s", err)
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

// GarbageCollector GCs MVCC key/values using a zone-specific GC
// policy allows either the union or intersection of maximum # of
// versions and maximum age.
type GarbageCollector struct {
	Threshold hlc.Timestamp
	policy    zonepb.GCPolicy
}

// MakeGarbageCollector allocates and returns a new GC, with expiration
// computed based on current time and policy.TTLSeconds.
func MakeGarbageCollector(now hlc.Timestamp, policy zonepb.GCPolicy) GarbageCollector {
	return GarbageCollector{
		Threshold: CalculateThreshold(now, policy),
		policy:    policy,
	}
}

// Filter makes decisions about garbage collection based on the
// garbage collection policy for batches of values for the same
// key. Returns the index of the first key to be GC'd and the
// timestamp including, and after which, all values should be garbage
// collected. If no values should be GC'd, returns -1 for the index
// and the zero timestamp. Keys must be in descending time
// order. Values deleted at or before the returned timestamp can be
// deleted without invalidating any reads in the time interval
// (gc.expiration, \infinity).
//
// The GC keeps all values (including deletes) above the expiration time, plus
// the first value before or at the expiration time. This allows reads to be
// guaranteed as described above. However if this were the only rule, then if
// the most recent write was a delete, it would never be removed. Thus, when a
// deleted value is the most recent before expiration, it can be deleted. This
// would still allow for the tombstone bugs in #6227, so in the future we will
// add checks that disallow writes before the last GC expiration time.
func (gc GarbageCollector) Filter(keys []storage.MVCCKey, values [][]byte) (int, hlc.Timestamp) {
	if gc.policy.TTLSeconds <= 0 {
		return -1, hlc.Timestamp{}
	}
	if len(keys) == 0 {
		return -1, hlc.Timestamp{}
	}

	// find the first expired key index using binary search
	i := sort.Search(len(keys), func(i int) bool { return keys[i].Timestamp.LessEq(gc.Threshold) })

	if i == len(keys) {
		return -1, hlc.Timestamp{}
	}

	// Now keys[i].Timestamp is <= gc.expiration, but the key-value pair is still
	// "visible" at timestamp gc.expiration (and up to the next version).
	if deleted := len(values[i]) == 0; deleted {
		// We don't have to keep a delete visible (since GCing it does not change
		// the outcome of the read). Note however that we can't touch deletes at
		// higher timestamps immediately preceding this one, since they're above
		// gc.expiration and are needed for correctness; see #6227.
		return i, keys[i].Timestamp
	} else if i+1 < len(keys) {
		// Otherwise mark the previous timestamp for deletion (since it won't ever
		// be returned for reads at gc.expiration and up).
		return i + 1, keys[i+1].Timestamp
	}

	return -1, hlc.Timestamp{}
}

func mvccVersionKey(key roachpb.Key, ts hlc.Timestamp) storage.MVCCKey {
	return storage.MVCCKey{Key: key, Timestamp: ts}
}

var (
	aKey  = roachpb.Key("a")
	bKey  = roachpb.Key("b")
	aKeys = []storage.MVCCKey{
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: 2e9, Logical: 0}),
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: 1e9, Logical: 1}),
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: 1e9, Logical: 0}),
	}
	bKeys = []storage.MVCCKey{
		mvccVersionKey(bKey, hlc.Timestamp{WallTime: 2e9, Logical: 0}),
		mvccVersionKey(bKey, hlc.Timestamp{WallTime: 1e9, Logical: 0}),
	}
)

// TestGarbageCollectorFilter verifies the filter policies for
// different sorts of MVCC keys.
func TestGarbageCollectorFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	gcA := MakeGarbageCollector(hlc.Timestamp{WallTime: 0, Logical: 0}, zonepb.GCPolicy{TTLSeconds: 1})
	gcB := MakeGarbageCollector(hlc.Timestamp{WallTime: 0, Logical: 0}, zonepb.GCPolicy{TTLSeconds: 2})
	n := []byte("data")
	d := []byte(nil)
	testData := []struct {
		gc       GarbageCollector
		time     hlc.Timestamp
		keys     []storage.MVCCKey
		values   [][]byte
		expIdx   int
		expDelTS hlc.Timestamp
	}{
		{gcA, hlc.Timestamp{WallTime: 0, Logical: 0}, aKeys, [][]byte{n, n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 0, Logical: 0}, aKeys, [][]byte{d, d, d}, -1, hlc.Timestamp{}},
		{gcB, hlc.Timestamp{WallTime: 0, Logical: 0}, bKeys, [][]byte{n, n}, -1, hlc.Timestamp{}},
		{gcB, hlc.Timestamp{WallTime: 0, Logical: 0}, bKeys, [][]byte{d, d}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 1e9, Logical: 0}, aKeys, [][]byte{n, n, n}, -1, hlc.Timestamp{}},
		{gcB, hlc.Timestamp{WallTime: 1e9, Logical: 0}, bKeys, [][]byte{n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 2e9, Logical: 0}, aKeys, [][]byte{n, n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 2e9, Logical: 0}, aKeys, [][]byte{d, d, d}, 2, hlc.Timestamp{WallTime: 1e9, Logical: 0}},
		{gcB, hlc.Timestamp{WallTime: 2e9, Logical: 0}, bKeys, [][]byte{n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 3e9, Logical: 0}, aKeys, [][]byte{n, n, n}, 1, hlc.Timestamp{WallTime: 1e9, Logical: 1}},
		{gcA, hlc.Timestamp{WallTime: 3e9, Logical: 0}, aKeys, [][]byte{d, n, n}, 0, hlc.Timestamp{WallTime: 2e9, Logical: 0}},
		{gcB, hlc.Timestamp{WallTime: 3e9, Logical: 0}, bKeys, [][]byte{n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 4e9, Logical: 0}, aKeys, [][]byte{n, n, n}, 1, hlc.Timestamp{WallTime: 1e9, Logical: 1}},
		{gcB, hlc.Timestamp{WallTime: 4e9, Logical: 0}, bKeys, [][]byte{n, n}, 1, hlc.Timestamp{WallTime: 1e9, Logical: 0}},
		{gcB, hlc.Timestamp{WallTime: 4e9, Logical: 0}, bKeys, [][]byte{d, n}, 0, hlc.Timestamp{WallTime: 2e9, Logical: 0}},
		{gcA, hlc.Timestamp{WallTime: 5e9, Logical: 0}, aKeys, [][]byte{n, n, n}, 1, hlc.Timestamp{WallTime: 1e9, Logical: 1}},
		{gcB, hlc.Timestamp{WallTime: 5e9, Logical: 0}, bKeys, [][]byte{n, n}, 1, hlc.Timestamp{WallTime: 1e9, Logical: 0}},
		{gcB, hlc.Timestamp{WallTime: 5e9, Logical: 0}, bKeys, [][]byte{d, n}, 0, hlc.Timestamp{WallTime: 2e9, Logical: 0}},
	}
	for i, test := range testData {
		test.gc.Threshold = test.time
		test.gc.Threshold.WallTime -= int64(test.gc.policy.TTLSeconds) * 1e9
		idx, delTS := test.gc.Filter(test.keys, test.values)
		if idx != test.expIdx {
			t.Errorf("%d: expected index %d; got %d", i, test.expIdx, idx)
		}
		if delTS != test.expDelTS {
			t.Errorf("%d: expected deletion timestamp %s; got %s", i, test.expDelTS, delTS)
		}
	}
}
