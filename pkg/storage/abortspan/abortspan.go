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

package abortspan

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// An AbortSpan sets markers for aborted transactions to provide protection
// against an aborted but active transaction not reading values it wrote (due to
// its intents having been removed).
//
// The span is range-specific. It is updated when an intent for an aborted txn
// is cleared from a range, and is consulted before read commands are processed
// on a range.
//
// An AbortSpan is not thread safe.
type AbortSpan struct {
	rangeID roachpb.RangeID
}

// New returns a new AbortSpan. Every range replica
// maintains an AbortSpan, not just the lease holder.
func New(rangeID roachpb.RangeID) *AbortSpan {
	return &AbortSpan{
		rangeID: rangeID,
	}
}

func fillUUID(b byte) uuid.UUID {
	var ret uuid.UUID
	for i := range ret.GetBytes() {
		ret.UUID[i] = b
	}
	return ret
}

var txnIDMin = fillUUID('\x00')
var txnIDMax = fillUUID('\xff')

// MinKey returns the lower bound of the key span associated to an instance for the given RangeID.
func MinKey(rangeID roachpb.RangeID) roachpb.Key {
	return keys.AbortSpanKey(rangeID, txnIDMin)
}

func (sc *AbortSpan) min() roachpb.Key {
	return MinKey(sc.rangeID)
}

// MaxKey returns the upper bound of the key span associated to an instance for the given RangeID.
func MaxKey(rangeID roachpb.RangeID) roachpb.Key {
	return keys.AbortSpanKey(rangeID, txnIDMax)
}

func (sc *AbortSpan) max() roachpb.Key {
	return MaxKey(sc.rangeID)
}

// ClearData removes all persisted items stored in the cache.
func (sc *AbortSpan) ClearData(e engine.Engine) error {
	iter := e.NewIterator(false)
	defer iter.Close()
	b := e.NewWriteOnlyBatch()
	defer b.Close()
	err := b.ClearIterRange(iter, engine.MakeMVCCMetadataKey(sc.min()),
		engine.MakeMVCCMetadataKey(sc.max()))
	if err != nil {
		return err
	}
	return b.Commit(false /* sync */)
}

// Get looks up an AbortSpan entry recorded for this transaction ID.
// Returns whether an abort record was found and any error.
func (sc *AbortSpan) Get(
	ctx context.Context, e engine.Reader, txnID uuid.UUID, entry *roachpb.AbortSpanEntry,
) (bool, error) {

	// Pull response from disk and read into reply if available.
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	ok, err := engine.MVCCGetProto(ctx, e, key, hlc.Timestamp{}, true /* consistent */, nil /* txn */, entry)
	return ok, err
}

// Iterate walks through the AbortSpan, invoking the given callback for
// each unmarshaled entry with the key, the transaction ID and the decoded
// entry.
// TODO(tschottdorf): should not use a pointer to UUID.
func (sc *AbortSpan) Iterate(
	ctx context.Context, e engine.Reader, f func([]byte, roachpb.AbortSpanEntry),
) {
	_, _ = engine.MVCCIterate(ctx, e, sc.min(), sc.max(), hlc.Timestamp{},
		true /* consistent */, false /* tombstones */, nil /* txn */, false, /* reverse */
		func(kv roachpb.KeyValue) (bool, error) {
			var entry roachpb.AbortSpanEntry
			if _, err := keys.DecodeAbortSpanKey(kv.Key, nil); err != nil {
				panic(err) // TODO(tschottdorf): ReplicaCorruptionError
			}
			if err := kv.Value.GetProto(&entry); err != nil {
				panic(err) // TODO(tschottdorf): ReplicaCorruptionError
			}
			f(kv.Key, entry)
			return false, nil
		})
}

func copySeqCache(
	e engine.ReadWriter,
	ms *enginepb.MVCCStats,
	srcID, dstID roachpb.RangeID,
	keyMin, keyMax engine.MVCCKey,
) (int, error) {
	var scratch [64]byte
	var count int
	var meta enginepb.MVCCMetadata
	// TODO(spencer): look into making this an MVCCIteration and writing
	// the values using MVCC so we can avoid the ugliness of updating
	// the MVCCStats by hand below.
	err := e.Iterate(keyMin, keyMax,
		func(kv engine.MVCCKeyValue) (bool, error) {
			// Decode the key, skipping on error. Otherwise, write it to the
			// corresponding key in the new cache.
			txnID, err := decodeAbortSpanMVCCKey(kv.Key, scratch[:0])
			if err != nil {
				return false, errors.Errorf("could not decode an AbortSpan key %s: %s", kv.Key, err)
			}
			key := keys.AbortSpanKey(dstID, txnID)
			encKey := engine.MakeMVCCMetadataKey(key)
			// Decode the MVCCMetadata value.
			if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
				return false, errors.Errorf("could not decode mvcc metadata %s [% x]: %s", kv.Key, kv.Value, err)
			}
			value := engine.MakeValue(meta)
			value.ClearChecksum()
			value.InitChecksum(key)
			meta.RawBytes = value.RawBytes

			keyBytes, valBytes, err := engine.PutProto(e, encKey, &meta)
			if err != nil {
				return false, err
			}
			count++
			if ms != nil {
				ms.SysBytes += keyBytes + valBytes
				ms.SysCount++
			}
			return false, nil
		})
	return count, err
}

// CopyInto copies all the results from this AbortSpan into the destRangeID
// AbortSpan. Failures decoding individual cache entries return an error.
// On success, returns the number of entries (key-value pairs) copied.
func (sc *AbortSpan) CopyInto(
	e engine.ReadWriter, ms *enginepb.MVCCStats, destRangeID roachpb.RangeID,
) (int, error) {
	return copySeqCache(e, ms, sc.rangeID, destRangeID,
		engine.MakeMVCCMetadataKey(sc.min()), engine.MakeMVCCMetadataKey(sc.max()))
}

// CopyFrom copies all the persisted results from the originRangeID
// AbortSpan into this one. Note that the cache will not be
// locked while copying is in progress. Failures decoding individual
// entries return an error. The copy is done directly using the engine
// instead of interpreting values through MVCC for efficiency.
// On success, returns the number of entries (key-value pairs) copied.
func (sc *AbortSpan) CopyFrom(
	ctx context.Context, e engine.ReadWriter, ms *enginepb.MVCCStats, originRangeID roachpb.RangeID,
) (int, error) {
	originMin := engine.MakeMVCCMetadataKey(keys.AbortSpanKey(originRangeID, txnIDMin))
	originMax := engine.MakeMVCCMetadataKey(keys.AbortSpanKey(originRangeID, txnIDMax))
	return copySeqCache(e, ms, originRangeID, sc.rangeID, originMin, originMax)
}

// Del removes all AbortSpan entries for the given transaction.
func (sc *AbortSpan) Del(
	ctx context.Context, e engine.ReadWriter, ms *enginepb.MVCCStats, txnID uuid.UUID,
) error {
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	return engine.MVCCDelete(ctx, e, ms, key, hlc.Timestamp{}, nil /* txn */)
}

// Put writes an entry for the specified transaction ID.
func (sc *AbortSpan) Put(
	ctx context.Context,
	e engine.ReadWriter,
	ms *enginepb.MVCCStats,
	txnID uuid.UUID,
	entry *roachpb.AbortSpanEntry,
) error {
	key := keys.AbortSpanKey(sc.rangeID, txnID)
	return engine.MVCCPutProto(ctx, e, ms, key, hlc.Timestamp{}, nil /* txn */, entry)
}

func decodeAbortSpanMVCCKey(encKey engine.MVCCKey, dest []byte) (uuid.UUID, error) {
	if encKey.IsValue() {
		return uuid.UUID{}, errors.Errorf("key %s is not a raw MVCC value", encKey)
	}
	return keys.DecodeAbortSpanKey(encKey.Key, dest)
}
