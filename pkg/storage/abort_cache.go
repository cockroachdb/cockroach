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
// Author: Tobias Schottdorf (tobias@cockroachlabs.com)

package storage

import (
	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// The AbortCache sets markers for aborted transactions to provide
// protection against an aborted but active transaction not reading
// values it wrote (due to its intents having been removed).
//
// The cache is range-specific. It is updated when an intent for an aborted txn
// is cleared from a range, and is consulted before read commands are processed
// on a range.
//
// The AbortCache stores responses in the underlying engine, using keys derived
// from Range ID and txn ID.
// Note that the epoch number is not used to query the cache: once aborted, even
// higher epochs are prohibited from reading data. That's because, for better or
// worse, the intent resolution process clears intents even from epochs higher
// than the txn meta used for clearing (see engine.MVCCResolveWriteIntent), and
// this clearing can race with the new epoch laying intents.
//
// A AbortCache is not thread safe. Access to it is serialized
// through Raft.
//
// TODO(tschottdorf): we seem to have made a half-hearted attempt at naming
// this the "AbortSpan" instead, but large parts of the code still call this
// "AbortCache". We should settle for one and rename everything post-yellow.
type AbortCache struct {
	rangeID roachpb.RangeID
}

// NewAbortCache returns a new abort cache. Every range replica
// maintains an abort cache, not just the lease holder.
func NewAbortCache(rangeID roachpb.RangeID) *AbortCache {
	return &AbortCache{
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

func abortCacheMinKey(rangeID roachpb.RangeID) roachpb.Key {
	return keys.AbortCacheKey(rangeID, txnIDMin)
}

func (sc *AbortCache) min() roachpb.Key {
	return abortCacheMinKey(sc.rangeID)
}

func abortCacheMaxKey(rangeID roachpb.RangeID) roachpb.Key {
	return keys.AbortCacheKey(rangeID, txnIDMax)
}

func (sc *AbortCache) max() roachpb.Key {
	return abortCacheMaxKey(sc.rangeID)
}

// ClearData removes all persisted items stored in the cache.
func (sc *AbortCache) ClearData(e engine.Engine) error {
	iter := e.NewIterator(false)
	defer iter.Close()
	b := e.NewWriteOnlyBatch()
	defer b.Close()
	err := b.ClearIterRange(iter, engine.MakeMVCCMetadataKey(sc.min()),
		engine.MakeMVCCMetadataKey(sc.max()))
	if err != nil {
		return err
	}
	return b.Commit(false /* !sync */)
}

// Get looks up an abort cache entry recorded for this transaction ID.
// Returns whether an abort record was found and any error.
func (sc *AbortCache) Get(
	ctx context.Context, e engine.Reader, txnID uuid.UUID, entry *roachpb.AbortCacheEntry,
) (bool, error) {

	// Pull response from disk and read into reply if available.
	key := keys.AbortCacheKey(sc.rangeID, txnID)
	ok, err := engine.MVCCGetProto(ctx, e, key, hlc.Timestamp{}, true /* consistent */, nil /* txn */, entry)
	return ok, err
}

// Iterate walks through the abort cache, invoking the given callback for
// each unmarshaled entry with the key, the transaction ID and the decoded
// entry.
// TODO(tschottdorf): should not use a pointer to UUID.
func (sc *AbortCache) Iterate(
	ctx context.Context, e engine.Reader, f func([]byte, roachpb.AbortCacheEntry),
) {
	_, _ = engine.MVCCIterate(ctx, e, sc.min(), sc.max(), hlc.Timestamp{},
		true /* consistent */, nil /* txn */, false, /* !reverse */
		func(kv roachpb.KeyValue) (bool, error) {
			var entry roachpb.AbortCacheEntry
			if _, err := keys.DecodeAbortCacheKey(kv.Key, nil); err != nil {
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
			txnID, err := decodeAbortCacheMVCCKey(kv.Key, scratch[:0])
			if err != nil {
				return false, errors.Errorf("could not decode an abort cache key %s: %s", kv.Key, err)
			}
			key := keys.AbortCacheKey(dstID, txnID)
			encKey := engine.MakeMVCCMetadataKey(key)
			// Decode the MVCCMetadata value.
			if err := proto.Unmarshal(kv.Value, &meta); err != nil {
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

// CopyInto copies all the results from this abort cache into the destRangeID
// abort cache. Failures decoding individual cache entries return an error.
// On success, returns the number of entries (key-value pairs) copied.
func (sc *AbortCache) CopyInto(
	e engine.ReadWriter, ms *enginepb.MVCCStats, destRangeID roachpb.RangeID,
) (int, error) {
	return copySeqCache(e, ms, sc.rangeID, destRangeID,
		engine.MakeMVCCMetadataKey(sc.min()), engine.MakeMVCCMetadataKey(sc.max()))
}

// CopyFrom copies all the persisted results from the originRangeID
// abort cache into this one. Note that the cache will not be
// locked while copying is in progress. Failures decoding individual
// entries return an error. The copy is done directly using the engine
// instead of interpreting values through MVCC for efficiency.
// On success, returns the number of entries (key-value pairs) copied.
func (sc *AbortCache) CopyFrom(
	ctx context.Context, e engine.ReadWriter, ms *enginepb.MVCCStats, originRangeID roachpb.RangeID,
) (int, error) {
	originMin := engine.MakeMVCCMetadataKey(keys.AbortCacheKey(originRangeID, txnIDMin))
	originMax := engine.MakeMVCCMetadataKey(keys.AbortCacheKey(originRangeID, txnIDMax))
	return copySeqCache(e, ms, originRangeID, sc.rangeID, originMin, originMax)
}

// Del removes all abort cache entries for the given transaction.
func (sc *AbortCache) Del(
	ctx context.Context, e engine.ReadWriter, ms *enginepb.MVCCStats, txnID uuid.UUID,
) error {
	key := keys.AbortCacheKey(sc.rangeID, txnID)
	return engine.MVCCDelete(ctx, e, ms, key, hlc.Timestamp{}, nil /* txn */)
}

// Put writes an entry for the specified transaction ID.
func (sc *AbortCache) Put(
	ctx context.Context,
	e engine.ReadWriter,
	ms *enginepb.MVCCStats,
	txnID uuid.UUID,
	entry *roachpb.AbortCacheEntry,
) error {
	key := keys.AbortCacheKey(sc.rangeID, txnID)
	return engine.MVCCPutProto(ctx, e, ms, key, hlc.Timestamp{}, nil /* txn */, entry)
}

func decodeAbortCacheMVCCKey(encKey engine.MVCCKey, dest []byte) (uuid.UUID, error) {
	if encKey.IsValue() {
		return uuid.UUID{}, errors.Errorf("key %s is not a raw MVCC value", encKey)
	}
	return keys.DecodeAbortCacheKey(encKey.Key, dest)
}
