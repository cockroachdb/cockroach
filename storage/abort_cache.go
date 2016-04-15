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
	"errors"

	"golang.org/x/net/context"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/uuid"
)

var errEmptyTxnID = errors.New("empty Transaction ID used in abort cache")

// The AbortCache sets markers for aborted transactions to provide
// protection against an aborted but active transaction not reading
// values it wrote (due to its intents having been removed).
//
// The AbortCache stores responses in the underlying engine, using
// keys derived from Range ID and txn ID.
//
// A AbortCache is not thread safe. Access to it is serialized
// through Raft.
type AbortCache struct {
	rangeID roachpb.RangeID
}

// NewAbortCache returns a new abort cache. Every range replica
// maintains an abort cache, not just the leader.
func NewAbortCache(rangeID roachpb.RangeID) *AbortCache {
	return &AbortCache{
		rangeID: rangeID,
	}
}

var txnIDMin = new(uuid.UUID)
var txnIDMax = new(uuid.UUID)

func init() {
	for i := range txnIDMin.GetBytes() {
		txnIDMin.UUID[i] = '\x00'
	}
	for i := range txnIDMax.GetBytes() {
		txnIDMax.UUID[i] = '\xff'
	}
}

func (sc *AbortCache) min() roachpb.Key {
	return keys.AbortCacheKey(sc.rangeID, txnIDMin)
}

func (sc *AbortCache) max() roachpb.Key {
	return keys.AbortCacheKey(sc.rangeID, txnIDMax)
}

// ClearData removes all persisted items stored in the cache.
func (sc *AbortCache) ClearData(e engine.Engine) error {
	_, err := engine.ClearRange(e, engine.MakeMVCCMetadataKey(sc.min()), engine.MakeMVCCMetadataKey(sc.max()))
	return err
}

// Get looks up an abort cache entry recorded for this transaction ID.
// Returns whether an abort record was found and any error.
func (sc *AbortCache) Get(
	ctx context.Context,
	e engine.Engine,
	txnID *uuid.UUID,
	entry *roachpb.AbortCacheEntry,
) (bool, error) {
	if txnID == nil {
		return false, errEmptyTxnID
	}

	// Pull response from disk and read into reply if available.
	key := keys.AbortCacheKey(sc.rangeID, txnID)
	ok, err := engine.MVCCGetProto(ctx, e, key, roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */, entry)
	return ok, err
}

// Iterate walks through the abort cache, invoking the given callback for
// each unmarshaled entry with the key, the transaction ID and the decoded
// entry.
// TODO(tschottdorf): should not use a pointer to UUID.
func (sc *AbortCache) Iterate(
	ctx context.Context,
	e engine.Engine,
	f func([]byte, *uuid.UUID, roachpb.AbortCacheEntry),
) {
	_, _ = engine.MVCCIterate(ctx, e, sc.min(), sc.max(), roachpb.ZeroTimestamp,
		true /* consistent */, nil /* txn */, false, /* !reverse */
		func(kv roachpb.KeyValue) (bool, error) {
			var entry roachpb.AbortCacheEntry
			txnID, err := keys.DecodeAbortCacheKey(kv.Key, nil)
			if err != nil {
				panic(err) // TODO(tschottdorf): ReplicaCorruptionError
			}
			if err := kv.Value.GetProto(&entry); err != nil {
				panic(err) // TODO(tschottdorf): ReplicaCorruptionError
			}
			f(kv.Key, txnID, entry)
			return false, nil
		})
}

func copySeqCache(
	e engine.Engine,
	ms *engine.MVCCStats,
	srcID, dstID roachpb.RangeID,
	keyMin, keyMax engine.MVCCKey,
) (int, error) {
	var scratch [64]byte
	var count int
	var meta engine.MVCCMetadata
	// TODO(spencer): look into making this an MVCCIteration and writing
	// the values using MVCC so we can avoid the ugliness of updating
	// the MVCCStats by hand below.
	err := e.Iterate(keyMin, keyMax,
		func(kv engine.MVCCKeyValue) (bool, error) {
			// Decode the key, skipping on error. Otherwise, write it to the
			// corresponding key in the new cache.
			txnID, err := decodeAbortCacheMVCCKey(kv.Key, scratch[:0])
			if err != nil {
				return false, util.Errorf("could not decode an abort cache key %s: %s", kv.Key, err)
			}
			key := keys.AbortCacheKey(dstID, txnID)
			encKey := engine.MakeMVCCMetadataKey(key)
			// Decode the MVCCMetadata value.
			if err := proto.Unmarshal(kv.Value, &meta); err != nil {
				return false, util.Errorf("could not decode mvcc metadata %s [% x]: %s", kv.Key, kv.Value, err)
			}
			value := meta.Value()
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
	e engine.Engine,
	ms *engine.MVCCStats,
	destRangeID roachpb.RangeID,
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
	ctx context.Context,
	e engine.Engine,
	ms *engine.MVCCStats,
	originRangeID roachpb.RangeID,
) (int, error) {
	originMin := engine.MakeMVCCMetadataKey(keys.AbortCacheKey(originRangeID, txnIDMin))
	originMax := engine.MakeMVCCMetadataKey(keys.AbortCacheKey(originRangeID, txnIDMax))
	return copySeqCache(e, ms, originRangeID, sc.rangeID, originMin, originMax)
}

// Del removes all abort cache entries for the given transaction.
func (sc *AbortCache) Del(
	ctx context.Context,
	e engine.Engine,
	ms *engine.MVCCStats,
	txnID *uuid.UUID,
) error {
	key := keys.AbortCacheKey(sc.rangeID, txnID)
	return engine.MVCCDelete(ctx, e, ms, key, roachpb.ZeroTimestamp, nil /* txn */)
}

// Put writes an entry for the specified transaction ID.
func (sc *AbortCache) Put(
	ctx context.Context,
	e engine.Engine,
	ms *engine.MVCCStats,
	txnID *uuid.UUID,
	entry *roachpb.AbortCacheEntry,
) error {
	if txnID == nil {
		return errEmptyTxnID
	}
	key := keys.AbortCacheKey(sc.rangeID, txnID)
	return engine.MVCCPutProto(ctx, e, ms, key, roachpb.ZeroTimestamp, nil /* txn */, entry)
}

func decodeAbortCacheMVCCKey(
	encKey engine.MVCCKey, dest []byte,
) (*uuid.UUID, error) {
	if encKey.IsValue() {
		return nil, util.Errorf("key %s is not a raw MVCC value", encKey)
	}
	return keys.DecodeAbortCacheKey(encKey.Key, dest)
}
