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
	"bytes"
	"errors"
	"math"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	// SequencePoisonAbort is a special value for the sequence cache which
	// commands a TransactionAbortedError.
	SequencePoisonAbort = math.MaxUint32
	// SequencePoisonRestart is a special value for the sequence cache which
	// commands a TransactionRestartError.
	SequencePoisonRestart = math.MaxUint32 - 1
)

var errEmptyTxnID = errors.New("empty Transaction ID used in sequence cache")

// The SequenceCache provides idempotence for request retries. Each
// transactional request to a range specifies an Transaction ID and sequence number
// which uniquely identifies a client command. After commands have
// been replicated via Raft, they are executed against the state
// machine and the results are stored in the SequenceCache.
//
// The SequenceCache stores responses in the underlying engine, using
// keys derived from Range ID, txn ID and sequence number.
//
// A SequenceCache is not thread safe. Access to it is serialized
// through Raft.
type SequenceCache struct {
	rangeID      roachpb.RangeID
	min, max     roachpb.Key
	scratchEntry roachpb.SequenceCacheEntry
	scratchBuf   [256]byte
}

// NewSequenceCache returns a new sequence cache. Every range replica
// maintains a sequence cache, not just the leader.
func NewSequenceCache(rangeID roachpb.RangeID) *SequenceCache {
	return &SequenceCache{
		rangeID: rangeID,
		// The epoch and sequence numbers are encoded in decreasing order.
		min: keys.SequenceCacheKey(rangeID, txnIDMin, math.MaxUint32, math.MaxUint32),
		max: keys.SequenceCacheKey(rangeID, txnIDMax, 0, 0),
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

// ClearData removes all persisted items stored in the cache.
func (sc *SequenceCache) ClearData(e engine.Engine) error {
	_, err := engine.ClearRange(e, engine.MakeMVCCMetadataKey(sc.min), engine.MakeMVCCMetadataKey(sc.max))
	return err
}

// Get looks up the latest sequence number recorded for this transaction ID.
// The latest entry is that with the highest epoch (and then, highest
// sequence). On a miss, zero is returned for both. If an entry is found and a
// SequenceCacheEntry is provided, it is populated from the found value.
func (sc *SequenceCache) Get(e engine.Engine, txnID *uuid.UUID, dest *roachpb.SequenceCacheEntry) (uint32, uint32, error) {
	if txnID == nil {
		return 0, 0, errEmptyTxnID
	}

	// Pull response from disk and read into reply if available. Sequence
	// number sorts in decreasing order, so this gives us the largest entry or
	// an entry which isn't ours. To avoid encoding an end key for the scan,
	// we just scan and check via a simple prefix check whether we read a
	// key for "our" cache id.
	prefix := keys.SequenceCacheKeyPrefix(sc.rangeID, txnID)
	kvs, _, err := engine.MVCCScan(e, prefix, sc.max, 1, /* num */
		roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */)
	if err != nil || len(kvs) == 0 || !bytes.HasPrefix(kvs[0].Key, prefix) {
		return 0, 0, err
	}
	_, epoch, seq, err := keys.DecodeSequenceCacheKey(kvs[0].Key, sc.scratchBuf[:0])
	if err != nil {
		return 0, 0, err
	}
	if dest != nil {
		dest.Reset()
		// Caller wants to have the unmarshaled value.
		if err := kvs[0].Value.GetProto(dest); err != nil {
			return 0, 0, err
		}
	}
	return epoch, seq, nil
}

// GetAllTransactionID returns all the key-value pairs for the given transaction ID from
// the engine.
func (sc *SequenceCache) GetAllTransactionID(e engine.Engine, txnID *uuid.UUID) ([]roachpb.KeyValue, error) {
	prefix := keys.SequenceCacheKeyPrefix(sc.rangeID, txnID)
	kvs, _, err := engine.MVCCScan(e, prefix, prefix.PrefixEnd(), 0, /* max */
		roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */)
	return kvs, err
}

// Iterate walks through the sequence cache, invoking the given callback for
// each unmarshaled entry with the key, the transaction ID and the decoded
// entry.
func (sc *SequenceCache) Iterate(e engine.Engine, f func([]byte, *uuid.UUID, roachpb.SequenceCacheEntry)) {
	_, _ = engine.MVCCIterate(e, sc.min, sc.max, roachpb.ZeroTimestamp,
		true /* consistent */, nil /* txn */, false, /* !reverse */
		func(kv roachpb.KeyValue) (bool, error) {
			var entry roachpb.SequenceCacheEntry
			txnID, _, _, err := keys.DecodeSequenceCacheKey(kv.Key, nil)
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

func copySeqCache(e engine.Engine, ms *engine.MVCCStats, srcID, dstID roachpb.RangeID, keyMin, keyMax engine.MVCCKey) (int, error) {
	var scratch [64]byte
	var count int
	err := e.Iterate(keyMin, keyMax,
		func(kv engine.MVCCKeyValue) (bool, error) {
			// Decode the key into a cmd, skipping on error. Otherwise,
			// write it to the corresponding key in the new cache.
			txnID, epoch, seq, err := decodeSequenceCacheMVCCKey(kv.Key, scratch[:0])
			if err != nil {
				return false, util.Errorf("could not decode a sequence cache key %s: %s",
					kv.Key, err)
			}
			key := keys.SequenceCacheKey(dstID, txnID, epoch, seq)
			encKey := engine.MakeMVCCMetadataKey(key)
			// Decode the value, update the checksum and re-encode.
			meta := &engine.MVCCMetadata{}
			if err := proto.Unmarshal(kv.Value, meta); err != nil {
				return false, util.Errorf("could not decode sequence cache value %s [% x]: %s",
					kv.Key, kv.Value, err)
			}
			value := meta.Value()
			value.ClearChecksum()
			value.InitChecksum(key)
			meta.RawBytes = value.RawBytes

			keyBytes, valBytes, err := engine.PutProto(e, encKey, meta)
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

// CopyInto copies all the results from this sequence cache into the destRangeID
// sequence cache. Failures decoding individual cache entries return an error.
// On success, returns the number of entries (key-value pairs) copied.
func (sc *SequenceCache) CopyInto(e engine.Engine, ms *engine.MVCCStats, destRangeID roachpb.RangeID) (int, error) {
	return copySeqCache(e, ms, sc.rangeID, destRangeID,
		engine.MakeMVCCMetadataKey(sc.min), engine.MakeMVCCMetadataKey(sc.max))
}

// CopyFrom copies all the persisted results from the originRangeID
// sequence cache into this one. Note that the cache will not be
// locked while copying is in progress. Failures decoding individual
// entries return an error. The copy is done directly using the engine
// instead of interpreting values through MVCC for efficiency.
// On success, returns the number of entries (key-value pairs) copied.
func (sc *SequenceCache) CopyFrom(e engine.Engine, ms *engine.MVCCStats, originRangeID roachpb.RangeID) (int, error) {
	originMin := engine.MakeMVCCMetadataKey(
		keys.SequenceCacheKey(originRangeID, txnIDMin, math.MaxUint32, math.MaxUint32))
	originMax := engine.MakeMVCCMetadataKey(
		keys.SequenceCacheKey(originRangeID, txnIDMax, 0, 0))
	return copySeqCache(e, ms, originRangeID, sc.rangeID, originMin, originMax)
}

// Del removes all sequence cache entries for the given transaction.
func (sc *SequenceCache) Del(e engine.Engine, ms *engine.MVCCStats, txnID *uuid.UUID) error {
	startKey := keys.SequenceCacheKeyPrefix(sc.rangeID, txnID)
	_, err := engine.MVCCDeleteRange(e, ms, startKey, startKey.PrefixEnd(), 0 /* max */, roachpb.ZeroTimestamp, nil /* txn */, false /*returnKeys*/)
	return err
}

// Put writes a sequence number for the specified transaction ID.
func (sc *SequenceCache) Put(e engine.Engine, ms *engine.MVCCStats, txnID *uuid.UUID, epoch, seq uint32, txnKey roachpb.Key, txnTS roachpb.Timestamp, pErr *roachpb.Error) error {
	if seq <= 0 || txnID == nil {
		return errEmptyTxnID
	}
	if !sc.shouldCacheError(pErr) {
		return nil
	}

	// Write the response value to the engine.
	key := keys.SequenceCacheKey(sc.rangeID, txnID, epoch, seq)
	sc.scratchEntry = roachpb.SequenceCacheEntry{Key: txnKey, Timestamp: txnTS}
	return engine.MVCCPutProto(e, ms, key, roachpb.ZeroTimestamp, nil /* txn */, &sc.scratchEntry)
}

// Responses with write-intent and not leader errors are retried on
// the server, and so are not recorded in the sequence cache in the
// hopes of retrying to a successful outcome.
func (sc *SequenceCache) shouldCacheError(pErr *roachpb.Error) bool {
	switch pErr.GetDetail().(type) {
	case *roachpb.WriteIntentError, *roachpb.NotLeaderError, *roachpb.RangeKeyMismatchError:
		return false
	}
	return true
}

func decodeSequenceCacheMVCCKey(encKey engine.MVCCKey, dest []byte) (*uuid.UUID, uint32, uint32, error) {
	if encKey.IsValue() {
		return nil, 0, 0, util.Errorf("key %s is not a raw MVCC value", encKey)
	}
	return keys.DecodeSequenceCacheKey(encKey.Key, dest)
}
