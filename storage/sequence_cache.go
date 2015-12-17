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
	"bytes"
	"errors"
	"math"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/gogo/protobuf/proto"
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

var txnIDMin = bytes.Repeat([]byte{'\x00'}, roachpb.TransactionIDLen)
var txnIDMax = bytes.Repeat([]byte{'\xff'}, roachpb.TransactionIDLen)

// ClearData removes all persisted items stored in the cache.
func (sc *SequenceCache) ClearData(e engine.Engine) error {
	_, err := engine.ClearRange(e, engine.MakeMVCCMetadataKey(sc.min), engine.MakeMVCCMetadataKey(sc.max))
	return err
}

// Get looks up the latest sequence number recorded for this transaction ID.
// The latest entry is that with the highest epoch (and then, highest
// sequence). On a miss, zero is returned for both. If an entry is found and a
// SequenceCacheEntry is provided, it is populated from the found value.
func (sc *SequenceCache) Get(e engine.Engine, id []byte, dest *roachpb.SequenceCacheEntry) (uint32, uint32, error) {
	if len(id) == 0 {
		return 0, 0, errEmptyTxnID
	}

	// Pull response from disk and read into reply if available. Sequence
	// number sorts in decreasing order, so this gives us the largest entry or
	// an entry which isn't ours. To avoid encoding an end key for the scan,
	// we just scan and check via a simple prefix check whether we read a
	// key for "our" cache id.
	prefix := keys.SequenceCacheKeyPrefix(sc.rangeID, id)
	kvs, _, err := engine.MVCCScan(e, prefix, sc.max, 1, /* num */
		roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */)
	if err != nil || len(kvs) == 0 || !bytes.HasPrefix(kvs[0].Key, prefix) {
		return 0, 0, err
	}
	_, epoch, seq, err := decodeSequenceCacheKey(kvs[0].Key, sc.scratchBuf[:0])
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
func (sc *SequenceCache) GetAllTransactionID(e engine.Engine, id []byte) ([]roachpb.KeyValue, error) {
	prefix := keys.SequenceCacheKeyPrefix(sc.rangeID, id)
	kvs, _, err := engine.MVCCScan(e, prefix, prefix.PrefixEnd(), 0, /* max */
		roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */)
	return kvs, err
}

// Iterate walks through the sequence cache, invoking the given callback for
// each unmarshaled entry with the key, the transaction ID and the decoded
// entry.
func (sc *SequenceCache) Iterate(e engine.Engine, f func([]byte, []byte, roachpb.SequenceCacheEntry)) {
	_, _ = engine.MVCCIterate(e, sc.min, sc.max, roachpb.ZeroTimestamp,
		true /* consistent */, nil /* txn */, false, /* !reverse */
		func(kv roachpb.KeyValue) (bool, error) {
			var entry roachpb.SequenceCacheEntry
			id, _, _, err := decodeSequenceCacheKey(kv.Key, nil)
			if err != nil {
				panic(err) // TODO(tschottdorf): ReplicaCorruptionError
			}
			if err := kv.Value.GetProto(&entry); err != nil {
				panic(err) // TODO(tschottdorf): ReplicaCorruptionError
			}
			f(kv.Key, id, entry)
			return false, nil
		})
}

func copySeqCache(e engine.Engine, srcID, dstID roachpb.RangeID, keyMin, keyMax engine.MVCCKey) error {
	var scratch [64]byte
	return e.Iterate(keyMin, keyMax,
		func(kv engine.MVCCKeyValue) (bool, error) {
			// Decode the key into a cmd, skipping on error. Otherwise,
			// write it to the corresponding key in the new cache.
			id, epoch, seq, err := decodeSequenceCacheMVCCKey(kv.Key, scratch[:0])
			if err != nil {
				return false, util.Errorf("could not decode a sequence cache key %s: %s",
					kv.Key, err)
			}
			key := keys.SequenceCacheKey(dstID, id, epoch, seq)
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
			_, _, err = engine.PutProto(e, encKey, meta)
			return false, err
		})
}

// CopyInto copies all the results from this sequence cache into the destRangeID
// sequence cache. Failures decoding individual cache entries return an error.
func (sc *SequenceCache) CopyInto(e engine.Engine, destRangeID roachpb.RangeID) error {
	return copySeqCache(e, sc.rangeID, destRangeID,
		engine.MakeMVCCMetadataKey(sc.min), engine.MakeMVCCMetadataKey(sc.max))
}

// CopyFrom copies all the persisted results from the originRangeID
// sequence cache into this one. Note that the cache will not be
// locked while copying is in progress. Failures decoding individual
// entries return an error. The copy is done directly using the engine
// instead of interpreting values through MVCC for efficiency.
func (sc *SequenceCache) CopyFrom(e engine.Engine, originRangeID roachpb.RangeID) error {
	originMin := engine.MakeMVCCMetadataKey(
		keys.SequenceCacheKey(originRangeID, txnIDMin, math.MaxUint32, math.MaxUint32))
	originMax := engine.MakeMVCCMetadataKey(
		keys.SequenceCacheKey(originRangeID, txnIDMax, 0, 0))
	return copySeqCache(e, originRangeID, sc.rangeID, originMin, originMax)
}

// Put writes a sequence number for the specified transaction ID.
func (sc *SequenceCache) Put(e engine.Engine, id []byte, epoch, seq uint32, txnKey roachpb.Key, txnTS roachpb.Timestamp, err error) error {
	if seq <= 0 || len(id) == 0 {
		return errEmptyTxnID
	}
	if !sc.shouldCacheError(err) {
		return nil
	}

	// Write the response value to the engine.
	key := keys.SequenceCacheKey(sc.rangeID, id, epoch, seq)
	sc.scratchEntry = roachpb.SequenceCacheEntry{Key: txnKey, Timestamp: txnTS}
	return engine.MVCCPutProto(e, nil /* ms */, key, roachpb.ZeroTimestamp, nil /* txn */, &sc.scratchEntry)
}

// Responses with write-too-old, write-intent and not leader errors
// are retried on the server, and so are not recorded in the sequence
// cache in the hopes of retrying to a successful outcome.
func (sc *SequenceCache) shouldCacheError(err error) bool {
	switch err.(type) {
	case *roachpb.WriteTooOldError, *roachpb.WriteIntentError, *roachpb.NotLeaderError, *roachpb.RangeKeyMismatchError:
		return false
	}
	return true
}

func decodeSequenceCacheKey(key roachpb.Key, dest []byte) ([]byte, uint32, uint32, error) {
	// TODO(tschottdorf): redundant check.
	if !bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return nil, 0, 0, util.Errorf("key %s does not have %s prefix", key, keys.LocalRangeIDPrefix)
	}
	// Cut the prefix and the Range ID.
	b := key[len(keys.LocalRangeIDPrefix):]
	b, _, err := encoding.DecodeUvarint(b)
	if err != nil {
		return nil, 0, 0, err
	}
	if !bytes.HasPrefix(b, keys.LocalSequenceCacheSuffix) {
		return nil, 0, 0, util.Errorf("key %s does not contain the sequence cache suffix %s",
			key, keys.LocalSequenceCacheSuffix)
	}
	// Cut the sequence cache suffix.
	b = b[len(keys.LocalSequenceCacheSuffix):]
	// Decode the id.
	b, id, err := encoding.DecodeBytes(b, dest)
	if err != nil {
		return nil, 0, 0, err
	}
	// Decode the epoch.
	b, epoch, err := encoding.DecodeUint32Decreasing(b)
	if err != nil {
		return nil, 0, 0, err
	}
	// Decode the sequence number.
	b, seq, err := encoding.DecodeUint32Decreasing(b)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(b) > 0 {
		return nil, 0, 0, util.Errorf("key %q has leftover bytes after decode: %s; indicates corrupt key",
			key, b)
	}
	return id, epoch, seq, nil
}

func decodeSequenceCacheMVCCKey(encKey engine.MVCCKey, dest []byte) ([]byte, uint32, uint32, error) {
	if encKey.IsValue() {
		return nil, 0, 0, util.Errorf("key %s is not a raw MVCC value", encKey)
	}
	return decodeSequenceCacheKey(encKey.Key, dest)
}
