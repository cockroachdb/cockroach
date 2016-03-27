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
var errNoCacheEntry = errors.New("no sequence cache entry for Transaction ID")

// The SequenceCache provides idempotence for request retries. Each
// transactional request to a range specifies a Transaction ID and
// sequence number which uniquely identifies a client command. After
// commands have been replicated via Raft, they are executed against
// the state machine and the results are stored in the SequenceCache.
//
// The SequenceCache stores responses in the underlying engine, using
// keys derived from Range ID, txn ID and sequence number.
//
// A SequenceCache is not thread safe. Access to it is serialized
// through Raft.
type SequenceCache struct {
	rangeID roachpb.RangeID
}

// NewSequenceCache returns a new sequence cache. Every range replica
// maintains a sequence cache, not just the leader.
func NewSequenceCache(rangeID roachpb.RangeID) *SequenceCache {
	return &SequenceCache{
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

func (sc *SequenceCache) min() roachpb.Key {
	return keys.SequenceCacheKey(sc.rangeID, txnIDMin)
}

func (sc *SequenceCache) max() roachpb.Key {
	return keys.SequenceCacheKey(sc.rangeID, txnIDMax)
}

// ClearData removes all persisted items stored in the cache.
func (sc *SequenceCache) ClearData(e engine.Engine) error {
	_, err := engine.ClearRange(e, engine.MakeMVCCMetadataKey(sc.min()), engine.MakeMVCCMetadataKey(sc.max()))
	return err
}

// Get looks up the latest sequence cache entry recorded for this
// transaction ID as a raw value. The latest entry is that with the
// highest epoch. On a miss, returns errNoCacheEntry.
func (sc *SequenceCache) Get(e engine.Engine, txnID *uuid.UUID, entry *roachpb.SequenceCacheEntry) error {
	if txnID == nil {
		return errEmptyTxnID
	}

	// Pull response from disk and read into reply if available. Sequence
	// number sorts in decreasing order, so this gives us the largest entry or
	// an entry which isn't ours. To avoid encoding an end key for the scan,
	// we just scan and check via a simple prefix check whether we read a
	// key for "our" cache id.
	key := keys.SequenceCacheKey(sc.rangeID, txnID)
	value, _, err := engine.MVCCGet(e, key, roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */)
	if err != nil {
		return err
	} else if value == nil {
		return errNoCacheEntry
	}
	if entry != nil {
		entry.Reset()
		// Caller wants to have the unmarshaled value.
		if err := value.GetProto(entry); err != nil {
			return err
		}
	}
	return nil
}

// Iterate walks through the sequence cache, invoking the given callback for
// each unmarshaled entry with the key, the transaction ID and the decoded
// entry.
func (sc *SequenceCache) Iterate(
	e engine.Engine, f func([]byte, *uuid.UUID, roachpb.SequenceCacheEntry),
) {
	_, _ = engine.MVCCIterate(e, sc.min(), sc.max(), roachpb.ZeroTimestamp,
		true /* consistent */, nil /* txn */, false, /* !reverse */
		func(kv roachpb.KeyValue) (bool, error) {
			var entry roachpb.SequenceCacheEntry
			txnID, err := keys.DecodeSequenceCacheKey(kv.Key, nil)
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
	var value roachpb.Value
	var entry, exEntry roachpb.SequenceCacheEntry
	var meta, exMeta engine.MVCCMetadata
	err := e.Iterate(keyMin, keyMax,
		func(kv engine.MVCCKeyValue) (bool, error) {
			// Decode the key into a cmd, skipping on error. Otherwise,
			// write it to the corresponding key in the new cache.
			txnID, err := decodeSequenceCacheMVCCKey(kv.Key, scratch[:0])
			if err != nil {
				return false, util.Errorf("could not decode a sequence cache key %s: %s", kv.Key, err)
			}
			key := keys.SequenceCacheKey(dstID, txnID)
			encKey := engine.MakeMVCCMetadataKey(key)
			// Decode the MVCCMetadata value.
			if err := proto.Unmarshal(kv.Value, &meta); err != nil {
				return false, util.Errorf("could not decode mvcc metadata %s [% x]: %s", kv.Key, kv.Value, err)
			}
			// Check whether there's already a sequence cache entry for the
			// same txn ID. If so, we must unmarshal both values to construct
			// a replacement value with the highest epoch and sequence number.
			// This is a rare code path.
			exists, exKeyBytes, exValBytes, err := e.GetProto(encKey, &exMeta)
			if err != nil {
				return false, util.Errorf("could not decode existing mvcc metadata %s: %s", key, err)
			}
			// If it does exist, first subtract the existing bytes as we're
			// going to replace with new bytes below. The decode, construct
			// new cache entry, and re-encode.
			if exists {
				if ms != nil {
					ms.SysBytes -= (exKeyBytes + exValBytes)
					ms.SysCount--
				}
				if err := exMeta.Value().GetProto(&exEntry); err != nil {
					return false, util.Errorf("could not decode existing sequence cache entry %s: %s", key, err)
				}
				if err := meta.Value().GetProto(&entry); err != nil {
					return false, util.Errorf("could not decode sequence cache entry %s: %s", kv.Key, err)
				}
				if exEntry.Epoch > entry.Epoch || exEntry.Sequence > entry.Sequence {
					entry = exEntry
				}
				if err := value.SetProto(&entry); err != nil {
					return false, util.Errorf("could not encode sequence cache entry %+v: %s", entry, err)
				}
			} else {
				value = meta.Value()
			}

			// Update the checksum and re-encode.
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

// CopyInto copies all the results from this sequence cache into the destRangeID
// sequence cache. Failures decoding individual cache entries return an error.
// On success, returns the number of entries (key-value pairs) copied.
func (sc *SequenceCache) CopyInto(e engine.Engine, ms *engine.MVCCStats, destRangeID roachpb.RangeID) (int, error) {
	return copySeqCache(e, ms, sc.rangeID, destRangeID,
		engine.MakeMVCCMetadataKey(sc.min()), engine.MakeMVCCMetadataKey(sc.max()))
}

// CopyFrom copies all the persisted results from the originRangeID
// sequence cache into this one. Note that the cache will not be
// locked while copying is in progress. Failures decoding individual
// entries return an error. The copy is done directly using the engine
// instead of interpreting values through MVCC for efficiency.
// On success, returns the number of entries (key-value pairs) copied.
func (sc *SequenceCache) CopyFrom(e engine.Engine, ms *engine.MVCCStats, originRangeID roachpb.RangeID) (int, error) {
	originMin := engine.MakeMVCCMetadataKey(keys.SequenceCacheKey(originRangeID, txnIDMin))
	originMax := engine.MakeMVCCMetadataKey(keys.SequenceCacheKey(originRangeID, txnIDMax))
	return copySeqCache(e, ms, originRangeID, sc.rangeID, originMin, originMax)
}

// Del removes all sequence cache entries for the given transaction.
func (sc *SequenceCache) Del(e engine.Engine, ms *engine.MVCCStats, txnID *uuid.UUID) error {
	key := keys.SequenceCacheKey(sc.rangeID, txnID)
	return engine.MVCCDelete(e, ms, key, roachpb.ZeroTimestamp, nil /* txn */)
}

// Put writes a sequence number for the specified transaction ID.
func (sc *SequenceCache) Put(
	e engine.Engine,
	ms *engine.MVCCStats,
	txnID *uuid.UUID,
	entry *roachpb.SequenceCacheEntry,
	pErr *roachpb.Error,
) error {
	if entry.Sequence <= 0 || txnID == nil {
		return errEmptyTxnID
	}
	if !sc.shouldCacheError(pErr) {
		return nil
	}

	// Write the response value to the engine.
	key := keys.SequenceCacheKey(sc.rangeID, txnID)
	return engine.MVCCPutProto(e, ms, key, roachpb.ZeroTimestamp, nil /* txn */, entry)
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

func decodeSequenceCacheMVCCKey(encKey engine.MVCCKey, dest []byte) (*uuid.UUID, error) {
	if encKey.IsValue() {
		return nil, util.Errorf("key %s is not a raw MVCC value", encKey)
	}
	return keys.DecodeSequenceCacheKey(encKey.Key, dest)
}
