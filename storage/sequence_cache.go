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
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/gogo/protobuf/proto"
)

var errEmptyID = errors.New("empty CommandID used in sequence cache")

// The SequenceCache provides idempotence for request retries. Each
// transactional request to a range specifies an ID and sequence number
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
	rangeID  roachpb.RangeID
	min, max roachpb.Key
}

// NewSequenceCache returns a new sequence cache. Every range replica
// maintains a sequence cache, not just the leader.
func NewSequenceCache(rangeID roachpb.RangeID) *SequenceCache {
	seqCacheKeyMin, _ := keys.SequenceCacheKey(rangeID, uuidMin, math.MaxUint32)
	seqCacheKeyMax, _ := keys.SequenceCacheKey(rangeID, uuidMax, 0)

	return &SequenceCache{
		rangeID: rangeID,
		min:     seqCacheKeyMin,
		max:     seqCacheKeyMax,
	}
}

// TODO(tschottdorf): should have a constant for this length somewhere.
var uuidMin = bytes.Repeat([]byte{'\x00'}, len(uuid.NewUUID4()))
var uuidMax = bytes.Repeat([]byte{'\xff'}, len(uuid.NewUUID4()))

// ClearData removes all persisted items stored in the persistent.
func (rc *SequenceCache) ClearData(e engine.Engine) error {
	// The sequence number is encoded in decreasing order. We could get away
	// with zero due to the fact that UUIDs always contain a '4', but let's
	// not.
	_, err := engine.ClearRange(e, engine.MVCCEncodeKey(rc.min), engine.MVCCEncodeKey(rc.max))
	return err
}

// GetSequence looks up the latest sequence number recorded for this id. On a
// miss, zero is returned.
func (rc *SequenceCache) GetSequence(e engine.Engine, id []byte) (uint32, error) {
	if len(id) == 0 {
		return 0, errEmptyID
	}

	// Pull response from disk and read into reply if available. Sequence
	// number sorts in decreasing order, so this gives us the largest entry or
	// an entry which isn't our. To avoid encoding an end key for the scan,
	// we just scan and check via a simple prefix check whether we read a
	// key for "our" cache id.
	key, prefixLen := keys.SequenceCacheKey(rc.rangeID, id, math.MaxUint32)
	kvs, _, err := engine.MVCCScan(e, key, rc.max, 1, /* num */
		roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */)
	if err != nil || len(kvs) == 0 || len(kvs[0].Key) < prefixLen || !bytes.Equal(key[:prefixLen], kvs[0].Key[:prefixLen]) {
		return 0, err
	}
	key = kvs[0].Key
	_, seq, err := rc.decodeKey(key)
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// CopyInto copies all the results from this sequence cache into the destRangeID
// sequence cache. Failures decoding individual cache entries return an error.
func (rc *SequenceCache) CopyInto(e engine.Engine, destRangeID roachpb.RangeID) error {
	return e.Iterate(engine.MVCCEncodeKey(rc.min), engine.MVCCEncodeKey(rc.max),
		func(kv engine.MVCCKeyValue) (bool, error) {
			// Decode the key into a cmd, skipping on error. Otherwise,
			// write it to the corresponding key in the new cache.
			id, seq, err := rc.decodeMVCCKey(kv.Key)
			if err != nil {
				return false, util.Errorf("could not decode a sequence cache key %s: %s",
					roachpb.Key(kv.Key), err)
			}
			key, _ := keys.SequenceCacheKey(destRangeID, id, seq)
			encKey := engine.MVCCEncodeKey(key)
			// Decode the value, update the checksum and re-encode.
			meta := &engine.MVCCMetadata{}
			if err := proto.Unmarshal(kv.Value, meta); err != nil {
				return false, util.Errorf("could not decode sequence cache value %s [% x]: %s",
					roachpb.Key(kv.Key), kv.Value, err)
			}
			meta.Value.Checksum = nil
			meta.Value.InitChecksum(key)
			_, _, err = engine.PutProto(e, encKey, meta)
			return false, err
		})
}

// CopyFrom copies all the persisted results from the originRangeID
// sequence cache into this one. Note that the cache will not be
// locked while copying is in progress. Failures decoding individual
// entries return an error. The copy is done directly using the engine
// instead of interpreting values through MVCC for efficiency.
func (rc *SequenceCache) CopyFrom(e engine.Engine, originRangeID roachpb.RangeID) error {
	originMin, _ := keys.SequenceCacheKey(originRangeID, uuidMin, math.MaxUint32)
	originMax, _ := keys.SequenceCacheKey(originRangeID, uuidMax, 0)

	return e.Iterate(engine.MVCCEncodeKey(originMin), engine.MVCCEncodeKey(originMax),
		func(kv engine.MVCCKeyValue) (bool, error) {
			// Decode the key into a cmd, skipping on error. Otherwise,
			// write it to the corresponding key in the new cache.
			id, seq, err := rc.decodeMVCCKey(kv.Key)
			if err != nil {
				return false, util.Errorf("could not decode a sequence cache key %s: %s",
					roachpb.Key(kv.Key), err)
			}
			key, _ := keys.SequenceCacheKey(rc.rangeID, id, seq)
			encKey := engine.MVCCEncodeKey(key)
			// Decode the value, update the checksum and re-encode.
			meta := &engine.MVCCMetadata{}
			if err := proto.Unmarshal(kv.Value, meta); err != nil {
				return false, util.Errorf("could not decode sequence cache value %s [% x]: %s",
					roachpb.Key(kv.Key), kv.Value, err)
			}
			meta.Value.Checksum = nil
			meta.Value.InitChecksum(key)
			_, _, err = engine.PutProto(e, encKey, meta)
			return false, err
		})
}

// PutSequence writes a sequence number for the specified id.
func (rc *SequenceCache) PutSequence(e engine.Engine, id []byte, seq uint32, err error) error {
	if seq <= 0 || len(id) == 0 {
		return errEmptyID
	}
	if !rc.shouldCacheError(err) {
		return nil
	}

	// Write the response value to the engine.
	key, _ := keys.SequenceCacheKey(rc.rangeID, id, seq)
	v := roachpb.MakeValueFromBytes(nil) // TODO(tschottdorf)
	return engine.MVCCPut(e, nil /* ms */, key, roachpb.ZeroTimestamp, v, nil /* txn */)
}

// Responses with write-too-old, write-intent and not leader errors
// are retried on the server, and so are not recorded in the sequence
// cache in the hopes of retrying to a successful outcome.
func (rc *SequenceCache) shouldCacheError(err error) bool {
	switch err.(type) {
	case *roachpb.WriteTooOldError, *roachpb.WriteIntentError, *roachpb.NotLeaderError:
		return false
	}
	return true
}

func (rc *SequenceCache) decodeKey(key roachpb.Key) ([]byte, uint32, error) {
	// TODO(tschottdorf): redundant check.
	if !bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return nil, 0, util.Errorf("key %s does not have %s prefix", key, keys.LocalRangeIDPrefix)
	}
	// Cut the prefix and the Range ID.
	b := key[len(keys.LocalRangeIDPrefix):]
	b, _, err := encoding.DecodeUvarint(b)
	if err != nil {
		return nil, 0, err
	}
	if !bytes.HasPrefix(b, keys.LocalSequenceCacheSuffix) {
		return nil, 0, util.Errorf("key %s does not contain the sequence cache suffix %s",
			key, keys.LocalSequenceCacheSuffix)
	}
	// Cut the sequence cache suffix.
	b = b[len(keys.LocalSequenceCacheSuffix):]
	// Decode the id.
	b, id, err := encoding.DecodeBytes(b, nil)
	if err != nil {
		return nil, 0, err
	}
	// Decode the sequence number.
	b, seq, err := encoding.DecodeUint32Decreasing(b)
	if err != nil {
		return nil, 0, err
	}
	if len(b) > 0 {
		return nil, 0, util.Errorf("key %q has leftover bytes after decode: %s; indicates corrupt key",
			key, b)
	}
	return id, seq, nil
}

func (rc *SequenceCache) decodeMVCCKey(encKey engine.MVCCKey) ([]byte, uint32, error) {
	key, _, isValue, err := engine.MVCCDecodeKey(encKey)
	if err != nil {
		return nil, 0, err
	}
	if isValue {
		return nil, 0, util.Errorf("key %s is not a raw MVCC value", encKey)
	}
	return rc.decodeKey(key)
}
