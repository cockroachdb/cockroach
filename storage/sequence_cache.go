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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
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
	rangeID roachpb.RangeID
}

// NewSequenceCache returns a new sequence cache. Every range replica
// maintains a sequence cache, not just the leader.
func NewSequenceCache(rangeID roachpb.RangeID) *SequenceCache {
	return &SequenceCache{
		rangeID: rangeID,
	}
}

// ClearData removes all persisted items stored in the persistent.
func (rc *SequenceCache) ClearData(e engine.Engine) error {
	from := keys.SequenceCacheKey(rc.rangeID, roachpb.KeyMin)
	to := keys.SequenceCacheKey(rc.rangeID, roachpb.KeyMax)
	_, err := engine.ClearRange(e, engine.MVCCEncodeKey(from), engine.MVCCEncodeKey(to))
	return err
}

// GetSequence looks up the latest sequence number recorded for this family. On a
// miss, zero is returned.
func (rc *SequenceCache) GetSequence(e engine.Engine, family []byte) (int64, error) {
	if len(family) == 0 {
		return 0, errEmptyID
	}

	// Pull response from disk and read into reply if available.
	key := keys.SequenceCacheKey(rc.rangeID, family)
	v, _, err := engine.MVCCGet(e, key, roachpb.ZeroTimestamp, true, nil)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	return v.GetInt()
}

// CopyInto copies all the results from this sequence cache into the destRangeID
// sequence cache. Failures decoding individual cache entries return an error.
func (rc *SequenceCache) CopyInto(e engine.Engine, destRangeID roachpb.RangeID) error {
	start := engine.MVCCEncodeKey(
		keys.SequenceCacheKey(rc.rangeID, roachpb.KeyMin))
	end := engine.MVCCEncodeKey(
		keys.SequenceCacheKey(rc.rangeID, roachpb.KeyMax))

	return e.Iterate(start, end, func(kv engine.MVCCKeyValue) (bool, error) {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		family, err := rc.decodeSequenceCacheKey(kv.Key)
		if err != nil {
			return false, util.Errorf("could not decode a sequence cache key %s: %s",
				roachpb.Key(kv.Key), err)
		}
		key := keys.SequenceCacheKey(destRangeID, family)
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
	start := engine.MVCCEncodeKey(
		keys.SequenceCacheKey(originRangeID, roachpb.KeyMin))
	end := engine.MVCCEncodeKey(
		keys.SequenceCacheKey(originRangeID, roachpb.KeyMax))

	return e.Iterate(start, end, func(kv engine.MVCCKeyValue) (bool, error) {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		family, err := rc.decodeSequenceCacheKey(kv.Key)
		if err != nil {
			return false, util.Errorf("could not decode a sequence cache key %s: %s",
				roachpb.Key(kv.Key), err)
		}
		key := keys.SequenceCacheKey(rc.rangeID, family)
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

// PutSequence writes a sequence number for the specified family.
func (rc *SequenceCache) PutSequence(e engine.Engine, family []byte, sequence int64, err error) error {
	if sequence <= 0 || len(family) == 0 {
		return errEmptyID
	}
	if !rc.shouldCacheError(err) {
		return nil
	}

	// Write the response value to the engine.
	key := keys.SequenceCacheKey(rc.rangeID, family)
	var v roachpb.Value
	v.SetInt(sequence)
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

func (rc *SequenceCache) decodeSequenceCacheKey(encKey engine.MVCCKey) ([]byte, error) {
	key, _, isValue, err := engine.MVCCDecodeKey(encKey)
	if err != nil {
		return nil, err
	}
	if isValue {
		return nil, util.Errorf("key %s is not a raw MVCC value", encKey)
	}
	if !bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return nil, util.Errorf("key %s does not have %s prefix", key, keys.LocalRangeIDPrefix)
	}
	// Cut the prefix and the Range ID.
	b := key[len(keys.LocalRangeIDPrefix):]
	b, _, err = encoding.DecodeUvarint(b)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(b, keys.LocalSequenceCacheSuffix) {
		return nil, util.Errorf("key %s does not contain the sequence cache suffix %s",
			key, keys.LocalSequenceCacheSuffix)
	}
	// Cut the sequence cache suffix.
	b = b[len(keys.LocalSequenceCacheSuffix):]
	// Decode the family.
	b, fm, err := encoding.DecodeBytes(b, nil)
	if err != nil {
		return nil, err
	}
	if len(b) > 0 {
		return nil, util.Errorf("key %s has leftover bytes after decode: %s; indicates corrupt key",
			encKey, b)
	}
	return fm, nil
}
