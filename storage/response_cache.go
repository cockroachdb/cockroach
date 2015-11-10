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

type cmdIDKey string

var errEmptyCmdID = errors.New("empty CommandID used in response cache")

func makeCmdIDKey(cmdID roachpb.ClientCmdID) cmdIDKey {
	buf := make([]byte, 0, 16)
	buf = encoding.EncodeUint64(buf, uint64(cmdID.WallTime))
	buf = encoding.EncodeUint64(buf, uint64(cmdID.Random))
	return cmdIDKey(string(buf))
}

// A ResponseCache provides idempotence for request retries. Each
// request to a range specifies a ClientCmdID in the request header
// which uniquely identifies a client command. After commands have
// been replicated via Raft, they are executed against the state
// machine and the results are stored in the ResponseCache.
//
// The ResponseCache stores responses in the underlying engine, using
// keys derived from the Range ID and the ClientCmdID.
//
// A ResponseCache is not thread safe. Access to it is serialized
// through Raft.
type ResponseCache struct {
	rangeID roachpb.RangeID
}

// NewResponseCache returns a new response cache. Every range replica
// maintains a response cache, not just the leader. However, when a
// replica loses or gains leadership of the Raft consensus group, the
// inflight map should be cleared.
func NewResponseCache(rangeID roachpb.RangeID) *ResponseCache {
	return &ResponseCache{
		rangeID: rangeID,
	}
}

// ClearData removes all items stored in the persistent cache. It does not alter
// the inflight map.
func (rc *ResponseCache) ClearData(e engine.Engine) error {
	from := keys.ResponseCacheKey(rc.rangeID, roachpb.KeyMin)
	to := keys.ResponseCacheKey(rc.rangeID, roachpb.KeyMax)
	_, err := engine.ClearRange(e, engine.MVCCEncodeKey(from), engine.MVCCEncodeKey(to))
	return err
}

// GetResponse looks up a response matching the specified cmdID. If the
// response is found, it is returned along with its associated error.
// If the response is not found, nil is returned for both the response
// and its error. In all cases, the third return value is the error
// returned from the engine when reading the on-disk cache.
func (rc *ResponseCache) GetResponse(e engine.Engine, family []byte) (int64, error) {
	if len(family) == 0 {
		return 0, errEmptyCmdID
	}

	// Pull response from the cache and read into reply if available.
	key := keys.ResponseCacheKey(rc.rangeID, family)
	v, _, err := engine.MVCCGet(e, key, roachpb.ZeroTimestamp, true, nil)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	return v.GetInt()
}

// CopyInto copies all the cached results from this response cache
// into the destRangeID response cache. Failures decoding individual
// cache entries return an error.
func (rc *ResponseCache) CopyInto(e engine.Engine, destRangeID roachpb.RangeID) error {
	start := engine.MVCCEncodeKey(
		keys.ResponseCacheKey(rc.rangeID, roachpb.KeyMin))
	end := engine.MVCCEncodeKey(
		keys.ResponseCacheKey(rc.rangeID, roachpb.KeyMax))

	return e.Iterate(start, end, func(kv roachpb.RawKeyValue) (bool, error) {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		family, err := rc.decodeResponseCacheKey(kv.Key)
		if err != nil {
			return false, util.Errorf("could not decode a response cache key %s: %s",
				roachpb.Key(kv.Key), err)
		}
		key := keys.ResponseCacheKey(destRangeID, family)
		encKey := engine.MVCCEncodeKey(key)
		// Decode the value, update the checksum and re-encode.
		meta := &engine.MVCCMetadata{}
		if err := proto.Unmarshal(kv.Value, meta); err != nil {
			return false, util.Errorf("could not decode response cache value %s [% x]: %s",
				roachpb.Key(kv.Key), kv.Value, err)
		}
		meta.Value.Checksum = nil
		meta.Value.InitChecksum(key)
		_, _, err = engine.PutProto(e, encKey, meta)
		return false, err
	})
}

// CopyFrom copies all the cached results from the originRangeID
// response cache into this one. Note that the cache will not be
// locked while copying is in progress. Failures decoding individual
// cache entries return an error. The copy is done directly using the
// engine instead of interpreting values through MVCC for efficiency.
func (rc *ResponseCache) CopyFrom(e engine.Engine, originRangeID roachpb.RangeID) error {
	start := engine.MVCCEncodeKey(
		keys.ResponseCacheKey(originRangeID, roachpb.KeyMin))
	end := engine.MVCCEncodeKey(
		keys.ResponseCacheKey(originRangeID, roachpb.KeyMax))

	return e.Iterate(start, end, func(kv roachpb.RawKeyValue) (bool, error) {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		family, err := rc.decodeResponseCacheKey(kv.Key)
		if err != nil {
			return false, util.Errorf("could not decode a response cache key %s: %s",
				roachpb.Key(kv.Key), err)
		}
		key := keys.ResponseCacheKey(rc.rangeID, family)
		encKey := engine.MVCCEncodeKey(key)
		// Decode the value, update the checksum and re-encode.
		meta := &engine.MVCCMetadata{}
		if err := proto.Unmarshal(kv.Value, meta); err != nil {
			return false, util.Errorf("could not decode response cache value %s [% x]: %s",
				roachpb.Key(kv.Key), kv.Value, err)
		}
		meta.Value.Checksum = nil
		meta.Value.InitChecksum(key)
		_, _, err = engine.PutProto(e, encKey, meta)
		return false, err
	})
}

// PutResponse writes a response or a error associated with it to the
// cache for the specified cmdID.
func (rc *ResponseCache) PutResponse(e engine.Engine, family []byte, sequence int64, err error) error {
	if sequence <= 0 || len(family) == 0 {
		return errEmptyCmdID
	}
	if !rc.shouldCacheError(err) {
		return nil
	}

	// Write the response value to the engine.
	key := keys.ResponseCacheKey(rc.rangeID, family)
	var v roachpb.Value
	v.SetInt(sequence)
	return engine.MVCCPut(e, nil /* ms */, key, roachpb.ZeroTimestamp, v, nil /* txn */)
}

// Responses with write-too-old, write-intent and not leader errors
// are retried on the server, and so are not recorded in the response
// cache in the hopes of retrying to a successful outcome.
func (rc *ResponseCache) shouldCacheError(err error) bool {
	switch err.(type) {
	case *roachpb.WriteTooOldError, *roachpb.WriteIntentError, *roachpb.NotLeaderError:
		return false
	}
	return true
}

func (rc *ResponseCache) decodeResponseCacheKey(encKey roachpb.EncodedKey) ([]byte, error) {
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
	if !bytes.HasPrefix(b, keys.LocalResponseCacheSuffix) {
		return nil, util.Errorf("key %s does not contain the response cache suffix %s",
			key, keys.LocalResponseCacheSuffix)
	}
	// Cut the response cache suffix.
	b = b[len(keys.LocalResponseCacheSuffix):]
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
