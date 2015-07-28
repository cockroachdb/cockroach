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
	"fmt"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	gogoproto "github.com/gogo/protobuf/proto"
)

type cmdIDKey string

func makeCmdIDKey(cmdID proto.ClientCmdID) cmdIDKey {
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
// keys derived from the Raft ID and the ClientCmdID.
//
// A ResponseCache is not thread safe. Access to it is serialized
// through Raft.
type ResponseCache struct {
	raftID proto.RaftID
}

// NewResponseCache returns a new response cache. Every range replica
// maintains a response cache, not just the leader. However, when a
// replica loses or gains leadership of the Raft consensus group, the
// inflight map should be cleared.
func NewResponseCache(raftID proto.RaftID) *ResponseCache {
	return &ResponseCache{
		raftID: raftID,
	}
}

// ClearData removes all items stored in the persistent cache. It does not alter
// the inflight map.
func (rc *ResponseCache) ClearData(e engine.Engine) error {
	p := keys.ResponseCacheKey(rc.raftID, nil) // prefix for all response cache entries with this raft ID
	end := p.PrefixEnd()
	_, err := engine.ClearRange(e, engine.MVCCEncodeKey(p), engine.MVCCEncodeKey(end))
	return err
}

// GetResponse looks up a response matching the specified cmdID. If the
// response is found, it is returned along with its associated error.
// If the response is not found, nil is returned for both the response
// and its error. In all cases, the third return value is the error
// returned from the engine when reading the on-disk cache.
func (rc *ResponseCache) GetResponse(e engine.Engine, cmdID proto.ClientCmdID) (proto.ResponseWithError, error) {
	// Do nothing if command ID is empty.
	if cmdID.IsEmpty() {
		return proto.ResponseWithError{}, nil
	}

	// Pull response from the cache and read into reply if available.
	var rwResp proto.ReadWriteCmdResponse
	key := keys.ResponseCacheKey(rc.raftID, &cmdID)
	ok, err := engine.MVCCGetProto(e, key, proto.ZeroTimestamp, true, nil, &rwResp)
	if err != nil {
		return proto.ResponseWithError{}, err
	}
	if ok {
		resp := rwResp.GetValue().(proto.Response)
		header := resp.Header()
		defer func() { header.Error = nil }()
		return proto.ResponseWithError{Reply: resp, Err: header.GoError()}, nil
	}
	return proto.ResponseWithError{}, nil
}

// CopyInto copies all the cached results from this response cache
// into the destRaftID response cache. Failures decoding individual
// cache entries return an error.
func (rc *ResponseCache) CopyInto(e engine.Engine, destRaftID proto.RaftID) error {
	prefix := keys.ResponseCacheKey(rc.raftID, nil) // response cache prefix
	start := engine.MVCCEncodeKey(prefix)
	end := engine.MVCCEncodeKey(prefix.PrefixEnd())

	return e.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		cmdID, err := rc.decodeResponseCacheKey(kv.Key)
		if err != nil {
			return false, util.Errorf("could not decode a response cache key %s: %s",
				proto.Key(kv.Key), err)
		}
		key := keys.ResponseCacheKey(destRaftID, &cmdID)
		encKey := engine.MVCCEncodeKey(key)
		// Decode the value, update the checksum and re-encode.
		meta := &engine.MVCCMetadata{}
		if err := gogoproto.Unmarshal(kv.Value, meta); err != nil {
			return false, util.Errorf("could not decode response cache value %s [% x]: %s",
				proto.Key(kv.Key), kv.Value, err)
		}
		meta.Value.Checksum = nil
		meta.Value.InitChecksum(key)
		_, _, err = engine.PutProto(e, encKey, meta)
		return false, err
	})
}

// CopyFrom copies all the cached results from the originRaftID
// response cache into this one. Note that the cache will not be
// locked while copying is in progress. Failures decoding individual
// cache entries return an error. The copy is done directly using the
// engine instead of interpreting values through MVCC for efficiency.
func (rc *ResponseCache) CopyFrom(e engine.Engine, originRaftID proto.RaftID) error {
	prefix := keys.ResponseCacheKey(originRaftID, nil) // response cache prefix
	start := engine.MVCCEncodeKey(prefix)
	end := engine.MVCCEncodeKey(prefix.PrefixEnd())

	return e.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		cmdID, err := rc.decodeResponseCacheKey(kv.Key)
		if err != nil {
			return false, util.Errorf("could not decode a response cache key %s: %s",
				proto.Key(kv.Key), err)
		}
		key := keys.ResponseCacheKey(rc.raftID, &cmdID)
		encKey := engine.MVCCEncodeKey(key)
		// Decode the value, update the checksum and re-encode.
		meta := &engine.MVCCMetadata{}
		if err := gogoproto.Unmarshal(kv.Value, meta); err != nil {
			return false, util.Errorf("could not decode response cache value %s [% x]: %s",
				proto.Key(kv.Key), kv.Value, err)
		}
		meta.Value.Checksum = nil
		meta.Value.InitChecksum(key)
		_, _, err = engine.PutProto(e, encKey, meta)
		return false, err
	})
}

// PutResponse writes a response and an error associated with it to the
// cache for the specified cmdID.
func (rc *ResponseCache) PutResponse(e engine.Engine, cmdID proto.ClientCmdID, replyWithErr proto.ResponseWithError) error {
	// Do nothing if command ID is empty.
	if cmdID.IsEmpty() {
		return nil
	}

	// Write the response value to the engine.
	if rc.shouldCacheResponse(replyWithErr) {
		// Write the error into the reply before caching.
		header := replyWithErr.Reply.Header()
		header.SetGoError(replyWithErr.Err)
		// Be sure to clear it when you're done!
		defer func() { header.Error = nil }()

		key := keys.ResponseCacheKey(rc.raftID, &cmdID)
		var rwResp proto.ReadWriteCmdResponse
		if !rwResp.SetValue(replyWithErr.Reply) {
			panic(fmt.Sprintf("response %T not supported by response cache", replyWithErr.Reply))
		}
		return engine.MVCCPutProto(e, nil, key, proto.ZeroTimestamp, nil, &rwResp)
	}

	return nil
}

// shouldCacheResponse returns whether the response should be cached.
// Responses with write-too-old, write-intent and not leader errors
// are retried on the server, and so are not recorded in the response
// cache in the hopes of retrying to a successful outcome.
func (rc *ResponseCache) shouldCacheResponse(replyWithErr proto.ResponseWithError) bool {
	if err := replyWithErr.Reply.Header().Error; err != nil {
		panic(proto.ErrorUnexpectedlySet)
	}

	switch replyWithErr.Err.(type) {
	case *proto.WriteTooOldError, *proto.WriteIntentError, *proto.NotLeaderError:
		return false
	}
	return true
}

func (rc *ResponseCache) decodeResponseCacheKey(encKey proto.EncodedKey) (proto.ClientCmdID, error) {
	ret := proto.ClientCmdID{}
	key, _, isValue := engine.MVCCDecodeKey(encKey)
	if isValue {
		return ret, util.Errorf("key %s is not a raw MVCC value", encKey)
	}
	if !bytes.HasPrefix(key, keys.LocalRangeIDPrefix) {
		return ret, util.Errorf("key %s does not have %s prefix", key, keys.LocalRangeIDPrefix)
	}
	// Cut the prefix and the Raft ID.
	b := key[len(keys.LocalRangeIDPrefix):]
	b, _ = encoding.DecodeUvarint(b)
	if !bytes.HasPrefix(b, keys.LocalResponseCacheSuffix) {
		return ret, util.Errorf("key %s does not contain the response cache suffix %s",
			key, keys.LocalResponseCacheSuffix)
	}
	// Cut the response cache suffix.
	b = b[len(keys.LocalResponseCacheSuffix):]
	// Now, decode the command ID.
	b, wt := encoding.DecodeUvarint(b)
	b, rd := encoding.DecodeUint64(b)
	if len(b) > 0 {
		return ret, util.Errorf("key %s has leftover bytes after decode: %s; indicates corrupt key",
			encKey, b)
	}
	ret.WallTime = int64(wt)
	ret.Random = int64(rd)
	return ret, nil
}
