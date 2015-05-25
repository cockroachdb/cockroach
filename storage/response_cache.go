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
	"sync"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
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
// A ResponseCache is safe for concurrent access.
type ResponseCache struct {
	raftID int64
	engine engine.Engine
	sync.Mutex
}

// NewResponseCache returns a new response cache. Every range replica
// maintains a response cache, not just the leader. However, when a
// replica loses or gains leadership of the Raft consensus group, the
// inflight map should be cleared.
func NewResponseCache(raftID int64, engine engine.Engine) *ResponseCache {
	return &ResponseCache{
		raftID: raftID,
		engine: engine,
	}
}

// ClearData removes all items stored in the persistent cache. It does not alter
// the inflight map.
func (rc *ResponseCache) ClearData() error {
	p := keys.ResponseCacheKey(rc.raftID, nil) // prefix for all response cache entries with this raft ID
	end := p.PrefixEnd()
	_, err := engine.ClearRange(rc.engine, engine.MVCCEncodeKey(p), engine.MVCCEncodeKey(end))
	return err
}

// GetResponse looks up a response matching the specified cmdID and
// returns true if found. The response is deserialized into the
// supplied reply parameter. If no response is found, returns
// false. If a command is pending already for the cmdID, then this
// method will block until the the command is completed or the
// response cache is cleared.
func (rc *ResponseCache) GetResponse(cmdID proto.ClientCmdID, reply proto.Response) (bool, error) {
	// Do nothing if command ID is empty.
	if cmdID.IsEmpty() {
		return false, nil
	}

	// If the response is in the cache or we experienced an error, return.
	rwResp := proto.ReadWriteCmdResponse{}
	key := keys.ResponseCacheKey(rc.raftID, &cmdID)
	if ok, err := engine.MVCCGetProto(rc.engine, key, proto.ZeroTimestamp, true, nil, &rwResp); ok || err != nil {
		if err == nil && rwResp.GetValue() != nil {
			gogoproto.Merge(reply.(gogoproto.Message), rwResp.GetValue().(gogoproto.Message))
		}
		return ok, err
	}
	// There's no command result cached for this ID.
	return false, nil
}

// CopyInto copies all the cached results from one response cache into
// another. The cache will be locked while copying is in progress;
// failures decoding individual cache entries return an error. The
// copy is done directly using the engine instead of interpreting
// values through MVCC for efficiency.
func (rc *ResponseCache) CopyInto(e engine.Engine, destRaftID int64) error {
	rc.Lock()
	defer rc.Unlock()

	prefix := keys.ResponseCacheKey(rc.raftID, nil) // response cache prefix
	start := engine.MVCCEncodeKey(prefix)
	end := engine.MVCCEncodeKey(prefix.PrefixEnd())

	return rc.engine.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
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
		meta := &proto.MVCCMetadata{}
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

// CopyFrom copies all the cached results from another response cache
// into this one. Note that the cache will not be locked while copying
// is in progress. Failures decoding individual cache entries return an
// error. The copy is done directly using the engine instead of interpreting
// values through MVCC for efficiency.
func (rc *ResponseCache) CopyFrom(e engine.Engine, originRaftID int64) error {
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
		meta := &proto.MVCCMetadata{}
		if err := gogoproto.Unmarshal(kv.Value, meta); err != nil {
			return false, util.Errorf("could not decode response cache value %s [% x]: %s",
				proto.Key(kv.Key), kv.Value, err)
		}
		meta.Value.Checksum = nil
		meta.Value.InitChecksum(key)
		_, _, err = engine.PutProto(rc.engine, encKey, meta)
		return false, err
	})
}

// PutResponse writes a response to the cache for the specified cmdID.
func (rc *ResponseCache) PutResponse(cmdID proto.ClientCmdID, reply proto.Response) error {
	// Do nothing if command ID is empty.
	if cmdID.IsEmpty() {
		return nil
	}
	// Write the response value to the engine.
	var err error
	if rc.shouldCacheResponse(reply) {
		key := keys.ResponseCacheKey(rc.raftID, &cmdID)
		rwResp := &proto.ReadWriteCmdResponse{}
		if !rwResp.SetValue(reply) {
			log.Fatalf("response %T not supported by response cache", reply)
		}
		err = engine.MVCCPutProto(rc.engine, nil, key, proto.ZeroTimestamp, nil, rwResp)
	}

	return err
}

// shouldCacheResponse returns whether the response should be cached.
// Responses with write-too-old and write-intent errors are are
// retried on the server, and so are not recorded in the response
// cache in the hopes of retrying to a successful outcome.
func (rc *ResponseCache) shouldCacheResponse(reply proto.Response) bool {
	switch reply.Header().GoError().(type) {
	case *proto.WriteTooOldError, *proto.WriteIntentError:
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
