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
	"fmt"
	"sync"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
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
// keys derived from KeyLocalResponseCachePrefix, Raft ID and the
// ClientCmdID.
//
// A ResponseCache is safe for concurrent access.
type ResponseCache struct {
	raftID   int64
	engine   engine.Engine
	inflight map[cmdIDKey]*sync.Cond
	sync.Mutex
}

// NewResponseCache returns a new response cache. Every range replica
// maintains a response cache, not just the leader. However, when a
// replica loses or gains leadership of the Raft consensus group, the
// inflight map should be cleared.
func NewResponseCache(raftID int64, engine engine.Engine) *ResponseCache {
	return &ResponseCache{
		raftID:   raftID,
		engine:   engine,
		inflight: map[cmdIDKey]*sync.Cond{},
	}
}

// ClearInflight removes all pending commands from the inflight map,
// signaling and clearing any inflight waiters.
func (rc *ResponseCache) ClearInflight() {
	rc.Lock()
	defer rc.Unlock()
	for _, cond := range rc.inflight {
		cond.Broadcast()
	}
	rc.inflight = map[cmdIDKey]*sync.Cond{}
}

// ClearData removes all items stored in the persistent cache. It does not alter
// the inflight map.
func (rc *ResponseCache) ClearData() error {
	p := responseCacheKeyPrefix(rc.raftID)
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
	// If the command is inflight, wait for it to complete.
	rc.Lock()
	for {
		if cond, ok := rc.inflight[makeCmdIDKey(cmdID)]; ok {
			log.Infof("waiting on cmdID: %s", &cmdID)
			cond.Wait()
		} else {
			break
		}
	}
	// Adding inflight here is preemptive; we don't want to hold lock
	// while fetching from the on-disk cache. The vast, vast majority of
	// calls to GetResponse will be cache misses, so this saves us
	// from acquiring the lock twice: once here and once below in the
	// event we experience a cache miss.
	rc.addInflightLocked(cmdID)
	rc.Unlock()

	// If the response is in the cache or we experienced an error, return.
	rwResp := proto.ReadWriteCmdResponse{}
	key := responseCacheKey(rc.raftID, cmdID)
	if ok, err := engine.MVCCGetProto(rc.engine, key, proto.ZeroTimestamp, nil, &rwResp); ok || err != nil {
		rc.Lock() // Take lock after fetching response from cache.
		defer rc.Unlock()
		rc.removeInflightLocked(cmdID)
		if err == nil && rwResp.GetValue() != nil {
			gogoproto.Merge(reply.(gogoproto.Message), rwResp.GetValue().(gogoproto.Message))
		}
		return ok, err
	}
	// There's no command result cached for this ID; but inflight was added above.
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

	prefix := responseCacheKeyPrefix(rc.raftID)
	start := engine.MVCCEncodeKey(prefix)
	end := engine.MVCCEncodeKey(prefix.PrefixEnd())

	return rc.engine.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		cmdID, err := rc.decodeKey(kv.Key)
		if err != nil {
			return false, util.Errorf("could not decode a response cache key %q: %s", kv.Key, err)
		}
		encKey := engine.MVCCEncodeKey(responseCacheKey(destRaftID, cmdID))
		return false, e.Put(encKey, kv.Value)
	})
}

// CopyFrom copies all the cached results from another response cache
// into this one. Note that the cache will not be locked while copying
// is in progress. Failures decoding individual cache entries return an
// error. The copy is done directly using the engine instead of interpreting
// values through MVCC for efficiency.
func (rc *ResponseCache) CopyFrom(e engine.Engine, originRaftID int64) error {
	prefix := responseCacheKeyPrefix(originRaftID)
	start := engine.MVCCEncodeKey(prefix)
	end := engine.MVCCEncodeKey(prefix.PrefixEnd())

	return e.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		cmdID, err := rc.decodeKey(kv.Key)
		if err != nil {
			return false, util.Errorf("could not decode a response cache key %q: %s", kv.Key, err)
		}
		encKey := engine.MVCCEncodeKey(responseCacheKey(rc.raftID, cmdID))
		return false, rc.engine.Put(encKey, kv.Value)
	})
}

// PutResponse writes a response to the cache for the specified cmdID.
// The inflight entry corresponding to cmdID is removed from the
// inflight map. Any requests waiting on the outcome of the inflight
// command will be signaled to wakeup and read the command response
// from the cache.
func (rc *ResponseCache) PutResponse(cmdID proto.ClientCmdID, reply proto.Response) error {
	// Do nothing if command ID is empty.
	if cmdID.IsEmpty() {
		return nil
	}
	// Write the response value to the engine.
	var err error
	if rc.shouldCacheResponse(reply) {
		key := responseCacheKey(rc.raftID, cmdID)
		rwResp := &proto.ReadWriteCmdResponse{}
		rwResp.SetValue(reply)
		err = engine.MVCCPutProto(rc.engine, nil, key, proto.ZeroTimestamp, nil, rwResp)
	}

	// Take lock after writing response to cache!
	rc.Lock()
	defer rc.Unlock()
	// Even on error, we remove the entry from the inflight map.
	rc.removeInflightLocked(cmdID)

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

// addInflightLocked adds the supplied ClientCmdID to the inflight
// map. Any subsequent invocations of GetResponse for the same client
// command will block on the inflight cond var until either the
// response cache is cleared or this command is removed via
// PutResponse().
func (rc *ResponseCache) addInflightLocked(cmdID proto.ClientCmdID) {
	if _, ok := rc.inflight[makeCmdIDKey(cmdID)]; ok {
		panic(fmt.Sprintf("command %+v is already inflight; GetResponse() should have been "+
			"invoked first", cmdID))
	}
	rc.inflight[makeCmdIDKey(cmdID)] = sync.NewCond(&rc.Mutex)
}

// removeInflightLocked removes an entry matching cmdID from the
// inflight map and broadcasts a wakeup to all waiters.
func (rc *ResponseCache) removeInflightLocked(cmdID proto.ClientCmdID) {
	key := makeCmdIDKey(cmdID)
	if cond, ok := rc.inflight[key]; ok {
		cond.Broadcast()
		delete(rc.inflight, key)
	}
}

// responseCacheKeyPrefix generates the prefix under which all entries
// for the given range are stored in the engine.
func responseCacheKeyPrefix(raftID int64) proto.Key {
	b := append([]byte(nil), engine.KeyLocalResponseCachePrefix...)
	return encoding.EncodeInt(b, raftID)
}

// responseCacheKey encodes the Raft ID and client command ID into a
// key for storage in the underlying engine. Note that the prefix for
// response cache keys sorts them at the very top of the engine's
// keyspace.
func responseCacheKey(raftID int64, cmdID proto.ClientCmdID) proto.Key {
	b := responseCacheKeyPrefix(raftID)
	b = encoding.EncodeInt(b, cmdID.WallTime) // wall time helps sort for locality
	b = encoding.EncodeInt(b, cmdID.Random)   // TODO(spencer): encode as Fixed64
	return b
}

func (rc *ResponseCache) decodeKey(encKey []byte) (proto.ClientCmdID, error) {
	ret := proto.ClientCmdID{}
	key, _, isValue := engine.MVCCDecodeKey(encKey)
	if isValue {
		return ret, util.Errorf("key %q is not a raw MVCC value", encKey)
	}
	minLen := len(engine.KeyLocalResponseCachePrefix)
	if len(key) < minLen {
		return ret, util.Errorf("key not long enough to be decoded: %q", key)
	}
	// First, Cut the prefix and the Raft ID.
	b := key[minLen:]
	b, _ = encoding.DecodeInt(b)
	// Second, read the wall time.
	b, wt := encoding.DecodeInt(b)
	// Third, read the Random component.
	b, rd := encoding.DecodeInt(b)
	if len(b) > 0 {
		return ret, util.Errorf("key %q has leftover bytes after decode: %q; indicates corrupt key", encKey, b)
	}
	ret.WallTime = wt
	ret.Random = rd
	return ret, nil
}
