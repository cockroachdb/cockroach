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
	"github.com/golang/glog"
)

type cmdIDKey struct {
	walltime, random int64
}

func makeCmdIDKey(cmdID proto.ClientCmdID) cmdIDKey {
	return cmdIDKey{
		walltime: cmdID.WallTime,
		random:   cmdID.Random,
	}
}

// A ResponseCache provides idempotence for request retries. Each
// request to a range specifies a ClientCmdID in the request header
// which uniquely identifies a client command. After commands have
// been replicated via Raft, they are executed against the state
// machine and the results are stored in the ResponseCache.
//
// The ResponseCache stores responses in the underlying engine, using
// keys derived from KeyLocalRangeResponseCachePrefix, range ID and
// the ClientCmdID.
//
// A ResponseCache is safe for concurrent access.
type ResponseCache struct {
	rangeID  int64
	engine   engine.Engine
	inflight map[cmdIDKey]*sync.Cond
	sync.Mutex
}

// NewResponseCache returns a new response cache. Every range replica
// maintains a response cache, not just the leader. However, when a
// replica loses or gains leadership of the Raft consensus group, the
// inflight map should be cleared.
func NewResponseCache(rangeID int64, engine engine.Engine) *ResponseCache {
	return &ResponseCache{
		rangeID:  rangeID,
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
	p := rc.makePrefix()
	_, err := engine.ClearRange(rc.engine, p, engine.PrefixEndKey(p), 0)
	return err
}

// GetResponse looks up a response matching the specified cmdID and
// returns true if found. The response is deserialized into the
// supplied reply parameter. If no response is found, returns
// false. If a command is pending already for the cmdID, then this
// method will block until the the command is completed or the
// response cache is cleared.
func (rc *ResponseCache) GetResponse(cmdID proto.ClientCmdID, reply interface{}) (bool, error) {
	// Do nothing if command ID is empty.
	if cmdID.IsEmpty() {
		return false, nil
	}
	// If the command is inflight, wait for it to complete.
	rc.Lock()
	for {
		if cond, ok := rc.inflight[makeCmdIDKey(cmdID)]; ok {
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
	if ok, err := engine.GetProto(rc.engine, rc.makeKey(cmdID), &rwResp); ok || err != nil {
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

// CopyInto copies all the cached results from one response cache into another.
// The cache will be locked while copying is in progress; failures decoding
// individual cache entries will only trigger a warning.
func (rc *ResponseCache) CopyInto(destRC *ResponseCache) error {
	if destRC == nil {
		return util.Errorf("destination response cache missing")
	}
	rc.Lock()
	defer rc.Unlock()
	prefix := rc.makePrefix()
	kvs, err := rc.engine.Scan(prefix, engine.PrefixEndKey(prefix), 0)
	if err != nil {
		return err
	}
	batch := []interface{}(nil)
	for _, kv := range kvs {
		// Decode the key into a cmd, skipping on error. Otherwise,
		// write it to the corresponding key in the new cache.
		if cmdID, err := rc.decodeKey(kv.Key); err == nil {
			batch = append(batch, engine.BatchPut{
				Key: destRC.makeKey(cmdID), Value: kv.Value,
			})
			//destRC.engine.Put(destRC.makeKey(cmdID), kv.Value)
		} else {
			// This is near impossible to ever happen in practice, so if it happens
			// we're very interested in finding out.
			glog.Warningf("could not copy a response cache entry: %v", err)
		}
	}
	return destRC.engine.WriteBatch(batch)
}

// PutResponse writes a response to the cache for the specified cmdID.
// The inflight entry corresponding to cmdID is removed from the
// inflight map. Any requests waiting on the outcome of the inflight
// command will be signaled to wakeup and read the command response
// from the cache.
func (rc *ResponseCache) PutResponse(cmdID proto.ClientCmdID, reply interface{}) error {
	// Do nothing if command ID is empty.
	if cmdID.IsEmpty() {
		return nil
	}
	// Write the response value to the engine.
	key := rc.makeKey(cmdID)
	rwResp := &proto.ReadWriteCmdResponse{}
	rwResp.SetValue(reply)
	err := engine.PutProto(rc.engine, key, rwResp)

	// Take lock after writing response to cache!
	rc.Lock()
	defer rc.Unlock()
	// Even on error, we remove the entry from the inflight map.
	rc.removeInflightLocked(cmdID)

	return err
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

// makeKey encodes the range ID and client command ID into a key
// for storage in the underlying engine. Note that the prefix for
// response cache keys sorts them at the very top of the engine's
// keyspace.
func (rc *ResponseCache) makeKey(cmdID proto.ClientCmdID) engine.Key {
	b := rc.makePrefix()
	b = encoding.EncodeInt(b, cmdID.WallTime) // wall time helps sort for locality
	b = encoding.EncodeInt(b, cmdID.Random)   // TODO(spencer): encode as Fixed64
	return b
}

// makePrefix generates the prefix under which all entries for the given range
// are stored in the engine.
func (rc *ResponseCache) makePrefix() engine.Key {
	b := append([]byte{}, engine.KeyLocalRangeResponseCachePrefix...)
	return encoding.EncodeInt(b, rc.rangeID)
}

func (rc *ResponseCache) decodeKey(encKey engine.Key) (proto.ClientCmdID, error) {
	minLen := len(engine.KeyLocalRangeResponseCachePrefix)
	ret := proto.ClientCmdID{}
	if len(encKey) < minLen {
		return ret, util.Errorf("key not long enough to be decoded: %q", encKey)
	}
	// First, Cut the prefix and the range ID.
	binKey := encKey[minLen:]
	ts, _ := encoding.DecodeInt(binKey)
	// Second, read the wall time.
	ts, wt := encoding.DecodeInt(ts)
	// Third, read the Random component.
	ts, rd := encoding.DecodeInt(ts)
	if len(ts) > 0 {
		return ret, util.Errorf("key %q has leftover bytes after decode: %q; indicates corrupt key", encKey, ts)
	}
	ret.WallTime = wt
	ret.Random = rd
	return ret, nil
}
