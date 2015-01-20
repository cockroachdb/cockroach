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

package engine

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// GarbageCollector GCs MVCC key/values using a zone-specific GC
// policy allows either the union or intersection of maximum # of
// versions and maximum age.
type GarbageCollector struct {
	now    proto.Timestamp // time at start of GC
	policy *proto.GCPolicy
}

// NewGarbageCollector allocates and returns a new GC.
func NewGarbageCollector(now proto.Timestamp, policy *proto.GCPolicy) *GarbageCollector {
	return &GarbageCollector{
		now:    now,
		policy: policy,
	}
}

// MVCCPrefix returns the full key as prefix for non-version MVCC
// keys and otherwise just the encoded key portion of version MVCC keys.
func (gc *GarbageCollector) MVCCPrefix(key proto.EncodedKey) int {
	remaining, _ := encoding.DecodeBinary(key)
	return len(key) - len(remaining)
}

// Filter makes decisions about garbage collection based on the
// garbage collection policy for batches of values for the same key.
// Returns a timestamp representing the most recent value to be
// deleted, if any. If no values should be GC'd, returns
// proto.ZeroTimestamp.
func (gc *GarbageCollector) Filter(keys []proto.EncodedKey, values [][]byte) proto.Timestamp {
	if gc.policy.TTLSeconds <= 0 {
		return proto.ZeroTimestamp
	}
	if len(keys) == 1 {
		return proto.ZeroTimestamp
	}
	// Decode the first key and make sure it's an MVCC metadata key.
	_, ts, isValue := MVCCDecodeKey(keys[0])
	if isValue {
		log.Errorf("unexpected MVCC value encountered: %q", keys[0])
		return proto.ZeroTimestamp
	}
	expiration := gc.now
	expiration.WallTime -= int64(gc.policy.TTLSeconds) * 1E9

	delTS := proto.ZeroTimestamp
	var survivors bool
	// Loop over remaining values. All should be MVCC versions.
	for i, key := range keys[1:] {
		_, ts, isValue = MVCCDecodeKey(key)
		if !isValue {
			log.Errorf("unexpected MVCC metadata encountered: %q", key)
			return proto.ZeroTimestamp
		}
		mvccVal := proto.MVCCValue{}
		if err := gogoproto.Unmarshal(values[i+1], &mvccVal); err != nil {
			log.Errorf("unable to unmarshal MVCC value %q: %v", key, err)
			return proto.ZeroTimestamp
		}
		if i == 0 {
			// If the first value isn't a deletion tombstone, don't consider GC.
			if !mvccVal.Deleted {
				survivors = true
				continue
			}
		}
		// If we encounter a version older than our GC timestamp, mark for deletion.
		if ts.Less(expiration) {
			delTS = ts
			break
		} else if !mvccVal.Deleted {
			survivors = true
		}
	}
	// If there are no non-deleted survivors, return timestamp of first key
	// to delete all entries.
	if !survivors {
		_, ts, _ = MVCCDecodeKey(keys[1])
		return ts
	}
	return delTS
}
