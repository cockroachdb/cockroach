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
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

// GarbageCollector GCs MVCC key/values using a zone-specific GC
// policy allows either the union or intersection of maximum # of
// versions and maximum age.
type GarbageCollector struct {
	expiration proto.Timestamp
	policy     proto.GCPolicy
}

// NewGarbageCollector allocates and returns a new GC, with expiration
// computed based on current time and policy.TTLSeconds.
func NewGarbageCollector(now proto.Timestamp, policy proto.GCPolicy) *GarbageCollector {
	ttlNanos := int64(policy.TTLSeconds) * 1E9
	return &GarbageCollector{
		expiration: proto.Timestamp{WallTime: now.WallTime - ttlNanos},
		policy:     policy,
	}
}

// Filter makes decisions about garbage collection based on the
// garbage collection policy for batches of values for the same key.
// Returns the timestamp including, and after which, all values should
// be garbage collected. If no values should be GC'd, returns
// proto.ZeroTimestamp.
func (gc *GarbageCollector) Filter(keys []proto.EncodedKey, values [][]byte) proto.Timestamp {
	if gc.policy.TTLSeconds <= 0 {
		return proto.ZeroTimestamp
	}
	if len(keys) == 0 {
		return proto.ZeroTimestamp
	}

	// Loop over remaining values. All should be MVCC versions.
	var delTS proto.Timestamp
	var survivors bool
	for i, key := range keys {
		_, ts, isValue := MVCCDecodeKey(key)
		if !isValue {
			log.Errorf("unexpected MVCC metadata encountered: %q", key)
			return proto.ZeroTimestamp
		}
		mvccVal := proto.MVCCValue{}
		if err := gogoproto.Unmarshal(values[i], &mvccVal); err != nil {
			log.Errorf("unable to unmarshal MVCC value %q: %v", key, err)
			return proto.ZeroTimestamp
		}
		if i == 0 {
			// If the first value isn't a deletion tombstone, don't con
			if !mvccVal.Deleted {
				survivors = true
				continue
			}
		}
		// If we encounter a version older than our GC timestamp, mark for deletion.
		if ts.Less(gc.expiration) {
			delTS = ts
			break
		} else if !mvccVal.Deleted {
			survivors = true
		}
	}
	// If there are no non-deleted survivors, return timestamp of first key
	// to delete all entries.
	if !survivors {
		_, ts, _ := MVCCDecodeKey(keys[0])
		return ts
	}
	return delTS
}
