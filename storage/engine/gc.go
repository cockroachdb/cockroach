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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// GarbageCollector GCs MVCC key/values using a zone-specific GC
// policy allows either the union or intersection of maximum # of
// versions and maximum age.
type GarbageCollector struct {
	now      proto.Timestamp // time at start of GC
	policyFn func(key Key) *proto.GCPolicy
}

// NewGarbageCollector allocates and returns a new GC.
func NewGarbageCollector(now proto.Timestamp, policyFn func(key Key) *proto.GCPolicy) *GarbageCollector {
	return &GarbageCollector{
		now:      now,
		policyFn: policyFn,
	}
}

// MVCCPrefix returns the full key as prefix for non-version MVCC
// keys and otherwise just the encoded key portion of version MVCC keys.
func (gc *GarbageCollector) MVCCPrefix(key Key) int {
	if dKey, _, isValue := mvccDecodeKey(key); isValue {
		return len(dKey)
	}
	return len(key)
}

// Filter makes decisions about garbage collection based on the
// garbage collection policy for batches of values for the same key.
// The GC policy is determined via the policyFn specified when the
// GarbageCollector was created. Returns a slice of deletions, one
// per incoming keys. If an index in the returned array is set to
// true, then that value will be garbage collected.
func (gc *GarbageCollector) Filter(keys []Key, values [][]byte) []bool {
	if len(keys) == 1 {
		return nil
	}
	// Look up the policy which applies to this set of MVCC values.
	_, decKey := encoding.DecodeBinary(keys[0])
	policy := gc.policyFn(decKey)
	if policy == nil || policy.TTLSeconds <= 0 {
		return nil
	}
	toDelete := make([]bool, len(keys))
	expiration := gc.now
	expiration.WallTime -= int64(policy.TTLSeconds) * 1E9

	var survivors bool
	for i, key := range keys {
		_, ts, isValue := mvccDecodeKey(key)
		if i == 0 {
			if isValue {
				log.Errorf("unexpected MVCC value encountered: %q", key)
				return make([]bool, len(keys))
			}
			continue
		}
		if !isValue {
			log.Errorf("unexpected MVCC metadata encountered: %q", key)
			return make([]bool, len(keys))
		}
		mvccVal := proto.MVCCValue{}
		if err := gogoproto.Unmarshal(values[i], &mvccVal); err != nil {
			log.Errorf("unable to unmarshal MVCC value %q: %v", key, err)
			return make([]bool, len(keys))
		}
		if i == 1 {
			// If the first value isn't a deletion tombstone, set survivors to true.
			if !mvccVal.Deleted {
				survivors = true
			}
		} else {
			if ts.Less(expiration) {
				// If we encounter a version older than our GC timestamp, mark for deletion.
				toDelete[i] = true
			} else if !mvccVal.Deleted {
				// Otherwise, if not marked for GC and not a tombstone, set survivors true.
				survivors = true
			}
		}
	}
	// If there are no remaining non-deleted, versioned entries, mark
	// all keys for deletion, including the MVCC metadata entry.
	if !survivors {
		for i := range keys {
			toDelete[i] = true
		}
	}
	return toDelete
}
