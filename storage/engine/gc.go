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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

// GarbageCollector GCs MVCC key/values using a zone-specific GC
// policy allows either the union or intersection of maximum # of
// versions and maximum age.
type GarbageCollector struct {
	expiration roachpb.Timestamp
	policy     config.GCPolicy
}

// MakeGarbageCollector allocates and returns a new GC, with expiration
// computed based on current time and policy.TTLSeconds.
func MakeGarbageCollector(now roachpb.Timestamp, policy config.GCPolicy) GarbageCollector {
	ttlNanos := int64(policy.TTLSeconds) * 1E9
	return GarbageCollector{
		expiration: roachpb.Timestamp{WallTime: now.WallTime - ttlNanos},
		policy:     policy,
	}
}

// Filter makes decisions about garbage collection based on the
// garbage collection policy for batches of values for the same key.
// Returns the timestamp including, and after which, all values should
// be garbage collected. If no values should be GC'd, returns
// roachpb.ZeroTimestamp.
func (gc GarbageCollector) Filter(keys []MVCCKey, values [][]byte) roachpb.Timestamp {
	if gc.policy.TTLSeconds <= 0 {
		return roachpb.ZeroTimestamp
	}
	if len(keys) == 0 {
		return roachpb.ZeroTimestamp
	}

	// Loop over values. All should be MVCC versions.
	delTS := roachpb.ZeroTimestamp
	survivors := false
	for i, key := range keys {
		if !key.IsValue() {
			log.Errorf("unexpected MVCC metadata encountered: %q", key)
			return roachpb.ZeroTimestamp
		}
		deleted := len(values[i]) == 0
		if i == 0 {
			// If the first value isn't a deletion tombstone, don't consider
			// it for GC. It should always survive if non-deleted.
			if !deleted {
				survivors = true
				continue
			}
		}
		// If we encounter a version older than our GC timestamp, mark for deletion.
		if key.Timestamp.Less(gc.expiration) {
			delTS = key.Timestamp
			break
		} else if !deleted {
			survivors = true
		}
	}
	// If there are no non-deleted survivors, return timestamp of first key
	// to delete all entries.
	if !survivors {
		return keys[0].Timestamp
	}
	return delTS
}
