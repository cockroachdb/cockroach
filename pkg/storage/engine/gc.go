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

package engine

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// GarbageCollector GCs MVCC key/values using a zone-specific GC
// policy allows either the union or intersection of maximum # of
// versions and maximum age.
type GarbageCollector struct {
	Threshold hlc.Timestamp
	policy    config.GCPolicy
}

// MakeGarbageCollector allocates and returns a new GC, with expiration
// computed based on current time and policy.TTLSeconds.
func MakeGarbageCollector(now hlc.Timestamp, policy config.GCPolicy) GarbageCollector {
	ttlNanos := int64(policy.TTLSeconds) * 1E9
	return GarbageCollector{
		Threshold: hlc.Timestamp{WallTime: now.WallTime - ttlNanos},
		policy:    policy,
	}
}

// Filter makes decisions about garbage collection based on the
// garbage collection policy for batches of values for the same
// key. Returns the index of the first key to be GC'd and the
// timestamp including, and after which, all values should be garbage
// collected. If no values should be GC'd, returns -1 for the index
// and the zero timestamp. Keys must be in descending time
// order. Values deleted at or before the returned timestamp can be
// deleted without invalidating any reads in the time interval
// (gc.expiration, \infinity).
//
// The GC keeps all values (including deletes) above the expiration time, plus
// the first value before or at the expiration time. This allows reads to be
// guaranteed as described above. However if this were the only rule, then if
// the most recent write was a delete, it would never be removed. Thus, when a
// deleted value is the most recent before expiration, it can be deleted. This
// would still allow for the tombstone bugs in #6227, so in the future we will
// add checks that disallow writes before the last GC expiration time.
func (gc GarbageCollector) Filter(keys []MVCCKey, values [][]byte) (int, hlc.Timestamp) {
	if gc.policy.TTLSeconds <= 0 {
		return -1, hlc.Timestamp{}
	}
	if len(keys) == 0 {
		return -1, hlc.Timestamp{}
	}

	// find the first expired key index using binary search
	i := sort.Search(len(keys), func(i int) bool { return !gc.Threshold.Less(keys[i].Timestamp) })

	if i == len(keys) {
		return -1, hlc.Timestamp{}
	}

	// Now keys[i].Timestamp is <= gc.expiration, but the key-value pair is still
	// "visible" at timestamp gc.expiration (and up to the next version).
	if deleted := len(values[i]) == 0; deleted {
		// We don't have to keep a delete visible (since GCing it does not change
		// the outcome of the read). Note however that we can't touch deletes at
		// higher timestamps immediately preceding this one, since they're above
		// gc.expiration and are needed for correctness; see #6227.
		return i, keys[i].Timestamp
	} else if i+1 < len(keys) {
		// Otherwise mark the previous timestamp for deletion (since it won't ever
		// be returned for reads at gc.expiration and up).
		return i + 1, keys[i+1].Timestamp
	}

	return -1, hlc.Timestamp{}
}
