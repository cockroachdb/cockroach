// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
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
	ttlNanos := int64(policy.TTLSeconds) * 1e9
	return GarbageCollector{
		Threshold: hlc.Timestamp{WallTime: now.WallTime - ttlNanos},
		policy:    policy,
	}
}

// IsGarbage makes a determination whether a key (cur) is garbage. Next, if
// non-nil should be the chronologically next version of the same key (or the
// metadata KV if cur is an intent). If isNewest is false, next must be non-nil.
// isNewest implies that this is the highest timestamp committed version for
// this key. If isNewest is true and next is non-nil, it is an intent.
//
// The GC keeps all values (including deletes) above the expiration time, plus
// the first value before or at the expiration time. This allows reads to be
// guaranteed as described above. However if this were the only rule, then if
// the most recent write was a delete, it would never be removed. Thus, when a
// deleted value is the most recent before expiration, it can be deleted.
func (gc GarbageCollector) IsGarbage(cur, next *MVCCKeyValue, isNewest bool) bool {
	// If the value is not at or below the threshold then it's not garbage.
	if belowThreshold := cur.Key.Timestamp.LessEq(gc.Threshold); !belowThreshold {
		return false
	}
	isDelete := len(cur.Value) == 0
	if isNewest && !isDelete {
		return false
	}
	// If this value is not a delete, then we need to make sure that the next
	// value is also at or below the threshold.
	// NB: This doesn't need to check whether next is nil because we know
	// isNewest is false when evaluating rhs of the or below.
	return isDelete || next.Key.Timestamp.LessEq(gc.Threshold)
}
