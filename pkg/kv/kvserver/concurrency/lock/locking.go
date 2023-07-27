// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package lock provides type definitions for locking-related concepts used by
// concurrency control in the key-value layer.
package lock

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ExclusiveLocksBlockNonLockingReads dictates locking interactions between
// non-locking reads and exclusive locks. Configuring this setting to true makes
// it such that non-locking reads block on exclusive locks if their read
// timestamp is at or above the timestamp at which the lock is held; however,
// if this setting is set to false, non-locking reads do not block on exclusive
// locks, regardless of their relative timestamp.
//
// The tradeoff here is between increased concurrency (non-locking reads become
// non-blocking in the face of Exclusive locks) at the expense of forcing
// Exclusive lock holders to perform a read refresh, which in turn may force
// them to restart if the refresh fails.
var ExclusiveLocksBlockNonLockingReads = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.lock.exclusive_locks_block_non_locking_reads.enabled",
	"dictates the locking interactions between exclusive locks and non-locking reads",
	true,
)

// MaxDurability is the maximum value in the Durability enum.
const MaxDurability = Unreplicated

func init() {
	for v := range Durability_name {
		if d := Durability(v); d > MaxDurability {
			panic(fmt.Sprintf("Durability (%s) with value larger than MaxDurability", d))
		}
	}
}

// Conflicts returns whether the supplied lock mode conflicts with the receiver.
// The conflict rules are as described in the compatibility matrix in
// locking.pb.go.
func (m *Mode) Conflicts(sv *settings.Values, o *Mode) bool {
	if m.Empty() || o.Empty() {
		panic("cannot check conflict for uninitialized locks")
	}
	switch m.Strength {
	case None:
		return false
	case Shared:
		return o.Strength == Exclusive || o.Strength == Intent
	case Update:
		return o.Strength == Update || o.Strength == Exclusive || o.Strength == Intent
	case Exclusive:
		if ExclusiveLocksBlockNonLockingReads.Get(sv) {
			// Only non-locking reads below the timestamp at which the lock is held
			// are allowed.
			return !(o.Strength == None && o.Timestamp.Less(m.Timestamp))
		}
		return o.Strength != None // non-locking read.
	case Intent:
		// Only non-locking read, below held the timestamp at which the lock is held
		// are allowed.
		return !(o.Strength == None && o.Timestamp.Less(m.Timestamp))
	default:
		panic(errors.AssertionFailedf("unknown strength: %s", m.Strength))
	}
}

// Empty returns true if m is an empty (uninitialized) lock Mode.
func (m *Mode) Empty() bool {
	return m.Strength == None && m.Timestamp.IsEmpty()
}

// MakeModeNone constructs a Mode with strength None.
func MakeModeNone(ts hlc.Timestamp) Mode {
	return Mode{
		Strength:  None,
		Timestamp: ts,
	}
}

// MakeModeShared constructs a Mode with strength Shared.
func MakeModeShared() Mode {
	return Mode{
		Strength: Shared,
	}
}

// MakeModeUpdate constructs a Mode with strength Update.
func MakeModeUpdate() Mode {
	return Mode{
		Strength: Update,
	}
}

// MakeModeExclusive constructs a Mode with strength Exclusive.
func MakeModeExclusive(ts hlc.Timestamp) Mode {
	return Mode{
		Strength:  Exclusive,
		Timestamp: ts,
	}
}

// MakeModeIntent constructs a Mode with strength Intent.
func MakeModeIntent(ts hlc.Timestamp) Mode {
	return Mode{
		Strength:  Intent,
		Timestamp: ts,
	}
}

// SafeValue implements redact.SafeValue.
func (Strength) SafeValue() {}

// SafeValue implements redact.SafeValue.
func (Mode) SafeValue() {}

// SafeValue implements redact.SafeValue.
func (Durability) SafeValue() {}

// SafeValue implements redact.SafeValue.
func (WaitPolicy) SafeValue() {}
