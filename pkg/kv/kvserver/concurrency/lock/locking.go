// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package lock provides type definitions for locking-related concepts used by
// concurrency control in the key-value layer.
package lock

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ExclusiveLocksBlockNonLockingReads dictates locking interactions between
// non-locking reads and exclusive locks. Configuring this setting to true makes
// it such that non-locking reads from serializable transactions block on
// exclusive locks held by serializable transactions if the read's timestamp is
// at or above the timestamp at which the lock is held. If set to false,
// non-locking reads do not block on exclusive locks, regardless of isolation
// level or timestamp.
//
// Note that the setting only applies if both the reader and lock holder are
// running with serializable isolation level. If either of them is running with
// weaker isolation levels, the setting has no effect. To understand why,
// consider the tradeoff this setting presents -- the tradeoff here is increased
// concurrency (non-locking reads become non-blocking in the face of Exclusive
// locks) at the expense of forcing Exclusive lock holders to perform a read
// refresh (to prevent write skew), which in turn may force them to restart if
// the refresh fails.
//
// If the lock holder is running at a weaker isolation level (snapshot,
// read committed), then it is able to tolerate write skew. Thus, there is no
// tradeoff -- it is always a good idea to allow any non-locking read to
// proceed. On the other hand, non-locking reads running at weaker isolation
// levels should never block on exclusive locks.
var ExclusiveLocksBlockNonLockingReads = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.lock.exclusive_locks_block_non_locking_reads.enabled",
	"dictates the locking interactions between exclusive locks and non-locking reads",
	true,
)

// MaxStrength is the maximum value in the Strength enum.
const MaxStrength = Intent

// NumLockStrength is the total number of lock strengths in the Strength enum.
const NumLockStrength = MaxStrength + 1

// MaxDurability is the maximum value in the Durability enum.
const MaxDurability = Replicated

func init() {
	for v := range Strength_name {
		if st := Strength(v); st > MaxStrength {
			panic(fmt.Sprintf("Strength (%s) with value larger than MaxDurability", st))
		}
	}
	if int(NumLockStrength) != len(Strength_name) {
		panic(fmt.Sprintf("mismatched numer of lock strengths: NumLockStrength %d, lock strengths %d",
			int(NumLockStrength), len(Strength_name)))
	}
	for v := range Durability_name {
		if d := Durability(v); d > MaxDurability {
			panic(fmt.Sprintf("Durability (%s) with value larger than MaxDurability", d))
		}
	}
}

// Conflicts returns whether the supplied lock modes conflict with each other.
// Conflict rules are as described in the compatibility matrix in locking.pb.go.
func Conflicts(m1, m2 Mode, sv *settings.Values) bool {
	if m1.Empty() || m2.Empty() {
		return false // no conflict with empty lock modes
	}
	if m1.Strength > m2.Strength {
		// Conflict rules are symmetric, so reduce the number of cases we need to
		// handle.
		m1, m2 = m2, m1
	}
	switch m1.Strength {
	case None:
		switch m2.Strength {
		case None, Shared, Update:
			return false
		case Exclusive:
			// Non-locking reads only conflict with Exclusive locks if the following
			// conditions hold:
			// 1. both the non-locking read and the Exclusive lock belong to
			// transactions that cannot tolerate write skew.
			// 2. AND the non-locking read is reading at or above the timestamp of the
			// lock.
			// 3. AND the ExclusiveLocksBlockNonLockingReads cluster setting is
			// configured to do as much.
			return !m1.IsoLevel.ToleratesWriteSkew() &&
				!m2.IsoLevel.ToleratesWriteSkew() && m2.Timestamp.LessEq(m1.Timestamp) &&
				ExclusiveLocksBlockNonLockingReads.Get(sv)
		case Intent:
			// Non-locking read (m1) conflicts if the intent is at a lower or equal
			// timestamp.
			return m2.Timestamp.LessEq(m1.Timestamp)
		default:
			panic(errors.AssertionFailedf("unknown strength: %s", m2.Strength))
		}
	case Shared:
		// m2.Strength >= Shared due to the normalization above.
		return m2.Strength == Exclusive || m2.Strength == Intent
	case Update, Exclusive, Intent:
		// m2.Strength >= Update due to the normalization above.
		return true
	default:
		panic(errors.AssertionFailedf("unknown strength: %s", m1.Strength))
	}
}

// Empty returns true if m is an empty (uninitialized) lock Mode.
func (m *Mode) Empty() bool {
	return m.Strength == None && m.Timestamp.IsEmpty()
}

// Weaker returns true if the receiver conflicts with fewer requests than the
// Mode supplied.
func (m Mode) Weaker(o Mode) bool {
	if m.Strength == o.Strength {
		return !m.Timestamp.Less(o.Timestamp) // lower timestamp conflicts with more requests
	}
	return m.Strength < o.Strength
}

// MakeModeNone constructs a Mode with strength None.
func MakeModeNone(ts hlc.Timestamp, isoLevel isolation.Level) Mode {
	return Mode{
		Strength:  None,
		Timestamp: ts,
		IsoLevel:  isoLevel,
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
func MakeModeExclusive(ts hlc.Timestamp, isoLevel isolation.Level) Mode {
	return Mode{
		Strength:  Exclusive,
		Timestamp: ts,
		IsoLevel:  isoLevel,
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
