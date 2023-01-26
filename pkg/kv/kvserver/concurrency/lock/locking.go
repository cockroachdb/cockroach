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
	hlc "github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	"kv.lock.exclusive_locks_block_non_locking_reads",
	"dictates the locking interactions between exclusive locks and non-locking reads",
	true,
).WithPublic()

// MaxDurability is the maximum value in the Durability enum.
const MaxDurability = Unreplicated

func init() {
	for v := range Durability_name {
		if d := Durability(v); d > MaxDurability {
			panic(fmt.Sprintf("Durability (%s) with value larger than MaxDurability", d))
		}
	}
}

// CheckLockConflicts returns whether the supplied lock strength conflicts with
// the receiver. The conflict rules are as described in the compatibility matrix
// in locking.pb.go.
func (s *Strength) CheckLockConflicts(sv *settings.Values, o *Strength) bool {
	return !s.CheckLockCompatibility(sv, o)
}

// CheckLockCompatibility returns whether the supplied lock strength is
// compatible with the receiver. Given a key is locked with the receiver's lock
// strength, the return value indicates whether an operation with locking
// intentions of the supplied lock strength can proceed or not.
//
// The compatibility rules are as illustrated in the matrix in locking.pb.go.
func (s *Strength) CheckLockCompatibility(sv *settings.Values, o *Strength) bool {
	switch s.Mode {
	case None:
		return true
	case Shared:
		return o.Mode == None || o.Mode == Shared
	case Upgrade:
		return o.Mode == None
	case Exclusive:
		if ExclusiveLocksBlockNonLockingReads.Get(sv) {
			return o.Mode == None && o.Timestamp.Less(s.Timestamp)
		}
		return o.Mode == None
	case Intent:
		return o.Mode == None && o.Timestamp.Less(s.Timestamp)
	default:
		panic(errors.AssertionFailedf("unknown mode: %s", s.Mode))
	}
}

// StrengthOption optionally modify the construction of lock Strengths.
type StrengthOption func(*Strength)

// AtTimestamp associates a timestamp with a Strength. Should only be used in
// conjunction with None, Exclusive, and Intent lock modes.
func AtTimestamp(ts hlc.Timestamp) StrengthOption {
	return func(s *Strength) {
		s.Timestamp = ts
	}
}

// MakeStrength constructs a well-formed lock Strength.
func MakeStrength(mode LockMode, opts ...StrengthOption) Strength {
	s := Strength{
		Mode: mode,
	}
	for _, opt := range opts {
		opt(&s)
	}
	if err := s.validate(); err != nil {
		panic(err)
	}
	return s
}

// validate ensures the Strength is well-formed.
func (s *Strength) validate() error {
	switch s.Mode {
	case None, Exclusive, Intent:
		if s.Timestamp.IsEmpty() {
			return errors.AssertionFailedf("mode %s must have a non-empty timestamp", s.Mode)
		}
		return nil
	case Shared, Upgrade:
		if !s.Timestamp.IsEmpty() {
			return errors.AssertionFailedf("mode %s must have a empty timestamp", s.Mode)
		}
		return nil
	default:
		return errors.AssertionFailedf("unknown mode: %s", s.Mode)
	}
}

// SafeValue implements redact.SafeValue.
func (LockMode) SafeValue() {}

// SafeValue implements redact.SafeValue.
func (s Strength) SafeValue() {}

// SafeValue implements redact.SafeValue.
func (Durability) SafeValue() {}

// SafeValue implements redact.SafeValue.
func (WaitPolicy) SafeValue() {}
