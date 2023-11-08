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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
