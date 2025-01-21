// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func encodeLockTableKey(ltk LockTableKey) []byte {
	ek, _ := ltk.ToEngineKey(nil)
	return ek.Encode()
}

// interestingEngineKeys is a slice of byte slices that may be used in tests as
// engine keys. Not all the keys are valid keys.
var interestingEngineKeys = [][]byte{
	{0x00, 0x00},
	{0x01, 0x11, 0x01},
	EncodeMVCCKey(MVCCKey{Key: roachpb.Key("a"), Timestamp: hlc.Timestamp{WallTime: math.MaxInt64}}),
	EncodeMVCCKey(MVCCKey{Key: roachpb.Key("foo"), Timestamp: hlc.Timestamp{WallTime: 1691183078362053000}}),
	EncodeMVCCKey(MVCCKey{Key: roachpb.Key("bar")}),
	EncodeMVCCKey(MVCCKey{Key: roachpb.Key("bar"), Timestamp: hlc.Timestamp{WallTime: 1643550788737652545}}),
	EncodeMVCCKey(MVCCKey{Key: roachpb.Key("bar"), Timestamp: hlc.Timestamp{WallTime: 1643550788737652545, Logical: 1}}),
	encodeLockTableKey(LockTableKey{
		Key:      roachpb.Key("foo"),
		Strength: lock.Exclusive,
		TxnUUID:  uuid.Must(uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")),
	}),
	encodeLockTableKey(LockTableKey{
		Key:      keys.RangeDescriptorKey(roachpb.RKey("baz")),
		Strength: lock.Exclusive,
		TxnUUID:  uuid.Must(uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")),
	}),
}

// FuzzEngineKeysInvariants fuzz tests various functions over engine keys,
// ensuring that invariants over engine keys hold.
func FuzzEngineKeysInvariants(f *testing.F) {
	for i := 0; i < len(interestingEngineKeys); i++ {
		for j := 0; j < len(interestingEngineKeys); j++ {
			f.Add(interestingEngineKeys[i], interestingEngineKeys[j])
		}
	}

	compareEngineKeys := func(t *testing.T, a, b []byte) int {
		cmp := EngineComparer.Compare(a, b)
		eq := EngineComparer.Equal(a, b)
		// Invariant: Iff EngineKeyCompare(a, b) == 0, EngineKeyEqual(a, b)
		if eq != (cmp == 0) {
			t.Errorf("EngineKeyEqual(0x%x, 0x%x) = %t; EngineKeyCompare(0x%x, 0x%x) = %d",
				a, b, eq, a, b, cmp)
		}
		return cmp
	}
	computeImmediateSuccessor := func(t *testing.T, a []byte) []byte {
		succ := EngineComparer.ImmediateSuccessor(nil, a)
		// Invariant: ImmediateSuccessor(a) > a
		if cmp := compareEngineKeys(t, a, succ); cmp >= 0 {
			t.Errorf("ImmediateSuccessor(0x%x) = 0x%x, but EngineKeyCompare(0x%x, 0x%x) = %d",
				a, succ, a, succ, cmp)
		}
		return succ
	}
	decodeEngineKey := func(t *testing.T, a []byte) (EngineKey, bool) {
		// Invariant: DecodeEngineKey(a) ok iff GetKeyPartFromEngineKey(a) ok
		// Invariant: DecodeEngineKey(a).Key == GetKeyPartFromEngineKey(a)
		ek, ok1 := DecodeEngineKey(a)
		kp, ok2 := GetKeyPartFromEngineKey(a)
		if ok1 != ok2 || ok1 && !bytes.Equal(ek.Key, kp) {
			t.Errorf("DecodeEngineKey(0x%x) = (%s, %t); but GetKeyPartFromEngineKey(0x%x) = (0x%x, %t)",
				a, ek, ok1, a, kp, ok2)
		}

		return ek, ok1
	}

	f.Fuzz(func(t *testing.T, a []byte, b []byte) {
		t.Logf("a = 0x%x; b = 0x%x", a, b)
		// We can only pass valid keys to the comparer.
		ekA, okA := decodeEngineKey(t, a)
		ekB, okB := decodeEngineKey(t, b)
		if !okA || !okB {
			return
		}
		errA := ekA.Validate()
		errB := ekB.Validate()
		if errA != nil || errB != nil {
			return
		}
		cmp := compareEngineKeys(t, a, b)
		if cmp == 0 {
			return
		}
		if len(a) == 0 || len(b) == 0 {
			return
		}

		// Make a < b.
		if cmp > 0 {
			a, b = b, a
			t.Logf("Swapped: a = 0x%x; b = 0x%x", a, b)
		}
		// Invariant: Separator(a, b) >= a
		// Invariant: Separator(a, b) < b
		sep := EngineComparer.Separator(nil, a, b)
		if cmp = compareEngineKeys(t, a, b); cmp > 0 {
			t.Errorf("Separator(0x%x, 0x%x) = 0x%x; but EngineKeyCompare(0x%x, 0x%x) = %d",
				a, b, sep, a, sep, cmp)
		}
		if cmp = compareEngineKeys(t, sep, b); cmp >= 0 {
			t.Errorf("Separator(0x%x, 0x%x) = 0x%x; but EngineKeyCompare(0x%x, 0x%x) = %d",
				a, b, sep, sep, b, cmp)
		}
		t.Logf("ekA = %s (Key: 0x%x, Version: 0x%x); ekB = %s (Key: 0x%x, Version: 0x%x)",
			ekA, ekA.Key, ekA.Version, ekB, ekB.Key, ekB.Version)

		splitA := EngineComparer.Split(a)
		splitB := EngineComparer.Split(b)
		aIsSuffixless := splitA == len(a)
		bIsSuffixless := splitB == len(b)
		// ImmediateSuccessor is only defined on prefix keys.
		var immediateSuccessorA, immediateSuccessorB []byte
		if aIsSuffixless {
			immediateSuccessorA = computeImmediateSuccessor(t, a)
		}
		if bIsSuffixless {
			immediateSuccessorB = computeImmediateSuccessor(t, b)
		}
		if aIsSuffixless && bIsSuffixless {
			// Invariant: ImmediateSuccessor(a) < ImmediateSuccessor(b)
			if cmp = compareEngineKeys(t, immediateSuccessorA, immediateSuccessorB); cmp >= 0 {
				t.Errorf("ImmediateSuccessor(0x%x) = 0x%x, ImmediateSuccessor(0x%x) = 0x%x; but EngineKeyCompare(0x%x, 0x%x) = %d",
					a, immediateSuccessorA, b, immediateSuccessorB, immediateSuccessorA, immediateSuccessorB, cmp)
			}
		}
	})
}
