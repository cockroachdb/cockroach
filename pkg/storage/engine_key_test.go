// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestLockTableKeyEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	uuid1 := uuid.Must(uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
	uuid2 := uuid.Must(uuid.FromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8"))
	testCases := []struct {
		key LockTableKey
	}{
		// Shared locks.
		{key: LockTableKey{Key: roachpb.Key("foo"), Strength: lock.Shared, TxnUUID: uuid1}},
		{key: LockTableKey{Key: roachpb.Key("a"), Strength: lock.Shared, TxnUUID: uuid2}},
		{key: LockTableKey{
			Key:      keys.RangeDescriptorKey(roachpb.RKey("baz")), // causes a doubly-local range local key
			Strength: lock.Shared,
			TxnUUID:  uuid1}},
		// Exclusive locks.
		{key: LockTableKey{Key: roachpb.Key("foo"), Strength: lock.Exclusive, TxnUUID: uuid1}},
		{key: LockTableKey{Key: roachpb.Key("a"), Strength: lock.Exclusive, TxnUUID: uuid2}},
		{key: LockTableKey{
			Key:      keys.RangeDescriptorKey(roachpb.RKey("baz")), // causes a doubly-local range local key
			Strength: lock.Exclusive,
			TxnUUID:  uuid1}},
		// Intent locks.
		{key: LockTableKey{Key: roachpb.Key("foo"), Strength: lock.Intent, TxnUUID: uuid1}},
		{key: LockTableKey{Key: roachpb.Key("a"), Strength: lock.Intent, TxnUUID: uuid2}},
		{key: LockTableKey{
			Key:      keys.RangeDescriptorKey(roachpb.RKey("baz")), // causes a doubly-local range local key
			Strength: lock.Intent,
			TxnUUID:  uuid1}},
	}
	buf := make([]byte, 100)
	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			eKey, buf2 := test.key.ToEngineKey(nil)
			// buf2 should be larger than the non-versioned part.
			require.Less(t, len(eKey.Key), cap(buf2))
			eKey2, buf3 := test.key.ToEngineKey(buf)
			// buf3 should be larger than the non-versioned part.
			require.Less(t, len(eKey.Key), cap(buf3))
			// buf is big enough for all keys we encode here, so buf3 should be the same as buf
			require.True(t, unsafe.Pointer(&buf3[0]) == unsafe.Pointer(&buf[0]))
			require.Equal(t, eKey, eKey2)

			b1 := eKey.Encode()
			require.Equal(t, len(b1), eKey.EncodedLen())
			var b2 []byte
			if rand.Intn(2) == 0 {
				b2 = make([]byte, len(b1)*2)
			}
			b2 = eKey.EncodeToBuf(b2)
			require.Equal(t, b1, b2)
			require.True(t, eKey.IsLockTableKey())
			require.False(t, eKey.IsMVCCKey())
			require.NoError(t, eKey.Validate())
			eKeyDecoded, ok := DecodeEngineKey(b2)
			require.True(t, ok)
			keyPart, ok := GetKeyPartFromEngineKey(b2)
			require.True(t, ok)
			require.Equal(t, eKeyDecoded.Key, roachpb.Key(keyPart))
			require.True(t, eKeyDecoded.IsLockTableKey())
			require.False(t, eKeyDecoded.IsMVCCKey())
			require.NoError(t, eKeyDecoded.Validate())
			keyDecoded, err := eKeyDecoded.ToLockTableKey()
			require.NoError(t, err)
			require.Equal(t, test.key, keyDecoded)
		})
	}
}

// TestLockTableKeyMixedVersionV23_123_2 ensures a lock table key written by a
// <= v23.1 node can be decoded by a 23.2 node and a lock table key written by
// a 23.2 node cna be decoded by a 23.1 node.
func TestLockTableKeyMixedVersionV23_1V23_2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	uuid := uuid.MakeV4()
	t.Run("decode_v23_1", func(t *testing.T) {
		key := LockTableKey{
			Key:      roachpb.Key("foo"),
			Strength: lock.Intent, // strength corresponding to an intent written by a 23.2 node
			TxnUUID:  uuid,
		}

		eKey, _ := key.ToEngineKey(nil)
		require.True(t, eKey.IsLockTableKey())
		eKeyDecoded, ok := DecodeEngineKey(eKey.Encode())
		require.True(t, ok)
		require.True(t, eKeyDecoded.IsLockTableKey())
		keyDecoded, err := eKeyDecoded.ToLockTableKey()
		require.NoError(t, err)
		// v23.1 nodes expect intents to have 3 as the strength byte.
		require.Equal(t, byte(3), getByteForReplicatedLockStrength(keyDecoded.Strength))
	})

	t.Run("encode_v23_1", func(t *testing.T) {
		key := LockTableKey{
			Key:      roachpb.Key("foo"),
			Strength: mustGetReplicatedLockStrengthForByte(3), // strength byte used by v23.1 nodes
			TxnUUID:  uuid,
		}
		eKey, _ := key.ToEngineKey(nil)
		require.True(t, eKey.IsLockTableKey())
		eKeyDecoded, ok := DecodeEngineKey(eKey.Encode())
		require.True(t, ok)
		require.True(t, eKeyDecoded.IsLockTableKey())
		keyDecoded, err := eKeyDecoded.ToLockTableKey()
		require.NoError(t, err)
		require.Equal(t, lock.Intent, keyDecoded.Strength)
	})
}

func TestMVCCAndEngineKeyEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		key MVCCKey
	}{
		{key: MVCCKey{Key: roachpb.Key("a")}},
		{key: MVCCKey{Key: roachpb.Key("glue"), Timestamp: hlc.Timestamp{WallTime: 89999}}},
		{key: MVCCKey{Key: roachpb.Key("foo"), Timestamp: hlc.Timestamp{WallTime: 99, Logical: 45}}},
		{key: MVCCKey{Key: roachpb.Key("bar"), Timestamp: hlc.Timestamp{WallTime: 99, Logical: 45, Synthetic: true}}},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			encodedTS := encodeMVCCTimestamp(test.key.Timestamp)
			eKey := EngineKey{Key: test.key.Key, Version: encodedTS}
			b1 := eKey.Encode()
			require.Equal(t, len(b1), eKey.EncodedLen())
			var b2 []byte
			if rand.Intn(2) == 0 {
				b2 = make([]byte, len(b1)*2)
			}
			b2 = eKey.EncodeToBuf(b2)
			require.Equal(t, b1, b2)
			require.False(t, eKey.IsLockTableKey())
			require.True(t, eKey.IsMVCCKey())
			require.NoError(t, eKey.Validate())
			eKeyDecoded, ok := DecodeEngineKey(b2)
			require.True(t, ok)
			require.False(t, eKeyDecoded.IsLockTableKey())
			require.True(t, eKeyDecoded.IsMVCCKey())
			require.NoError(t, eKeyDecoded.Validate())
			keyDecoded, err := eKeyDecoded.ToMVCCKey()
			require.NoError(t, err)
			require.Equal(t, test.key, keyDecoded)
			keyPart, ok := GetKeyPartFromEngineKey(b2)
			require.True(t, ok)
			require.Equal(t, eKeyDecoded.Key, roachpb.Key(keyPart))
			b3 := EncodeMVCCKey(test.key)
			require.Equal(t, b3, b1)
			k3, ts, ok := enginepb.SplitMVCCKey(b3)
			require.True(t, ok)
			require.Equal(t, k3, []byte(test.key.Key))
			require.Equal(t, ts, encodedTS)
			k, ok := DecodeEngineKey(b3)
			require.True(t, ok)
			require.Equal(t, k.Key, test.key.Key)
			require.Equal(t, k.Version, encodedTS)
		})
	}
}

func TestEngineKeyValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	uuid1 := uuid.Must(uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
	testCases := []struct {
		key     interface{}
		invalid bool
	}{
		// Valid MVCCKeys.
		{key: MVCCKey{Key: roachpb.Key("a")}},
		{key: MVCCKey{Key: roachpb.Key("glue"), Timestamp: hlc.Timestamp{WallTime: 89999}}},
		{key: MVCCKey{Key: roachpb.Key("foo"), Timestamp: hlc.Timestamp{WallTime: 99, Logical: 45}}},
		{key: MVCCKey{Key: roachpb.Key("bar"), Timestamp: hlc.Timestamp{WallTime: 99, Logical: 45, Synthetic: true}}},

		// Valid LockTableKeys.
		{
			key: LockTableKey{
				Key:      roachpb.Key("foo"),
				Strength: lock.Intent,
				TxnUUID:  uuid1,
			},
		},
		{
			key: LockTableKey{
				Key:      keys.RangeDescriptorKey(roachpb.RKey("bar")),
				Strength: lock.Intent,
				TxnUUID:  uuid1,
			},
		},

		// Invalid keys - versions of various invalid lengths.
		{key: EngineKey{Key: roachpb.Key("foo"), Version: make([]byte, 1)}, invalid: true},
		{key: EngineKey{Key: roachpb.Key("bar"), Version: make([]byte, 7)}, invalid: true},
		{key: EngineKey{Key: roachpb.Key("baz"), Version: make([]byte, 14)}, invalid: true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.key), func(t *testing.T) {
			var ek EngineKey
			switch k := tc.key.(type) {
			case EngineKey:
				ek = k
			case MVCCKey:
				ek = EngineKey{Key: k.Key, Version: encodeMVCCTimestamp(k.Timestamp)}
			case LockTableKey:
				key, _ := k.ToEngineKey(nil)
				ek = key
			}

			err := ek.Validate()
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Ensure EngineKey.Validate doesn't allocate for valid keys.
				// Regression test for #106464.
				require.Equal(t, 0.0, testing.AllocsPerRun(1, func() {
					_ = ek.Validate()
				}))
			}
		})
	}

	// Test randomly generated valid keys.
	rng, _ := randutil.NewTestRand()
	t.Run("randomized", func(t *testing.T) {
		t.Run("MVCCKey", func(t *testing.T) {
			for i := 0; i < 100; i++ {
				k := randomMVCCKey(rng)
				ek := EngineKey{Key: k.Key, Version: encodeMVCCTimestamp(k.Timestamp)}
				if err := ek.Validate(); err != nil {
					t.Errorf("%q.Validate() = %q, want nil", ek, err)
				}
				require.Equal(t, 0.0, testing.AllocsPerRun(1000, func() {
					_ = ek.Validate()
				}))
			}
		})
		t.Run("LockTableKey", func(t *testing.T) {
			var buf []byte
			for i := 0; i < 100; i++ {
				k := randomLockTableKey(rng)
				ek, _ := k.ToEngineKey(buf)
				if err := ek.Validate(); err != nil {
					t.Errorf("%q.Validate() = %q, want nil", ek, err)
				}
				require.Equal(t, 0.0, testing.AllocsPerRun(1000, func() {
					_ = ek.Validate()
				}))
				buf = ek.Key[:0]
			}
		})
	})
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
	EncodeMVCCKey(MVCCKey{Key: roachpb.Key("bar"), Timestamp: hlc.Timestamp{WallTime: 1643550788737652545, Logical: 1, Synthetic: true}}),
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
		cmp := EngineKeyCompare(a, b)
		eq := EngineKeyEqual(a, b)
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

	f.Fuzz(func(t *testing.T, a, b []byte) {
		t.Logf("a = 0x%x; b = 0x%x", a, b)
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
		ekA, okA := decodeEngineKey(t, a)
		ekB, okB := decodeEngineKey(t, b)
		if !okA || !okB {
			return
		}
		errA := ekA.Validate()
		errB := ekB.Validate()
		// The below invariants only apply for valid keys.
		if errA != nil || errB != nil {
			return
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

func encodeLockTableKey(ltk LockTableKey) []byte {
	ek, _ := ltk.ToEngineKey(nil)
	return ek.Encode()
}

func randomMVCCKey(r *rand.Rand) MVCCKey {
	k := MVCCKey{
		Key:       randutil.RandBytes(r, randutil.RandIntInRange(r, 1, 12)),
		Timestamp: hlc.Timestamp{WallTime: r.Int63()},
	}
	if r.Intn(2) == 1 {
		k.Timestamp.Logical = r.Int31()
	}
	k.Timestamp.Synthetic = r.Intn(5) == 1
	return k
}

func randomLockTableKey(r *rand.Rand) LockTableKey {
	k := LockTableKey{
		Key:      randutil.RandBytes(r, randutil.RandIntInRange(r, 1, 12)),
		Strength: lock.Intent,
	}
	var txnID uuid.UUID
	txnID.DeterministicV4(r.Uint64(), math.MaxUint64)
	k.TxnUUID = txnID
	return k
}

func engineKey(key string, ts int) EngineKey {
	return EngineKey{
		Key:     roachpb.Key(key),
		Version: encodeMVCCTimestamp(wallTS(ts)),
	}
}
