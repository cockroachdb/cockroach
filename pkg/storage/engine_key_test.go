// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"encoding/hex"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

// TestMVCCAndEngineKeyDecodeSyntheticTimestamp tests decoding an MVCC key with
// a synthetic timestamp. The synthetic timestamp bit is now ignored during key
// encoding and decoding, but synthetic timestamps may still be present in the
// wild, so they must not confuse decoding.
func TestMVCCAndEngineKeyDecodeSyntheticTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// key := MVCCKey{Key: roachpb.Key("bar"), Timestamp: hlc.Timestamp{WallTime: 99, Logical: 45, Synthetic: true}}
	keyNoSynthetic := MVCCKey{Key: roachpb.Key("bar"), Timestamp: hlc.Timestamp{WallTime: 99, Logical: 45}}

	// encodedStr was computed from key using a previous version of the code that
	// that included synthetic timestamps in the MVCC key encoding.
	encodedStr := "6261720000000000000000630000002d010e"
	encoded, err := hex.DecodeString(encodedStr)
	require.NoError(t, err)

	// Decode to demonstrate that the synthetic timestamp can be decoded.
	eKeyDecoded, ok := DecodeEngineKey(encoded)
	require.True(t, ok)
	require.False(t, eKeyDecoded.IsLockTableKey())
	require.True(t, eKeyDecoded.IsMVCCKey())
	require.NoError(t, eKeyDecoded.Validate())
	keyDecoded, err := eKeyDecoded.ToMVCCKey()
	require.NoError(t, err)
	require.Equal(t, keyNoSynthetic, keyDecoded)
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

func TestEngineKeyVerifyMVCC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := NewDefaultInMemForTesting()
	defer eng.Close()

	b := eng.NewBatch()
	defer b.Close()

	// Test MVCC Key.
	k := pointKey("mvccKey", 1)
	v := MVCCValue{Value: roachpb.MakeValueFromString("test")}
	v.Value.InitChecksum(k.Key)
	require.NoError(t, b.PutMVCC(k, v))
	r, err := NewBatchReader(b.Repr())
	require.NoError(t, err)
	require.True(t, r.Next())
	ek, _ := r.EngineKey()
	require.NoError(t, ek.Verify(r.Value()))
	// Simulate data corruption
	r.value[len(r.value)-1]++
	require.ErrorContains(t, ek.Verify(r.Value()), "invalid checksum")
}

func TestEngineKeyVerifyLockTableKV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := NewDefaultInMemForTesting()
	defer eng.Close()

	b := eng.NewBatch()
	defer b.Close()

	// Test Lock table Key.
	lk := LockTableKey{
		Key:      keys.RangeDescriptorKey(roachpb.RKey("baz")), // causes a doubly-local range local key
		Strength: lock.Exclusive,
		TxnUUID:  uuid.MakeV4(),
	}
	mvccValue := MVCCValue{
		Value: roachpb.MakeValueFromString("test"),
	}
	mvccValue.Value.InitChecksum(lk.Key)
	ek, _ := lk.ToEngineKey(nil)
	meta := enginepb.MVCCMetadata{
		RawBytes: mvccValue.Value.RawBytes,
	}
	buf := make([]byte, meta.Size())
	n, _ := protoutil.MarshalToSizedBuffer(&meta, buf)
	require.NoError(t, b.PutEngineKey(ek, buf[:n]))
	r, err := NewBatchReader(b.Repr())
	require.NoError(t, err)
	require.True(t, r.Next())
	require.NoError(t, ek.Verify(r.Value()))
	// Simulate data corruption
	r.value[len(r.value)-1]++
	require.ErrorContains(t, ek.Verify(r.Value()), "invalid checksum")
}

// BenchmarkEngineKeyVerify evaluates EngineKey's performance
// on verifying checksums on different keys value types.
// Example results with apple m3 cpu:
// BenchmarkEngineKeyVerify/SimpleMVCCValue
// BenchmarkEngineKeyVerify/SimpleMVCCValue-12         	  662324	      1806 ns/op
// BenchmarkEngineKeyVerify/ExtendedMVCCValue
// BenchmarkEngineKeyVerify/ExtendedMVCCValue-12       	  645627	      1817 ns/op
// BenchmarkEngineKeyVerify/LockTableKey
// BenchmarkEngineKeyVerify/LockTableKey-12            	  587613	      2024 ns/op
func BenchmarkEngineKeyVerify(b *testing.B) {
	defer log.Scope(b).Close(b)

	type testKV struct {
		ek  EngineKey
		val []byte
	}
	type testCase struct {
		name string
		testKV
	}

	mustDecodeEngineKey := func(raw []byte) EngineKey {
		ek, ok := DecodeEngineKey(raw)
		require.True(b, ok)
		return ek
	}
	mustEncodeMVCC := func(key MVCCKey, v MVCCValue) testKV {
		encodedKey := EncodeMVCCKey(key)
		encodedVal, err := EncodeMVCCValue(v)
		require.NoError(b, err)
		return testKV{ek: mustDecodeEngineKey(encodedKey), val: encodedVal}
	}
	mustEncodeLockTable := func(key LockTableKey, meta *enginepb.MVCCMetadata, v MVCCValue) testKV {
		encodedMVCCValue, err := EncodeMVCCValue(v)
		require.NoError(b, err)
		meta.RawBytes = encodedMVCCValue
		encodedVal, err := protoutil.Marshal(meta)
		require.NoError(b, err)
		ek, _ := key.ToEngineKey(nil)
		return testKV{ek: ek, val: encodedVal}
	}

	testCases := []testCase{
		{
			name: "SimpleMVCCValue",
			testKV: mustEncodeMVCC(
				MVCCKey{
					Key:       roachpb.Key("foobar"),
					Timestamp: hlc.Timestamp{WallTime: 1711383740550067000, Logical: 2},
				},
				MVCCValue{Value: roachpb.Value{RawBytes: []byte("hello world")}},
			),
		},
		{
			name: "ExtendedMVCCValue",
			testKV: mustEncodeMVCC(
				MVCCKey{
					Key:       roachpb.Key("foobar"),
					Timestamp: hlc.Timestamp{WallTime: 1711383740550067000, Logical: 2},
				},
				MVCCValue{
					MVCCValueHeader: enginepb.MVCCValueHeader{
						LocalTimestamp:   hlc.ClockTimestamp{WallTime: 1711383740550069000},
						OmitInRangefeeds: true,
					},
					Value: roachpb.Value{RawBytes: []byte("hello world")},
				},
			),
		},
		{
			name: "LockTableKey",
			testKV: mustEncodeLockTable(
				LockTableKey{
					Key:      roachpb.Key("foobar"),
					Strength: lock.Exclusive,
					TxnUUID:  uuid.UUID{},
				},
				&enginepb.MVCCMetadata{
					KeyBytes: 100,
					ValBytes: 100,
				},
				MVCCValue{
					Value: roachpb.Value{RawBytes: []byte("hello world")},
				},
			),
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = tc.ek.Verify(tc.val)
			}
		})
	}
}

func randomMVCCKey(r *rand.Rand) MVCCKey {
	k := MVCCKey{
		Key:       randutil.RandBytes(r, randutil.RandIntInRange(r, 1, 12)),
		Timestamp: hlc.Timestamp{WallTime: r.Int63()},
	}
	if r.Intn(2) == 1 {
		k.Timestamp.Logical = r.Int31()
	}
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

var possibleVersionLens = []int{
	engineKeyNoVersion,
	engineKeyVersionWallTimeLen,
	engineKeyVersionWallAndLogicalTimeLen,
	engineKeyVersionWallLogicalAndSyntheticTimeLen,
	engineKeyVersionLockTableLen,
}

func randomSerializedEngineKey(r *rand.Rand, maxUserKeyLen int) []byte {
	userKeyLen := randutil.RandIntInRange(r, 1, maxUserKeyLen)
	versionLen := possibleVersionLens[r.Intn(len(possibleVersionLens))]
	serializedLen := userKeyLen + versionLen + 1
	if versionLen > 0 {
		serializedLen++ // sentinel
	}
	k := randutil.RandBytes(r, serializedLen)
	k[userKeyLen] = 0x00
	if versionLen > 0 {
		k[len(k)-1] = byte(versionLen + 1)
	}
	return k
}

var _ = randomSerializedEngineKey
