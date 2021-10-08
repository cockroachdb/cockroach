// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"fmt"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
		{key: LockTableKey{Key: roachpb.Key("foo"), Strength: lock.Exclusive, TxnUUID: uuid1[:]}},
		{key: LockTableKey{Key: roachpb.Key("a"), Strength: lock.Exclusive, TxnUUID: uuid2[:]}},
		// Causes a doubly-local range local key.
		{key: LockTableKey{
			Key:      keys.RangeDescriptorKey(roachpb.RKey("baz")),
			Strength: lock.Exclusive,
			TxnUUID:  uuid1[:]}},
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
			encodedTS := encodeTimestamp(test.key.Timestamp)
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
			b3 := EncodeKey(test.key)
			require.Equal(t, b3, b1)
			k3, ts, ok := enginepb.SplitMVCCKey(b3)
			require.True(t, ok)
			require.Equal(t, k3, []byte(test.key.Key))
			require.Equal(t, ts, encodedTS)
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
				Strength: lock.Exclusive,
				TxnUUID:  uuid1[:],
			},
		},
		{
			key: LockTableKey{
				Key:      keys.RangeDescriptorKey(roachpb.RKey("bar")),
				Strength: lock.Exclusive,
				TxnUUID:  uuid1[:],
			},
		},

		// Invalid keys - versions of various invalid lengths.
		{key: EngineKey{Key: roachpb.Key("foo"), Version: make([]byte, 1)}, invalid: true},
		{key: EngineKey{Key: roachpb.Key("bar"), Version: make([]byte, 7)}, invalid: true},
		{key: EngineKey{Key: roachpb.Key("baz"), Version: make([]byte, 14)}, invalid: true},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			var ek EngineKey
			switch k := tc.key.(type) {
			case EngineKey:
				ek = k
			case MVCCKey:
				ek = EngineKey{Key: k.Key, Version: encodeTimestamp(k.Timestamp)}
			case LockTableKey:
				key, _ := k.ToEngineKey(nil)
				ek = key
			}

			if err := ek.Validate(); err != nil {
				require.True(t, tc.invalid)
				return
			}
			require.False(t, tc.invalid)
		})
	}
}
