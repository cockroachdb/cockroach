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
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			eKey := test.key.ToEngineKey()
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
			eKeyDecoded, ok := DecodeEngineKey(b2)
			require.True(t, ok)
			require.True(t, eKeyDecoded.IsLockTableKey())
			require.False(t, eKeyDecoded.IsMVCCKey())
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
		{key: MVCCKey{Key: roachpb.Key("foo"), Timestamp: hlc.Timestamp{WallTime: 99, Logical: 45}}},
		{key: MVCCKey{Key: roachpb.Key("glue"), Timestamp: hlc.Timestamp{WallTime: 89999}}},
		{key: MVCCKey{Key: roachpb.Key("a")}},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			var encodedTS []byte
			if !(test.key.Timestamp == hlc.Timestamp{}) {
				if test.key.Timestamp.Logical == 0 {
					encodedTS = make([]byte, 8)
				} else {
					encodedTS = make([]byte, 12)
				}
				binary.BigEndian.PutUint64(encodedTS, uint64(test.key.Timestamp.WallTime))
				if test.key.Timestamp.Logical != 0 {
					binary.BigEndian.PutUint32(encodedTS[8:], uint32(test.key.Timestamp.Logical))
				}
			}
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
			eKeyDecoded, ok := DecodeEngineKey(b2)
			require.True(t, ok)
			require.False(t, eKeyDecoded.IsLockTableKey())
			require.True(t, eKeyDecoded.IsMVCCKey())
			keyDecoded, err := eKeyDecoded.ToMVCCKey()
			require.NoError(t, err)
			require.Equal(t, test.key, keyDecoded)
		})
	}
}
