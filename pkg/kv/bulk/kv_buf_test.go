// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"bytes"
	"context"
	"math"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// kvPair is a bytes -> bytes kv pair.
type kvPair struct {
	key   roachpb.Key
	value []byte
}

func makeTestData(num int) (kvs []kvPair, totalSize sz) {
	kvs = make([]kvPair, num)
	r, _ := randutil.NewTestRand()
	alloc := make([]byte, num*500)
	randutil.ReadTestdataBytes(r, alloc)
	for i := range kvs {
		if len(alloc) < 1500 {
			const refill = 15000
			alloc = make([]byte, refill)
			randutil.ReadTestdataBytes(r, alloc)
		}
		kvs[i].key = alloc[:randutil.RandIntInRange(r, 2, 100)]
		alloc = alloc[len(kvs[i].key):]
		kvs[i].value = alloc[:randutil.RandIntInRange(r, 0, 1000)]
		alloc = alloc[len(kvs[i].value):]
		totalSize += sz(len(kvs[i].key) + len(kvs[i].value))
	}
	return kvs, totalSize
}

func TestKvBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()

	src, totalSize := makeTestData(50000)

	ctx := context.Background()
	noneMonitor := mon.NewMonitorWithLimit("none", mon.MemoryResource, 0, nil, nil, 0, 0, nil)
	noneMonitor.StartNoReserved(ctx, nil /* pool */)
	none := noneMonitor.MakeBoundAccount()
	lots := mon.NewUnlimitedMonitor(ctx, "lots", mon.MemoryResource, nil, nil, 0, nil).MakeBoundAccount()

	// Write everything to our buf.
	b := kvBuf{compareKeyFunc: func(left, right roachpb.Key) int {
		return left.Compare(right)
	}}
	for i := range src {
		size := sz(len(src[i].key) + len(src[i].value))
		fits := len(b.entries) <= cap(b.entries) && len(b.slab)+int(size) <= cap(b.slab)

		require.Equal(t, fits, b.fits(ctx, size, 0, &none))
		if !fits {
			// Ensure trying to grow with either, but not both, allowing fails.
			require.False(t, b.fits(ctx, size, 0, &lots))
			require.False(t, b.fits(ctx, size, math.MaxInt64, &none))
			// Allow it to grow and then re-check a no-alloc fits call.
			require.True(t, b.fits(ctx, size, math.MaxInt64, &lots))
			require.True(t, b.fits(ctx, size, 0, &none))
		}
		before := b.MemSize()
		if err := b.append(src[i].key, src[i].value); err != nil {
			t.Fatal(err)
		}
		require.Equal(t, before, b.MemSize())
	}

	// Sanity check our buf has right size.
	if expected, actual := len(src), b.Len(); expected != actual {
		t.Fatalf("expected len %d got %d", expected, actual)
	}
	if expected, actual := totalSize, b.KVSize(); expected != actual {
		t.Fatalf("expected len %d got %d", expected, actual)
	}
	if expected, actual := cap(b.entries)*16+cap(b.slab), int(b.MemSize()); expected != actual {
		t.Fatalf("expected len %d got %d", expected, actual)
	}

	// Read back what we wrote.
	for i := range src {
		if expected, actual := src[i].key, b.Key(i); !bytes.Equal(expected, actual) {
			t.Fatalf("expected %s\ngot %s", expected, actual)
		}
		if expected, actual := src[i].value, b.Value(i); !bytes.Equal(expected, actual) {
			t.Fatalf("expected %s\ngot %s", expected, actual)
		}
	}
	// Sort both and then ensure they match.
	sort.Slice(src, func(i, j int) bool { return bytes.Compare(src[i].key, src[j].key) < 0 })
	sort.Sort(&b)
	for i := range src {
		if expected, actual := src[i].key, b.Key(i); !bytes.Equal(expected, actual) {
			t.Fatalf("expected %s\ngot %s", expected, actual)
		}
		if expected, actual := src[i].value, b.Value(i); !bytes.Equal(expected, actual) {
			t.Fatalf("expected %s\ngot %s", expected, actual)
		}
	}
}

func TestKvBufWithTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := kvBuf{compareKeyFunc: CompareTimestampedKey}
	r, _ := randutil.NewTestRand()
	randomValue := func() []byte {
		valBytes := make([]byte, randutil.RandIntInRange(r, 0, 1000))
		randutil.ReadTestdataBytes(r, valBytes)
		return valBytes
	}

	assertKeyValue := func(i int, expectedKey roachpb.Key, expectedVal []byte) {
		require.Equal(t, expectedKey, b.Key(i))
		require.Equal(t, expectedVal, b.Value(i))
	}

	key1, val1 := EncodeTimestampKey(roachpb.Key("b"), hlc.Timestamp{WallTime: 1, Logical: 2}), randomValue()
	require.NoError(t, b.append(key1, val1))
	key2, val2 := EncodeTimestampKey(roachpb.Key("a"), hlc.Timestamp{WallTime: 1, Logical: 1}), randomValue()
	require.NoError(t, b.append(key2, val2))
	key3, val3 := EncodeTimestampKey(roachpb.Key("b"), hlc.Timestamp{WallTime: 1, Logical: 1}), randomValue()
	require.NoError(t, b.append(key3, val3))
	key4, val4 := EncodeTimestampKey(roachpb.Key("a"), hlc.Timestamp{WallTime: 2, Logical: 1}), randomValue()
	require.NoError(t, b.append(key4, val4))

	// key2 < key4 < key 3 < key1
	sort.Sort(&b)
	require.Equal(t, 4, b.Len())
	assertKeyValue(0, key2, val2)
	assertKeyValue(1, key4, val4)
	assertKeyValue(2, key3, val3)
	assertKeyValue(3, key1, val1)
}
