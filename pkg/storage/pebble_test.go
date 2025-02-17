// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestEngineComparer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// encodeKey encodes a key. For the version, it supports arbitrary bytes or
	// hlc.Timestamp, using either the EngineKey or MVCCKey or encoder.
	encodeKey := func(key roachpb.Key, version any) []byte {
		switch t := version.(type) {
		case []byte:
			ek := EngineKey{Key: key, Version: t}
			return ek.Encode()
		case hlc.Timestamp:
			return EncodeMVCCKey(MVCCKey{Key: key, Timestamp: t})
		default:
			panic(t)
		}
	}
	encodeVersion := func(version any) []byte {
		kBare := encodeKey(roachpb.Key("foo"), hlc.Timestamp{})
		k := encodeKey(roachpb.Key("foo"), version)
		result, ok := bytes.CutPrefix(k, kBare)
		if !ok {
			panic(fmt.Sprintf("expected %s to have prefix %s", k, kBare))
		}
		return result
	}

	appendBytesToTimestamp := func(ts hlc.Timestamp, bytes []byte) []byte {
		suffix := encodeVersion(ts)
		// Strip off sentinel byte.
		version := suffix[:len(suffix)-1]
		return slices.Concat(version, bytes)
	}

	ts1 := hlc.Timestamp{}
	require.Len(t, encodeVersion(ts1), 0)
	ts2 := hlc.Timestamp{WallTime: 2, Logical: 1}
	ts3 := hlc.Timestamp{WallTime: 2}
	ts4 := hlc.Timestamp{WallTime: 1, Logical: 1}
	ts5 := hlc.Timestamp{WallTime: 1}

	syntheticBit := []byte{1}
	var zeroLogical [mvccEncodedTimeLogicalLen]byte
	ts2a := appendBytesToTimestamp(ts2, syntheticBit)
	ts3a := appendBytesToTimestamp(ts3, zeroLogical[:])
	ts3b := appendBytesToTimestamp(ts3, slices.Concat(zeroLogical[:], syntheticBit))

	// We group versions by equality and in the expected point key ordering.
	orderedVersions := [][]any{
		{ts1}, // Empty version sorts first.
		{ts2a, ts2},
		{ts3b, ts3a, ts3},
		{ts4},
		{ts5},
	}

	// Compare range suffixes.
	for i := range orderedVersions {
		for j := range orderedVersions {
			for _, v1 := range orderedVersions[i] {
				for _, v2 := range orderedVersions[j] {
					result := EngineComparer.ComparePointSuffixes(encodeVersion(v1), encodeVersion(v2))
					if expected := cmp.Compare(i, j); result != expected {
						t.Fatalf("CompareSuffixes(%x, %x) = %d, expected %d", v1, v2, result, expected)
					}
				}
			}
		}
	}

	// CompareRangeSuffixes has a more strict ordering.
	rangeOrderedVersions := []any{
		ts1,  // Empty version sorts first.
		ts2a, // Synthetic bit is not ignored when comparing range suffixes.
		ts2,
		ts3b, // Higher timestamps sort before lower timestamps.
		ts3a,
		ts3,
		ts4,
		ts5,
	}

	// Compare range suffixes.
	for i, v1 := range rangeOrderedVersions {
		for j, v2 := range rangeOrderedVersions {
			result := EngineComparer.CompareRangeSuffixes(encodeVersion(v1), encodeVersion(v2))
			if expected := cmp.Compare(i, j); result != expected {
				t.Fatalf("CompareSuffixes(%x, %x) = %d, expected %d", v1, v2, result, expected)
			}
		}
	}

	lock1 := bytes.Repeat([]byte{1}, engineKeyVersionLockTableLen)
	lock2 := bytes.Repeat([]byte{2}, engineKeyVersionLockTableLen)
	require.Equal(t, 0, EngineComparer.CompareRangeSuffixes(encodeVersion(lock1), encodeVersion(lock1)))
	require.Equal(t, 0, EngineComparer.CompareRangeSuffixes(encodeVersion(lock2), encodeVersion(lock2)))
	require.Equal(t, +1, EngineComparer.CompareRangeSuffixes(encodeVersion(lock1), encodeVersion(lock2)))
	require.Equal(t, -1, EngineComparer.CompareRangeSuffixes(encodeVersion(lock2), encodeVersion(lock1)))

	require.Equal(t, 0, EngineComparer.ComparePointSuffixes(encodeVersion(lock1), encodeVersion(lock1)))
	require.Equal(t, 0, EngineComparer.ComparePointSuffixes(encodeVersion(lock2), encodeVersion(lock2)))
	require.Equal(t, +1, EngineComparer.ComparePointSuffixes(encodeVersion(lock1), encodeVersion(lock2)))
	require.Equal(t, -1, EngineComparer.ComparePointSuffixes(encodeVersion(lock2), encodeVersion(lock1)))

	keys := []roachpb.Key{
		roachpb.Key(""),
		roachpb.Key("a"),
		roachpb.Key("bcd"),
		roachpb.Key("fg"),
	}

	// We group keys by equality and the groups are in the expected order.
	var orderedKeys [][][]byte
	for _, k := range keys {
		orderedKeys = append(orderedKeys,
			[][]byte{encodeKey(k, ts1)},
			[][]byte{encodeKey(k, ts2), encodeKey(k, ts2a)},
			[][]byte{encodeKey(k, ts3), encodeKey(k, ts3a), encodeKey(k, ts3b)},
			[][]byte{encodeKey(k, ts4)},
			[][]byte{encodeKey(k, ts5)},
		)
	}
	// Compare keys.
	for i := range orderedKeys {
		for j := range orderedKeys {
			for _, k1 := range orderedKeys[i] {
				for _, k2 := range orderedKeys[j] {
					result := EngineComparer.Compare(k1, k2)
					if expected := cmp.Compare(i, j); result != expected {
						t.Fatalf("Compare(%x, %x) = %d, expected %d", k1, k2, result, expected)
					}
				}
			}
		}
	}

	// Run the Pebble test suite to check the internal consistency of the comparator.
	var prefixes, suffixes [][]byte
	for _, k := range keys {
		prefixes = append(prefixes, encodeKey(k, ts1))
	}
	for _, v := range []any{ts1, ts2, ts2a, ts3, ts3a, ts3b, ts4, ts5, lock1, lock2} {
		suffixes = append(suffixes, encodeVersion(v))
	}
	require.NoError(t, pebble.CheckComparer(&EngineComparer, prefixes, suffixes))
}

func TestPebbleIterReuse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Regression test for https://github.com/cockroachdb/cockroach/issues/42354
	// and similar issues arising from improper re-initialization of cached
	// iterators.

	eng := createTestPebbleEngine()
	defer eng.Close()

	batch := eng.NewBatch()
	defer batch.Close()
	for i := 0; i < 100; i++ {
		key := MVCCKey{Key: []byte{byte(i)}, Timestamp: hlc.Timestamp{WallTime: 100}}
		value := MVCCValue{Value: roachpb.MakeValueFromString("foo")}
		if err := batch.PutMVCC(key, value); err != nil {
			t.Fatal(err)
		}
	}

	iter1, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{LowerBound: []byte{40}, UpperBound: []byte{50}})
	if err != nil {
		t.Fatal(err)
	}
	valuesCount := 0
	// Seek to a value before the lower bound. Identical to seeking to the lower bound.
	iter1.SeekGE(MVCCKey{Key: []byte{30}})
	for ; ; iter1.Next() {
		ok, err := iter1.Valid()
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		i := iter1.UnsafeKey().Key[0]
		if i < 40 || i >= 50 {
			t.Fatalf("iterator returned key out of bounds: %d", i)
		}

		valuesCount++
	}

	if valuesCount != 10 {
		t.Fatalf("expected 10 values, got %d", valuesCount)
	}
	iter1.Close()
	iter1 = nil

	// Create another iterator, with no lower bound but an upper bound that
	// is lower than the previous iterator's lower bound. This should still result
	// in the right amount of keys being returned; the lower bound from the
	// previous iterator should get zeroed.
	iter2, err := batch.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: []byte{10}})
	if err != nil {
		t.Fatal(err)
	}
	valuesCount = 0
	// This is a peculiar test that is disregarding how local and global keys
	// affect the behavior of MVCCIterators. This test is writing []byte{0}
	// which precedes the localPrefix. Ignore the local and preceding keys in
	// this seek.
	iter2.SeekGE(MVCCKey{Key: []byte{2}})
	for ; ; iter2.Next() {
		ok, err := iter2.Valid()
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}

		i := iter2.UnsafeKey().Key[0]
		if i >= 10 {
			t.Fatalf("iterator returned key out of bounds: %d", i)
		}
		valuesCount++
	}

	if valuesCount != 8 {
		t.Fatalf("expected 8 values, got %d", valuesCount)
	}
	iter2.Close()
}

func makeMVCCKey(a string) MVCCKey {
	return MVCCKey{Key: []byte(a)}
}

func TestPebbleSeparatorSuccessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sepCases := []struct {
		a, b, want MVCCKey
	}{
		// Many cases here are adapted from a Pebble unit test.

		// Non-empty b values.
		{makeMVCCKey("black"), makeMVCCKey("blue"), makeMVCCKey("blb")},
		{makeMVCCKey(""), makeMVCCKey("2"), makeMVCCKey("")},
		{makeMVCCKey("1"), makeMVCCKey("2"), makeMVCCKey("1")},
		{makeMVCCKey("1"), makeMVCCKey("29"), makeMVCCKey("2")},
		{makeMVCCKey("13"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("13"), makeMVCCKey("99"), makeMVCCKey("2")},
		{makeMVCCKey("135"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1357"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1357"), makeMVCCKey("2"), makeMVCCKey("14")},
		{makeMVCCKey("13\xff"), makeMVCCKey("14"), makeMVCCKey("13\xff")},
		{makeMVCCKey("13\xff"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("19"), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("2"), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("9"), makeMVCCKey("2")},
		{makeMVCCKey("1\xfd\xff"), makeMVCCKey("1\xff"), makeMVCCKey("1\xfe")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("9"), makeMVCCKey("2")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("19"), MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		},
		},
		// Empty b values.
		{makeMVCCKey(""), makeMVCCKey(""), makeMVCCKey("")},
		{makeMVCCKey("green"), makeMVCCKey(""), makeMVCCKey("green")},
		{makeMVCCKey("1"), makeMVCCKey(""), makeMVCCKey("1")},
		{makeMVCCKey("11\xff"), makeMVCCKey(""), makeMVCCKey("11\xff")},
		{makeMVCCKey("1\xff"), makeMVCCKey(""), makeMVCCKey("1\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey(""), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("\xff"), makeMVCCKey(""), makeMVCCKey("\xff")},
		{makeMVCCKey("\xff\xff"), makeMVCCKey(""), makeMVCCKey("\xff\xff")},
	}
	for _, tc := range sepCases {
		t.Run("", func(t *testing.T) {
			got := string(EngineComparer.Separator(nil, EncodeMVCCKey(tc.a), EncodeMVCCKey(tc.b)))
			if got != string(EncodeMVCCKey(tc.want)) {
				t.Errorf("a, b = %q, %q: got %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}

	succCases := []struct {
		a, want MVCCKey
	}{
		// Many cases adapted from Pebble test.
		{makeMVCCKey("black"), makeMVCCKey("c")},
		{makeMVCCKey("green"), makeMVCCKey("h")},
		{makeMVCCKey(""), makeMVCCKey("")},
		{makeMVCCKey("13"), makeMVCCKey("2")},
		{makeMVCCKey("135"), makeMVCCKey("2")},
		{makeMVCCKey("13\xff"), makeMVCCKey("2")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("2")},
		{makeMVCCKey("\xff"), makeMVCCKey("\xff")},
		{makeMVCCKey("\xff\xff"), makeMVCCKey("\xff\xff")},
		{makeMVCCKey("\xff\xff\xff"), makeMVCCKey("\xff\xff\xff")},
		{makeMVCCKey("\xfe\xff\xff"), makeMVCCKey("\xff")},
		{MVCCKey{
			Key:       []byte("\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, MVCCKey{
			Key:       []byte("\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}},
	}
	for _, tc := range succCases {
		t.Run("", func(t *testing.T) {
			got := string(EngineComparer.Successor(nil, EncodeMVCCKey(tc.a)))
			if got != string(EncodeMVCCKey(tc.want)) {
				t.Errorf("a = %q: got %q, want %q", tc.a, got, tc.want)
			}
		})
	}

}

func TestPebbleMetricEventListener(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettings()
	fs.MaxSyncDurationFatalOnExceeded.Override(ctx, &settings.SV, false)
	p, err := Open(ctx, InMemory(), settings, CacheSize(1<<20 /* 1 MiB */))
	require.NoError(t, err)
	defer p.Close()

	require.Equal(t, int64(0), p.writeStallCount)
	require.Equal(t, int64(0), p.diskSlowCount)
	require.Equal(t, int64(0), p.diskStallCount)
	p.eventListener.WriteStallBegin(pebble.WriteStallBeginInfo{})
	require.Equal(t, int64(1), p.writeStallCount)
	require.Equal(t, int64(0), p.diskSlowCount)
	require.Equal(t, int64(0), p.diskStallCount)
	p.eventListener.DiskSlow(pebble.DiskSlowInfo{Duration: 1 * time.Second})
	require.Equal(t, int64(1), p.writeStallCount)
	require.Equal(t, int64(1), p.diskSlowCount)
	require.Equal(t, int64(0), p.diskStallCount)
	p.eventListener.DiskSlow(pebble.DiskSlowInfo{Duration: 70 * time.Second})
	require.Equal(t, int64(1), p.writeStallCount)
	require.Equal(t, int64(1), p.diskSlowCount)
	require.Equal(t, int64(1), p.diskStallCount)
}

func TestPebbleIterConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eng := createTestPebbleEngine()
	defer eng.Close()
	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	k1 := MVCCKey{Key: []byte("a"), Timestamp: ts1}
	v1 := MVCCValue{Value: roachpb.MakeValueFromString("a1")}
	require.NoError(t, eng.PutMVCC(k1, v1))

	var (
		roEngine  = eng.NewReader(StandardDurability)
		batch     = eng.NewBatch()
		roEngine2 = eng.NewReader(StandardDurability)
		batch2    = eng.NewBatch()
	)
	defer roEngine.Close()
	defer batch.Close()
	defer roEngine2.Close()
	defer batch2.Close()

	require.False(t, eng.ConsistentIterators())
	require.True(t, roEngine.ConsistentIterators())
	require.True(t, batch.ConsistentIterators())
	require.True(t, roEngine2.ConsistentIterators())
	require.True(t, batch2.ConsistentIterators())

	// Since an iterator is created on pebbleReadOnly, pebbleBatch before
	// writing a newer version of "a", the newer version will not be visible to
	// iterators that are created later.
	iter, err := roEngine.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("a")})
	require.NoError(t, err)
	iter.Close()
	batchIter, err := batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("a")})
	require.NoError(t, err)
	batchIter.Close()
	engIter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("a")})
	require.NoError(t, err)
	engIter.Close()
	// Pin the state for iterators.
	require.Nil(t, roEngine2.PinEngineStateForIterators(fs.UnknownReadCategory))
	require.Nil(t, batch2.PinEngineStateForIterators(fs.UnknownReadCategory))

	// Write a newer version of "a"
	k2 := MVCCKey{Key: []byte("a"), Timestamp: ts2}
	v2 := MVCCValue{Value: roachpb.MakeValueFromString("a2")}
	require.NoError(t, eng.PutMVCC(k2, v2))

	checkMVCCIter := func(iter MVCCIterator, err error) {
		require.NoError(t, err)
		defer iter.Close()
		iter.SeekGE(MVCCKey{Key: []byte("a")})
		valid, err := iter.Valid()
		require.Equal(t, true, valid)
		require.NoError(t, err)
		k := iter.UnsafeKey()
		require.True(t, k1.Equal(k), "expected %s != actual %s", k1.String(), k.String())
		iter.Next()
		valid, err = iter.Valid()
		require.False(t, valid)
		require.NoError(t, err)
	}
	checkEngineIter := func(iter EngineIterator, err error) {
		require.NoError(t, err)
		defer iter.Close()
		valid, err := iter.SeekEngineKeyGE(EngineKey{Key: []byte("a")})
		require.Equal(t, true, valid)
		require.NoError(t, err)
		k, err := iter.UnsafeEngineKey()
		require.NoError(t, err)
		require.True(t, k.IsMVCCKey())
		mvccKey, err := k.ToMVCCKey()
		require.NoError(t, err)
		require.True(
			t, k1.Equal(mvccKey), "expected %s != actual %s", k1.String(), mvccKey.String())
		valid, err = iter.NextEngineKey()
		require.False(t, valid)
		require.NoError(t, err)
	}

	checkMVCCIter(roEngine.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkMVCCIter(roEngine.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{Prefix: true}))
	checkMVCCIter(batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkMVCCIter(batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{Prefix: true}))
	checkMVCCIter(roEngine2.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkMVCCIter(roEngine2.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{Prefix: true}))
	checkMVCCIter(batch2.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkMVCCIter(batch2.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{Prefix: true}))

	checkEngineIter(roEngine.NewEngineIterator(context.Background(), IterOptions{UpperBound: []byte("b")}))
	checkEngineIter(roEngine.NewEngineIterator(context.Background(), IterOptions{Prefix: true}))
	checkEngineIter(batch.NewEngineIterator(context.Background(), IterOptions{UpperBound: []byte("b")}))
	checkEngineIter(batch.NewEngineIterator(context.Background(), IterOptions{Prefix: true}))
	checkEngineIter(roEngine2.NewEngineIterator(context.Background(), IterOptions{UpperBound: []byte("b")}))
	checkEngineIter(roEngine2.NewEngineIterator(context.Background(), IterOptions{Prefix: true}))
	checkEngineIter(batch2.NewEngineIterator(context.Background(), IterOptions{UpperBound: []byte("b")}))
	checkEngineIter(batch2.NewEngineIterator(context.Background(), IterOptions{Prefix: true}))

	checkIterSeesBothValues := func(iter MVCCIterator, err error) {
		require.NoError(t, err)
		defer iter.Close()
		iter.SeekGE(MVCCKey{Key: []byte("a")})
		count := 0
		for ; ; iter.Next() {
			valid, err := iter.Valid()
			require.NoError(t, err)
			if !valid {
				break
			}
			count++
		}
		require.Equal(t, 2, count)
	}
	// The eng iterator will see both values.
	checkIterSeesBothValues(eng.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	// The indexed batches will see 2 values since the second one is written to the batch.
	require.NoError(t, batch.PutMVCC(
		MVCCKey{Key: []byte("a"), Timestamp: ts2},
		MVCCValue{Value: roachpb.MakeValueFromString("a2")},
	))
	require.NoError(t, batch2.PutMVCC(
		MVCCKey{Key: []byte("a"), Timestamp: ts2},
		MVCCValue{Value: roachpb.MakeValueFromString("a2")},
	))
	checkIterSeesBothValues(batch.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
	checkIterSeesBothValues(batch2.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: []byte("b")}))
}

func BenchmarkMVCCKeyCompare(b *testing.B) {
	keys := makeRandEncodedKeys()
	b.ResetTimer()
	for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
		_ = EngineComparer.Compare(keys[i%len(keys)], keys[j%len(keys)])
	}
}

func BenchmarkMVCCKeyEqual(b *testing.B) {
	keys := makeRandEncodedKeys()
	b.ResetTimer()
	for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
		_ = EngineComparer.Equal(keys[i%len(keys)], keys[j%len(keys)])
	}
}

func BenchmarkMVCCKeySplit(b *testing.B) {
	keys := makeRandEncodedKeys()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EngineComparer.Split(keys[i%len(keys)])
	}
}

func makeRandEncodedKeys() [][]byte {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	keys := make([][]byte, 1000)
	for i := range keys {
		k := MVCCKey{
			Key: []byte("shared" + [...]string{"a", "b", "c"}[rng.Intn(3)]),
			Timestamp: hlc.Timestamp{
				WallTime: rng.Int63n(5),
			},
		}
		if rng.Int31n(5) == 0 {
			// 20% of keys have a logical component.
			k.Timestamp.Logical = rng.Int31n(4) + 1
		}
		keys[i] = EncodeMVCCKey(k)
	}
	return keys
}

type testValue struct {
	key       roachpb.Key
	value     roachpb.Value
	timestamp hlc.Timestamp
	txn       *roachpb.Transaction
}

func intent(key roachpb.Key, val string, ts hlc.Timestamp) testValue {
	var value = roachpb.MakeValueFromString(val)
	value.InitChecksum(key)
	tx := roachpb.MakeTransaction(fmt.Sprintf("txn-%v", key), key, isolation.Serializable, roachpb.NormalUserPriority, ts, 1000, 99, 0, false /* omitInRangefeeds */)
	var txn = &tx
	return testValue{key, value, ts, txn}
}

func value(key roachpb.Key, val string, ts hlc.Timestamp) testValue {
	var value = roachpb.MakeValueFromString(val)
	value.InitChecksum(key)
	return testValue{key, value, ts, nil}
}

func fillInData(ctx context.Context, engine Engine, data []testValue) error {
	batch := engine.NewBatch()
	defer batch.Close()
	for _, val := range data {
		if _, err := MVCCPut(ctx, batch, val.key, val.timestamp, val.value, MVCCWriteOptions{Txn: val.txn}); err != nil {
			return err
		}
	}
	return batch.Commit(true)
}

func ts(ts int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: ts}
}

func key(k int) roachpb.Key {
	return []byte(fmt.Sprintf("%05d", k))
}

// nonFatalLogger implements pebble.Logger by recording that a fatal log event
// was encountered at least once. Fatal log events are downgraded to Info level.
type nonFatalLogger struct {
	pebble.LoggerAndTracer
	t      *testing.T
	caught atomic.Value
}

func (l *nonFatalLogger) Fatalf(format string, args ...interface{}) {
	l.caught.Store(true)
	l.t.Logf(format, args...)
}

func TestPebbleValidateKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Capture fatal errors by swapping out the logger.
	// nonFatalLogger.LoggerAndTracer is nil, since the test exercises only
	// Fatalf.
	l := &nonFatalLogger{t: t}
	opt := func(cfg *engineConfig) error {
		cfg.opts.LoggerAndTracer = l
		comparer := *cfg.opts.Comparer
		comparer.ValidateKey = func(k []byte) error {
			if bytes.Contains(k, []byte("foo")) {
				return errors.Errorf("key contains 'foo'")
			}
			return nil
		}
		cfg.opts.Comparer = &comparer
		return nil
	}
	engine := createTestPebbleEngine(opt).(*Pebble)
	defer engine.Close()

	ek := EngineKey{Key: roachpb.Key("foo")}
	require.NoError(t, engine.PutEngineKey(ek, []byte("bar")))
	// Force a flush to trigger the compaction error.
	require.NoError(t, engine.Flush())

	// A fatal error was captured by the logger.
	require.True(t, l.caught.Load().(bool))
}

func TestPebbleBackgroundError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	env := mustInitTestEnv(t, &errorFS{FS: vfs.NewMem(), errorCount: 3}, "")
	eng, err := Open(context.Background(), env, cluster.MakeClusterSettings())
	require.NoError(t, err)
	defer eng.Close()

	require.NoError(t, eng.PutMVCC(
		MVCCKey{Key: []byte("a"), Timestamp: hlc.Timestamp{WallTime: 1}},
		MVCCValue{Value: roachpb.MakeValueFromString("a")},
	))
	require.NoError(t, eng.db.Flush())
}

type errorFS struct {
	vfs.FS
	errorCount int32
}

func (fs *errorFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if filepath.Ext(name) == ".sst" && atomic.AddInt32(&fs.errorCount, -1) >= 0 {
		return nil, errors.New("background error")
	}
	return fs.FS.Create(name, category)
}

func TestPebbleMVCCIntervalMapper(t *testing.T) {
	defer leaktest.AfterTest(t)()

	m := pebbleIntervalMapper{}
	aKey := roachpb.Key("a")
	uuid := uuid.Must(uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))

	for _, tc := range []struct {
		userKey  []byte
		expected sstable.BlockInterval
	}{
		{
			userKey: func() []byte {
				ek, _ := LockTableKey{aKey, lock.Intent, uuid}.ToEngineKey(nil)
				return ek.Encode()
			}(),
			// Lock keys are not MVCC keys.
			expected: sstable.BlockInterval{},
		},
		{
			userKey:  EncodeMVCCKey(MVCCKey{aKey, hlc.Timestamp{WallTime: 2, Logical: 1}}),
			expected: sstable.BlockInterval{Lower: 2, Upper: 3},
		},
		{
			userKey:  EncodeMVCCKey(MVCCKey{aKey, hlc.Timestamp{WallTime: 22, Logical: 1}}),
			expected: sstable.BlockInterval{Lower: 22, Upper: 23},
		},
		{
			userKey:  EncodeMVCCKey(MVCCKey{aKey, hlc.Timestamp{WallTime: 25}}),
			expected: sstable.BlockInterval{Lower: 25, Upper: 26},
		},
	} {
		i, err := m.MapPointKey(sstable.InternalKey{UserKey: tc.userKey}, nil)
		require.NoError(t, err)
		require.Equal(t, tc.expected, i)
	}
	// An invalid key (malformed sentinel) results in an error.
	key := EncodeMVCCKey(MVCCKey{aKey, hlc.Timestamp{WallTime: 2, Logical: 1}})
	sentinelPos := len(key) - 1 - int(key[len(key)-1])
	key[sentinelPos] = '\xff'
	_, err := m.MapPointKey(sstable.InternalKey{UserKey: key}, nil)
	require.Error(t, err)
}

func TestPebbleMVCCBlockIntervalSuffixReplacer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := MVCCBlockIntervalSuffixReplacer{}
	suffix := EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 42, Logical: 1})
	before := sstable.BlockInterval{Lower: 10, Upper: 15}
	after, err := r.ApplySuffixReplacement(before, suffix)
	require.NoError(t, err)
	require.Equal(t, sstable.BlockInterval{Lower: 42, Upper: 43}, after)

	// An invalid suffix (too short) results in an error.
	suffix = EncodeMVCCTimestampSuffix(hlc.Timestamp{WallTime: 42, Logical: 1})[1:]
	_, err = r.ApplySuffixReplacement(sstable.BlockInterval{Lower: 1, Upper: 2}, suffix)
	require.Error(t, err)
}

// TestPebbleMVCCTimeIntervalCollectorAndFilter tests that point and range key
// time interval collection and filtering works. It only tests basic
// integration.
func TestPebbleMVCCTimeIntervalCollectorAndFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up an engine with tiny blocks, so each point key gets its own block,
	// and disable compactions to keep SSTs separate.
	overrideOptions := func(cfg *engineConfig) error {
		cfg.opts.DisableAutomaticCompactions = true
		for i := range cfg.opts.Levels {
			cfg.opts.Levels[i].BlockSize = 1
			cfg.opts.Levels[i].IndexBlockSize = 1
		}
		return nil
	}
	eng := NewDefaultInMemForTesting(overrideOptions)
	defer eng.Close()

	// Point keys a@3, a@5, a@7 in separate blocks in a single SST.
	require.NoError(t, eng.PutMVCC(pointKey("a", 3), stringValue("a3")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 5), stringValue("a5")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 7), stringValue("a7")))
	require.NoError(t, eng.Flush())

	// Separate range keys [b-c)@5, [c-d)@7, [d-e)@9 share a block in a single SST.
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("b", "c", 5), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("c", "d", 7), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("d", "e", 9), MVCCValue{}))
	require.NoError(t, eng.Flush())

	// Overlapping range keys [x-z)@5, [x-z)@7 share a block in a single SST.
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("x", "z", 5), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("x", "z", 7), MVCCValue{}))
	require.NoError(t, eng.Flush())

	// NB: Range key filtering is currently disabled, see comment in
	// pebbleIterator.setOptions().
	testcases := map[string]struct {
		minTimestamp hlc.Timestamp
		maxTimestamp hlc.Timestamp
		expect       []interface{}
	}{
		"no bounds": {wallTS(0), wallTS(0), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 5, "a5"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"all": {wallTS(1), wallTS(10), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 5, "a5"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"above all": {wallTS(10), wallTS(11), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"below all": {wallTS(0), wallTS(1), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"intersect": {wallTS(5), wallTS(5), []interface{}{
			pointKV("a", 5, "a5"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"between": {wallTS(6), wallTS(6), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"touches lower": {wallTS(1), wallTS(3), []interface{}{
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
		"touches upper": {wallTS(9), wallTS(10), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("c", "d", 7, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
			rangeKV("x", "z", 7, MVCCValue{}),
			rangeKV("x", "z", 5, MVCCValue{}),
		}},
	}

	keyTypes := []IterKeyType{IterKeyTypePointsAndRanges, IterKeyTypePointsOnly, IterKeyTypeRangesOnly}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			for _, keyType := range keyTypes {
				t.Run(keyType.String(), func(t *testing.T) {
					// Filter out expected values based on key type.
					var expect []interface{}
					for _, kv := range tc.expect {
						if _, isPoint := kv.(MVCCKeyValue); !isPoint && keyType == IterKeyTypePointsOnly {
							continue
						} else if _, isRange := kv.(MVCCRangeKeyValue); !isRange && keyType == IterKeyTypeRangesOnly {
							continue
						}
						expect = append(expect, kv)
					}
					iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
						KeyTypes:     keyType,
						UpperBound:   keys.MaxKey,
						MinTimestamp: tc.minTimestamp,
						MaxTimestamp: tc.maxTimestamp,
					})
					require.NoError(t, err)
					defer iter.Close()
					require.Equal(t, expect, scanIter(t, iter))
				})
			}
		})
	}
}

// TestPebbleMVCCTimeIntervalWithClears tests that point and range key
// time interval collection and filtering works in the presence of
// point/range clears.
func TestPebbleMVCCTimeIntervalWithClears(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up an engine. In this case, we use large blocks to force all point and
	// range keys into the same blocks, to demonstrate the effect of clears in
	// separate SSTs when the clearing SST does not satisfy the filter. We
	// disable compactions to keep SSTs separate.
	overrideOptions := func(cfg *engineConfig) error {
		cfg.opts.DisableAutomaticCompactions = true
		for i := range cfg.opts.Levels {
			cfg.opts.Levels[i].BlockSize = 65536
			cfg.opts.Levels[i].IndexBlockSize = 65536
		}
		return nil
	}
	eng := NewDefaultInMemForTesting(overrideOptions)
	defer eng.Close()

	// Point keys a@3, a@5, a@7 in a single block in a single SST.
	require.NoError(t, eng.PutMVCC(pointKey("a", 3), stringValue("a3")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 5), stringValue("a5")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 7), stringValue("a7")))
	require.NoError(t, eng.Flush())

	// Separate range keys [b-c)@5, [c-d)@7, [d-e)@9 in a single block in a single SST.
	//
	// NB: Range key filtering is currently disabled, see comment in
	// pebbleIterator.setOptions().
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("b", "c", 5), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("c", "d", 7), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("d", "e", 9), MVCCValue{}))
	require.NoError(t, eng.Flush())

	// Clear a@5 and [c-d)@7 in a separate SST.
	require.NoError(t, eng.ClearMVCC(pointKey("a", 5), ClearOptions{}))
	require.NoError(t, eng.ClearMVCCRangeKey(rangeKey("c", "d", 7)))
	require.NoError(t, eng.Flush())

	testcases := map[string]struct {
		minTimestamp hlc.Timestamp
		maxTimestamp hlc.Timestamp
		expect       []interface{}
	}{
		"no bounds": {wallTS(0), wallTS(0), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		"all": {wallTS(1), wallTS(10), []interface{}{
			pointKV("a", 7, "a7"),
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		"at cleared point": {wallTS(5), wallTS(5), []interface{}{
			// a@7 and a@3's timestamps fall outside [5,5].
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		"at cleared range": {wallTS(7), wallTS(7), []interface{}{
			// a@5 and a@3's timestamps fall outside [7,7].
			pointKV("a", 7, "a7"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		"touches lower": {wallTS(1), wallTS(3), []interface{}{
			// a@7 and a@5's timestamps fall outside [1,3].
			pointKV("a", 3, "a3"),
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
		// Range keys are not filtered, so we see the [c-d)@7 clear even though it
		// wouldn't satisfy the [9-10] filter.
		"touches upper": {wallTS(9), wallTS(10), []interface{}{
			rangeKV("b", "c", 5, MVCCValue{}),
			rangeKV("d", "e", 9, MVCCValue{}),
		}},
	}

	keyTypes := []IterKeyType{IterKeyTypePointsAndRanges, IterKeyTypePointsOnly, IterKeyTypeRangesOnly}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			for _, keyType := range keyTypes {
				t.Run(keyType.String(), func(t *testing.T) {
					// Filter out expected values based on key type.
					var expect []interface{}
					for _, kv := range tc.expect {
						if _, isPoint := kv.(MVCCKeyValue); !isPoint && keyType == IterKeyTypePointsOnly {
							continue
						} else if _, isRange := kv.(MVCCRangeKeyValue); !isRange && keyType == IterKeyTypeRangesOnly {
							continue
						}
						expect = append(expect, kv)
					}
					iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
						KeyTypes:     keyType,
						UpperBound:   keys.MaxKey,
						MinTimestamp: tc.minTimestamp,
						MaxTimestamp: tc.maxTimestamp,
					})
					if err != nil {
						t.Fatal(err)
					}
					defer iter.Close()
					require.Equal(t, expect, scanIter(t, iter))
				})
			}
		})
	}
}

// TestPebbleMVCCTimeIntervalWithRangeClears tests how point and range key
// time interval collection and filtering works in the presence of
// a ranged clear (i.e. Pebble range tombstone).
func TestPebbleMVCCTimeIntervalWithRangeClears(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up an engine with tiny blocks, so each point key gets its own block,
	// and disable compactions to keep SSTs separate.
	overrideOptions := func(cfg *engineConfig) error {
		cfg.opts.DisableAutomaticCompactions = true
		for i := range cfg.opts.Levels {
			cfg.opts.Levels[i].BlockSize = 1
			cfg.opts.Levels[i].IndexBlockSize = 1
		}
		return nil
	}
	eng := NewDefaultInMemForTesting(overrideOptions)
	defer eng.Close()

	// Point keys a@3, a@5, a@7 in separate blocks in a single SST.
	require.NoError(t, eng.PutMVCC(pointKey("a", 3), stringValue("a3")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 5), stringValue("a5")))
	require.NoError(t, eng.PutMVCC(pointKey("a", 7), stringValue("a7")))
	require.NoError(t, eng.Flush())

	// Separate range keys [b-c)@5, [c-d)@7, [d-e)@9 in a single block in a single SST.
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("b", "c", 5), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("c", "d", 7), MVCCValue{}))
	require.NoError(t, eng.PutMVCCRangeKey(rangeKey("d", "e", 9), MVCCValue{}))
	require.NoError(t, eng.Flush())

	// Clear [a-z) in a separate SST.
	require.NoError(t, eng.ClearMVCCRange(roachpb.Key("a"), roachpb.Key("z"), true, true))
	require.NoError(t, eng.Flush())

	testcases := map[string]struct {
		minTimestamp hlc.Timestamp
		maxTimestamp hlc.Timestamp
		expect       []interface{}
	}{
		"no bounds":     {wallTS(0), wallTS(0), nil},
		"all":           {wallTS(1), wallTS(10), nil},
		"above all":     {wallTS(10), wallTS(11), nil},
		"below all":     {wallTS(0), wallTS(1), nil},
		"intersect":     {wallTS(5), wallTS(5), nil},
		"between":       {wallTS(6), wallTS(6), nil},
		"touches lower": {wallTS(1), wallTS(3), nil},
		"touches upper": {wallTS(9), wallTS(10), nil},
	}

	keyTypes := []IterKeyType{IterKeyTypePointsAndRanges, IterKeyTypePointsOnly, IterKeyTypeRangesOnly}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			for _, keyType := range keyTypes {
				t.Run(keyType.String(), func(t *testing.T) {
					// Filter out expected values based on key type.
					var expect []interface{}
					for _, kv := range tc.expect {
						if _, isPoint := kv.(MVCCKeyValue); !isPoint && keyType == IterKeyTypePointsOnly {
							continue
						} else if _, isRange := kv.(MVCCRangeKeyValue); !isRange && keyType == IterKeyTypeRangesOnly {
							continue
						}
						expect = append(expect, kv)
					}
					iter, err := eng.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
						KeyTypes:     keyType,
						UpperBound:   keys.MaxKey,
						MinTimestamp: tc.minTimestamp,
						MaxTimestamp: tc.maxTimestamp,
					})
					if err != nil {
						t.Fatal(err)
					}
					defer iter.Close()
					require.Equal(t, expect, scanIter(t, iter))
				})
			}
		})
	}
}

func TestPebbleFlushCallbackAndDurabilityRequirement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	DisableMetamorphicSimpleValueEncoding(t)

	eng := createTestPebbleEngine()
	defer eng.Close()

	ts := hlc.Timestamp{WallTime: 1}
	k := MVCCKey{Key: []byte("a"), Timestamp: ts}
	v := MVCCValue{Value: roachpb.MakeValueFromString("a1")}
	// Write.
	require.NoError(t, eng.PutMVCC(k, v))
	cbCount := int32(0)
	eng.RegisterFlushCompletedCallback(func() {
		atomic.AddInt32(&cbCount, 1)
	})
	roStandard := eng.NewReader(StandardDurability)
	defer roStandard.Close()
	roGuaranteed := eng.NewReader(GuaranteedDurability)
	defer roGuaranteed.Close()
	roGuaranteedPinned := eng.NewReader(GuaranteedDurability)
	defer roGuaranteedPinned.Close()
	require.NoError(t, roGuaranteedPinned.PinEngineStateForIterators(fs.UnknownReadCategory))
	// Returns the value found or nil.
	checkGetAndIter := func(reader Reader) []byte {
		v := mvccGetRaw(t, reader, k)
		iter, err := reader.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{UpperBound: k.Key.Next()})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		iter.SeekGE(k)
		valid, err := iter.Valid()
		require.NoError(t, err)
		require.Equal(t, v != nil, valid)
		if valid {
			value, err := iter.Value()
			require.NoError(t, err)
			require.Equal(t, v, value)
		}
		return v
	}
	require.Equal(t, v.Value.RawBytes, checkGetAndIter(roStandard))
	// Write is not visible yet.
	require.Nil(t, checkGetAndIter(roGuaranteed))
	require.Nil(t, checkGetAndIter(roGuaranteedPinned))

	// Flush the engine and wait for it to complete.
	require.NoError(t, eng.Flush())
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt32(&cbCount) < 1 {
			return errors.Errorf("not flushed")
		}
		return nil
	})
	// Write is visible to new guaranteed reader. We need to use a new reader
	// due to iterator caching.
	roGuaranteed2 := eng.NewReader(GuaranteedDurability)
	defer roGuaranteed2.Close()
	require.Equal(t, v.Value.RawBytes, checkGetAndIter(roGuaranteed2))
}

// TestPebbleReaderMultipleIterators tests that all Pebble readers support
// multiple concurrent iterators of the same type.
func TestPebbleReaderMultipleIterators(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := NewDefaultInMemForTesting()
	defer eng.Close()

	a1 := MVCCKey{Key: roachpb.Key("a"), Timestamp: hlc.Timestamp{WallTime: 1}}
	b1 := MVCCKey{Key: roachpb.Key("b"), Timestamp: hlc.Timestamp{WallTime: 1}}
	c1 := MVCCKey{Key: roachpb.Key("c"), Timestamp: hlc.Timestamp{WallTime: 1}}

	v1 := MVCCValue{Value: roachpb.MakeValueFromString("1")}
	v2 := MVCCValue{Value: roachpb.MakeValueFromString("2")}
	v3 := MVCCValue{Value: roachpb.MakeValueFromString("3")}
	vx := MVCCValue{Value: roachpb.MakeValueFromString("x")}

	decodeValue := func(encoded []byte, err error) MVCCValue {
		require.NoError(t, err)
		value, err := DecodeMVCCValue(encoded)
		require.NoError(t, err)
		return value
	}

	require.NoError(t, eng.PutMVCC(a1, v1))
	require.NoError(t, eng.PutMVCC(b1, v2))
	require.NoError(t, eng.PutMVCC(c1, v3))

	readOnly := eng.NewReader(StandardDurability)
	defer readOnly.Close()
	require.NoError(t, readOnly.PinEngineStateForIterators(fs.UnknownReadCategory))

	snapshot := eng.NewSnapshot()
	defer snapshot.Close()
	require.NoError(t, snapshot.PinEngineStateForIterators(fs.UnknownReadCategory))

	efos := eng.NewEventuallyFileOnlySnapshot([]roachpb.Span{{Key: keys.MinKey, EndKey: keys.MaxKey}})
	defer efos.Close()
	require.NoError(t, efos.PinEngineStateForIterators(fs.UnknownReadCategory))

	batch := eng.NewBatch()
	defer batch.Close()
	require.NoError(t, batch.PinEngineStateForIterators(fs.UnknownReadCategory))

	// These writes should not be visible to any of the pinned iterators.
	require.NoError(t, eng.PutMVCC(a1, vx))
	require.NoError(t, eng.PutMVCC(b1, vx))
	require.NoError(t, eng.PutMVCC(c1, vx))

	testcases := map[string]Reader{
		"Engine":   eng,
		"ReadOnly": readOnly,
		"Snapshot": snapshot,
		"EFOS":     efos,
		"Batch":    batch,
	}
	for name, r := range testcases {
		t.Run(name, func(t *testing.T) {
			// Make sure we can create two iterators of the same type.
			i1, err := r.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{LowerBound: a1.Key, UpperBound: keys.MaxKey})
			if err != nil {
				t.Fatal(err)
			}
			i2, err := r.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{LowerBound: b1.Key, UpperBound: keys.MaxKey})
			if err != nil {
				t.Fatal(err)
			}

			// Make sure the iterators are independent.
			i1.SeekGE(a1)
			i2.SeekGE(a1)
			require.Equal(t, a1, i1.UnsafeKey())
			require.Equal(t, b1, i2.UnsafeKey()) // b1 because of LowerBound

			// Check iterator consistency.
			if r.ConsistentIterators() {
				require.Equal(t, v1, decodeValue(i1.UnsafeValue()))
				require.Equal(t, v2, decodeValue(i2.UnsafeValue()))
			} else {
				require.Equal(t, vx, decodeValue(i1.UnsafeValue()))
				require.Equal(t, vx, decodeValue(i2.UnsafeValue()))
			}

			// Closing one iterator shouldn't affect the other.
			i1.Close()
			i2.Next()
			require.Equal(t, c1, i2.UnsafeKey())
			i2.Close()

			// Quick check for engine iterators too.
			e1, err := r.NewEngineIterator(context.Background(), IterOptions{UpperBound: keys.MaxKey})
			if err != nil {
				t.Fatal(err)
			}
			defer e1.Close()
			e2, err := r.NewEngineIterator(context.Background(), IterOptions{UpperBound: keys.MaxKey})
			if err != nil {
				t.Fatal(err)
			}
			defer e2.Close()
		})
	}
}

func TestShortAttributeExtractor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var txnUUID [uuid.Size]byte
	lockKey, _ := LockTableKey{
		Key: roachpb.Key("a"), Strength: lock.Intent, TxnUUID: txnUUID}.ToEngineKey(nil)
	v := MVCCValue{}
	tombstoneVal, err := EncodeMVCCValue(v)
	require.NoError(t, err)
	var sv roachpb.Value
	sv.SetString("foo")
	v = MVCCValue{Value: sv}
	strVal, err := EncodeMVCCValue(v)
	require.NoError(t, err)
	valHeader := enginepb.MVCCValueHeader{LocalTimestamp: hlc.ClockTimestamp{WallTime: 5}}
	v = MVCCValue{MVCCValueHeader: valHeader}
	tombstoneWithHeaderVal, err := EncodeMVCCValue(v)
	require.NoError(t, err)
	v = MVCCValue{MVCCValueHeader: valHeader, Value: sv}
	strWithHeaderVal, err := EncodeMVCCValue(v)
	require.NoError(t, err)
	mvccKey := EncodeMVCCKey(MVCCKey{Key: roachpb.Key("a"), Timestamp: hlc.Timestamp{WallTime: 20}})
	testCases := []struct {
		name   string
		key    []byte
		value  []byte
		attr   pebble.ShortAttribute
		errStr string
	}{
		{
			name:  "no-version",
			key:   EncodeMVCCKey(MVCCKey{Key: roachpb.Key("a")}),
			value: []byte(nil),
		},
		{
			name:  "lock-key",
			key:   lockKey.Encode(),
			value: []byte(nil),
		},
		{
			name:  "tombstone-val",
			key:   mvccKey,
			value: tombstoneVal,
			attr:  1,
		},
		{
			name:  "str-val",
			key:   mvccKey,
			value: strVal,
		},
		{
			name:  "tombstone-with-header-val",
			key:   mvccKey,
			value: tombstoneWithHeaderVal,
			attr:  1,
		},
		{
			name:  "str-with-header-val",
			key:   mvccKey,
			value: strWithHeaderVal,
		},
		{
			name:   "invalid-val",
			key:    mvccKey,
			value:  []byte("v"),
			errStr: "invalid encoded mvcc value",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			prefixLen := EngineComparer.Split(tc.key)
			attr, err := shortAttributeExtractorForValues(tc.key, prefixLen, tc.value)
			if len(tc.errStr) != 0 {
				require.ErrorContains(t, err, tc.errStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.attr, attr)
			}
		})
	}
}

func TestIncompatibleVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	memFS := vfs.NewMem()
	env := mustInitTestEnv(t, memFS, "")

	p, err := Open(ctx, env, cluster.MakeTestingClusterSettings())
	require.NoError(t, err)
	p.Close()

	// Overwrite the min version file with an unsupported version.
	version := roachpb.Version{Major: 21, Minor: 1}
	b, err := protoutil.Marshal(&version)
	require.NoError(t, err)
	require.NoError(t, fs.SafeWriteToFile(memFS, "", MinVersionFilename, b, fs.UnspecifiedWriteCategory))

	env = mustInitTestEnv(t, memFS, "")
	_, err = Open(ctx, env, cluster.MakeTestingClusterSettings())
	require.Error(t, err)
	msg := err.Error()
	if !strings.Contains(msg, "is too old for running version") &&
		!strings.Contains(msg, "cannot be opened by development version") {
		t.Fatalf("unexpected error %v", err)
	}
	env.Close()
}

func TestNoMinVerFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	memFS := vfs.NewMem()
	env := mustInitTestEnv(t, memFS, "")
	st := cluster.MakeTestingClusterSettings()
	p, err := Open(ctx, env, st)
	require.NoError(t, err)
	p.Close()

	// Remove the min version filename.
	require.NoError(t, memFS.Remove(MinVersionFilename))

	// We are still allowed the open the store if we haven't written anything to it.
	// This is useful in case the initial Open crashes right before writinng the
	// min version file.
	env = mustInitTestEnv(t, memFS, "")
	p, err = Open(ctx, env, st)
	require.NoError(t, err)

	// Now write something to the store.
	k := MVCCKey{Key: []byte("a"), Timestamp: hlc.Timestamp{WallTime: 1}}
	v := MVCCValue{Value: roachpb.MakeValueFromString("a1")}
	require.NoError(t, p.PutMVCC(k, v))
	p.Close()

	// Remove the min version filename.
	require.NoError(t, memFS.Remove(MinVersionFilename))

	env = mustInitTestEnv(t, memFS, "")
	_, err = Open(ctx, env, st)
	require.ErrorContains(t, err, "store has no min-version file")
	env.Close()
}

func TestApproximateDiskBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rng, _ := randutil.NewTestRand()

	p, err := Open(ctx, InMemory(), cluster.MakeTestingClusterSettings())
	require.NoError(t, err)
	defer p.Close()

	keyFunc := func(i int) roachpb.Key {
		return keys.SystemSQLCodec.TablePrefix(uint32(i))
	}

	// Write keys 0000...0999.
	b := p.NewWriteBatch()
	for i := 0; i < 1000; i++ {
		require.NoError(t, b.PutMVCC(
			MVCCKey{Key: keyFunc(i), Timestamp: hlc.Timestamp{WallTime: int64(i + 1)}},
			MVCCValue{Value: roachpb.Value{RawBytes: randutil.RandBytes(rng, 100)}},
		))
	}
	require.NoError(t, b.Commit(true /* sync */))
	require.NoError(t, p.Flush())

	approxBytes := func(span roachpb.Span) uint64 {
		v, _, _, err := p.ApproximateDiskBytes(span.Key, span.EndKey)
		require.NoError(t, err)
		t.Logf("%s (%x-%x): %d bytes", span, span.Key, span.EndKey, v)
		return v
	}

	all := approxBytes(roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax})
	for i := 0; i < 1000; i++ {
		s := roachpb.Span{Key: keyFunc(i), EndKey: keyFunc(i + 1)}
		if v := approxBytes(s); v >= all {
			t.Errorf("ApproximateDiskBytes(%q) = %d >= entire DB size %d", s, v, all)
		}
	}
}

func TestConvertFilesToBatchAndCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	const ingestEngine = 0
	const batchEngine = 1
	var engs [batchEngine + 1]Engine
	mem := vfs.NewMem()
	for i := range engs {
		var err error
		engs[i], err = Open(ctx, mustInitTestEnv(t, mem, fmt.Sprintf("eng-%d", i)), st)
		require.NoError(t, err)
		defer engs[i].Close()
	}
	// Populate points that will have MVCC value and an intent.
	points := []testValue{
		intent(key(1), "value1", ts(1000)),
		value(key(2), "value2", ts(1000)),
		intent(key(2), "value3", ts(2000)),
		intent(key(5), "value4", ts(1500)),
		intent(key(6), "value4", ts(2500)),
	}
	for i := range engs {
		// Put points
		require.NoError(t, fillInData(ctx, engs[i], points))
		// Put wide range keys that will be partially cleared.
		require.NoError(t, engs[i].PutMVCCRangeKey(MVCCRangeKey{
			StartKey:  key(1),
			EndKey:    key(7),
			Timestamp: ts(2300),
		}, MVCCValue{}))
		require.NoError(t, engs[i].PutMVCCRangeKey(MVCCRangeKey{
			StartKey:  key(0),
			EndKey:    key(8),
			Timestamp: ts(2700),
		}, MVCCValue{}))
	}
	// Ingest into [2, 6) with 2 files.
	fileName1 := "file1"
	f1, err := mem.Create(fileName1, fs.UnspecifiedWriteCategory)
	require.NoError(t, err)
	w1 := MakeIngestionSSTWriter(ctx, st, objstorageprovider.NewFileWritable(f1))
	startKey := key(2)
	endKey := key(6)
	lkStart, _ := keys.LockTableSingleKey(startKey, nil)
	lkEnd, _ := keys.LockTableSingleKey(endKey, nil)
	require.NoError(t, w1.ClearRawRange(lkStart, lkEnd, true, true))
	// Not a real lock table key, since lacks a version, but it doesn't matter
	// for this test. We can't use MVCCPut since the intent and the
	// corresponding provisional value belong in different ssts.
	lk3, _ := keys.LockTableSingleKey(key(3), nil)
	require.NoError(t, w1.PutEngineKey(EngineKey{Key: lk3}, []byte("")))
	require.NoError(t, w1.Finish())
	w1.Close()

	fileName2 := "file2"
	f2, err := mem.Create(fileName2, fs.UnspecifiedWriteCategory)
	require.NoError(t, err)
	w2 := MakeIngestionSSTWriter(ctx, st, objstorageprovider.NewFileWritable(f2))
	require.NoError(t, w2.ClearRawRange(startKey, endKey, true, true))
	val := roachpb.MakeValueFromString("value5")
	val.InitChecksum(key(3))
	require.NoError(t, w2.PutMVCC(
		MVCCKey{Key: key(3), Timestamp: ts(2800)}, MVCCValue{Value: val}))
	require.NoError(t, w2.Finish())
	w2.Close()

	require.NoError(t, engs[batchEngine].ConvertFilesToBatchAndCommit(
		ctx, []string{fileName1, fileName2}, []roachpb.Span{
			{Key: lkStart, EndKey: lkEnd}, {Key: startKey, EndKey: endKey},
		}))
	require.NoError(t, engs[ingestEngine].IngestLocalFiles(ctx, []string{fileName1, fileName2}))
	outputState := func(eng Engine) []string {
		it, err := eng.NewEngineIterator(context.Background(), IterOptions{
			UpperBound: roachpb.KeyMax,
			KeyTypes:   IterKeyTypePointsAndRanges,
		})
		require.NoError(t, err)
		defer it.Close()
		var state []string
		valid, err := it.SeekEngineKeyGE(EngineKey{Key: roachpb.KeyMin})
		for valid {
			hasPoint, hasRange := it.HasPointAndRange()
			if hasPoint {
				k, err := it.EngineKey()
				require.NoError(t, err)
				v, err := it.UnsafeValue()
				require.NoError(t, err)
				state = append(state, fmt.Sprintf("point %s = %x\n", k, v))
			}
			if hasRange {
				k, err := it.EngineRangeBounds()
				require.NoError(t, err)
				v := it.EngineRangeKeys()
				for i := range v {
					state = append(state, fmt.Sprintf(
						"range [%s,%s) = %x/%x\n", k.Key, k.EndKey, v[i].Value, v[i].Version))
				}
			}
			valid, err = it.NextEngineKey()
		}
		require.NoError(t, err)
		return state
	}
	require.Equal(t, outputState(engs[ingestEngine]), outputState(engs[batchEngine]))
}

func TestCompactionConcurrencyEnvVars(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	maxProcsBefore := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(maxProcsBefore)

	type testCase struct {
		maxProcs             int
		rocksDBConcurrency   string
		cockroachConcurrency string
		want                 int
	}
	cases := []testCase{
		// Defaults
		{32, "", "", 3},
		{4, "", "", 3},
		{3, "", "", 2},
		{2, "", "", 1},
		{1, "", "", 1},
		// Old COCKROACH_ROCKSDB_CONCURRENCY env var is set. The user-provided
		// value includes 1 slot for flushes, so resulting compaction
		// concurrency is n-1.
		{32, "4", "", 3},
		{4, "4", "", 3},
		{2, "4", "", 3},
		{1, "4", "", 3},
		{32, "8", "", 7},
		{4, "8", "", 7},
		{2, "8", "", 7},
		{1, "8", "", 7},
		// New COCKROACH_CONCURRENT_COMPACTIONS env var is set.
		{32, "", "4", 4},
		{4, "", "4", 4},
		{2, "", "4", 4},
		{1, "", "4", 4},
		{32, "", "8", 8},
		{4, "", "8", 8},
		{2, "", "8", 8},
		{1, "", "8", 8},
		// Both settings are set; COCKROACH_CONCURRENT_COMPACTIONS supersedes
		// COCKROACH_ROCKSDB_CONCURRENCY.
		{32, "8", "4", 4},
		{4, "1", "4", 4},
		{2, "2", "4", 4},
		{1, "5", "4", 4},
		{32, "1", "8", 8},
		{4, "2", "8", 8},
		{2, "4", "8", 8},
		{1, "1", "8", 8},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("GOMAXPROCS=%d,old=%q,new=%q", tc.maxProcs, tc.rocksDBConcurrency, tc.cockroachConcurrency),
			func(t *testing.T) {
				runtime.GOMAXPROCS(tc.maxProcs)

				if tc.rocksDBConcurrency == "" {
					defer envutil.TestUnsetEnv(t, "COCKROACH_ROCKSDB_CONCURRENCY")()
				} else {
					defer envutil.TestSetEnv(t, "COCKROACH_ROCKSDB_CONCURRENCY", tc.rocksDBConcurrency)()
				}
				if tc.cockroachConcurrency == "" {
					defer envutil.TestUnsetEnv(t, "COCKROACH_CONCURRENT_COMPACTIONS")()
				} else {
					defer envutil.TestSetEnv(t, "COCKROACH_CONCURRENT_COMPACTIONS", tc.cockroachConcurrency)()
				}
				require.Equal(t, tc.want, getMaxConcurrentCompactions())
			})
	}
}

func TestMinimumSupportedFormatVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.Equal(t, pebbleFormatVersionMap[clusterversion.MinSupported], MinimumSupportedFormatVersion,
		"MinimumSupportedFormatVersion must match the format version for %s", clusterversion.MinSupported)
}

// delayFS injects a delay on each read.
type delayFS struct {
	vfs.FS
}

func (fs delayFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return delayFile{File: f}, nil
}

type delayFile struct {
	vfs.File
}

func (f delayFile) ReadAt(p []byte, off int64) (n int, err error) {
	time.Sleep(10 * time.Millisecond)
	return f.File.ReadAt(p, off)
}

func TestPebbleLoggingSlowReads(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFunc := func(t *testing.T, fileStr string) int {
		s := log.ScopeWithoutShowLogs(t)
		prevVModule := log.GetVModule()
		_ = log.SetVModule(fileStr + "=2")
		defer func() { _ = log.SetVModule(prevVModule) }()
		defer s.Close(t)

		ctx := context.Background()
		testStartTs := timeutil.Now()

		memFS := vfs.NewMem()
		dFS := delayFS{FS: memFS}
		e, err := fs.InitEnv(context.Background(), dFS, "" /* dir */, fs.EnvConfig{}, nil /* statsCollector */)
		require.NoError(t, err)
		// Tiny block cache, so all reads go to FS.
		db, err := Open(ctx, e, cluster.MakeClusterSettings(), CacheSize(1024))
		require.NoError(t, err)
		defer db.Close()
		// Write some data and flush to disk.
		ts1 := hlc.Timestamp{WallTime: 1}
		k1 := MVCCKey{Key: []byte("a"), Timestamp: ts1}
		v1 := MVCCValue{Value: roachpb.MakeValueFromString("a1")}
		require.NoError(t, db.PutMVCC(k1, v1))
		require.NoError(t, db.Flush())
		// Read the data.
		require.NoError(t, db.MVCCIterate(ctx, roachpb.Key("a"), roachpb.Key("b"), MVCCKeyIterKind, IterKeyTypePointsOnly,
			fs.UnknownReadCategory, func(MVCCKeyValue, MVCCRangeKeyStack) error {
				return nil
			}))

		// Grab the logs and count the slow read entries.
		log.FlushFiles()
		entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
			math.MaxInt64, 2000,
			regexp.MustCompile(fileStr+`\.go`),
			log.WithMarkedSensitiveData)
		require.NoError(t, err)

		// Looking for entries like the following (when fileStr is "pebble_logger_and_tracer"):
		// I240708 14:47:54.610060 12 storage/pebble_logger_and_tracer.go:49  [-] 15  reading 32 bytes took 11.246041ms
		slowReadRegexp, err := regexp.Compile("reading .* bytes took .*")
		require.NoError(t, err)
		slowCount := 0
		for i := range entries {
			if slowReadRegexp.MatchString(entries[i].Message) {
				slowCount++
			}
			t.Logf("%d: %s", i, entries[i].Message)
		}
		return slowCount
	}
	t.Run("pebble_logger_and_tracer", func(t *testing.T) {
		slowCount := testFunc(t, "pebble_logger_and_tracer")
		require.Equal(t, 0, slowCount)
	})
	t.Run("block", func(t *testing.T) {
		slowCount := testFunc(t, "block")
		require.Less(t, 0, slowCount)
	})
}
