// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rditer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func uuidFromString(input string) uuid.UUID {
	u, err := uuid.FromString(input)
	if err != nil {
		panic(err)
	}
	return u
}

// createPointKeys creates sample range data (point keys) in all possible areas
// of the key space. Returns a pair of slices:
// - the encoded keys of all created data.
// - the subset of the encoded keys that are replicated keys.
//
// TODO(sumeer): add lock table and corrsponding MVCC keys.
func createPointKeys(
	t *testing.T, eng storage.Engine, desc roachpb.RangeDescriptor,
) ([]storage.MVCCKey, []storage.MVCCKey) {

	ctx := context.Background()
	unreplicatedPrefix := keys.MakeRangeIDUnreplicatedPrefix(desc.RangeID)

	testTxnID := uuidFromString("0ce61c17-5eb4-4587-8c36-dcf4062ada4c")
	testTxnID2 := uuidFromString("9855a1ef-8eb9-4c06-a106-cab1dda78a2b")
	value := roachpb.MakeValueFromString("value")

	ts0 := hlc.Timestamp{}
	ts := hlc.Timestamp{WallTime: 1}
	localTS := hlc.ClockTimestamp{}

	allKeys := []storage.MVCCKey{
		{Key: keys.AbortSpanKey(desc.RangeID, testTxnID), Timestamp: ts0},
		{Key: keys.AbortSpanKey(desc.RangeID, testTxnID2), Timestamp: ts0},
		{Key: keys.RangeGCThresholdKey(desc.RangeID), Timestamp: ts0},
		{Key: keys.RangeAppliedStateKey(desc.RangeID), Timestamp: ts0},
		{Key: keys.RangeLeaseKey(desc.RangeID), Timestamp: ts0},
		{Key: keys.RangeTombstoneKey(desc.RangeID), Timestamp: ts0},
		{Key: keys.RaftHardStateKey(desc.RangeID), Timestamp: ts0},
		{Key: keys.RaftLogKey(desc.RangeID, 1), Timestamp: ts0},
		{Key: keys.RaftLogKey(desc.RangeID, 2), Timestamp: ts0},
		{Key: keys.RangeLastReplicaGCTimestampKey(desc.RangeID), Timestamp: ts0},
		{Key: keys.RangeDescriptorKey(desc.StartKey), Timestamp: ts},
		{Key: keys.TransactionKey(roachpb.Key(desc.StartKey), uuid.MakeV4()), Timestamp: ts0},
		{Key: keys.TransactionKey(roachpb.Key(desc.StartKey.Next()), uuid.MakeV4()), Timestamp: ts0},
		{Key: keys.TransactionKey(roachpb.Key(desc.EndKey).Prevish(100), uuid.MakeV4()), Timestamp: ts0},
		// TODO(bdarnell): KeyMin.Next() results in a key in the reserved system-local space.
		// Once we have resolved https://github.com/cockroachdb/cockroach/issues/437,
		// replace this with something that reliably generates the first valid key in the range.
		//{r.Desc().StartKey.Next(), ts},
		// The following line is similar to StartKey.Next() but adds more to the key to
		// avoid falling into the system-local space.
		{Key: append(append([]byte{}, desc.StartKey...), '\x02'), Timestamp: ts},
		{Key: roachpb.Key(desc.EndKey).Prevish(100), Timestamp: ts},
	}

	var replicatedKeys []storage.MVCCKey
	for _, key := range allKeys {
		require.NoError(t, storage.MVCCPut(ctx, eng, nil, key.Key, key.Timestamp, localTS, value, nil))
		if !bytes.HasPrefix(key.Key, unreplicatedPrefix) {
			replicatedKeys = append(replicatedKeys, key)
		}
	}

	return allKeys, replicatedKeys
}

// createRangeKeys creates a couple of overlapping range keys in the user
// keyspace, which is the only part of the keyspace we currently expect range
// keys in.
func createRangeKeys(
	t *testing.T, eng storage.Engine, desc roachpb.RangeDescriptor,
) []storage.MVCCRangeKey {

	ctx := context.Background()

	rangeKey := func(start, end string, ts int64) storage.MVCCRangeKey {
		return storage.MVCCRangeKey{
			StartKey:  append(desc.StartKey.AsRawKey().Clone(), start...),
			EndKey:    append(desc.StartKey.AsRawKey().Clone(), end...),
			Timestamp: hlc.Timestamp{WallTime: ts},
		}
	}
	rangeKeys := []storage.MVCCRangeKey{
		rangeKey(":a", ":d", 10),
		rangeKey(":c", ":f", 20),
	}
	for _, rk := range rangeKeys {
		require.NoError(t, storage.ExperimentalMVCCDeleteRangeUsingTombstone(
			ctx, eng, nil, rk.StartKey, rk.EndKey, rk.Timestamp, hlc.ClockTimestamp{}, nil, nil, 0))
	}

	return []storage.MVCCRangeKey{
		rangeKey(":a", ":c", 10),
		rangeKey(":c", ":d", 20),
		rangeKey(":c", ":d", 10),
		rangeKey(":d", ":f", 20),
	}
}

func verifyRDReplicatedOnlyMVCCIter(
	t *testing.T, desc *roachpb.RangeDescriptor, eng storage.Engine, expectedKeys []storage.MVCCKey,
) {
	t.Helper()
	verify := func(t *testing.T, useSpanSet, reverse bool) {
		readWriter := eng.NewReadOnly(storage.StandardDurability)
		defer readWriter.Close()
		if useSpanSet {
			var spans spanset.SpanSet
			spans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
				Key:    keys.MakeRangeIDPrefix(desc.RangeID),
				EndKey: keys.MakeRangeIDPrefix(desc.RangeID).PrefixEnd(),
			})
			spans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
				Key:    keys.MakeRangeKeyPrefix(desc.StartKey),
				EndKey: keys.MakeRangeKeyPrefix(desc.EndKey),
			})
			spans.AddMVCC(spanset.SpanReadOnly, roachpb.Span{
				Key:    desc.StartKey.AsRawKey(),
				EndKey: desc.EndKey.AsRawKey(),
			}, hlc.Timestamp{WallTime: 42})
			readWriter = spanset.NewReadWriterAt(readWriter, &spans, hlc.Timestamp{WallTime: 42})
		}
		iter := NewReplicaMVCCDataIterator(desc, readWriter, reverse /* seekEnd */)
		defer iter.Close()
		actualKeys := []storage.MVCCKey{}
		for {
			ok, err := iter.Valid()
			require.NoError(t, err)
			if !ok {
				break
			}
			if !reverse {
				actualKeys = append(actualKeys, iter.Key())
				iter.Next()
			} else {
				actualKeys = append([]storage.MVCCKey{iter.Key()}, actualKeys...)
				iter.Prev()
			}
		}
		require.Equal(t, expectedKeys, actualKeys)
	}
	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		testutils.RunTrueAndFalse(t, "spanset", func(t *testing.T, useSpanSet bool) {
			verify(t, useSpanSet, reverse)
		})
	})
}

func verifyRDEngineIterPointKeys(
	t *testing.T, desc *roachpb.RangeDescriptor, eng storage.Engine, expectedKeys []storage.MVCCKey,
) {
	readWriter := eng.NewReadOnly(storage.StandardDurability)
	defer readWriter.Close()
	iter := NewReplicaEngineDataIterator(desc, readWriter, storage.IterKeyTypePointsOnly, false)
	defer iter.Close()

	actualKeys := []storage.MVCCKey{}
	var ok bool
	var err error
	for ok, err = iter.SeekStart(); ok && err == nil; ok, err = iter.Next() {
		key, err := iter.UnsafeKey()
		require.NoError(t, err)
		require.True(t, key.IsMVCCKey())
		mvccKey, err := key.ToMVCCKey()
		require.NoError(t, err)
		actualKeys = append(actualKeys, mvccKey.Clone())
	}
	require.NoError(t, err)
	require.Equal(t, expectedKeys, actualKeys)
}

func verifyRDEngineIterRangeKeys(
	t *testing.T,
	desc *roachpb.RangeDescriptor,
	eng storage.Engine,
	expectedKeys []storage.MVCCRangeKey,
) {
	readWriter := eng.NewReadOnly(storage.StandardDurability)
	defer readWriter.Close()
	iter := NewReplicaEngineDataIterator(desc, readWriter, storage.IterKeyTypeRangesOnly, false)
	defer iter.Close()

	var ok bool
	var err error
	actualKeys := []storage.MVCCRangeKey{}
	for ok, err = iter.SeekStart(); ok && err == nil; ok, err = iter.Next() {
		bounds, err := iter.RangeBounds()
		require.NoError(t, err)
		for _, rk := range iter.RangeKeys() {
			ts, err := storage.DecodeMVCCTimestampSuffix(rk.Version)
			require.NoError(t, err)
			actualKeys = append(actualKeys, storage.MVCCRangeKey{
				StartKey:  bounds.Key.Clone(),
				EndKey:    bounds.EndKey.Clone(),
				Timestamp: ts,
			})
		}
	}
	require.NoError(t, err)
	require.Equal(t, expectedKeys, actualKeys)
}

// TestReplicaDataIterator verifies correct operation of iterator if
// a range contains no data and never has.
func TestReplicaDataIteratorEmptyRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	desc := &roachpb.RangeDescriptor{
		RangeID:  12345,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("z"),
	}

	verifyRDReplicatedOnlyMVCCIter(t, desc, eng, []storage.MVCCKey{})
	verifyRDEngineIterPointKeys(t, desc, eng, []storage.MVCCKey{})
	verifyRDEngineIterRangeKeys(t, desc, eng, []storage.MVCCRangeKey{})
}

// TestReplicaDataIterator creates three ranges {"a"-"b" (pre), "b"-"c"
// (main test range), "c"-"d" (post)} and fills each with data. It
// first verifies the contents of the "b"-"c" range. Next, it makes sure
// a replicated-only iterator does not show any unreplicated keys from
// the range. Then, it deletes the range and verifies it's empty. Finally,
// it verifies the pre and post ranges still contain the expected data.
func TestReplicaDataIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	descPre := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	}
	desc := roachpb.RangeDescriptor{
		RangeID:  2,
		StartKey: roachpb.RKey("b"),
		EndKey:   roachpb.RKey("c"),
	}
	descPost := roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("c"),
		EndKey:   roachpb.RKey("d"),
	}

	// Create range data for all three ranges.
	preKeys, preReplicatedKeys := createPointKeys(t, eng, descPre)
	preRangeKeys := createRangeKeys(t, eng, descPre)
	curKeys, curReplicatedKeys := createPointKeys(t, eng, desc)
	curRangeKeys := createRangeKeys(t, eng, desc)
	postKeys, postReplicatedKeys := createPointKeys(t, eng, descPost)
	postRangeKeys := createRangeKeys(t, eng, descPost)

	// Verify the replicated contents of the "b"-"c" range.
	t.Run("cur-replicated", func(t *testing.T) {
		verifyRDReplicatedOnlyMVCCIter(t, &desc, eng, curReplicatedKeys)
	})
	// Verify the complete contents of the "b"-"c" range.
	t.Run("cur", func(t *testing.T) {
		verifyRDEngineIterPointKeys(t, &desc, eng, curKeys)
		verifyRDEngineIterRangeKeys(t, &desc, eng, curRangeKeys)
	})

	// Verify the replicated keys in pre & post ranges.
	for _, test := range []struct {
		name string
		desc *roachpb.RangeDescriptor
		keys []storage.MVCCKey
	}{
		{"pre-replicated", &descPre, preReplicatedKeys},
		{"post-replicated", &descPost, postReplicatedKeys},
	} {
		t.Run(test.name, func(t *testing.T) {
			verifyRDReplicatedOnlyMVCCIter(t, test.desc, eng, test.keys)
		})
	}
	// Verify the complete keys in pre & post ranges.
	for _, test := range []struct {
		name      string
		desc      *roachpb.RangeDescriptor
		keys      []storage.MVCCKey
		rangeKeys []storage.MVCCRangeKey
	}{
		{"pre", &descPre, preKeys, preRangeKeys},
		{"post", &descPost, postKeys, postRangeKeys},
	} {
		t.Run(test.name, func(t *testing.T) {
			verifyRDEngineIterPointKeys(t, test.desc, eng, test.keys)
			verifyRDEngineIterRangeKeys(t, test.desc, eng, test.rangeKeys)
		})
	}
}

func checkOrdering(t *testing.T, ranges []KeyRange) {
	for i := 1; i < len(ranges); i++ {
		if ranges[i].Start.Compare(ranges[i-1].End) < 0 {
			t.Fatalf("ranges need to be ordered and non-overlapping, but %s > %s",
				ranges[i-1].End, ranges[i].Start)
		}
	}
}

// TestReplicaDataIteratorCoveringRangeKey creates three ranges {"a"-"b" (pre),
// "b"-"c" (main test range), "c"-"d" (post)} and writes an MVCC range key
// across the entire keyspace (replicated and unreplicated). It then verifies
// that the range key is properly truncated and filtered to the iterator's
// key ranges.
func TestReplicaDataIteratorCoveringRangeKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up a new engine and write a single range key across the entire span.
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	require.NoError(t, eng.ExperimentalPutEngineRangeKey(keys.MinKey.Next(), keys.MaxKey, []byte{1}, []byte{}))

	// Use a pebbleReadOnly for the iteration, because we need consistent
	// iterators.
	r := eng.NewReadOnly(storage.StandardDurability)
	defer r.Close()

	// Iterate over three range descriptors, both replicated and unreplicated.
	descs := []roachpb.RangeDescriptor{
		{
			RangeID:  1,
			StartKey: roachpb.RKey("a"),
			EndKey:   roachpb.RKey("b"),
		},
		{
			RangeID:  2,
			StartKey: roachpb.RKey("b"),
			EndKey:   roachpb.RKey("c"),
		},
		{
			RangeID:  3,
			StartKey: roachpb.RKey("c"),
			EndKey:   roachpb.RKey("d"),
		},
	}
	for _, desc := range descs {
		t.Run(desc.KeySpan().String(), func(t *testing.T) {
			// A point key iterator should never see anything.
			pointIter := NewReplicaEngineDataIterator(&desc, r, storage.IterKeyTypePointsOnly, false)
			defer pointIter.Close()
			ok, err := pointIter.SeekStart()
			require.NoError(t, err)
			require.False(t, ok)

			// A range key iterator should see range keys spanning all relevant key ranges.
			testutils.RunTrueAndFalse(t, "replicatedOnly", func(t *testing.T, replicatedOnly bool) {
				rangeIter := NewReplicaEngineDataIterator(&desc, r, storage.IterKeyTypeRangesOnly, replicatedOnly)
				defer rangeIter.Close()

				var expectedRanges []KeyRange
				if replicatedOnly {
					expectedRanges = MakeReplicatedKeyRanges(&desc)
				} else {
					expectedRanges = MakeAllKeyRanges(&desc)
				}

				var actualRanges []KeyRange
				for ok, err = rangeIter.SeekStart(); ok && err == nil; ok, err = rangeIter.Next() {
					bounds, err := rangeIter.RangeBounds()
					require.NoError(t, err)
					actualRanges = append(actualRanges, KeyRange{
						Start: bounds.Key.Clone(),
						End:   bounds.EndKey.Clone(),
					})
				}
				require.NoError(t, err)
				require.Equal(t, expectedRanges, actualRanges)
			})
		})
	}
}

func TestReplicaKeyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
	}
	checkOrdering(t, MakeAllKeyRanges(&desc))
	checkOrdering(t, MakeReplicatedKeyRanges(&desc))
	checkOrdering(t, MakeReplicatedKeyRangesExceptLockTable(&desc))
	checkOrdering(t, MakeReplicatedKeyRangesExceptRangeID(&desc))
}

func BenchmarkReplicaEngineDataIterator(b *testing.B) {
	skip.UnderShort(b)
	for _, numRanges := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("ranges=%d", numRanges), func(b *testing.B) {
			for _, numKeysPerRange := range []int{1, 100, 10000} {
				b.Run(fmt.Sprintf("keysPerRange=%d", numKeysPerRange), func(b *testing.B) {
					for _, valueSize := range []int{32} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							benchReplicaEngineDataIterator(b, numRanges, numKeysPerRange, valueSize)
						})
					}
				})
			}
		})
	}
}

func benchReplicaEngineDataIterator(b *testing.B, numRanges, numKeysPerRange, valueSize int) {
	ctx := context.Background()

	// Set up ranges.
	var descs []roachpb.RangeDescriptor
	for i := 1; i <= numRanges; i++ {
		desc := roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i),
			StartKey: append([]byte{'k'}, 0, 0, 0, 0),
			EndKey:   append([]byte{'k'}, 0, 0, 0, 0),
		}
		binary.BigEndian.PutUint32(desc.StartKey[1:], uint32(i))
		binary.BigEndian.PutUint32(desc.EndKey[1:], uint32(i+1))
		descs = append(descs, desc)
	}

	// Write data for ranges.
	eng, err := storage.Open(ctx,
		storage.Filesystem(b.TempDir()),
		storage.CacheSize(1e9),
		storage.Settings(cluster.MakeTestingClusterSettings()))
	require.NoError(b, err)
	defer eng.Close()

	batch := eng.NewBatch()
	defer batch.Close()

	rng, _ := randutil.NewTestRand()
	value := randutil.RandBytes(rng, valueSize)

	for _, desc := range descs {
		var keyBuf roachpb.Key
		keyRanges := MakeAllKeyRanges(&desc)
		for i := 0; i < numKeysPerRange; i++ {
			keyBuf = append(keyBuf[:0], keyRanges[i%len(keyRanges)].Start...)
			keyBuf = append(keyBuf, 0, 0, 0, 0)
			binary.BigEndian.PutUint32(keyBuf[len(keyBuf)-4:], uint32(i))
			if err := batch.PutEngineKey(storage.EngineKey{Key: keyBuf}, value); err != nil {
				require.NoError(b, err) // slow, so check err != nil first
			}
		}
	}
	require.NoError(b, batch.Commit(true /* sync */))
	require.NoError(b, eng.Flush())
	require.NoError(b, eng.Compact())

	snapshot := eng.NewSnapshot()
	defer snapshot.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, desc := range descs {
			iter := NewReplicaEngineDataIterator(
				&desc, snapshot, storage.IterKeyTypePointsOnly, false /* replicatedOnly */)
			defer iter.Close()
			var ok bool
			var err error
			for ok, err = iter.SeekStart(); ok && err == nil; ok, err = iter.Next() {
				_, _ = iter.UnsafeKey()
				_ = iter.UnsafeValue()
			}
			if err != nil {
				require.NoError(b, err)
			}
		}
	}
}
