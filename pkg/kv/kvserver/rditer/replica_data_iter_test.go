// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rditer

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/olekukonko/tablewriter"
	"github.com/stretchr/testify/require"
)

func uuidFromString(input string) uuid.UUID {
	u, err := uuid.FromString(input)
	if err != nil {
		panic(err)
	}
	return u
}

// createRangeData creates sample range data (point and range keys) in all
// possible areas of the key space. Returns a pair of slices containing
// an ordered mix of MVCCKey and MVCCRangeKey with:
// - the encoded keys of all created data.
// - the subset of the encoded keys that are replicated keys.
func createRangeData(
	t *testing.T, eng storage.Engine, desc roachpb.RangeDescriptor,
) ([]storage.MVCCKey, []storage.MVCCRangeKey) {

	ctx := context.Background()
	unreplicatedPrefix := keys.MakeRangeIDUnreplicatedPrefix(desc.RangeID)
	replicatedPrefix := keys.MakeRangeIDReplicatedPrefix(desc.RangeID)

	testTxnID := uuidFromString("0ce61c17-5eb4-4587-8c36-dcf4062ada4c")
	testTxnID2 := uuidFromString("9855a1ef-8eb9-4c06-a106-cab1dda78a2b")
	testTxnID3 := uuidFromString("295e727c-8ca9-437c-bb5e-8e2ebbad996f")
	value := roachpb.MakeValueFromString("value")

	ts0 := hlc.Timestamp{}
	ts := hlc.Timestamp{WallTime: 1}
	localTS := hlc.ClockTimestamp{}

	var ps []storage.MVCCKey
	var rs []storage.MVCCRangeKey

	ps = append(ps,
		// StateMachine (i.e. replicated) keys that are keyed by RangeID.
		storage.MVCCKey{Key: keys.AbortSpanKey(desc.RangeID, testTxnID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.AbortSpanKey(desc.RangeID, testTxnID2), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeGCThresholdKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeAppliedStateKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeLeaseKey(desc.RangeID), Timestamp: ts0},
	)
	rs = append(rs, storage.MVCCRangeKey{ // emitted last because we emit all point keys before range keys
		StartKey:  append(replicatedPrefix.Clone(), []byte(":a")...),
		EndKey:    append(replicatedPrefix.Clone(), []byte(":x")...),
		Timestamp: ts,
	})

	ps = append(ps,
		// Non-StateMachine (i.e. unreplicated) keys that are keyed by RangeID.
		storage.MVCCKey{Key: keys.RangeTombstoneKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RaftHardStateKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RaftLogKey(desc.RangeID, 1), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RaftLogKey(desc.RangeID, 2), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeLastReplicaGCTimestampKey(desc.RangeID), Timestamp: ts0},
	)
	rs = append(rs, storage.MVCCRangeKey{ // emitted last because we emit all point keys before range keys
		StartKey:  append(unreplicatedPrefix.Clone(), []byte(":a")...),
		EndKey:    append(unreplicatedPrefix.Clone(), []byte(":x")...),
		Timestamp: ts,
	})

	ps = append(ps,
		// Replicated system keys: range descriptor, txns, locks, user keys.
		storage.MVCCKey{Key: keys.RangeDescriptorKey(desc.StartKey), Timestamp: ts}, // call this [1], referenced by locks
		storage.MVCCKey{Key: keys.TransactionKey(roachpb.Key(desc.StartKey), testTxnID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.TransactionKey(roachpb.Key(desc.StartKey.Next()), testTxnID2), Timestamp: ts0},
		storage.MVCCKey{Key: keys.TransactionKey(roachpb.Key(desc.EndKey).Prevish(5), testTxnID3), Timestamp: ts0},
		storage.MVCCKey{Key: desc.StartKey.AsRawKey(), Timestamp: ts}, // call this [2], referenced by locks
		storage.MVCCKey{Key: roachpb.Key(desc.EndKey).Prevish(5), Timestamp: ts},
	)

	locks := []storage.LockTableKey{
		{
			Key:      keys.RangeDescriptorKey(desc.StartKey), // mark [1] above as intent
			Strength: lock.Intent,
			TxnUUID:  testTxnID,
		}, {
			Key:      desc.StartKey.AsRawKey(), // mark [2] above as intent
			Strength: lock.Intent,
			TxnUUID:  testTxnID,
		},
	}
	lockMeta := enginepb.MVCCMetadata{
		Txn:      &enginepb.TxnMeta{ID: testTxnID},
		KeyBytes: storage.MVCCVersionTimestampSize,
		ValBytes: int64(len(value.RawBytes)),
	}
	lockVal, err := protoutil.Marshal(&lockMeta)
	require.NoError(t, err)

	rs = append(rs, storage.MVCCRangeKey{ // emitted last because we emit all point keys before range keys
		StartKey:  desc.StartKey.AsRawKey().Clone(),
		EndKey:    desc.EndKey.AsRawKey().Clone(),
		Timestamp: ts,
	})

	for _, pk := range ps {
		_, err := storage.MVCCPut(ctx, eng, pk.Key, pk.Timestamp, value, storage.MVCCWriteOptions{LocalTimestamp: localTS})
		require.NoError(t, err)
	}
	for _, rk := range rs {
		require.NoError(t, eng.PutMVCCRangeKey(rk, storage.MVCCValue{}))
	}
	for _, l := range locks {
		sl, _ := l.ToEngineKey(nil)
		require.NoError(t, eng.PutEngineKey(sl, lockVal))
	}

	return ps, rs
}

// verifyIterateReplicaKeySpans verifies that IterateReplicaKeySpans returns the
// expected keys in the expected order. The expected keys can be either MVCCKey
// or MVCCRangeKey.
func verifyIterateReplicaKeySpans(
	t *testing.T,
	tbl *tablewriter.Table,
	desc *roachpb.RangeDescriptor,
	eng storage.Engine,
	replicatedOnly bool,
	replicatedSpansFilter ReplicatedSpansFilter,
) {
	readWriter := eng.NewSnapshot()
	defer readWriter.Close()

	tbl.SetAlignment(tablewriter.ALIGN_LEFT)
	tbl.SetHeader([]string{
		"span",
		"key_hex",
		"endKey_hex",
		"version_hex",
		"pretty",
	})

	require.NoError(t, IterateReplicaKeySpans(context.Background(), desc, readWriter, replicatedOnly,
		replicatedSpansFilter,
		func(iter storage.EngineIterator, span roachpb.Span) error {
			var err error
			for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
				// Span should not be empty.
				require.NotZero(t, span)

				key, err := iter.UnsafeEngineKey()
				require.NoError(t, err)
				require.True(t, span.ContainsKey(key.Key), "%s not in %s", key, span)
				require.True(t, key.IsLockTableKey() || key.IsMVCCKey(), "%s neither lock nor MVCC", key)

				hasPoint, hasRange := iter.HasPointAndRange()
				if hasPoint {
					var mvccKey storage.MVCCKey
					if key.IsMVCCKey() {
						var err error
						mvccKey, err = key.ToMVCCKey()
						require.NoError(t, err)
						if replicatedSpansFilter == ReplicatedSpansExcludeUser && desc.KeySpan().AsRawSpanWithNoLocals().ContainsKey(key.Key) {
							t.Fatalf("unexpected user key when user key are expected to be skipped: %s", mvccKey)
						}
					} else { // lock key
						ltk, err := key.ToLockTableKey()
						require.NoError(t, err)
						mvccKey = storage.MVCCKey{
							Key: ltk.Key,
						}
						if replicatedSpansFilter == ReplicatedSpansUserOnly {
							t.Fatalf("unexpected lock table key when only table keys requested: %s", ltk.Key)
						}
					}
					tbl.Append([]string{
						span.String(),
						fmt.Sprintf("%x", key.Key),
						"",
						fmt.Sprintf("%x", key.Version),
						mvccKey.String(),
					})
				}
				if hasRange && iter.RangeKeyChanged() {
					bounds, err := iter.EngineRangeBounds()
					require.NoError(t, err)
					require.True(t, span.Contains(bounds), "%s not contained in %s", bounds, span)
					for _, rk := range iter.EngineRangeKeys() {
						ts, err := storage.DecodeMVCCTimestampSuffix(rk.Version)
						require.NoError(t, err)
						mvccRangeKey := storage.MVCCRangeKey{
							StartKey:  bounds.Key.Clone(),
							EndKey:    bounds.EndKey.Clone(),
							Timestamp: ts,
						}
						tbl.Append([]string{
							span.String(),
							fmt.Sprintf("%x", bounds.Key),
							fmt.Sprintf("%x", bounds.EndKey),
							fmt.Sprintf("%x", rk.Version),
							mvccRangeKey.String(),
						})
					}
				}
			}
			return err
		}))
}

// TestReplicaDataIterator creates three ranges (a-b, b-c, c-d) and fills each
// with data, then verifies the contents for MVCC and Engine iterators, both
// replicated and unreplicated.
func TestReplicaDataIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Create test cases with test data for each descriptor.
	testcases := []struct {
		desc roachpb.RangeDescriptor
	}{
		// TODO(tbg): add first and last range test here.
		{
			desc: roachpb.RangeDescriptor{
				RangeID:  1,
				StartKey: roachpb.RKey("a"),
				EndKey:   roachpb.RKey("b"),
			},
		},
		{
			desc: roachpb.RangeDescriptor{
				RangeID:  2,
				StartKey: roachpb.RKey("b"),
				EndKey:   roachpb.RKey("c"),
			},
		},
		{
			desc: roachpb.RangeDescriptor{
				RangeID:  3,
				StartKey: roachpb.RKey("c"),
				EndKey:   roachpb.RKey("d"),
			},
		},
	}

	for i := range testcases {
		createRangeData(t, eng, testcases[i].desc)
	}

	// Run tests.
	path := datapathutils.TestDataPath(t, t.Name())
	for _, tc := range testcases {
		parName := fmt.Sprintf("r%d", tc.desc.RangeID)
		t.Run(parName, func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "replicatedOnly", func(t *testing.T, replicatedOnly bool) {
				replicatedSpans := []ReplicatedSpansFilter{ReplicatedSpansAll, ReplicatedSpansExcludeUser, ReplicatedSpansUserOnly}
				for i := range replicatedSpans {
					replicatedKeysName := "all"
					switch replicatedSpans[i] {
					case ReplicatedSpansExcludeUser:
						replicatedKeysName = "exclude-user"
					case ReplicatedSpansUserOnly:
						replicatedKeysName = "user-only"
					}
					t.Run(fmt.Sprintf("replicatedSpans=%v", replicatedKeysName), func(t *testing.T) {
						name := "all"
						if replicatedOnly {
							name = "replicatedOnly"
						}
						w := echotest.NewWalker(t, filepath.Join(path, parName, name, replicatedKeysName))

						w.Run(t, "output", func(t *testing.T) string {
							var innerBuf strings.Builder
							tbl := tablewriter.NewWriter(&innerBuf)
							// Print contents of the Replica according to the iterator.
							verifyIterateReplicaKeySpans(t, tbl, &tc.desc, eng, replicatedOnly, replicatedSpans[i])

							tbl.Render()
							return innerBuf.String()
						})(t)
					})
				}
			})
		})
	}
}

func TestIterateMVCCReplicaKeySpansSpansSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	}

	createRangeData(t, eng, desc)

	// Verify that we're getting identical replicated MVCC contents across
	// multiple ways to scan. We already have TestReplicaDataIterator above
	// to show that the output is sane so here we only have to verify every
	// way to scan gives the same result.
	//
	// TODO(oleg): This is very naive and only works if keys don't overlap.
	// Needs some thinking on how could we use this if ranges become
	// fragmented.
	//
	get := func(t *testing.T, useSpanSet, reverse bool) ([]storage.MVCCKey, []storage.MVCCRangeKey) {
		reader := eng.NewReader(storage.StandardDurability)
		defer reader.Close()
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
			reader = spanset.NewReader(reader, &spans, hlc.Timestamp{WallTime: 42})
		}
		var rangeStart roachpb.Key
		var actualKeys []storage.MVCCKey
		var actualRanges []storage.MVCCRangeKey
		err := IterateMVCCReplicaKeySpans(context.Background(), &desc, reader, IterateOptions{
			CombineRangesAndPoints: false,
			Reverse:                reverse,
		}, func(iter storage.MVCCIterator, span roachpb.Span, keyType storage.IterKeyType) error {
			for {
				ok, err := iter.Valid()
				require.NoError(t, err)
				if !ok {
					break
				}
				p, r := iter.HasPointAndRange()
				if p {
					if !reverse {
						actualKeys = append(actualKeys, iter.UnsafeKey().Clone())
					} else {
						actualKeys = append([]storage.MVCCKey{iter.UnsafeKey().Clone()}, actualKeys...)
					}
				}
				if r {
					rangeKeys := iter.RangeKeys().Clone()
					if !rangeKeys.Bounds.Key.Equal(rangeStart) {
						rangeStart = rangeKeys.Bounds.Key
						if !reverse {
							for _, v := range rangeKeys.Versions {
								actualRanges = append(actualRanges, rangeKeys.AsRangeKey(v))
							}
						} else {
							for i := rangeKeys.Len() - 1; i >= 0; i-- {
								actualRanges = append([]storage.MVCCRangeKey{
									rangeKeys.AsRangeKey(rangeKeys.Versions[i]),
								}, actualRanges...)
							}
						}
					}
				}
				if reverse {
					iter.Prev()
				} else {
					iter.Next()
				}
			}
			return nil
		})
		require.NoError(t, err, "visitor failed")
		return actualKeys, actualRanges
	}
	goldenActualKeys, goldenActualRanges := get(t, false /* useSpanSet */, false /* reverse */)
	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		testutils.RunTrueAndFalse(t, "spanset", func(t *testing.T, useSpanSet bool) {
			actualKeys, actualRanges := get(t, useSpanSet, reverse)
			require.Equal(t, goldenActualKeys, actualKeys)
			require.Equal(t, goldenActualRanges, actualRanges)
		})
	})
}

func checkOrdering(t *testing.T, spans []roachpb.Span) {
	for i := 1; i < len(spans); i++ {
		if spans[i].Key.Compare(spans[i-1].EndKey) < 0 {
			t.Fatalf("ranges need to be ordered and non-overlapping, but %s > %s",
				spans[i-1].EndKey, spans[i].Key)
		}
	}
}

// TestReplicaDataIteratorGlobalRangeKey creates three ranges {a-b, b-c, c-d}
// and writes an MVCC range key across the entire keyspace (replicated and
// unreplicated). It then verifies that the range key is properly truncated and
// filtered to the iterator's key spans.
func TestReplicaDataIteratorGlobalRangeKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up a new engine and write a single range key across the entire span.
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	require.NoError(t, eng.PutEngineRangeKey(keys.MinKey.Next(), keys.MaxKey, []byte{1}, []byte{}))

	// Use a snapshot for the iteration, because we need consistent
	// iterators.
	snapshot := eng.NewSnapshot()
	defer snapshot.Close()

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
			// Iterators should see range keys spanning all relevant key spans.
			testutils.RunTrueAndFalse(t, "replicatedOnly", func(t *testing.T, replicatedOnly bool) {
				var expectedSpans []roachpb.Span
				if replicatedOnly {
					expectedSpans = MakeReplicatedKeySpans(&desc)
				} else {
					expectedSpans = MakeAllKeySpans(&desc)
				}

				var actualSpans []roachpb.Span
				require.NoError(t, IterateReplicaKeySpans(
					context.Background(), &desc, snapshot, replicatedOnly, ReplicatedSpansAll,
					func(iter storage.EngineIterator, span roachpb.Span) error {
						// We should never see any point keys.
						hasPoint, hasRange := iter.HasPointAndRange()
						require.False(t, hasPoint)
						require.True(t, hasRange)

						// The iterator should already be positioned on the range key, which should
						// span the entire key span and be the only range key.
						bounds, err := iter.EngineRangeBounds()
						require.NoError(t, err)
						require.Equal(t, span, bounds)
						actualSpans = append(actualSpans, bounds.Clone())

						ok, err := iter.NextEngineKey()
						require.NoError(t, err)
						require.False(t, ok)

						return nil
					}))
				require.Equal(t, expectedSpans, actualSpans)
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
	checkOrdering(t, MakeAllKeySpans(&desc))
	checkOrdering(t, MakeReplicatedKeySpans(&desc))
	checkOrdering(t, makeReplicatedKeySpansExceptLockTable(&desc))
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
		fs.MustInitPhysicalTestingEnv(b.TempDir()),
		cluster.MakeTestingClusterSettings(),
		storage.CacheSize(1e9))
	require.NoError(b, err)
	defer eng.Close()

	batch := eng.NewBatch()
	defer batch.Close()

	rng, _ := randutil.NewTestRand()
	value := randutil.RandBytes(rng, valueSize)

	for _, desc := range descs {
		var keyBuf roachpb.Key
		keySpans := MakeAllKeySpans(&desc)
		for i := 0; i < numKeysPerRange; i++ {
			keyBuf = append(keyBuf[:0], keySpans[i%len(keySpans)].Key...)
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
			err := IterateReplicaKeySpans(
				context.Background(), &desc, snapshot, false /* replicatedOnly */, ReplicatedSpansAll,
				func(iter storage.EngineIterator, _ roachpb.Span) error {
					var err error
					for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
						_, _ = iter.UnsafeEngineKey()
						_, _ = iter.UnsafeValue()
					}
					return err
				})
			if err != nil {
				require.NoError(b, err)
			}
		}
	}
}

func TestIterateMVCCReplicaKeySpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up a new engine and write a single range key across the entire span.
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	require.NoError(t, eng.PutMVCCRangeKey(storage.MVCCRangeKey{
		StartKey:  keys.MinKey.Next(),
		EndKey:    keys.MaxKey,
		Timestamp: hlc.Timestamp{WallTime: 10},
	}, storage.MVCCValue{}))
	require.NoError(t,
		eng.PutMVCC(storage.MVCCKey{Key: roachpb.Key("a1"), Timestamp: hlc.Timestamp{WallTime: 1}},
			storage.MVCCValue{}))
	require.NoError(t,
		eng.PutMVCC(storage.MVCCKey{Key: roachpb.Key("a2"), Timestamp: hlc.Timestamp{WallTime: 1}},
			storage.MVCCValue{}))
	require.NoError(t,
		eng.PutMVCC(storage.MVCCKey{Key: roachpb.Key("b1"), Timestamp: hlc.Timestamp{WallTime: 1}},
			storage.MVCCValue{}))
	require.NoError(t,
		eng.PutMVCC(storage.MVCCKey{Key: roachpb.Key("b2"), Timestamp: hlc.Timestamp{WallTime: 1}},
			storage.MVCCValue{}))
	require.NoError(t,
		eng.PutMVCC(storage.MVCCKey{Key: roachpb.Key("c1"), Timestamp: hlc.Timestamp{WallTime: 1}},
			storage.MVCCValue{}))
	require.NoError(t,
		eng.PutMVCC(storage.MVCCKey{Key: roachpb.Key("c2"), Timestamp: hlc.Timestamp{WallTime: 1}},
			storage.MVCCValue{}))

	// Use a snapshot for the iteration, because we need consistent
	// iterators.
	snapshot := eng.NewSnapshot()
	defer snapshot.Close()

	// Iterate over three range descriptors, both replicated and unreplicated.
	for _, d := range []struct {
		desc roachpb.RangeDescriptor
		pts  []roachpb.Key
	}{
		{
			desc: roachpb.RangeDescriptor{
				RangeID:  1,
				StartKey: roachpb.RKey("a"),
				EndKey:   roachpb.RKey("b"),
			},
			pts: []roachpb.Key{
				roachpb.Key("a1"),
				roachpb.Key("a2"),
			},
		},
		{
			desc: roachpb.RangeDescriptor{
				RangeID:  2,
				StartKey: roachpb.RKey("b"),
				EndKey:   roachpb.RKey("c"),
			},
			pts: []roachpb.Key{
				roachpb.Key("b1"),
				roachpb.Key("b2"),
			},
		},
		{
			desc: roachpb.RangeDescriptor{
				RangeID:  3,
				StartKey: roachpb.RKey("c"),
				EndKey:   roachpb.RKey("d"),
			},
			pts: []roachpb.Key{
				roachpb.Key("c1"),
				roachpb.Key("c2"),
			},
		},
	} {
		t.Run(d.desc.KeySpan().String(), func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
				// Each iteration would go through all spans because we usa a single
				// range key covering all spans of all ranges.
				expectedSpans := makeReplicatedKeySpansExceptLockTable(&d.desc)
				if reverse {
					for i, j := 0, len(expectedSpans)-1; i < j; i, j = i+1, j-1 {
						expectedSpans[i], expectedSpans[j] = expectedSpans[j], expectedSpans[i]
					}
				}
				points := append([]roachpb.Key(nil), d.pts...)
				if reverse {
					for i, j := 0, len(points)-1; i < j; i, j = i+1, j-1 {
						points[i], points[j] = points[j], points[i]
					}
				}
				advance := func(iter storage.MVCCIterator) {
					if reverse {
						iter.Prev()
					} else {
						iter.Next()
					}
				}
				t.Run("sequential", func(t *testing.T) {
					var actualSpans []roachpb.Span
					var actualPoints []roachpb.Key
					require.NoError(t, IterateMVCCReplicaKeySpans(context.Background(), &d.desc, snapshot,
						IterateOptions{CombineRangesAndPoints: false, Reverse: reverse},
						func(iter storage.MVCCIterator, span roachpb.Span, keyType storage.IterKeyType) error {
							if keyType == storage.IterKeyTypePointsOnly {
								for {
									ok, err := iter.Valid()
									require.NoError(t, err)
									if !ok {
										break
									}
									p, r := iter.HasPointAndRange()
									require.False(t, r, "unexpected ranges found")
									require.True(t, p, "no points found")
									actualPoints = append(actualPoints, iter.UnsafeKey().Key.Clone())
									advance(iter)
								}
							}
							if keyType == storage.IterKeyTypeRangesOnly {
								// We only count spans for range keys for simplicity. They should
								// register since we have a single range key spanning all key
								// space.
								actualSpans = append(actualSpans, span.Clone())

								p, r := iter.HasPointAndRange()
								require.True(t, r, "no ranges found")
								require.False(t, p, "unexpected points found")
								rk := iter.RangeBounds()
								require.True(t, span.Contains(rk), "found range key is not contained to iterator span")
								advance(iter)
								ok, err := iter.Valid()
								require.NoError(t, err)
								require.False(t, ok)
							}
							return nil
						}))
					require.Equal(t, points, actualPoints)
					require.Equal(t, expectedSpans, actualSpans)
				})

				t.Run("combined", func(t *testing.T) {
					var actualSpans []roachpb.Span
					var actualPoints []roachpb.Key
					require.NoError(t, IterateMVCCReplicaKeySpans(
						context.Background(), &d.desc, snapshot, IterateOptions{CombineRangesAndPoints: true, Reverse: reverse},
						func(iter storage.MVCCIterator, span roachpb.Span, keyType storage.IterKeyType) error {
							actualSpans = append(actualSpans, span.Clone())
							_, r := iter.HasPointAndRange()
							require.True(t, r, "must have range")
							rk := iter.RangeBounds()
							require.True(t, span.Contains(rk), "found range key is not contained to iterator span")
							for {
								ok, err := iter.Valid()
								require.NoError(t, err)
								if !ok {
									break
								}
								if p, _ := iter.HasPointAndRange(); p {
									actualPoints = append(actualPoints, iter.UnsafeKey().Key.Clone())
								}
								advance(iter)
							}
							return nil
						}))
					require.Equal(t, points, actualPoints)
					require.Equal(t, expectedSpans, actualSpans)
				})
			})
		})
	}
}
