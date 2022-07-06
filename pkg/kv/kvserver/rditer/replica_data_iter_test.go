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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

// createRangeData creates sample range data (point and range keys) in all
// possible areas of the key space. Returns a pair of slices containing
// an ordered mix of MVCCKey and MVCCRangeKey with:
// - the encoded keys of all created data.
// - the subset of the encoded keys that are replicated keys.
//
// TODO(sumeer): add lock table and corrsponding MVCC keys.
func createRangeData(
	t *testing.T, eng storage.Engine, desc roachpb.RangeDescriptor,
) ([]interface{}, []interface{}) {

	ctx := context.Background()
	unreplicatedPrefix := keys.MakeRangeIDUnreplicatedPrefix(desc.RangeID)
	replicatedPrefix := keys.MakeRangeIDReplicatedPrefix(desc.RangeID)

	testTxnID := uuidFromString("0ce61c17-5eb4-4587-8c36-dcf4062ada4c")
	testTxnID2 := uuidFromString("9855a1ef-8eb9-4c06-a106-cab1dda78a2b")
	value := roachpb.MakeValueFromString("value")

	ts0 := hlc.Timestamp{}
	ts := hlc.Timestamp{WallTime: 1}
	localTS := hlc.ClockTimestamp{}

	allKeys := []interface{}{
		storage.MVCCKey{Key: keys.AbortSpanKey(desc.RangeID, testTxnID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.AbortSpanKey(desc.RangeID, testTxnID2), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeGCThresholdKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeAppliedStateKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeLeaseKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeTombstoneKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RaftHardStateKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RaftLogKey(desc.RangeID, 1), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RaftLogKey(desc.RangeID, 2), Timestamp: ts0},
		storage.MVCCKey{Key: keys.RangeLastReplicaGCTimestampKey(desc.RangeID), Timestamp: ts0},
		storage.MVCCRangeKey{
			StartKey:  append(replicatedPrefix.Clone(), []byte(":a")...),
			EndKey:    append(replicatedPrefix.Clone(), []byte(":x")...),
			Timestamp: ts,
		},
		storage.MVCCRangeKey{
			StartKey:  append(unreplicatedPrefix.Clone(), []byte(":a")...),
			EndKey:    append(unreplicatedPrefix.Clone(), []byte(":x")...),
			Timestamp: ts,
		},
		storage.MVCCKey{Key: keys.RangeDescriptorKey(desc.StartKey), Timestamp: ts},
		storage.MVCCKey{Key: keys.TransactionKey(roachpb.Key(desc.StartKey), uuid.MakeV4()), Timestamp: ts0},
		storage.MVCCKey{Key: keys.TransactionKey(roachpb.Key(desc.StartKey.Next()), uuid.MakeV4()), Timestamp: ts0},
		storage.MVCCKey{Key: keys.TransactionKey(roachpb.Key(desc.EndKey).Prevish(100), uuid.MakeV4()), Timestamp: ts0},
		// TODO(bdarnell): KeyMin.Next() results in a key in the reserved system-local space.
		// Once we have resolved https://github.com/cockroachdb/cockroach/issues/437,
		// replace this with something that reliably generates the first valid key in the range.
		//{r.Desc().StartKey.Next(), ts},
		// The following line is similar to StartKey.Next() but adds more to the key to
		// avoid falling into the system-local space.
		storage.MVCCKey{Key: append(desc.StartKey.AsRawKey().Clone(), '\x02'), Timestamp: ts},
		storage.MVCCKey{Key: roachpb.Key(desc.EndKey).Prevish(100), Timestamp: ts},
		storage.MVCCRangeKey{
			StartKey:  desc.StartKey.AsRawKey().Clone(),
			EndKey:    desc.EndKey.AsRawKey().Clone(),
			Timestamp: ts,
		},
	}

	var replicatedKeys []interface{}
	for _, keyI := range allKeys {
		switch key := keyI.(type) {
		case storage.MVCCKey:
			require.NoError(t, storage.MVCCPut(ctx, eng, nil, key.Key, key.Timestamp, localTS, value, nil))
			if !bytes.HasPrefix(key.Key, unreplicatedPrefix) {
				replicatedKeys = append(replicatedKeys, key)
			}
		case storage.MVCCRangeKey:
			require.NoError(t, eng.PutMVCCRangeKey(key, storage.MVCCValue{}))
			if !bytes.HasPrefix(key.StartKey, unreplicatedPrefix) {
				replicatedKeys = append(replicatedKeys, key)
			}
		}
	}

	return allKeys, replicatedKeys
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
}

// TestReplicaDataIterator creates three ranges (a-b, b-c, c-d) and fills each
// with data, then verifies the contents for MVCC and Engine iterators, both
// replicated and unreplicated.
func TestReplicaDataIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

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

	// Create test cases with test data for each descriptor.
	testcases := make([]struct {
		desc                    roachpb.RangeDescriptor
		allKeys, replicatedKeys []interface{} // mixed MVCCKey and MVCCRangeKey
	}, len(descs))

	for i := range testcases {
		testcases[i].desc = descs[i]
		testcases[i].allKeys, testcases[i].replicatedKeys = createRangeData(t, eng, descs[i])
	}

	// Run tests.
	for _, tc := range testcases {
		t.Run(tc.desc.RSpan().String(), func(t *testing.T) {

			// Verify the replicated MVCC contents.
			//
			// TODO(erikgrinaker): This currently only supports MVCC point keys, so we
			// ignore MVCC range keys for now.
			var pointKeys []storage.MVCCKey
			for _, key := range tc.replicatedKeys {
				if pointKey, ok := key.(storage.MVCCKey); ok {
					pointKeys = append(pointKeys, pointKey)
				}
			}
			verifyRDReplicatedOnlyMVCCIter(t, &tc.desc, eng, pointKeys)
		})
	}
}
