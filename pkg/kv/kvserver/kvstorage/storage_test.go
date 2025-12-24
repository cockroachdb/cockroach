// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestValidateIsStateEngineSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := func(start, end roachpb.Key) roachpb.Span {
		return roachpb.Span{Key: start, EndKey: end}
	}
	testCases := []struct {
		span  roachpb.Span
		notOk bool
	}{
		// Full state engine spans.
		{span: s(roachpb.KeyMin, keys.LocalRangeIDPrefix.AsRawKey())},
		{span: s(keys.RangeForceFlushKey(1), keys.RangeLeaseKey(1))},
		{span: s(keys.LocalStoreMax, roachpb.KeyMax)},

		// Full non-state engine spans.
		{span: s(roachpb.KeyMin, keys.MakeRangeIDUnreplicatedPrefix(1)), notOk: true}, // partial overlap
		{span: s(roachpb.KeyMin, roachpb.Key(keys.LocalStorePrefix).Next()), notOk: true},
		{span: s(roachpb.KeyMin, keys.RaftTruncatedStateKey(1)), notOk: true},
		{span: s(keys.LocalRangeIDPrefix.AsRawKey(), keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd()),
			notOk: true},
		{span: s(keys.RaftTruncatedStateKey(1), keys.RaftTruncatedStateKey(2)), notOk: true},
		{span: s(keys.MakeRangeIDUnreplicatedPrefix(1).PrefixEnd(), roachpb.KeyMax), notOk: true}, // partial overlap
		{span: s(keys.MakeRangeIDUnreplicatedPrefix(1),
			keys.MakeRangeIDUnreplicatedPrefix(1).PrefixEnd()), notOk: true},
		{span: s(keys.RangeTombstoneKey(1), keys.RaftTruncatedStateKey(1)), notOk: true},
		{span: s(keys.RaftReplicaIDKey(1), keys.RaftTruncatedStateKey(1)), notOk: true},
		{span: s(keys.RaftTruncatedStateKey(1), roachpb.KeyMax), notOk: true},
		{span: s(keys.LocalStorePrefix, keys.LocalStoreMax), notOk: true},
		{span: s(keys.StoreGossipKey(), keys.StoreIdentKey()), notOk: true},
		{span: s(keys.LocalStoreMax.Prevish(1), roachpb.KeyMax), notOk: true},

		// Point state engine spans.
		{span: s(roachpb.KeyMin, nil)},
		{span: s(keys.LocalRangeIDPrefix.AsRawKey().Prevish(1), nil)},
		{span: s(keys.RangeForceFlushKey(1), nil)},
		{span: s(keys.MakeRangeIDUnreplicatedPrefix(1).Prevish(1), nil)},
		{span: s(keys.RangeTombstoneKey(1), nil)},
		{span: s(keys.RaftReplicaIDKey(1), nil)},
		{span: s(keys.MakeRangeIDUnreplicatedPrefix(1).PrefixEnd(), nil)},
		{span: s(keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd(), nil)},
		{span: s(roachpb.Key(keys.LocalStorePrefix).Prevish(1), nil)},
		{span: s(keys.LocalStoreMax, nil)},
		{span: s(keys.LocalStoreMax.Next(), nil)},
		{span: s(roachpb.KeyMax, nil)},

		// Point non-state engine spans.
		{span: s(keys.MakeRangeIDUnreplicatedPrefix(1), nil), notOk: true},
		{span: s(keys.RaftTruncatedStateKey(1), nil), notOk: true},
		{span: s(keys.RaftTruncatedStateKey(2), nil), notOk: true},
		{span: s(keys.MakeRangeIDUnreplicatedPrefix(1).PrefixEnd().Prevish(1), nil), notOk: true},
		{span: s(keys.LocalStorePrefix, nil), notOk: true},
		{span: s(keys.StoreGossipKey(), nil), notOk: true},
		{span: s(keys.LocalStoreMax.Prevish(1), nil), notOk: true},

		// Tricky state engine spans.
		{span: s(nil, keys.LocalRangeIDPrefix.AsRawKey())},
		{span: s(nil, keys.MakeRangeIDUnreplicatedPrefix(1))},
		{span: s(nil, keys.RangeForceFlushKey(1))},
		{span: s(nil, keys.MakeRangeIDUnreplicatedPrefix(1).PrefixEnd().Next())},
		{span: s(nil, keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd().Next())},
		{span: s(nil, keys.LocalStorePrefix)},
		{span: s(nil, keys.LocalStoreMax.Next())},

		// Tricky non-state engine spans.
		{span: s(nil, keys.MakeRangeIDUnreplicatedPrefix(1).Next()), notOk: true},
		{span: s(nil, keys.RaftTruncatedStateKey(1).Next()), notOk: true},
		{span: s(nil, keys.RaftTruncatedStateKey(2).Next()), notOk: true},
		{span: s(nil, keys.MakeRangeIDUnreplicatedPrefix(1).PrefixEnd()), notOk: true},
		{span: s(nil, keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd()), notOk: true}, // can't decode RangeID.
		{span: s(nil, roachpb.Key(keys.LocalStorePrefix).Next()), notOk: true},
		{span: s(nil, keys.StoreGossipKey()), notOk: true},
		{span: s(nil, keys.LocalStoreMax), notOk: true},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			err := validateIsStateEngineSpan(spanset.TrickySpan(tc.span))
			require.Equal(t, tc.notOk, err != nil, tc.span)
		})
	}
}

func TestValidateIsRaftEngineSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := func(start, end roachpb.Key) roachpb.Span {
		return roachpb.Span{Key: start, EndKey: end}
	}
	testCases := []struct {
		span  roachpb.Span
		notOk bool
	}{
		// Full spans not overlapping with state engine spans.
		{span: s(keys.RangeTombstoneKey(1).Next(), keys.RaftReplicaIDKey(1))},
		{span: s(keys.RaftReplicaIDKey(1).Next(), keys.RangeLastReplicaGCTimestampKey(1).Next())},
		{span: s(keys.LocalStorePrefix, keys.LocalStoreMax)},

		// Full spans overlapping with state engine spans.
		{span: s(keys.MinKey, keys.LocalStorePrefix), notOk: true},
		{span: s(keys.RangeGCThresholdKey(1), keys.RangeVersionKey(1)), notOk: true},
		{span: s(keys.RangeTombstoneKey(1), keys.RaftReplicaIDKey(1)), notOk: true},
		{span: s(keys.RaftReplicaIDKey(1), keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd()), notOk: true},
		{span: s(keys.RangeGCThresholdKey(1), keys.RangeGCThresholdKey(2)), notOk: true},
		{span: s(keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd(), keys.LocalStorePrefix), notOk: true},
		{span: s(keys.LocalStoreMax, keys.MaxKey), notOk: true},

		// Point spans not overlapping with state engine spans.
		{span: s(keys.RaftLogKey(1, 1), nil)},
		{span: s(keys.RangeTombstoneKey(1).Next(), nil)},
		{span: s(keys.RaftReplicaIDKey(1).Next(), nil)},
		{span: s(keys.RaftTruncatedStateKey(1).Next(), nil)},
		{span: s(keys.LocalStorePrefix, nil)},
		{span: s(roachpb.Key(keys.LocalStorePrefix).Next(), nil)},
		{span: s(keys.LocalStoreMax.Prevish(1), nil)},

		// Point spans overlapping with state engine spans.
		{span: s(keys.LocalRangeIDPrefix.AsRawKey(), nil), notOk: true}, // invalid start key
		{span: s(keys.RangeLeaseKey(1), nil), notOk: true},
		{span: s(keys.RangeTombstoneKey(1), nil), notOk: true},
		{span: s(keys.RaftReplicaIDKey(1), nil), notOk: true},
		{span: s(keys.RangeTombstoneKey(2), nil), notOk: true},
		{span: s(keys.RaftReplicaIDKey(2), nil), notOk: true},
		{span: s(keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd(), nil), notOk: true},

		// Tricky spans not overlapping with state engine spans.
		{span: s(nil, keys.RaftLogKey(1, 1))},
		{span: s(nil, keys.RangeTombstoneKey(1))},
		{span: s(nil, keys.RaftReplicaIDKey(1))},
		{span: s(nil, roachpb.Key(keys.LocalStorePrefix).PrefixEnd())},

		// Tricky spans overlapping with state engine spans.
		{span: s(nil, keys.LocalRangeIDPrefix.AsRawKey()), notOk: true},
		{span: s(nil, keys.LocalStorePrefix), notOk: true},
		{span: s(nil, keys.RangeLeaseKey(1)), notOk: true},
		{span: s(nil, keys.RangeTombstoneKey(1).Next()), notOk: true},
		{span: s(nil, keys.RaftReplicaIDKey(1).Next()), notOk: true},
		{span: s(nil, roachpb.Key(keys.LocalStorePrefix).PrefixEnd().Next()), notOk: true},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			err := validateIsRaftEngineSpan(spanset.TrickySpan(tc.span))
			require.Equal(t, tc.notOk, err != nil, tc.span)
		})
	}
}
