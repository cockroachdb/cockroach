// Copyright 2021 The Cockroach Authors.
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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMVCCRangeDeletions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eng, err := Open(context.Background(), InMemory())
	require.NoError(t, err)
	defer eng.Close()

	tombstones := []MVCCRangeTombstone{
		{
			Start:     roachpb.RKey(testKey1),
			End:       roachpb.RKey(testKey2),
			Timestamp: hlc.Timestamp{WallTime: 3},
		},
		{
			Start:     roachpb.RKey(testKey2),
			End:       roachpb.RKey(testKey4),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
		{
			Start:     roachpb.RKey(testKey4),
			End:       roachpb.RKey(testKey5),
			Timestamp: hlc.Timestamp{WallTime: 5},
		},
		{
			Start:     roachpb.RKey(testKey5),
			End:       roachpb.RKey(testKey6),
			Timestamp: hlc.Timestamp{WallTime: 5},
		},
	}

	tw := MVCCRangeTombstoneWriter{Writer: eng}
	for _, tombstone := range tombstones {
		require.NoError(t, tw.WriteTombstone(tombstone))
	}

	collectTombstones := func(start, end roachpb.RKey) []MVCCRangeTombstone {
		var ret []MVCCRangeTombstone
		iter := NewRangeTombstoneIterator(eng, start, end)
		defer iter.Close()
		ok, err := iter.First()
		for ; ok && err == nil; ok, err = iter.Next() {
			tomb, err := iter.Current()
			require.NoError(t, err)
			unsafeTomb, err := iter.UnsafeCurrent()
			require.NoError(t, err)
			require.EqualValues(t, tomb, unsafeTomb)

			ret = append(ret, tomb)
		}
		return ret
	}

	{
		// The last two tombstones start at or after the iterator's upper bound and
		// should be excluded.
		got := collectTombstones(roachpb.RKey(testKey1), roachpb.RKey(testKey4))
		require.EqualValues(t, tombstones[:len(tombstones)-2], got)
	}

	{
		// Clear the first tombstone.
		require.NoError(t, tw.ClearTombstone(tombstones[0]))
		tombstones = tombstones[1:]
		got := collectTombstones(roachpb.RKey(testKey1), roachpb.RKey(testKey4))
		require.EqualValues(t, tombstones[:len(tombstones)-2], got)
	}
}
