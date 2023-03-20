// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRangeKeyBatcherSplitSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	data := &storage.MemObject{}
	st := cluster.MakeTestingClusterSettings()
	w := storage.MakeIngestionSSTWriter(ctx, st, data)

	for _, key := range []storage.MVCCRangeKey{
		{
			StartKey:  roachpb.Key("a"),
			EndKey:    roachpb.Key("c"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
		{
			StartKey:  roachpb.Key("b"),
			EndKey:    roachpb.Key("f"),
			Timestamp: hlc.Timestamp{WallTime: 2},
		},
		{
			StartKey:  roachpb.Key("c"),
			EndKey:    roachpb.Key("e"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
		{
			StartKey:  roachpb.Key("d"),
			EndKey:    roachpb.Key("f"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
	} {
		require.NoError(t, w.PutRawMVCCRangeKey(key, []byte{}))
	}
	require.NoError(t, w.Finish())

	input := data.Data()
	actualStart := rangeKeysInSSTToString(t, input)
	expectedStart := `{a-b}[0.000000001,0=]
{b-f}[0.000000002,0= 0.000000001,0=]`
	require.Equal(t, expectedStart, actualStart)

	var (
		start = roachpb.Key("a")
		end   = roachpb.Key("f")
		split = roachpb.Key("c")
	)

	left, right, err := splitRangeKeySSTAtKey(ctx, st, start, end, split, input)
	require.NoError(t, err)

	actualLeft := rangeKeysInSSTToString(t, left.data)
	expectedLeft := `{a-b}[0.000000001,0=]
{b-c}[0.000000002,0= 0.000000001,0=]`
	require.Equal(t, expectedLeft, actualLeft)

	actualRight := rangeKeysInSSTToString(t, right.data)
	expectedRight := `{c-f}[0.000000002,0= 0.000000001,0=]`
	require.Equal(t, expectedRight, actualRight)
}

func rangeKeysInSSTToString(t *testing.T, data []byte) string {
	t.Helper()
	var buf strings.Builder
	iter, err := storage.NewMemSSTIterator(data, true, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypeRangesOnly,
		LowerBound: roachpb.KeyMin,
		UpperBound: roachpb.KeyMax,
	})

	require.NoError(t, err)
	iter.SeekGE(storage.MVCCKey{Key: roachpb.KeyMin})
	for {
		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		fmt.Fprintf(&buf, "%v\n", iter.RangeKeys())
		iter.Next()
	}
	return strings.TrimSpace(buf.String())
}
