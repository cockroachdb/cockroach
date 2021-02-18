// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestRangeFeedIntegration is a basic integration test demonstrating all of
// the pieces working together.
func TestRangeFeedIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.Server(0).DB()
	scratchKey := tc.ScratchRange(t)
	scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
	mkKey := func(k string) roachpb.Key {
		return encoding.EncodeStringAscending(scratchKey, k)
	}
	// Split the range a bunch of times.
	const splits = 10
	for i := 0; i < splits; i++ {
		_, _, err := tc.SplitRange(mkKey(string([]byte{'a' + byte(i)})))
		require.NoError(t, err)
	}

	require.NoError(t, db.Put(ctx, mkKey("a"), 1))
	require.NoError(t, db.Put(ctx, mkKey("b"), 2))
	afterB := db.Clock().Now()
	require.NoError(t, db.Put(ctx, mkKey("c"), 3))

	sp := roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	}
	{
		// Enable rangefeeds, otherwise the thing will retry until they are enabled.
		_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
		require.NoError(t, err)
	}

	f, err := rangefeed.NewFactory(tc.Stopper(), db, nil)
	require.NoError(t, err)
	rows := make(chan *roachpb.RangeFeedValue)
	initialScanDone := make(chan struct{})
	r, err := f.RangeFeed(ctx, "test", sp, afterB, func(
		ctx context.Context, value *roachpb.RangeFeedValue,
	) {
		select {
		case rows <- value:
		case <-ctx.Done():
		}
	}, rangefeed.WithDiff(), rangefeed.WithInitialScan(func(ctx context.Context) {
		close(initialScanDone)
	}))
	require.NoError(t, err)
	defer r.Close()
	{
		v1 := <-rows
		require.Equal(t, mkKey("a"), v1.Key)
		// Ensure the initial scan contract is fulfilled when WithDiff is specified.
		require.Equal(t, v1.Value, v1.PrevValue)
		require.Equal(t, v1.Value.Timestamp, afterB)
	}
	{
		v2 := <-rows
		require.Equal(t, mkKey("b"), v2.Key)
	}
	<-initialScanDone
	{
		v3 := <-rows
		require.Equal(t, mkKey("c"), v3.Key)
	}

	// Write a new value for "a" and make sure it is seen.
	require.NoError(t, db.Put(ctx, mkKey("a"), 4))
	{
		v4 := <-rows
		require.Equal(t, mkKey("a"), v4.Key)
		prev, err := v4.PrevValue.GetInt()
		require.NoError(t, err)
		require.Equal(t, int64(1), prev)
		updated, err := v4.Value.GetInt()
		require.NoError(t, err)
		require.Equal(t, int64(4), updated)
	}
}
