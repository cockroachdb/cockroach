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
	"errors"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	r, err := f.RangeFeed(ctx, "test", sp, afterB,
		func(ctx context.Context, value *roachpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithDiff(),
		rangefeed.WithInitialScan(func(ctx context.Context) {
			close(initialScanDone)
		}),
	)
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

// TestWithOnFrontierAdvance sets up a rangefeed on a span that has > 1 range
// and ensures that the OnFrontierAdvance callback is called with the correct
// timestamp.
func TestWithOnFrontierAdvance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.Server(0).DB()
	scratchKey := tc.ScratchRange(t)
	scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
	mkKey := func(k string) roachpb.Key {
		return encoding.EncodeStringAscending(scratchKey, k)
	}

	sp := roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	}
	{
		// Enable rangefeeds, otherwise the thing will retry until they are enabled.
		_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
		require.NoError(t, err)
	}
	{
		// Lower the closed timestamp target duration to speed up the test.
		_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
		require.NoError(t, err)
	}

	// Split the range into two so we know the frontier has > 1 spans to track
	// for certain. We later write to both these ranges.
	_, _, err := tc.SplitRange(mkKey("b"))
	require.NoError(t, err)

	f, err := rangefeed.NewFactory(tc.Stopper(), db, nil)
	require.NoError(t, err)

	// mu protects secondWriteTS.
	var mu syncutil.Mutex
	secondWriteFinished := false
	var once sync.Once
	frontierAdvancedAfterSecondWrite := make(chan struct{})

	// Track the checkpoint TS for spans belonging to both the ranges we split
	// above. This can then be used to compute the minimum timestamp for both
	// these spans. We use the key we write to for the ranges below as keys for
	// this map.
	spanCheckpointTimestamps := make(map[string]hlc.Timestamp)
	rows := make(chan *roachpb.RangeFeedValue)
	r, err := f.RangeFeed(ctx, "test", sp, db.Clock().Now(),
		func(ctx context.Context, value *roachpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *roachpb.RangeFeedCheckpoint) {
			forwardCheckpoint := func(key string) {
				ts := hlc.MinTimestamp
				if prevTS, found := spanCheckpointTimestamps[key]; found {
					ts = prevTS
				}
				ts.Forward(checkpoint.ResolvedTS)
				spanCheckpointTimestamps[key] = ts
			}
			if checkpoint.Span.ContainsKey(mkKey("a")) {
				forwardCheckpoint("a")
			}
			if checkpoint.Span.ContainsKey(mkKey("c")) {
				forwardCheckpoint("c")
			}
		}),
		rangefeed.WithOnFrontierAdvance(func(ctx context.Context, frontierTS hlc.Timestamp) {
			minTS := hlc.MaxTimestamp
			for _, ts := range spanCheckpointTimestamps {
				minTS.Backward(ts)
			}
			require.True(
				t,
				frontierTS.Equal(minTS),
				"frontier advanced to timestamp %v past minimum timestamp across spans %v",
				frontierTS,
				minTS,
			)
			mu.Lock()
			defer mu.Unlock()
			if secondWriteFinished {
				once.Do(func() {
					close(frontierAdvancedAfterSecondWrite)
				})
			}
		}),
	)
	require.NoError(t, err)
	defer r.Close()

	// Write to a key on both the ranges.
	require.NoError(t, db.Put(ctx, mkKey("a"), 1))

	v := <-rows
	require.Equal(t, mkKey("a"), v.Key)

	require.NoError(t, db.Put(ctx, mkKey("c"), 1))
	mu.Lock()
	secondWriteFinished = true
	mu.Unlock()

	v = <-rows
	require.Equal(t, mkKey("c"), v.Key)

	<-frontierAdvancedAfterSecondWrite
}

// TestWithOnCheckpoint verifies that we correctly emit rangefeed checkpoint
// events.
func TestWithOnCheckpoint(t *testing.T) {
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

	sp := roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	}
	{
		// Enable rangefeeds, otherwise the thing will retry until they are enabled.
		_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
		require.NoError(t, err)
	}
	{
		// Lower the closed timestamp target duration to speed up the test.
		_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
		require.NoError(t, err)
	}

	f, err := rangefeed.NewFactory(tc.Stopper(), db, nil)
	require.NoError(t, err)

	var mu syncutil.RWMutex
	var afterWriteTS hlc.Timestamp
	checkpoints := make(chan *roachpb.RangeFeedCheckpoint)

	// We need to start a goroutine that reads of the checkpoints channel, so to
	// not block the rangefeed itself.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// We should expect a checkpoint event covering the key we just wrote, at a
		// timestamp greater than when we wrote it.
		testutils.SucceedsSoon(t, func() error {
			for {
				select {
				case c := <-checkpoints:
					mu.RLock()
					writeTSUnset := afterWriteTS.IsEmpty()
					mu.RUnlock()
					if writeTSUnset {
						return errors.New("write to key hasn't gone through yet")
					}

					if afterWriteTS.LessEq(c.ResolvedTS) && c.Span.ContainsKey(mkKey("a")) {
						return nil
					}
				default:
					return errors.New("no valid checkpoints found")
				}
			}
		})
	}()

	rows := make(chan *roachpb.RangeFeedValue)
	r, err := f.RangeFeed(ctx, "test", sp, db.Clock().Now(),
		func(ctx context.Context, value *roachpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *roachpb.RangeFeedCheckpoint) {
			select {
			case checkpoints <- checkpoint:
			case <-ctx.Done():
			}
		}),
	)
	require.NoError(t, err)
	defer r.Close()

	require.NoError(t, db.Put(ctx, mkKey("a"), 1))
	mu.Lock()
	afterWriteTS = db.Clock().Now()
	mu.Unlock()
	{
		v := <-rows
		require.Equal(t, mkKey("a"), v.Key)
	}

	wg.Wait()
}
