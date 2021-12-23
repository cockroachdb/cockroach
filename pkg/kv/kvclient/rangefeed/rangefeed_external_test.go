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
	"runtime/pprof"
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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRangeFeedIntegration is a basic integration test demonstrating all of
// the pieces working together.
func TestRangeFeedIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	srv0 := tc.Server(0)
	db := srv0.DB()
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

	f, err := rangefeed.NewFactory(srv0.Stopper(), db, srv0.ClusterSettings(), nil)
	require.NoError(t, err)
	rows := make(chan *roachpb.RangeFeedValue)
	initialScanDone := make(chan struct{})
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, afterB,
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
		require.Equal(t, v1.Value.RawBytes, v1.PrevValue.RawBytes)
		require.Equal(t, v1.Value.Timestamp, afterB)
		require.True(t, v1.PrevValue.Timestamp.IsEmpty())
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

// TestWithOnFrontierAdvance sets up a rangefeed on a span that has more than
// one range and ensures that the OnFrontierAdvance callback is called with the
// correct timestamp.
func TestWithOnFrontierAdvance(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	srv0 := tc.Server(0)
	db := srv0.DB()
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

	// Split the range into two so we know the frontier has more than one span to
	// track for certain. We later write to both these ranges.
	_, _, err := tc.SplitRange(mkKey("b"))
	require.NoError(t, err)

	f, err := rangefeed.NewFactory(srv0.Stopper(), db, srv0.ClusterSettings(), nil)
	require.NoError(t, err)

	// mu protects secondWriteTS.
	var mu syncutil.Mutex
	secondWriteFinished := false
	frontierAdvancedAfterSecondWrite := false

	// Track the checkpoint TS for spans belonging to both the ranges we split
	// above. This can then be used to compute the minimum timestamp for both
	// these spans. We use the key we write to for the ranges below as keys for
	// this map.
	spanCheckpointTimestamps := make(map[string]hlc.Timestamp)
	forwardCheckpointForKey := func(key string, checkpoint *roachpb.RangeFeedCheckpoint) {
		ts := hlc.MinTimestamp
		if prevTS, found := spanCheckpointTimestamps[key]; found {
			ts = prevTS
		}
		ts.Forward(checkpoint.ResolvedTS)
		spanCheckpointTimestamps[key] = ts
	}
	rows := make(chan *roachpb.RangeFeedValue)
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, db.Clock().Now(),
		func(ctx context.Context, value *roachpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *roachpb.RangeFeedCheckpoint) {
			if checkpoint.Span.ContainsKey(mkKey("a")) {
				forwardCheckpointForKey("a", checkpoint)
			}
			if checkpoint.Span.ContainsKey(mkKey("c")) {
				forwardCheckpointForKey("c", checkpoint)
			}
		}),
		rangefeed.WithOnFrontierAdvance(func(ctx context.Context, frontierTS hlc.Timestamp) {
			minTS := hlc.MaxTimestamp
			for _, ts := range spanCheckpointTimestamps {
				minTS.Backward(ts)
			}
			assert.Truef(
				t,
				frontierTS.Equal(minTS),
				"expected frontier timestamp to be equal to minimum timestamp across spans %s, found %s",
				minTS,
				frontierTS,
			)
			mu.Lock()
			defer mu.Unlock()
			if secondWriteFinished {
				frontierAdvancedAfterSecondWrite = true
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

	testutils.SucceedsSoon(t, func() error {
		mu.Lock()
		defer mu.Unlock()
		if frontierAdvancedAfterSecondWrite {
			return nil
		}
		return errors.New("expected frontier to advance after second write")
	})
}

// TestWithOnCheckpoint verifies that we correctly emit rangefeed checkpoint
// events.
func TestWithOnCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	srv0 := tc.Server(0)
	db := srv0.DB()
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

	f, err := rangefeed.NewFactory(srv0.Stopper(), db, srv0.ClusterSettings(), nil)
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
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, db.Clock().Now(),
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

// TestRangefeedValueTimestamps tests that the rangefeed values (and previous
// values) have the kind of timestamps we expect when writing, overwriting, and
// deleting keys.
func TestRangefeedValueTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	srv0 := tc.Server(0)
	db := srv0.DB()
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

	f, err := rangefeed.NewFactory(srv0.Stopper(), db, srv0.ClusterSettings(), nil)
	require.NoError(t, err)

	rows := make(chan *roachpb.RangeFeedValue)
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, db.Clock().Now(),
		func(ctx context.Context, value *roachpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithDiff(),
	)
	require.NoError(t, err)
	defer r.Close()

	mustGetInt := func(value roachpb.Value) int {
		val, err := value.GetInt()
		require.NoError(t, err)
		return int(val)
	}

	{
		beforeWriteTS := db.Clock().Now()
		require.NoError(t, db.Put(ctx, mkKey("a"), 1))
		afterWriteTS := db.Clock().Now()

		v := <-rows
		require.Equal(t, mustGetInt(v.Value), 1)
		require.True(t, beforeWriteTS.Less(v.Value.Timestamp))
		require.True(t, v.Value.Timestamp.Less(afterWriteTS))

		require.False(t, v.PrevValue.IsPresent())
	}

	{
		beforeOverwriteTS := db.Clock().Now()
		require.NoError(t, db.Put(ctx, mkKey("a"), 2))
		afterOverwriteTS := db.Clock().Now()

		v := <-rows
		require.Equal(t, mustGetInt(v.Value), 2)
		require.True(t, beforeOverwriteTS.Less(v.Value.Timestamp))
		require.True(t, v.Value.Timestamp.Less(afterOverwriteTS))

		require.True(t, v.PrevValue.IsPresent())
		require.Equal(t, mustGetInt(v.PrevValue), 1)
		require.True(t, v.PrevValue.Timestamp.IsEmpty())
	}

	{
		beforeDelTS := db.Clock().Now()
		require.NoError(t, db.Del(ctx, mkKey("a")))
		afterDelTS := db.Clock().Now()

		v := <-rows
		require.False(t, v.Value.IsPresent())
		require.True(t, beforeDelTS.Less(v.Value.Timestamp))
		require.True(t, v.Value.Timestamp.Less(afterDelTS))

		require.True(t, v.PrevValue.IsPresent())
		require.Equal(t, mustGetInt(v.PrevValue), 2)
		require.True(t, v.PrevValue.Timestamp.IsEmpty())
	}

	{
		beforeDelTS := db.Clock().Now()
		require.NoError(t, db.Del(ctx, mkKey("a")))
		afterDelTS := db.Clock().Now()

		v := <-rows
		require.False(t, v.Value.IsPresent())
		require.True(t, beforeDelTS.Less(v.Value.Timestamp))
		require.True(t, v.Value.Timestamp.Less(afterDelTS))

		require.False(t, v.PrevValue.IsPresent())
		require.True(t, v.PrevValue.Timestamp.IsEmpty())
	}
}

// TestUnrecoverableErrors verifies that unrecoverable internal errors are surfaced
// to callers.
func TestUnrecoverableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	srv0 := tc.Server(0)
	db0 := srv0.DB()
	scratchKey := tc.ScratchRange(t)
	scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]

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

	f, err := rangefeed.NewFactory(srv0.Stopper(), db0, srv0.ClusterSettings(), nil)
	require.NoError(t, err)

	preGCThresholdTS := hlc.Timestamp{WallTime: 1}
	mu := struct {
		syncutil.Mutex
		internalErr error
	}{}
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, preGCThresholdTS,
		func(context.Context, *roachpb.RangeFeedValue) {},
		rangefeed.WithDiff(),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			mu.Lock()
			defer mu.Unlock()

			mu.internalErr = err
		}),
	)
	require.NoError(t, err)
	defer r.Close()

	testutils.SucceedsSoon(t, func() error {
		mu.Lock()
		defer mu.Unlock()

		if !errors.HasType(mu.internalErr, &roachpb.BatchTimestampBeforeGCError{}) {
			return errors.New("expected internal error")
		}
		return nil
	})
}

// TestRangefeedWithLabelsOption verifies go routines started by rangefeed are
// annotated with pprof labels.
func TestRangefeedWithLabelsOption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	srv0 := tc.Server(0)
	db := srv0.DB()
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

	const rangefeedName = "test-feed"
	type label struct {
		k, v string
	}
	defaultLabel := label{k: "rangefeed", v: rangefeedName}
	label1 := label{k: "caller-label", v: "foo"}
	label2 := label{k: "another-label", v: "bar"}

	allLabelsCorrect := struct {
		syncutil.Mutex
		correct bool
	}{correct: true}

	verifyLabels := func(ctx context.Context) {
		allLabelsCorrect.Lock()
		defer allLabelsCorrect.Unlock()
		if !allLabelsCorrect.correct {
			return
		}

		m := make(map[string]string)
		pprof.ForLabels(ctx, func(k, v string) bool {
			m[k] = v
			return true
		})

		allLabelsCorrect.correct =
			m[defaultLabel.k] == defaultLabel.v && m[label1.k] == label1.v && m[label2.k] == label2.v
	}

	f, err := rangefeed.NewFactory(srv0.Stopper(), db, srv0.ClusterSettings(), nil)
	require.NoError(t, err)
	initialScanDone := make(chan struct{})

	// We'll emit keyD a bit later, once initial scan completes.
	keyD := mkKey("d")
	var keyDSeen sync.Once
	keyDSeenCh := make(chan struct{})

	r, err := f.RangeFeed(ctx, rangefeedName, []roachpb.Span{sp}, afterB,
		func(ctx context.Context, value *roachpb.RangeFeedValue) {
			verifyLabels(ctx)
			if value.Key.Equal(keyD) {
				keyDSeen.Do(func() { close(keyDSeenCh) })
			}
		},
		rangefeed.WithPProfLabel(label1.k, label1.v),
		rangefeed.WithPProfLabel(label2.k, label2.v),
		rangefeed.WithInitialScanParallelismFn(func() int { return 3 }),
		rangefeed.WithOnScanCompleted(func(ctx context.Context, sp roachpb.Span) error {
			verifyLabels(ctx)
			return nil
		}),
		rangefeed.WithInitialScan(func(ctx context.Context) {
			verifyLabels(ctx)
			close(initialScanDone)
		}),
	)
	require.NoError(t, err)
	defer r.Close()

	<-initialScanDone

	// Write a new value for "a" and make sure it is seen.
	require.NoError(t, db.Put(ctx, keyD, 5))
	<-keyDSeenCh
	allLabelsCorrect.Lock()
	defer allLabelsCorrect.Unlock()
	require.True(t, allLabelsCorrect.correct)
}
