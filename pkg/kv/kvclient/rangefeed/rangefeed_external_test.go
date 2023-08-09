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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	pointKV = storageutils.PointKV
	rangeKV = storageutils.RangeKV

	// smallEngineBlocks configures Pebble with a block size of 1 byte, to provoke
	// bugs in time-bound iterators.
	smallEngineBlocks = util.ConstantWithMetamorphicTestBool("small-engine-blocks", false)
)

type kvs = storageutils.KVs

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
	rows := make(chan *kvpb.RangeFeedValue)
	initialScanDone := make(chan struct{})
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, afterB,
		func(ctx context.Context, value *kvpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithDiff(true),
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
	forwardCheckpointForKey := func(key string, checkpoint *kvpb.RangeFeedCheckpoint) {
		ts := hlc.MinTimestamp
		if prevTS, found := spanCheckpointTimestamps[key]; found {
			ts = prevTS
		}
		ts.Forward(checkpoint.ResolvedTS)
		spanCheckpointTimestamps[key] = ts
	}
	rows := make(chan *kvpb.RangeFeedValue)
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, db.Clock().Now(),
		func(ctx context.Context, value *kvpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
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
	checkpoints := make(chan *kvpb.RangeFeedCheckpoint)

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

	rows := make(chan *kvpb.RangeFeedValue)
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, db.Clock().Now(),
		func(ctx context.Context, value *kvpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
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

	rows := make(chan *kvpb.RangeFeedValue)
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, db.Clock().Now(),
		func(ctx context.Context, value *kvpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithDiff(true),
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
		_, err = db.Del(ctx, mkKey("a"))
		require.NoError(t, err)
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
		_, err = db.Del(ctx, mkKey("a"))
		require.NoError(t, err)
		afterDelTS := db.Clock().Now()

		v := <-rows
		require.False(t, v.Value.IsPresent())
		require.True(t, beforeDelTS.Less(v.Value.Timestamp))
		require.True(t, v.Value.Timestamp.Less(afterDelTS))

		require.False(t, v.PrevValue.IsPresent())
		require.True(t, v.PrevValue.Timestamp.IsEmpty())
	}
}

// TestWithOnSSTable tests that the rangefeed emits SST ingestions correctly.
func TestWithOnSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	srv := tc.Server(0)
	db := srv.DB()

	_, _, err := tc.SplitRange(roachpb.Key("a"))
	require.NoError(t, err)
	require.NoError(t, tc.WaitForFullReplication())

	_, err = tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)
	f, err := rangefeed.NewFactory(srv.Stopper(), db, srv.ClusterSettings(), nil)
	require.NoError(t, err)

	// We start the rangefeed over a narrower span than the AddSSTable (c-e vs
	// a-f), to ensure the entire SST is emitted even if the registration is
	// narrower.
	var once sync.Once
	checkpointC := make(chan struct{})
	sstC := make(chan kvcoord.RangeFeedMessage)
	spans := []roachpb.Span{{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}}
	r, err := f.RangeFeed(ctx, "test", spans, db.Clock().Now(),
		func(ctx context.Context, value *kvpb.RangeFeedValue) {},
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
			once.Do(func() {
				close(checkpointC)
			})
		}),
		rangefeed.WithOnSSTable(func(ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span) {
			select {
			case sstC <- kvcoord.RangeFeedMessage{
				RangeFeedEvent: &kvpb.RangeFeedEvent{
					SST: sst,
				},
				RegisteredSpan: registeredSpan,
			}:
			case <-ctx.Done():
			}
		}),
	)
	require.NoError(t, err)
	defer r.Close()

	// Wait for initial checkpoint.
	select {
	case <-checkpointC:
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for checkpoint")
	}

	// Ingest an SST.
	now := db.Clock().Now()
	now.Logical = 0
	ts := int(now.WallTime)
	sstKVs := kvs{
		pointKV("a", ts, "1"),
		pointKV("b", ts, "2"),
		pointKV("c", ts, "3"),
		rangeKV("d", "e", ts, ""),
	}
	sst, sstStart, sstEnd := storageutils.MakeSST(t, srv.ClusterSettings(), sstKVs)
	_, _, _, pErr := db.AddSSTableAtBatchTimestamp(ctx, sstStart, sstEnd, sst,
		false /* disallowConflicts */, false /* disallowShadowing */, hlc.Timestamp{}, nil, /* stats */
		false /* ingestAsWrites */, now)
	require.Nil(t, pErr)

	// Wait for the SST event and check its contents.
	var sstMessage kvcoord.RangeFeedMessage
	select {
	case sstMessage = <-sstC:
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for SST event")
	}

	require.Equal(t, roachpb.Span{Key: sstStart, EndKey: sstEnd}, sstMessage.SST.Span)
	require.Equal(t, now, sstMessage.SST.WriteTS)
	require.Equal(t, sstKVs, storageutils.ScanSST(t, sstMessage.SST.Data))
	require.Equal(t, spans[0], sstMessage.RegisteredSpan)
}

// TestWithOnSSTableCatchesUpIfNotSet tests that the rangefeed runs a catchup
// scan if an OnSSTable event is emitted and no OnSSTable event handler is set.
func TestWithOnSSTableCatchesUpIfNotSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	storage.DisableMetamorphicSimpleValueEncoding(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					SmallEngineBlocks: smallEngineBlocks,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	srv := tc.Server(0)
	db := srv.DB()

	_, _, err := tc.SplitRange(roachpb.Key("a"))
	require.NoError(t, err)
	require.NoError(t, tc.WaitForFullReplication())

	_, err = tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)
	f, err := rangefeed.NewFactory(srv.Stopper(), db, srv.ClusterSettings(), nil)
	require.NoError(t, err)

	// We start the rangefeed over a narrower span than the AddSSTable (c-e vs
	// a-f), to ensure only the restricted span is emitted by the catchup scan.
	var once sync.Once
	checkpointC := make(chan struct{})
	rowC := make(chan *kvpb.RangeFeedValue)
	spans := []roachpb.Span{{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}}
	r, err := f.RangeFeed(ctx, "test", spans, db.Clock().Now(),
		func(ctx context.Context, value *kvpb.RangeFeedValue) {
			select {
			case rowC <- value:
			case <-ctx.Done():
			}
		},
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
			once.Do(func() {
				close(checkpointC)
			})
		}),
	)
	require.NoError(t, err)
	defer r.Close()

	// Wait for initial checkpoint.
	select {
	case <-checkpointC:
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for checkpoint")
	}

	// Ingest an SST.
	now := db.Clock().Now()
	now.Logical = 0
	ts := int(now.WallTime)
	sstKVs := kvs{
		pointKV("a", ts, "1"),
		pointKV("b", ts, "2"),
		pointKV("c", ts, "3"),
		pointKV("d", ts, "4"),
		pointKV("e", ts, "5"),
	}
	expectKVs := kvs{pointKV("c", ts, "3"), pointKV("d", ts, "4")}
	sst, sstStart, sstEnd := storageutils.MakeSST(t, srv.ClusterSettings(), sstKVs)
	_, _, _, pErr := db.AddSSTableAtBatchTimestamp(ctx, sstStart, sstEnd, sst,
		false /* disallowConflicts */, false /* disallowShadowing */, hlc.Timestamp{}, nil, /* stats */
		false /* ingestAsWrites */, now)
	require.Nil(t, pErr)

	// Assert that we receive the KV pairs within the rangefeed span.
	timer := time.NewTimer(3 * time.Second)
	var seenKVs kvs
	for len(seenKVs) < len(expectKVs) {
		select {
		case row := <-rowC:
			seenKVs = append(seenKVs, storage.MVCCKeyValue{
				Key: storage.MVCCKey{
					Key:       row.Key,
					Timestamp: row.Value.Timestamp,
				},
				Value: row.Value.RawBytes,
			})
		case <-timer.C:
			require.Fail(t, "timed out waiting for catchup scan", "saw entries: %v", seenKVs)
		}
	}
	require.Equal(t, expectKVs, seenKVs)
}

// TestWithOnDeleteRange tests that the rangefeed emits MVCC range tombstones.
//
// TODO(erikgrinaker): These kinds of tests should really use a data-driven test
// harness, for more exhaustive testing. But it'll do for now.
func TestWithOnDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					SmallEngineBlocks: smallEngineBlocks,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	srv := tc.Server(0)
	db := srv.DB()

	_, _, err := tc.SplitRange(roachpb.Key("a"))
	require.NoError(t, err)
	require.NoError(t, tc.WaitForFullReplication())

	_, err = tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)
	f, err := rangefeed.NewFactory(srv.Stopper(), db, srv.ClusterSettings(), nil)
	require.NoError(t, err)

	// We lay down a few MVCC range tombstones and points. The first range
	// tombstone should not be visible, because initial scans do not emit
	// tombstones, nor should the points covered by it. The second range tombstone
	// should be visible, because catchup scans do emit tombstones. The range
	// tombstone should be ordered after the initial point, but before the foo
	// catchup point, and the previous values should respect the range tombstones.
	require.NoError(t, db.Put(ctx, "covered", "covered"))
	require.NoError(t, db.Put(ctx, "foo", "covered"))
	require.NoError(t, db.DelRangeUsingTombstone(ctx, "a", "z"))
	require.NoError(t, db.Put(ctx, "foo", "initial"))
	rangeFeedTS := db.Clock().Now()
	require.NoError(t, db.Put(ctx, "covered", "catchup"))
	require.NoError(t, db.DelRangeUsingTombstone(ctx, "a", "z"))
	require.NoError(t, db.Put(ctx, "foo", "catchup"))

	// We start the rangefeed over a narrower span than the DeleteRanges (c-g),
	// to ensure the DeleteRange event is truncated to the registration span.
	var checkpointOnce sync.Once
	checkpointC := make(chan struct{})
	deleteRangeC := make(chan *kvpb.RangeFeedDeleteRange)
	rowC := make(chan *kvpb.RangeFeedValue)

	spans := []roachpb.Span{{Key: roachpb.Key("c"), EndKey: roachpb.Key("g")}}
	r, err := f.RangeFeed(ctx, "test", spans, rangeFeedTS,
		func(ctx context.Context, e *kvpb.RangeFeedValue) {
			select {
			case rowC <- e:
			case <-ctx.Done():
			}
		},
		rangefeed.WithDiff(true),
		rangefeed.WithInitialScan(nil),
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
			checkpointOnce.Do(func() {
				close(checkpointC)
			})
		}),
		rangefeed.WithOnDeleteRange(func(ctx context.Context, e *kvpb.RangeFeedDeleteRange) {
			select {
			case deleteRangeC <- e:
			case <-ctx.Done():
			}
		}),
	)
	require.NoError(t, err)
	defer r.Close()

	// Wait for initial scan. We should see the foo=initial point, but not the
	// range tombstone nor the covered points.
	select {
	case e := <-rowC:
		require.Equal(t, roachpb.Key("foo"), e.Key)
		value, err := e.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, "initial", string(value))
		prevValue, err := e.PrevValue.GetBytes()
		require.NoError(t, err)
		require.Equal(t, "initial", string(prevValue)) // initial scans supply current as prev
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for initial scan event")
	}

	// Wait for catchup scan. We should see the second range tombstone, truncated
	// to the rangefeed bounds (c-g), and it should be ordered before the points
	// covered=catchup and foo=catchup. both points should have a tombstone as the
	// previous value.
	select {
	case e := <-deleteRangeC:
		require.Equal(t, roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("g")}, e.Span)
		require.NotEmpty(t, e.Timestamp)
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for DeleteRange event")
	}

	select {
	case e := <-rowC:
		require.Equal(t, roachpb.Key("covered"), e.Key)
		value, err := e.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, "catchup", string(value))
		prevValue, err := storage.DecodeMVCCValue(e.PrevValue.RawBytes)
		require.NoError(t, err)
		require.True(t, prevValue.IsTombstone())
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for foo=catchup event")
	}

	select {
	case e := <-rowC:
		require.Equal(t, roachpb.Key("foo"), e.Key)
		value, err := e.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, "catchup", string(value))
		prevValue, err := storage.DecodeMVCCValue(e.PrevValue.RawBytes)
		require.NoError(t, err)
		require.True(t, prevValue.IsTombstone())
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for foo=catchup event")
	}

	// Wait for checkpoint after catchup scan.
	select {
	case <-checkpointC:
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for checkpoint")
	}

	// Send another DeleteRange, and wait for the rangefeed event. This should
	// be truncated to the rangefeed bounds (c-g).
	require.NoError(t, db.DelRangeUsingTombstone(ctx, "a", "z"))
	select {
	case e := <-deleteRangeC:
		require.Equal(t, roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("g")}, e.Span)
		require.NotEmpty(t, e.Timestamp)
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for DeleteRange event")
	}

	// A final point write should be emitted with a tombstone as the previous value.
	require.NoError(t, db.Put(ctx, "foo", "final"))
	select {
	case e := <-rowC:
		require.Equal(t, roachpb.Key("foo"), e.Key)
		value, err := e.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, "final", string(value))
		prevValue, err := storage.DecodeMVCCValue(e.PrevValue.RawBytes)
		require.NoError(t, err)
		require.True(t, prevValue.IsTombstone())
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for foo=final event")
	}
}

// TestUnrecoverableErrors verifies that unrecoverable internal errors are surfaced
// to callers.
func TestUnrecoverableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
		},
	})
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

		// Lower the protectedts Cache refresh interval, so that the
		// `preGCThresholdTS` defined below is less than the protectedts
		// `readAt - GCTTL` window, resulting in a BatchTimestampBelowGCError.
		_, err = tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'")
		require.NoError(t, err)

		store, err := srv0.GetStores().(*kvserver.Stores).GetStore(srv0.GetFirstStoreID())
		require.NoError(t, err)
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader
		require.NoError(t,
			spanconfigptsreader.TestingRefreshPTSState(ctx, t, ptsReader, srv0.Clock().Now()))
	}

	f, err := rangefeed.NewFactory(srv0.Stopper(), db0, srv0.ClusterSettings(), nil)
	require.NoError(t, err)

	preGCThresholdTS := hlc.Timestamp{WallTime: 1}
	mu := struct {
		syncutil.Mutex
		internalErr error
	}{}

	testutils.SucceedsSoon(t, func() error {
		repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(scratchKey))
		conf, err := repl.SpanConfig()
		if err != nil {
			return err
		}
		if conf.GCPolicy.IgnoreStrictEnforcement {
			return errors.New("waiting for span config to apply")
		}
		require.NoError(t, repl.ReadProtectedTimestampsForTesting(ctx))
		return nil
	})

	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, preGCThresholdTS,
		func(context.Context, *kvpb.RangeFeedValue) {},
		rangefeed.WithDiff(true),
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

		if !errors.HasType(mu.internalErr, &kvpb.BatchTimestampBeforeGCError{}) {
			return errors.New("expected internal error")
		}
		return nil
	})
}

// TestMVCCHistoryMutationError verifies that applying a MVCC history mutation
// emits an unrecoverable error.
func TestMVCCHistoryMutationError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)
	_, err = tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	require.NoError(t, err)

	// Set up a rangefeed.
	f, err := rangefeed.NewFactory(srv0.Stopper(), db0, srv0.ClusterSettings(), nil)
	require.NoError(t, err)

	var once sync.Once
	checkpointC := make(chan struct{})
	errC := make(chan error)
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, srv0.Clock().Now(),
		func(context.Context, *kvpb.RangeFeedValue) {},
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
			once.Do(func() {
				close(checkpointC)
			})
		}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			select {
			case errC <- err:
			case <-ctx.Done():
			}
		}),
	)
	require.NoError(t, err)
	defer r.Close()

	// Wait for initial checkpoint.
	select {
	case <-checkpointC:
	case err := <-errC:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for checkpoint")
	}

	// Send a ClearRange request that mutates MVCC history.
	_, pErr := kv.SendWrapped(ctx, db0.NonTransactionalSender(), &kvpb.ClearRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    sp.Key,
			EndKey: sp.EndKey,
		},
	})
	require.Nil(t, pErr)

	// Wait for the MVCCHistoryMutationError.
	select {
	case err := <-errC:
		var mvccErr *kvpb.MVCCHistoryMutationError
		require.ErrorAs(t, err, &mvccErr)
		require.Equal(t, &kvpb.MVCCHistoryMutationError{Span: sp}, err)
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for error")
	}
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
		func(ctx context.Context, value *kvpb.RangeFeedValue) {
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

// TestRangeFeedStartTimeExclusive tests that the start timestamp of the
// rangefeed is in fact exclusive, as specified.
func TestRangeFeedStartTimeExclusive(t *testing.T) {
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
	span := roachpb.Span{Key: scratchKey, EndKey: scratchKey.PrefixEnd()}

	// Write three versions of "foo". Get the timestamp of the second version.
	require.NoError(t, db.Put(ctx, mkKey("foo"), 1))
	require.NoError(t, db.Put(ctx, mkKey("foo"), 2))
	kv, err := db.Get(ctx, mkKey("foo"))
	require.NoError(t, err)
	ts2 := kv.Value.Timestamp
	require.NoError(t, db.Put(ctx, mkKey("foo"), 3))

	// Enable rangefeeds, otherwise the thing will retry until they are enabled.
	_, err = tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)

	f, err := rangefeed.NewFactory(srv0.Stopper(), db, srv0.ClusterSettings(), nil)
	require.NoError(t, err)
	rows := make(chan *kvpb.RangeFeedValue)
	r, err := f.RangeFeed(ctx, "test", []roachpb.Span{span}, ts2,
		func(ctx context.Context, value *kvpb.RangeFeedValue) {
			select {
			case rows <- value:
			case <-ctx.Done():
			}
		},
	)
	require.NoError(t, err)
	defer r.Close()

	// The first emitted version should be 3.
	select {
	case row := <-rows:
		require.Equal(t, mkKey("foo"), row.Key)
		v, err := row.Value.GetInt()
		require.NoError(t, err)
		require.EqualValues(t, 3, v)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}
