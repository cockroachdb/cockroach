// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed_test

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
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
	smallEngineBlocks = metamorphic.ConstantWithTestBool("small-engine-blocks", false)
)

type kvs = storageutils.KVs

type rangefeedTestType struct {
	useBufferedSender bool
}

func (t rangefeedTestType) String() string {
	return fmt.Sprintf("mux/buffered_sender=%t", t.useBufferedSender)
}

var feedTypes = []rangefeedTestType{
	{
		useBufferedSender: false,
	},
}

// TestRangeFeedIntegration is a basic integration test demonstrating all of
// the pieces working together.
func TestRangeFeedIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
		mkKey := func(k string) roachpb.Key {
			return encoding.EncodeStringAscending(scratchKey, k)
		}
		// Split the range a bunch of times.
		const splits = 10
		for i := 0; i < splits; i++ {
			_, _, err := srv.SplitRange(mkKey(string([]byte{'a' + byte(i)})))
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

		// Enable rangefeeds, otherwise the thing will retry until they are enabled.
		for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
			// Whether rangefeeds are enabled is a property of both the
			// storage layer and the application layer.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
		}

		f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
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
	})
}

// TestWithOnFrontierAdvance sets up a rangefeed on a span that has more than
// one range and ensures that the OnFrontierAdvance callback is called with the
// correct timestamp.
func TestWithOnFrontierAdvance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      base.TestServerArgs{Settings: settings},
		})
		defer tc.Stopper().Stop(ctx)

		srv := tc.Server(0)
		ts := srv.ApplicationLayer()
		db := ts.DB()
		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
		mkKey := func(k string) roachpb.Key {
			return encoding.EncodeStringAscending(scratchKey, k)
		}

		sp := roachpb.Span{
			Key:    scratchKey,
			EndKey: scratchKey.PrefixEnd(),
		}
		testutils.RunTrueAndFalse(t, "quantized", func(t *testing.T, q bool) {
			var quant time.Duration
			if q {
				quant = time.Millisecond
			}
			for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
				// Enable rangefeeds, otherwise the thing will retry until they are enabled.
				kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
				// Lower the closed timestamp target duration to speed up the test.
				closedts.TargetDuration.Override(ctx, &l.ClusterSettings().SV, 100*time.Millisecond)
			}

			// Split the range into two so we know the frontier has more than one span to
			// track for certain. We later write to both these ranges.
			_, _, err = srv.SplitRange(mkKey("b"))
			require.NoError(t, err)

			f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
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
					if q {
						minTS.Backward(hlc.Timestamp{WallTime: minTS.WallTime - (minTS.WallTime % int64(quant))})
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
				rangefeed.WithFrontierQuantized(quant),
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
		})
	})
}

// TestWithOnCheckpoint verifies that we correctly emit rangefeed checkpoint
// events.
func TestWithOnCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
		mkKey := func(k string) roachpb.Key {
			return encoding.EncodeStringAscending(scratchKey, k)
		}

		sp := roachpb.Span{
			Key:    scratchKey,
			EndKey: scratchKey.PrefixEnd(),
		}
		for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
			// Lower the closed timestamp target duration to speed up the test.
			closedts.TargetDuration.Override(ctx, &l.ClusterSettings().SV, 100*time.Millisecond)
		}

		f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
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
	})
}

// TestRangefeedValueTimestamps tests that the rangefeed values (and previous
// values) have the kind of timestamps we expect when writing, overwriting, and
// deleting keys.
func TestRangefeedValueTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
		mkKey := func(k string) roachpb.Key {
			return encoding.EncodeStringAscending(scratchKey, k)
		}

		sp := roachpb.Span{
			Key:    scratchKey,
			EndKey: scratchKey.PrefixEnd(),
		}
		for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
			// Lower the closed timestamp target duration to speed up the test.
			closedts.TargetDuration.Override(ctx, &l.ClusterSettings().SV, 100*time.Millisecond)
		}

		f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
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
	})
}

// TestWithOnSSTable tests that the rangefeed emits SST ingestions correctly.
func TestWithOnSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109473),
		})
		defer srv.Stopper().Stop(ctx)
		tsrv := srv.ApplicationLayer()

		_, _, err := srv.SplitRange(roachpb.Key("a"))
		require.NoError(t, err)

		for _, l := range []serverutils.ApplicationLayerInterface{tsrv, srv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
		}

		f, err := rangefeed.NewFactory(tsrv.AppStopper(), db, tsrv.ClusterSettings(), nil)
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
			rangefeed.WithOnSSTable(func(
				ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span,
			) {
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
		sst, sstStart, sstEnd := storageutils.MakeSST(t, tsrv.ClusterSettings(), sstKVs)
		_, _, _, pErr := db.AddSSTableAtBatchTimestamp(ctx, sstStart, sstEnd, sst,
			false /* disallowConflicts */, hlc.Timestamp{},
			nil, /* stats */
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
	})
}

// TestWithOnSSTableCatchesUpIfNotSet tests that the rangefeed runs a catchup
// scan if an OnSSTable event is emitted and no OnSSTable event handler is set.
func TestWithOnSSTableCatchesUpIfNotSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109473),
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						SmallEngineBlocks: smallEngineBlocks,
					},
				},
				Settings: settings,
			},
		})
		defer tc.Stopper().Stop(ctx)
		tsrv := tc.Server(0)
		srv := tsrv.ApplicationLayer()
		db := srv.DB()

		_, _, err := tc.SplitRange(roachpb.Key("a"))
		require.NoError(t, err)
		require.NoError(t, tc.WaitForFullReplication())

		for _, l := range []serverutils.ApplicationLayerInterface{srv, tsrv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
		}

		f, err := rangefeed.NewFactory(srv.AppStopper(), db, srv.ClusterSettings(), nil)
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
			false /* disallowConflicts */, hlc.Timestamp{},
			nil, /* stats */
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
	})
}

// TestWithOnDeleteRange tests that the rangefeed emits MVCC range tombstones.
//
// TODO(erikgrinaker): These kinds of tests should really use a data-driven test
// harness, for more exhaustive testing. But it'll do for now.
func TestWithOnDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: settings,
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						SmallEngineBlocks: smallEngineBlocks,
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)
		tsrv := tc.Server(0)
		srv := tsrv.ApplicationLayer()
		db := srv.DB()

		_, _, err := tc.SplitRange(roachpb.Key("a"))
		require.NoError(t, err)
		require.NoError(t, tc.WaitForFullReplication())

		for _, l := range []serverutils.ApplicationLayerInterface{srv, tsrv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
		}

		f, err := rangefeed.NewFactory(srv.AppStopper(), db, srv.ClusterSettings(), nil)
		require.NoError(t, err)

		mkKey := func(s string) string {
			return string(append(srv.Codec().TenantPrefix(), roachpb.Key(s)...))
		}
		// We lay down a few MVCC range tombstones and points. The first range
		// tombstone should not be visible, because initial scans do not emit
		// tombstones, nor should the points covered by it. The second range tombstone
		// should be visible, because catchup scans do emit tombstones. The range
		// tombstone should be ordered after the initial point, but before the foo
		// catchup point, and the previous values should respect the range tombstones.
		require.NoError(t, db.Put(ctx, mkKey("covered"), "covered"))
		require.NoError(t, db.Put(ctx, mkKey("foo"), "covered"))
		require.NoError(t, db.DelRangeUsingTombstone(ctx, mkKey("a"), mkKey("z")))
		require.NoError(t, db.Put(ctx, mkKey("foo"), "initial"))
		rangeFeedTS := db.Clock().Now()
		require.NoError(t, db.Put(ctx, mkKey("covered"), "catchup"))
		require.NoError(t, db.DelRangeUsingTombstone(ctx, mkKey("a"), mkKey("z")))
		require.NoError(t, db.Put(ctx, mkKey("foo"), "catchup"))

		// We start the rangefeed over a narrower span than the DeleteRanges (c-g),
		// to ensure the DeleteRange event is truncated to the registration span.
		var checkpointOnce sync.Once
		checkpointC := make(chan struct{})
		deleteRangeC := make(chan *kvpb.RangeFeedDeleteRange)
		rowC := make(chan *kvpb.RangeFeedValue)

		spans := []roachpb.Span{{
			Key:    append(srv.Codec().TenantPrefix(), roachpb.Key("c")...),
			EndKey: append(srv.Codec().TenantPrefix(), roachpb.Key("g")...),
		}}
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
			require.Equal(t, roachpb.Key(mkKey("foo")), e.Key)
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
			require.Equal(t, roachpb.Span{
				Key:    append(srv.Codec().TenantPrefix(), roachpb.Key("c")...),
				EndKey: append(srv.Codec().TenantPrefix(), roachpb.Key("g")...),
			}, e.Span)
			require.NotEmpty(t, e.Timestamp)
		case <-time.After(3 * time.Second):
			require.Fail(t, "timed out waiting for DeleteRange event")
		}

		select {
		case e := <-rowC:
			require.Equal(t, roachpb.Key(mkKey("covered")), e.Key)
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
			require.Equal(t, roachpb.Key(mkKey("foo")), e.Key)
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
		require.NoError(t, db.DelRangeUsingTombstone(ctx, mkKey("a"), mkKey("z")))
		select {
		case e := <-deleteRangeC:
			require.Equal(t, roachpb.Span{
				Key:    append(srv.Codec().TenantPrefix(), roachpb.Key("c")...),
				EndKey: append(srv.Codec().TenantPrefix(), roachpb.Key("g")...),
			}, e.Span)
			require.NotEmpty(t, e.Timestamp)
		case <-time.After(3 * time.Second):
			require.Fail(t, "timed out waiting for DeleteRange event")
		}

		// A final point write should be emitted with a tombstone as the previous value.
		require.NoError(t, db.Put(ctx, mkKey("foo"), "final"))
		select {
		case e := <-rowC:
			require.Equal(t, roachpb.Key(mkKey("foo")), e.Key)
			value, err := e.Value.GetBytes()
			require.NoError(t, err)
			require.Equal(t, "final", string(value))
			prevValue, err := storage.DecodeMVCCValue(e.PrevValue.RawBytes)
			require.NoError(t, err)
			require.True(t, prevValue.IsTombstone())
		case <-time.After(3 * time.Second):
			require.Fail(t, "timed out waiting for foo=final event")
		}
	})
}

// TestUnrecoverableErrors verifies that unrecoverable internal errors are surfaced
// to callers.
func TestUnrecoverableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
			DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109472),
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
			Settings: settings,
		})

		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]

		sp := roachpb.Span{
			Key:    scratchKey,
			EndKey: scratchKey.PrefixEnd(),
		}
		for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
			// Lower the closed timestamp target duration to speed up the test.
			closedts.TargetDuration.Override(ctx, &l.ClusterSettings().SV, 100*time.Millisecond)
		}

		store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
		require.NoError(t, err)

		{
			// Lower the protectedts Cache refresh interval, so that the
			// `preGCThresholdTS` defined below is less than the protectedts
			// `readAt - GCTTL` window, resulting in a BatchTimestampBelowGCError.
			_, err = sqlDB.Exec("SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'")
			require.NoError(t, err)

			ptsReader := store.GetStoreConfig().ProtectedTimestampReader
			require.NoError(t,
				spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, srv.SystemLayer().Clock().Now()))
		}

		f, err := rangefeed.NewFactory(ts.AppStopper(), kvDB, ts.ClusterSettings(), nil)
		require.NoError(t, err)

		preGCThresholdTS := hlc.Timestamp{WallTime: 1}
		mu := struct {
			syncutil.Mutex
			internalErr error
		}{}

		testutils.SucceedsSoon(t, func() error {
			repl := store.LookupReplica(roachpb.RKey(scratchKey))
			if conf, err := repl.LoadSpanConfig(ctx); err != nil || conf.GCPolicy.IgnoreStrictEnforcement {
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
	})
}

// TestMVCCHistoryMutationError verifies that applying a MVCC history mutation
// emits an unrecoverable error.
func TestMVCCHistoryMutationError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
		sp := roachpb.Span{
			Key:    scratchKey,
			EndKey: scratchKey.PrefixEnd(),
		}
		for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
			// Lower the closed timestamp target duration to speed up the test.
			closedts.TargetDuration.Override(ctx, &l.ClusterSettings().SV, 100*time.Millisecond)
		}

		// Set up a rangefeed.
		f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
		require.NoError(t, err)

		var once sync.Once
		checkpointC := make(chan struct{})
		errC := make(chan error)
		r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, ts.Clock().Now(),
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
		_, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(), &kvpb.ClearRangeRequest{
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
	})
}

// TestRangefeedWithLabelsOption verifies go routines started by rangefeed are
// annotated with pprof labels.
func TestRangefeedWithLabelsOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
		mkKey := func(k string) roachpb.Key {
			return encoding.EncodeStringAscending(scratchKey, k)
		}
		// Split the range a bunch of times.
		const splits = 10
		for i := 0; i < splits; i++ {
			_, _, err := srv.SplitRange(mkKey(string([]byte{'a' + byte(i)})))
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
		for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
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

		f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
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
	})
}

// TestRangeFeedStartTimeExclusive tests that the start timestamp of the
// rangefeed is in fact exclusive, as specified.
func TestRangeFeedStartTimeExclusive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
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

		for _, l := range []serverutils.ApplicationLayerInterface{ts, srv.SystemLayer()} {
			// Enable rangefeeds, otherwise the thing will retry until they are enabled.
			kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
		}

		f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
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
	})
}

// TestRangeFeedIntentResolutionRace is a regression test for
// https://github.com/cockroachdb/cockroach/issues/104309, i.e. the
// following scenario:
//
//   - A rangefeed is running on a lagging follower.
//   - A txn writes an intent, which is replicated to the follower.
//   - The closed timestamp advances past the intent.
//   - The txn commits and resolves the intent at the original write timestamp,
//     then GCs its txn record. This is not yet applied on the follower.
//   - The rangefeed pushes the txn to advance its resolved timestamp.
//   - The txn is GCed, so the push returns ABORTED (it can't know whether the
//     txn was committed or aborted after its record is GCed).
//   - The rangefeed disregards the "aborted" txn and advances the resolved
//     timestamp, emitting a checkpoint.
//   - The follower applies the resolved intent and emits an event below
//     the checkpoint, violating the checkpoint guarantee.
//
// This scenario is addressed by running a Barrier request through Raft and
// waiting for it to apply locally before removing the txn from resolved ts
// tracking. This ensures the pending intent resolution is applied before
// the resolved ts can advance.
func TestRangeFeedIntentResolutionRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t) // too slow, times out

	// Use a timeout, to prevent a hung test.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel() after Stopper.Stop(), so the context cancels first.
	// Otherwise, the stopper will hang waiting for a rangefeed whose context is
	// not yet cancelled.

	// Set up an apply filter that blocks Raft application on n3 (follower) for
	// the given range.
	var blockRangeOnN3 atomic.Int64
	unblockRangeC := make(chan struct{})
	applyFilter := func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		if args.StoreID == 3 {
			if rangeID := blockRangeOnN3.Load(); rangeID > 0 && rangeID == int64(args.RangeID) {
				t.Logf("blocked r%d on s%d", args.RangeID, args.StoreID)
				select {
				case <-unblockRangeC:
					t.Logf("unblocked r%d on s%d", args.RangeID, args.StoreID)
				case <-ctx.Done():
					return 0, kvpb.NewError(ctx.Err())
				}
			}
		}
		return 0, nil
	}

	// Set up a request filter that blocks transaction pushes for a specific key.
	// This is used to prevent the rangefeed txn pusher from pushing the txn
	// timestamp above the closed timestamp before it commits, only allowing the
	// push to happen after the transaction has committed and GCed below the
	// closed timestamp.
	var blockPush atomic.Pointer[roachpb.Key]
	unblockPushC := make(chan struct{})
	reqFilter := func(ctx context.Context, br *kvpb.BatchRequest) *kvpb.Error {
		if br.IsSinglePushTxnRequest() {
			req := br.Requests[0].GetPushTxn()
			if key := blockPush.Load(); key != nil && req.Key.Equal(*key) {
				t.Logf("blocked push for txn %s", req.PusheeTxn)
				select {
				case <-unblockPushC:
					t.Logf("unblocked push for txn %s", req.PusheeTxn)
				case <-ctx.Done():
					return kvpb.NewError(ctx.Err())
				}
			}
		}
		return nil
	}

	// Speed up the test by reducing various closed/resolved timestamp intervals.
	const interval = 100 * time.Millisecond
	st := cluster.MakeTestingClusterSettings()
	kvserver.RangeFeedRefreshInterval.Override(ctx, &st.SV, interval)
	closedts.SideTransportCloseInterval.Override(ctx, &st.SV, interval)
	closedts.TargetDuration.Override(ctx, &st.SV, interval)

	// Start a cluster with 3 nodes.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter:          reqFilter,
					TestingApplyCalledTwiceFilter: applyFilter,
					RangeFeedPushTxnsInterval:     interval,
					RangeFeedPushTxnsAge:          interval,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	defer cancel()

	n1 := tc.Server(0)
	s3 := tc.GetFirstStoreFromServer(t, 2)
	clock := n1.ApplicationLayer().Clock()

	// Determine the key/value we're going to write.
	prefix := append(n1.ApplicationLayer().Codec().TenantPrefix(), keys.ScratchRangeMin...)
	key := append(prefix.Clone(), []byte("/foo")...)
	value := []byte("value")

	// Split off a range and upreplicate it, with leaseholder on n1.
	_, _, err := n1.StorageLayer().SplitRange(prefix)
	require.NoError(t, err)
	desc := tc.AddVotersOrFatal(t, prefix, tc.Targets(1, 2)...)
	t.Logf("split off range %s", desc)

	repl1 := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(prefix)) // leaseholder
	repl3 := tc.GetFirstStoreFromServer(t, 2).LookupReplica(roachpb.RKey(prefix)) // lagging follower

	require.True(t, repl1.OwnsValidLease(ctx, clock.NowAsClockTimestamp()))

	// Block pushes of the txn, to ensure it can write at a fixed timestamp.
	// Otherwise, the rangefeed or someone else may push it beyond the closed
	// timestamp.
	blockPush.Store(&key)

	// We'll use n3 as our lagging follower. Start a rangefeed on it directly.
	req := kvpb.RangeFeedRequest{
		Header: kvpb.Header{
			RangeID: desc.RangeID,
		},
		Span: desc.RSpan().AsRawSpanWithNoLocals(),
	}
	eventC := make(chan *kvpb.RangeFeedEvent)
	sink := newChannelSink(ctx, eventC)
	_, rErr := s3.RangeFeed(sink.ctx, &req, sink, nil)
	require.NoError(t, rErr) // check if we've errored yet
	require.NoError(t, sink.Error())
	t.Logf("started rangefeed on %s", repl3)

	// Spawn a rangefeed monitor, which posts checkpoint updates to checkpointC.
	// This uses a buffered channel of size 1, and empties it out before posting a
	// new update, such that it contains the latest known checkpoint and does not
	// block the rangefeed. It also posts emitted values for our key to valueC,
	// which should only happen once.
	checkpointC := make(chan *kvpb.RangeFeedCheckpoint, 1)
	valueC := make(chan *kvpb.RangeFeedValue, 1)
	go func() {
		defer close(checkpointC)
		defer close(valueC)
		for {
			select {
			case e := <-eventC:
				switch {
				case e.Checkpoint != nil:
					// Clear checkpointC such that it always contains the latest.
					select {
					case <-checkpointC:
					default:
					}
					checkpointC <- e.Checkpoint
				case e.Val != nil && e.Val.Key.Equal(key):
					select {
					case valueC <- e.Val:
					default:
						t.Errorf("duplicate value event for %s: %s", key, e)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	waitForCheckpoint := func(t *testing.T, ts hlc.Timestamp) hlc.Timestamp {
		t.Helper()
		timeoutC := time.After(3 * time.Second)
		for {
			select {
			case c := <-checkpointC:
				require.NotNil(t, c, "nil checkpoint")
				if ts.LessEq(c.ResolvedTS) {
					t.Logf("rangefeed checkpoint at %s >= %s", c.ResolvedTS, ts)
					return c.ResolvedTS
				}
			case <-timeoutC:
				require.Fail(t, "timed out waiting for checkpoint", "wanted %s", ts)
			}
		}
	}

	// Wait for the initial checkpoint.
	rts := waitForCheckpoint(t, clock.Now())
	t.Logf("initial checkpoint at %s", rts)

	// Start a new transaction on n1 with a fixed timestamp (to make sure it
	// remains below the closed timestamp). Write an intent, and read it back to
	// make sure it has applied.
	writeTS := clock.Now()
	txn := n1.ApplicationLayer().DB().NewTxn(ctx, "test")
	require.NoError(t, txn.SetFixedTimestamp(ctx, writeTS))
	require.NoError(t, txn.Put(ctx, key, value))
	_, err = txn.Get(ctx, key)
	require.NoError(t, err)
	t.Logf("wrote %s", key)

	// Wait for both the leaseholder and the follower to close the transaction's
	// write timestamp.
	waitForClosedTimestamp := func(t *testing.T, repl *kvserver.Replica, ts hlc.Timestamp) hlc.Timestamp {
		t.Helper()
		timeoutC := time.After(3 * time.Second)
		for {
			if closedTS := repl.GetCurrentClosedTimestamp(ctx); ts.LessEq(closedTS) {
				t.Logf("replica %s closed timestamp at %s >= %s", repl, closedTS, ts)
				return closedTS
			}
			select {
			case <-time.After(10 * time.Millisecond):
			case <-timeoutC:
				require.Fail(t, "timeout out waiting for closed timestamp", "wanted %s", ts)
			}
		}
	}
	cts := waitForClosedTimestamp(t, repl1, writeTS)
	_ = waitForClosedTimestamp(t, repl3, writeTS)
	t.Logf("closed timestamp %s is above txn write timestamp %s", cts, writeTS)

	// Wait for the rangefeed checkpoint to reach writeTS.Prev(). This ensures the
	// rangefeed's view of the closed timestamp has been updated, and is now only
	// blocked by the intent.
	waitTS := writeTS.Prev()
	waitTS.Logical = 0
	rts = waitForCheckpoint(t, waitTS)
	t.Logf("rangefeed caught up to txn write timestamp at %s", rts)

	// Block Raft application on repl3.
	blockRangeOnN3.Store(int64(desc.RangeID))

	// Commit the transaction, and check its commit timestamp.
	require.NoError(t, txn.Commit(ctx))
	commitTS, err := txn.CommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, commitTS, writeTS)
	t.Logf("txn committed at %s", writeTS)

	// Unblock transaction pushes. Since repl3 won't apply the intent resolution
	// yet, the rangefeed will keep trying to push the transaction. Once the
	// transaction record is GCed (which happens async), the rangefeed will see an
	// ABORTED status.
	//
	// It may see the intermediate COMMITTED state too, but at the time of writing
	// that does not matter, since the rangefeed needs to keep tracking the
	// intent until it applies the resolution, and it will also see the ABORTED
	// status before that happens.
	blockPush.Store(nil)
	close(unblockPushC)

	// Make sure repl3 does not emit a checkpoint beyond the write timestamp. Its
	// closed timestamp has already advanced past it, but the unresolved intent
	// should prevent the resolved timestamp from advancing, despite the false
	// ABORTED state. We also make sure no value has been emitted.
	waitC := time.After(3 * time.Second)
	for done := false; !done; {
		select {
		case c := <-checkpointC:
			require.NotNil(t, c)
			require.False(t, commitTS.LessEq(c.ResolvedTS),
				"repl %s emitted checkpoint %s beyond write timestamp %s", repl3, c.ResolvedTS, commitTS)
		case v := <-valueC:
			require.Failf(t, "repl3 emitted premature value", "value: %#+v", v)
		case <-waitC:
			done = true
		}
	}
	t.Logf("checkpoint still below write timestamp")

	// Unblock application on repl3. Wait for the checkpoint to catch up to the
	// commit timestamp, and the committed value to be emitted.
	blockRangeOnN3.Store(0)
	close(unblockRangeC)

	rts = waitForCheckpoint(t, writeTS)
	t.Logf("checkpoint %s caught up to write timestamp %s", rts, writeTS)

	select {
	case v := <-valueC:
		require.Equal(t, v.Key, key)
		require.Equal(t, v.Value.Timestamp, writeTS)
		t.Logf("received value %s", v)
	case <-time.After(3 * time.Second):
		require.Fail(t, "timed out waiting for event")
	}

	// The rangefeed should still be running.
	require.NoError(t, sink.Error())
}

// channelSink is a rangefeed sink which posts events to a channel.
type channelSink struct {
	ctx  context.Context
	ch   chan<- *kvpb.RangeFeedEvent
	done chan *kvpb.Error
}

func newChannelSink(ctx context.Context, ch chan<- *kvpb.RangeFeedEvent) *channelSink {
	return &channelSink{ctx: ctx, ch: ch, done: make(chan *kvpb.Error, 1)}
}

func (c *channelSink) SendUnbufferedIsThreadSafe() {}

func (c *channelSink) SendUnbuffered(e *kvpb.RangeFeedEvent) error {
	select {
	case c.ch <- e:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

// Error returns the error sent to the done channel if the sink has been
// disconnected. It returns nil otherwise.
func (c *channelSink) Error() error {
	select {
	case err := <-c.done:
		return err.GoError()
	default:
		return nil
	}
}

// SendError implements the Stream interface. It mocks the disconnect behavior
// by sending the error to the done channel.
func (c *channelSink) SendError(err *kvpb.Error) {
	c.done <- err
}

// TestRangeFeedMetadataManualSplit tests that a spawned rangefeed emits a
// metadata event which indicates if it spawned to due a manual split. The
// test specifically conducts the following:
// 1) set up a rangefeed over some key space
// 2) manually split the key space
// 3) Verify that 2 metadata events are sent for the LHS and RHS rangefeeds.
func TestRangeFeedMetadataManualSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		kvserver.RangefeedEnabled.Override(ctx, &settings.SV, true)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		_, _, err := srv.SplitRange(scratchKey)
		require.NoError(t, err)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
		mkKey := func(k string) roachpb.Key {
			return encoding.EncodeStringAscending(scratchKey, k)
		}

		sp := roachpb.Span{
			Key:    scratchKey,
			EndKey: scratchKey.PrefixEnd(),
		}

		f, err := rangefeed.NewFactory(ts.AppStopper(), db, ts.ClusterSettings(), nil)
		require.NoError(t, err)

		metadata := make(chan *kvpb.RangeFeedMetadata)
		initialScanDone := make(chan struct{})
		rows := make(chan *kvpb.RangeFeedValue)
		r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, ts.DB().Clock().Now(),
			func(ctx context.Context, value *kvpb.RangeFeedValue) {
				select {
				case rows <- value:
				case <-ctx.Done():
				}
			},
			rangefeed.WithInitialScan(func(ctx context.Context) {
				close(initialScanDone)
			}),
			rangefeed.WithOnMetadata(func(ctx context.Context, value *kvpb.RangeFeedMetadata) {
				select {
				case metadata <- value:
				case <-ctx.Done():
					return
				}
			}),
		)
		require.NoError(t, err)

		defer func() {
			r.Close()
			close(metadata)
			close(rows)
		}()
		{
			// First meta msg for the new rangefeed.
			meta := <-metadata
			t.Logf("initial span %s-%s", meta.Span.Key, meta.Span.EndKey)
			require.False(t, meta.FromManualSplit)
			require.Equal(t, sp.Key, meta.Span.Key)
			require.Equal(t, sp.EndKey, meta.Span.EndKey)
			require.Empty(t, meta.ParentStartKey)
		}
		<-initialScanDone

		// Confirm the rangefeed is running on the expected range.
		require.NoError(t, db.Put(ctx, mkKey("c"), 2))
		val := <-rows
		require.Equal(t, mkKey("c"), val.Key)

		go func() {
			// The key above may be emitted again due to rangefeed semantics. Receive
			// these repeat updates to ensure the rows channel is never blocked.
			for range rows {
			}
		}()

		splitKey := mkKey("b")
		_, _, err = srv.SplitRange(splitKey)
		require.NoError(t, err)
		for {
			// Expect a metadata event from the manual split.
			meta := <-metadata
			t.Logf("manual split new range key span %s-%s; manual split %t", meta.Span.Key, meta.Span.EndKey, meta.FromManualSplit)
			require.Equal(t, true, meta.FromManualSplit)
			if !meta.Span.EndKey.Equal(sp.EndKey) {
				// New Rangefeed for LHS.
				require.Equal(t, splitKey, meta.Span.EndKey)
				require.Equal(t, sp.Key, meta.Span.Key)
				require.Equal(t, sp.Key, meta.ParentStartKey)
				break
			}
			// Due to outdated rangefeed cache, we could spawn a rangefeed with the og
			// span induced by manual split. We expect this rangefeed to error before
			// starting with a rangekey mismatch error, which should spawn the correct
			// rangefeeds with the manual split flag.
			require.Equal(t, sp.Key, meta.Span.Key)
			require.Equal(t, sp.EndKey, meta.Span.EndKey)
		}
		{
			// New Rangefeed for the RHS.
			meta := <-metadata
			t.Logf("another split new range key span %s-%s; manual %t", meta.Span.Key, meta.Span.EndKey, meta.FromManualSplit)
			require.True(t, meta.FromManualSplit)
			require.Equal(t, splitKey, meta.Span.Key)
			require.Equal(t, sp.EndKey, meta.Span.EndKey)
			require.Equal(t, sp.Key, meta.ParentStartKey)
		}

	})
}

// TestRangeFeedMetadataAutoSplit tests that a spawned rangefeed emits metadata
// events which indicates if a rangefeed spawned _not_ due to a manual split.
// This test relies on the fact that a newly created table with data will be
// automatically split into its own range.
func TestRangeFeedMetadataAutoSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		// We must enable desired scheduler settings before we start cluster,
		// otherwise we will trigger processor restarts later and this test can't
		// handle duplicated events.
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		kvserver.RangefeedEnabled.Override(ctx, &settings.SV, true)
		// Lower the closed timestamp target duration to speed up the test.
		closedts.TargetDuration.Override(ctx, &settings.SV, 100*time.Millisecond)
		srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		sql := sqlutils.MakeSQLRunner(conn)

		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		var maxTableID uint32
		sql.QueryRow(t, "SELECT max(id) FROM system.namespace").Scan(&maxTableID)
		tenantPrefixEnd := srv.Codec().TenantPrefix().PrefixEnd()

		// Create a rangefeed that listens to key updates at the highest table in
		// system key space and above.
		sp := roachpb.Span{
			Key:    ts.Codec().TablePrefix(maxTableID),
			EndKey: tenantPrefixEnd,
		}

		// Wait for foo to have its own range.
		f, err := rangefeed.NewFactory(ts.AppStopper(), ts.DB(), ts.ClusterSettings(), nil)
		require.NoError(t, err)

		metadata := make(chan *kvpb.RangeFeedMetadata)
		initialScanDone := make(chan struct{})
		r, err := f.RangeFeed(ctx, "test", []roachpb.Span{sp}, ts.DB().Clock().Now(),
			func(ctx context.Context, value *kvpb.RangeFeedValue) {
			},
			rangefeed.WithInitialScan(func(ctx context.Context) {
				close(initialScanDone)
			}),
			rangefeed.WithOnMetadata(func(ctx context.Context, value *kvpb.RangeFeedMetadata) {
				select {
				case metadata <- value:
				case <-ctx.Done():
					return
				}
			}),
		)

		require.NoError(t, err)
		defer r.Close()
		<-initialScanDone
		{
			// First meta msg for the new rangefeed.
			meta := <-metadata
			require.False(t, meta.FromManualSplit)
			require.Equal(t, sp.Key, meta.Span.Key)
			require.Equal(t, sp.EndKey, meta.Span.EndKey)
			require.Empty(t, meta.ParentStartKey)
		}

		sql.Exec(t, "CREATE TABLE foo (key INT PRIMARY KEY)")
		for {
			meta := <-metadata
			t.Logf("new range key span %s-%s; manual split %t", meta.Span.Key, meta.Span.EndKey, meta.FromManualSplit)
			require.False(t, meta.FromManualSplit)
			if !meta.Span.EndKey.Equal(sp.EndKey) {
				// Verify metadata for LHS rangefeed.
				require.Equal(t, sp.Key, meta.Span.Key)
				require.Equal(t, sp.Key, meta.ParentStartKey)
				break
			}
			// Due to an outdated rangefeed cache, we could spawn a rangefeed with the
			// og span induced by the split. We expect this rangefeed to error before
			// starting with a rangekey mismatch error, which should spawn the correct
			// rangefeeds with the manual split flag.
			require.Equal(t, sp.Key, meta.Span.Key)
			require.Equal(t, sp.EndKey, meta.Span.EndKey)
			require.Equal(t, sp.Key, meta.ParentStartKey)
		}
		{
			// Verify the RHS rangefeed metadata.
			meta := <-metadata
			require.False(t, meta.FromManualSplit)
			require.NotEqual(t, sp.Key, meta.Span.Key)
			require.Equal(t, sp.EndKey, meta.Span.EndKey)
			require.Equal(t, sp.Key, meta.ParentStartKey)
		}
	})
}

// TestRangefeedCatchupStarvation tests that a single MuxRangefeed
// call cannot starve other users. Note that starvation is still
// possible if there are more than 2 consumers of a given range.
func TestRangefeedCatchupStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "feed_type", feedTypes, func(t *testing.T, rt rangefeedTestType) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		kvserver.RangefeedUseBufferedSender.Override(ctx, &settings.SV, rt.useBufferedSender)
		kvserver.RangefeedEnabled.Override(ctx, &settings.SV, true)
		// Lower the limit to make it more likely to get starved.
		kvserver.ConcurrentRangefeedItersLimit.Override(ctx, &settings.SV, 8)
		kvserver.PerConsumerCatchupLimit.Override(ctx, &settings.SV, 6)
		srv, _, db := serverutils.StartServer(t, base.TestServerArgs{
			Settings: settings,
		})
		defer srv.Stopper().Stop(ctx)
		s := srv.ApplicationLayer()
		ts := s.Clock().Now()
		scratchKey := append(s.Codec().TenantPrefix(), keys.ScratchRangeMin...)
		scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
		mkKey := func(k string) roachpb.Key {
			return encoding.EncodeStringAscending(scratchKey, k)
		}
		ranges := 32
		keysPerRange := 128
		totalKeys := ranges * keysPerRange
		for i := range ranges {
			for j := range keysPerRange {
				k := mkKey(fmt.Sprintf("%d-%d", i, j))
				require.NoError(t, db.Put(ctx, k, 1))
			}
			_, _, err := srv.SplitRange(mkKey(fmt.Sprintf("%d", i)))
			require.NoError(t, err)
		}

		span := roachpb.Span{Key: scratchKey, EndKey: scratchKey.PrefixEnd()}
		f, err := rangefeed.NewFactory(s.AppStopper(), db, s.ClusterSettings(), nil)
		require.NoError(t, err)

		blocked := make(chan struct{})
		r1, err := f.RangeFeed(ctx, "consumer-1-rf-1", []roachpb.Span{span}, ts,
			func(ctx context.Context, value *kvpb.RangeFeedValue) {
				blocked <- struct{}{}
				<-ctx.Done()
			},
			rangefeed.WithConsumerID(1),
		)
		require.NoError(t, err)
		defer r1.Close()
		<-blocked

		// Multiple rangefeeds from the same ConsumeID should
		// be treated as the same consumer and thus they
		// shouldn't be able to overwhelm the overall store
		// quota.
		for i := range 8 {
			r1, err := f.RangeFeed(ctx, fmt.Sprintf("consumer-1-rf-%d", i+2), []roachpb.Span{span}, ts,
				func(ctx context.Context, value *kvpb.RangeFeedValue) { <-ctx.Done() },
				rangefeed.WithConsumerID(1),
			)
			require.NoError(t, err)
			defer r1.Close()
		}

		// Despite 9 rangefeeds above each needing 32 catchup
		// scans, the following rangefeed should always make
		// progress because it has a different consumer ID.
		r2ConsumedRow := make(chan roachpb.Key)
		r2, err := f.RangeFeed(ctx, "rf2", []roachpb.Span{span}, ts,
			func(ctx context.Context, value *kvpb.RangeFeedValue) {
				r2ConsumedRow <- value.Key
			},
			rangefeed.WithConsumerID(2),
		)
		require.NoError(t, err)
		defer r2.Close()

		// Wait until we see every key we've writen on rf2.
		seen := make(map[string]struct{}, 0)
		for {
			select {
			case r := <-r2ConsumedRow:
				seen[r.String()] = struct{}{}
				if len(seen) >= totalKeys {
					return
				}
			case <-time.After(testutils.DefaultSucceedsSoonDuration):
				t.Fatal("test timed out")
			}
		}
	})
}
