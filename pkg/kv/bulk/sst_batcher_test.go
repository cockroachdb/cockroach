// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestAddBatched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("batch=default", func(t *testing.T) {
		runTestImport(t, 32<<20)
	})
	t.Run("batch=smaller", func(t *testing.T) {
		runTestImport(t, 1<<20)
	})
}

func TestDuplicateHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	mem := mon.NewUnlimitedMonitor(ctx, mon.Options{Name: mon.MakeMonitorName("lots")})
	reqs := limit.MakeConcurrentRequestLimiter("reqs", 1000)
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	expectRevisionCount := func(startKey roachpb.Key, endKey roachpb.Key, count int, exportStartTime hlc.Timestamp) {
		req := &kvpb.ExportRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
			MVCCFilter: kvpb.MVCCFilter_All,
			StartTime:  exportStartTime,
		}
		header := kvpb.Header{Timestamp: s.Clock().Now()}
		resp, err := kv.SendWrappedWith(ctx,
			kvDB.NonTransactionalSender(), header, req)
		require.NoError(t, err.GoError())
		keyCount := 0
		for _, file := range resp.(*kvpb.ExportResponse).Files {
			iterOpts := storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsOnly,
				LowerBound: keys.LocalMax,
				UpperBound: keys.MaxKey,
			}
			it, err := storage.NewMemSSTIterator(file.SST, false /* verify */, iterOpts)
			require.NoError(t, err)
			defer it.Close()
			for it.SeekGE(storage.NilKey); ; it.Next() {
				ok, err := it.Valid()
				require.NoError(t, err)
				if !ok {
					break
				}
				keyCount++
			}
		}
		require.Equal(t, count, keyCount)
	}

	// Set a start time that's well within the gc threshold.
	tsStart := timeutil.Now().Add(-time.Minute).UnixNano()
	keyCount := 10
	value := storageutils.StringValueRaw("value")

	type keyBuilder func(i int, ts int64) storage.MVCCKey

	type testCase struct {
		name            string
		skipDuplicates  bool
		ingestAll       bool
		addKeys         func(*testing.T, *bulk.SSTBatcher, keyBuilder) storage.MVCCKey
		expectedCount   int
		exportStartTime hlc.Timestamp
	}
	testCases := []testCase{
		{
			name:      "ingestAll does not add key-timestamp-value matches to SST",
			ingestAll: true,
			addKeys: func(t *testing.T, b *bulk.SSTBatcher, k keyBuilder) storage.MVCCKey {
				for i := 0; i < keyCount; i++ {
					key := k(i+1, tsStart)
					require.NoError(t, b.AddMVCCKey(ctx, key, value))
					require.NoError(t, b.AddMVCCKey(ctx, key, value))
				}
				return k(keyCount+1, tsStart)
			},
			expectedCount: keyCount,
		},
		{
			name:      "ingestAll does not error on key-value matches at different timestamps",
			ingestAll: true,
			// Set the export startTime to ensure all revisions are read, or fail if
			// the gc threshold has advance past the start time
			exportStartTime: hlc.Timestamp{WallTime: tsStart - 1},
			addKeys: func(t *testing.T, b *bulk.SSTBatcher, k keyBuilder) storage.MVCCKey {
				for i := 0; i < keyCount; i++ {
					require.NoError(t, b.AddMVCCKey(ctx, k(i+1, tsStart+1), value))
					require.NoError(t, b.AddMVCCKey(ctx, k(i+1, tsStart), value))
				}
				return k(keyCount+1, tsStart)
			},
			expectedCount: keyCount * 2,
		},
		{
			name:      "ingestAll does not error on key matches at different timestamps",
			ingestAll: true,
			// Set the export startTime to ensure all revisions are read, or fail if
			// the gc threshold has advance past the start time
			exportStartTime: hlc.Timestamp{WallTime: tsStart - 1},
			addKeys: func(t *testing.T, b *bulk.SSTBatcher, k keyBuilder) storage.MVCCKey {
				for i := 0; i < keyCount; i++ {
					require.NoError(t, b.AddMVCCKey(ctx, k(i+1, tsStart+1), value))
					require.NoError(t, b.AddMVCCKey(ctx, k(i+1, tsStart), storageutils.StringValueRaw("value2")))
				}
				return k(keyCount+1, tsStart)
			},
			expectedCount: keyCount * 2,
		},
		{
			name:      "ingestAll returns error one key-timestamp matches where value differs",
			ingestAll: true,
			addKeys: func(t *testing.T, b *bulk.SSTBatcher, k keyBuilder) storage.MVCCKey {
				key := k(1, tsStart)
				require.NoError(t, b.AddMVCCKey(ctx, key, value))
				require.Error(t, b.AddMVCCKey(ctx, key, storageutils.StringValueRaw("clobber")))
				return key
			},
		},
		{
			name:           "skip duplicates does not add keys with key-value matches at different timestamps",
			skipDuplicates: true,
			addKeys: func(t *testing.T, b *bulk.SSTBatcher, k keyBuilder) storage.MVCCKey {
				for i := 0; i < keyCount; i++ {
					require.NoError(t, b.AddMVCCKey(ctx, k(i+1, tsStart+1), value))
					require.NoError(t, b.AddMVCCKey(ctx, k(i+1, tsStart), value))
				}
				return k(keyCount+1, tsStart)
			},
			expectedCount: keyCount,
		},
		{
			name:           "skip duplicates does not add keys with key-value matches at the same timestamps",
			skipDuplicates: true,
			addKeys: func(t *testing.T, b *bulk.SSTBatcher, k keyBuilder) storage.MVCCKey {
				for i := 0; i < keyCount; i++ {
					require.NoError(t, b.AddMVCCKey(ctx, k(i+1, tsStart), value))
					require.NoError(t, b.AddMVCCKey(ctx, k(i+1, tsStart), value))
				}
				return k(keyCount+1, tsStart)
			},
			expectedCount: keyCount,
		},
		{
			name:           "skip duplicates errors if keys match but values do not",
			skipDuplicates: true,
			addKeys: func(t *testing.T, b *bulk.SSTBatcher, k keyBuilder) storage.MVCCKey {
				require.NoError(t, b.AddMVCCKey(ctx, k(1, tsStart+1), value))
				require.Error(t, b.AddMVCCKey(ctx, k(1, tsStart), storageutils.StringValueRaw("value2")))
				return k(1, tsStart+1)
			},
		},
		{
			name:           "skip duplicates errors if keys and timestamps match but values do not",
			skipDuplicates: true,
			addKeys: func(t *testing.T, b *bulk.SSTBatcher, k keyBuilder) storage.MVCCKey {
				require.NoError(t, b.AddMVCCKey(ctx, k(1, tsStart), value))
				require.Error(t, b.AddMVCCKey(ctx, k(1, tsStart), storageutils.StringValueRaw("value2")))
				return k(1, tsStart+1)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := bulk.MakeTestingSSTBatcher(ctx, kvDB, s.ClusterSettings(),
				tc.skipDuplicates, tc.ingestAll, mem.MakeConcurrentBoundAccount(), reqs)
			require.NoError(t, err)
			defer b.Close(ctx)
			k := func(i int, ts int64) storage.MVCCKey {
				return storageutils.PointKey(fmt.Sprintf("bulk-test-%s-%04d", tc.name, i+1), int(ts))
			}
			endKey := tc.addKeys(t, b, k)
			if tc.expectedCount > 0 {
				require.NoError(t, b.Flush(ctx))
				expectRevisionCount(k(0, tsStart).Key, endKey.Key, tc.expectedCount, tc.exportStartTime)
			}
		})
	}

}

func runTestImport(t *testing.T, batchSizeValue int64) {

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	batchSize := func() int64 { return batchSizeValue }

	const split1, split2 = 3, 5

	// Each test case consists of some number of batches of keys, represented as
	// ints [0, 8). Splits are at 3 and 5.
	for i, testCase := range [][][]int{
		// Simple cases, no spanning splits, try first, last, middle, etc in each.
		// r1
		{{0}},
		{{1}},
		{{2}},
		{{0, 1, 2}},
		{{0}, {1}, {2}},

		// r2
		{{3}},
		{{4}},
		{{3, 4}},
		{{3}, {4}},

		// r3
		{{5}},
		{{5, 6, 7}},
		{{6}},

		// batches exactly matching spans.
		{{0, 1, 2}, {3, 4}, {5, 6, 7}},

		// every key, in its own batch.
		{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}},

		// every key in one big batch.
		{{0, 1, 2, 3, 4, 5, 6, 7}},

		// Look for off-by-ones on and around the splits.
		{{2, 3}},
		{{1, 3}},
		{{2, 4}},
		{{1, 4}},
		{{1, 5}},
		{{2, 5}},

		// Mixture of split-aligned and non-aligned batches.
		{{1}, {5}, {6}},
		{{1, 2, 3}, {4, 5}, {6, 7}},
		{{0}, {2, 3, 5}, {7}},
		{{0, 4}, {5, 7}},
		{{0, 3}, {4}},
	} {
		t.Run(fmt.Sprintf("%d-%v", i, testCase), func(t *testing.T) {
			prefix := keys.SystemSQLCodec.IndexPrefix(uint32(100+i), 1)
			key := func(i int) roachpb.Key {
				return encoding.EncodeStringAscending(append([]byte{}, prefix...), fmt.Sprintf("k%d", i))
			}

			t.Logf("splitting at %s", key(split1))
			require.NoError(t, kvDB.AdminSplit(
				ctx,
				key(split1),
				hlc.MaxTimestamp, /* expirationTime */
			))

			// We want to make sure our range-aware batching knows about one of our
			// splits to exercise that code path, but we also want to make sure we
			// still handle an unexpected split, so we make our own range cache and
			// populate it after the first split but before the second split.
			ds := s.DistSenderI().(*kvcoord.DistSender)
			mockCache := rangecache.NewRangeCache(s.ClusterSettings(), ds,
				func() int64 { return 2 << 10 }, s.Stopper())
			for _, k := range []int{0, split1} {
				ent, err := ds.RangeDescriptorCache().Lookup(ctx, keys.MustAddr(key(k)))
				require.NoError(t, err)
				mockCache.Insert(ctx, ent)
			}

			t.Logf("splitting at %s", key(split2))
			require.NoError(t, kvDB.AdminSplit(
				ctx,
				key(split2),
				hlc.MaxTimestamp, /* expirationTime */
			))

			ts := hlc.Timestamp{WallTime: 100}
			mem := mon.NewUnlimitedMonitor(ctx, mon.Options{Name: mon.MakeMonitorName("lots")})
			reqs := limit.MakeConcurrentRequestLimiter("reqs", 1000)
			b, err := bulk.MakeBulkAdder(
				ctx, kvDB, mockCache, s.ClusterSettings(), ts,
				kvserverbase.BulkAdderOptions{MaxBufferSize: batchSize}, mem, reqs,
			)
			require.NoError(t, err)

			defer b.Close(ctx)

			var expected []kv.KeyValue

			// Since the batcher automatically handles any retries due to spanning the
			// range-bounds internally, it can be difficult to observe from outside if
			// we correctly split on the first attempt to avoid those retires.
			// However we log an event when forced to retry (in case we need to debug)
			// slow requests or something, so we can inspect the trace in the test to
			// determine if requests required the expected number of retries.
			tr := s.TracerI().(*tracing.Tracer)
			addCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "add")
			defer getRecAndFinish()
			expectedSplitRetries := 0
			for _, batch := range testCase {
				for idx, x := range batch {
					k := key(x)
					// if our adds is batching multiple keys and we've previously added
					// a key prior to split2 and are now adding one after split2, then we
					// should expect this batch to span split2 and thus cause a retry.
					if batchSize() > 1 && idx > 0 && batch[idx-1] < split2 && batch[idx-1] >= split1 && batch[idx] >= split2 {
						expectedSplitRetries = 1
					}
					v := roachpb.MakeValueFromString(fmt.Sprintf("value-%d", x))
					v.Timestamp = ts
					v.InitChecksum(k)
					t.Logf("adding: %v", k)
					require.NoError(t, b.Add(addCtx, k, v.RawBytes))
					expected = append(expected, kv.KeyValue{Key: k, Value: &v})
				}
				if err := b.Flush(addCtx); err != nil {
					t.Fatal(err)
				}
			}
			var splitRetries int
			for _, sp := range getRecAndFinish() {
				t.Log(sp.String())
				splitRetries += tracing.CountLogMessages(sp, "SSTable cannot be added spanning range bounds")
			}
			require.Equal(t, expectedSplitRetries, splitRetries, "split-caused retries")

			t.Logf("Wrote %d total", b.GetSummary().DataSize)

			got, err := kvDB.Scan(ctx, key(0), key(8), 0)
			require.NoError(t, err)
			require.Equal(t, expected, got)
		})
	}
}

var DummyImportEpoch uint32 = 3

func TestImportEpochIngestion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer log.Scope(t).Close(t)
	ctx := context.Background()

	mem := mon.NewUnlimitedMonitor(ctx, mon.Options{Name: mon.MakeMonitorName("lots")})
	reqs := limit.MakeConcurrentRequestLimiter("reqs", 1000)
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	b, err := bulk.MakeTestingSSTBatcher(ctx, kvDB, s.ClusterSettings(),
		false, true, mem.MakeConcurrentBoundAccount(), reqs)
	require.NoError(t, err)
	defer b.Close(ctx)

	startKey := storageutils.PointKey("a", 1)
	endKey := storageutils.PointKey("b", 1)
	value := storageutils.StringValueRaw("myHumbleValue")
	mvccValue, err := storage.DecodeMVCCValue(value)
	require.NoError(t, err)

	require.NoError(t, b.AddMVCCKeyWithImportEpoch(ctx, startKey, value, DummyImportEpoch))
	require.NoError(t, b.AddMVCCKeyWithImportEpoch(ctx, endKey, value, DummyImportEpoch))
	require.NoError(t, b.Flush(ctx))

	// Check that ingested key contains the dummy job ID
	req := &kvpb.ExportRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    startKey.Key,
			EndKey: endKey.Key,
		},
		MVCCFilter:             kvpb.MVCCFilter_All,
		StartTime:              hlc.Timestamp{},
		IncludeMVCCValueHeader: true,
	}

	header := kvpb.Header{Timestamp: s.Clock().Now()}
	resp, roachErr := kv.SendWrappedWith(ctx,
		kvDB.NonTransactionalSender(), header, req)
	require.NoError(t, roachErr.GoError())
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: startKey.Key,
		UpperBound: endKey.Key,
	}

	checkedJobId := false
	for _, file := range resp.(*kvpb.ExportResponse).Files {
		it, err := storage.NewMemSSTIterator(file.SST, false /* verify */, iterOpts)
		require.NoError(t, err)
		defer it.Close()
		for it.SeekGE(storage.NilKey); ; it.Next() {
			ok, err := it.Valid()
			require.NoError(t, err)
			if !ok {
				break
			}
			rawVal, err := it.UnsafeValue()
			require.NoError(t, err)
			val, err := storage.DecodeMVCCValue(rawVal)
			require.NoError(t, err)
			require.Equal(t, startKey, it.UnsafeKey())
			require.Equal(t, mvccValue.Value, val.Value)
			require.Equal(t, DummyImportEpoch, val.ImportEpoch)
			require.Equal(t, hlc.ClockTimestamp{}, val.LocalTimestamp)
			checkedJobId = true
		}
	}
	require.Equal(t, true, checkedJobId)
}
