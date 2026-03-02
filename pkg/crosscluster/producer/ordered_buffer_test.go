// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// makeOrderedBufferConfig creates a config with in-memory temp storage for tests.
func makeOrderedBufferConfig(
	t *testing.T, flushByteSizeThreshold int64,
) (OrderedBufferConfig, func()) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), nil)
	require.NoError(t, err)
	cfg := OrderedBufferConfig{
		settings:               st,
		streamID:               streampb.StreamID(1),
		tempStorage:            tempEngine,
		flushByteSizeThreshold: flushByteSizeThreshold,
	}
	return cfg, func() { tempEngine.Close() }
}

func makeKV(key string, ts int64, value string) *kvpb.RangeFeedValue {
	return &kvpb.RangeFeedValue{
		Key: roachpb.Key(key),
		Value: roachpb.Value{
			RawBytes:  []byte(value),
			Timestamp: hlc.Timestamp{WallTime: ts},
		},
	}
}

func ts(wall int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: wall}
}

// getAllKVValuesFromDisk calls GetKVsFromDisk in a loop until iterExhausted.
func getAllKVValuesFromDisk(
	t *testing.T, b *OrderedBuffer, ctx context.Context, resolvedTs hlc.Timestamp,
) []*kvpb.RangeFeedValue {
	var out []*kvpb.RangeFeedValue
	for {
		values, iterExhausted, err := b.GetKVsFromDisk(ctx, resolvedTs)
		require.NoError(t, err)
		out = append(out, values...)
		if iterExhausted {
			break
		}
	}
	return out
}

// isMVCCSorted returns whether values are in (ts asc, key asc) order.
func isMVCCSorted(values []*kvpb.RangeFeedValue) bool {
	for i := 0; i < len(values)-1; i++ {
		a, b := values[i], values[i+1]
		tsCmp := a.Value.Timestamp.Compare(b.Value.Timestamp)
		keyCmp := bytes.Compare(a.Key, b.Key)
		if tsCmp > 0 || (tsCmp == 0 && keyCmp > 0) {
			return false
		}
	}
	return true
}

// filterKVsInTimestampRange returns KVs with after < ts <= atOrBefore from the
// given slice (which must already be sorted by (ts, key)).
func filterKVsInTimestampRange(
	kvs []*kvpb.RangeFeedValue, after, atOrBefore hlc.Timestamp,
) []*kvpb.RangeFeedValue {
	var out []*kvpb.RangeFeedValue
	for _, kv := range kvs {
		ts := kv.Value.Timestamp
		if ts.Compare(after) > 0 && ts.Compare(atOrBefore) <= 0 {
			out = append(out, kv)
		}
	}
	return out
}

func TestEncodeDecodeTimestampKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pairs := []struct {
		ts  hlc.Timestamp
		key roachpb.Key
	}{
		{ts(1), roachpb.Key("key")},
		{hlc.Timestamp{WallTime: 123, Logical: 456}, roachpb.Key("x")},
		{ts(0), roachpb.Key("a")},
		{ts(0), roachpb.Key("b")},
	}

	sortedPairs := make([][]byte, len(pairs))

	for i, tc := range pairs {
		enc := encodeTimestampKey(tc.ts, tc.key)
		decTs, decKey, err := decodeTimestampKey(enc)
		require.NoError(t, err)
		require.True(t, tc.ts.Equal(decTs))
		require.True(t, tc.key.Equal(decKey))
		sortedPairs[i] = enc
	}

	// Test that sorting encoded keys gives us (ts asc, key asc) order.
	slices.SortFunc(sortedPairs, func(a, b []byte) int { return bytes.Compare(a, b) })
	slices.SortFunc(pairs, func(a, b struct {
		ts  hlc.Timestamp
		key roachpb.Key
	}) int {
		cmpTs := a.ts.Compare(b.ts)
		if cmpTs != 0 {
			return cmpTs
		}
		return a.key.Compare(b.key)
	})
	for i, enc := range sortedPairs {
		decTs, decKey, err := decodeTimestampKey(enc)
		require.NoError(t, err)
		require.True(t, pairs[i].ts.Equal(decTs))
		require.True(t, pairs[i].key.Equal(decKey))
	}
}

func TestDecodeTimestampKeyTooShort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, _, err := decodeTimestampKey([]byte("short"))
	require.Error(t, err)
}

func TestOrderedBufferAddAndLen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Increase threshold so MaybeFlushToDisk won't flush.
	cfg, cleanup := makeOrderedBufferConfig(t, 1<<20)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	require.Equal(t, 0, buf.Len())

	require.NoError(t, buf.Add(ctx, makeKV("k1", 1, "v1")))
	require.Equal(t, 1, buf.Len())
	require.NoError(t, buf.Add(ctx, makeKV("k2", 2, "v2")))
	require.Equal(t, 2, buf.Len())

	// No flush yet (byte threshold not reached), so disk map not created.
	values, _, err := buf.GetKVsFromDisk(ctx, ts(10))
	require.NoError(t, err)
	require.Nil(t, values)
}

func TestFlushToDiskWhenResolvedTsBelowMin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Increase threshold so MaybeFlushToDisk won't flush.
	cfg, cleanup := makeOrderedBufferConfig(t, 1<<20)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	require.NoError(t, buf.Add(ctx, makeKV("k1", 10, "v1")))
	require.NoError(t, buf.Add(ctx, makeKV("k2", 20, "v2")))

	// resolvedTs < minTsInBuffer, so no flush.
	err := buf.FlushToDisk(ctx, ts(5))
	require.NoError(t, err)
	require.Equal(t, 2, buf.Len(), "buffer should be unchanged")

	// Nothing on disk.
	values, _, err := buf.GetKVsFromDisk(ctx, ts(5))
	require.NoError(t, err)
	require.Nil(t, values)
}

func TestFlushToDiskWhenResolvedTsAboveMin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	cfg, cleanup := makeOrderedBufferConfig(t, 1<<20)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	require.NoError(t, buf.Add(ctx, makeKV("k2", 20, "v2")))
	require.NoError(t, buf.Add(ctx, makeKV("k1", 10, "v1")))
	require.NoError(t, buf.Add(ctx, makeKV("k1", 20, "v2")))
	require.NoError(t, buf.Add(ctx, makeKV("k1", 30, "v3")))

	err := buf.FlushToDisk(ctx, ts(20))
	require.NoError(t, err)
	require.Equal(t, 0, buf.Len())

	// Values should be in (ts asc, key asc) order.
	values := getAllKVValuesFromDisk(t, buf, ctx, ts(20))
	require.Len(t, values, 3)
	require.True(t, isMVCCSorted(values), "values not in (ts, key) order")
	// When timestamps tie, keys sort in ascending order so k1 comes before k2.
	expected := []struct {
		ts  int64
		key string
	}{
		{10, "k1"},
		{20, "k1"},
		{20, "k2"},
	}
	require.Len(t, values, len(expected))
	for i, exp := range expected {
		require.Equal(t, ts(exp.ts), values[i].Value.Timestamp, "value %d timestamp", i)
		require.Equal(t, roachpb.Key(exp.key), values[i].Key, "value %d key", i)
	}
}

func TestMaybeFlushToDisk(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// EncodedSize is len(key)+1+12 for the sentinel and timestamp, so each of
	// these keys ("k1", "k2", "k3") is 15 bytes and each value is 2, so 17 bytes
	// per KV. We set the threshold to 51 so the buffer flushes after the third Add.
	cfg, cleanup := makeOrderedBufferConfig(t, 51)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	require.NoError(t, buf.Add(ctx, makeKV("k1", 1, "v1")))
	require.Equal(t, 1, buf.Len())
	require.NoError(t, buf.Add(ctx, makeKV("k2", 2, "v2")))
	require.Equal(t, 2, buf.Len())
	require.NoError(t, buf.Add(ctx, makeKV("k3", 3, "v3")))
	require.Equal(t, 0, buf.Len())

	values := getAllKVValuesFromDisk(t, buf, ctx, ts(100))
	require.Len(t, values, 3)
}

// TestOrderedBufferByteSizeFlush verifies that MaybeFlushToDisk flushes once
// the buffer's key+value byte count reaches the configured threshold.
func TestOrderedBufferByteSizeFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	cfg, cleanup := makeOrderedBufferConfig(t, 55)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	// We count key EncodedSize (len+1+12) plus value; k1+ab is 17, k2+abc is 18,
	// k3+abcde is 20. Total 55 triggers flush.
	require.NoError(t, buf.Add(ctx, makeKV("k1", 1, "ab")))
	require.NoError(t, buf.Add(ctx, makeKV("k2", 2, "abc")))
	require.Equal(t, 2, buf.Len())
	require.NoError(t, buf.Add(ctx, makeKV("k3", 3, "abcde")))
	require.Equal(t, 0, buf.Len())

	values := getAllKVValuesFromDisk(t, buf, ctx, ts(100))
	require.Len(t, values, 3)
	totalBytes := 0
	for _, v := range values {
		totalBytes += len(v.Key) + 1 + 12 + len(v.Value.RawBytes)
	}
	require.Equal(t, 17+18+20, totalBytes)
}

// TestGetKVsFromDiskByteSizeBound verifies that GetKVsFromDisk
// returns batches bounded by flushByteSizeThreshold (key+value bytes, same as flush).
func TestGetMVCCValuesFromDiskByteSizeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// On disk we store the key as encodeTimestampKey (12 bytes fixed plus key bytes) then the value.
	// So "key1" (4 key bytes) plus 5-byte value is 12+4+5 = 21 bytes per entry. With threshold 22,
	// we return 2 entries (21+21 = 42 bytes).
	const threshold = 22
	cfg, cleanup := makeOrderedBufferConfig(t, threshold)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	for i := 1; i <= 5; i++ {
		val := fmt.Sprintf("%05d", i)
		require.NoError(t, buf.Add(ctx, makeKV(fmt.Sprintf("key%d", i), int64(i*10), val)))
	}
	require.NoError(t, buf.FlushToDisk(ctx, ts(100)))

	// Each entry is 21 bytes; with threshold 22 we get 2 entries per batch, then 1 for the last.
	expectedBatchSizes := []int{2, 2, 1}
	var allValues []*kvpb.RangeFeedValue
	for iter, expectedLen := range expectedBatchSizes {
		values, iterExhausted, err := buf.GetKVsFromDisk(ctx, ts(100))
		require.NoError(t, err)
		require.Len(t, values, expectedLen, "iteration %d", iter)
		allValues = append(allValues, values...)
		if iter < len(expectedBatchSizes)-1 {
			require.False(t, iterExhausted, "iteration %d: expected more data", iter)
		} else {
			require.True(t, iterExhausted, "expected iterator exhausted after last batch")
		}
	}
	require.True(t, isMVCCSorted(allValues), "values across all iterations not sorted")
}

func TestOrderedBufferClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	cfg, cleanup := makeOrderedBufferConfig(t, 1<<20)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	require.NoError(t, buf.Add(ctx, makeKV("k1", 1, "v1")))
	require.NoError(t, buf.FlushToDisk(ctx, ts(10)))
	require.NoError(t, buf.Close(ctx))
	// Second close is safe.
	require.NoError(t, buf.Close(ctx))
}

func TestOrderedBufferRandomizedSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()

	const numKVs = 100
	const numCheckpoints = 8

	streamKVs, _ := replicationtestutils.GenerateRandomKVSequence(rng, numKVs,
		replicationtestutils.StreamSeqOptions{MaxTxnSize: 5, KeyPrefix: roachpb.Key("/a/")})
	resolvedWalls := replicationtestutils.GenerateResolvedTimestamps(rng, streamKVs, numCheckpoints)
	resolvedTs := make([]hlc.Timestamp, len(resolvedWalls))
	for i, w := range resolvedWalls {
		resolvedTs[i] = hlc.Timestamp{WallTime: w}
	}

	// Shuffle so we add in random order; we'll sort for expected order.
	rng.Shuffle(len(streamKVs), func(i, j int) { streamKVs[i], streamKVs[j] = streamKVs[j], streamKVs[i] })

	cfg, cleanup := makeOrderedBufferConfig(t, 1<<20)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	for _, skv := range streamKVs {
		val := skv.Value
		if len(val) == 0 {
			val = []byte(fmt.Sprintf("val%d", rng.Intn(1000)))
		}
		require.NoError(t, buf.Add(ctx, &kvpb.RangeFeedValue{
			Key: skv.Key,
			Value: roachpb.Value{
				RawBytes:  val,
				Timestamp: hlc.Timestamp{WallTime: skv.WallTime},
			},
		}))
	}

	// Flush all to disk.
	require.NoError(t, buf.FlushToDisk(ctx, hlc.MaxTimestamp))

	// For each checkpoint, read KVs with ts <= resolvedTs and assert order + bound.
	// GetKVsFromDisk consumes returned keys, so each call gets the next batch.
	for _, rts := range resolvedTs {
		values := getAllKVValuesFromDisk(t, buf, ctx, rts)
		for i := 0; i < len(values); i++ {
			require.False(t, values[i].Value.Timestamp.Compare(rts) > 0,
				"value ts %s > resolvedTs %s", values[i].Value.Timestamp, rts)
		}
		require.True(t, isMVCCSorted(values),
			"values not in (ts, key) order for resolvedTs %s", rts)
	}
}

// TestOrderedBufferRandomizedSequenceWithCheckpoints runs a randomized test that
// interleaves Add and FlushToDisk(resolvedTs) to simulate the
// ordered stream handler behavior. GetKVsFromDisk consumes returned keys.
// Uses shared stream test helpers (same pattern as txnfeed's generateMergeFeedInputs + addCheckpoints).
func TestOrderedBufferRandomizedSequenceWithCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	const numKVs = 40
	const numCheckpoints = 5

	streamKVs, _ := replicationtestutils.GenerateRandomKVSequence(rng, numKVs,
		replicationtestutils.StreamSeqOptions{MaxTxnSize: 4, KeyPrefix: roachpb.Key("/a/")})
	checkpointWalls := replicationtestutils.GenerateResolvedTimestamps(rng, streamKVs, numCheckpoints)
	checkpoints := make([]hlc.Timestamp, len(checkpointWalls))
	for i, w := range checkpointWalls {
		checkpoints[i] = hlc.Timestamp{WallTime: w}
	}

	// Build allKVs as *kvpb.RangeFeedValue for buffer and expected comparison.
	allKVs := make([]*kvpb.RangeFeedValue, len(streamKVs))
	for i, skv := range streamKVs {
		val := skv.Value
		if len(val) == 0 {
			val = []byte("v")
		}
		allKVs[i] = &kvpb.RangeFeedValue{
			Key: skv.Key,
			Value: roachpb.Value{
				RawBytes:  val,
				Timestamp: hlc.Timestamp{WallTime: skv.WallTime},
			},
		}
	}

	cfg, cleanup := makeOrderedBufferConfig(t, 1<<20)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	// Add KVs in random order.
	shuffled := make([]*kvpb.RangeFeedValue, len(allKVs))
	copy(shuffled, allKVs)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	for _, v := range shuffled {
		require.NoError(t, buf.Add(ctx, v))
	}

	// Simulate handler: at each checkpoint, FlushToDisk, then get all up to resolvedTs.
	var delivered [][]*kvpb.RangeFeedValue
	for _, rts := range checkpoints {
		require.NoError(t, buf.FlushToDisk(ctx, rts))
		values := getAllKVValuesFromDisk(t, buf, ctx, rts)
		delivered = append(delivered, values)
	}

	// Build expected: for each checkpoint, all KVs with ts <= rts in (ts, key) order.
	sortedAll := make([]*kvpb.RangeFeedValue, len(allKVs))
	copy(sortedAll, allKVs)
	slices.SortFunc(sortedAll, func(a, b *kvpb.RangeFeedValue) int {
		cmp := a.Value.Timestamp.Compare(b.Value.Timestamp)
		if cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.Key, b.Key)
	})

	// Each checkpoint delivers KVs in (prevRts, rts]; earlier batches were already consumed.
	for cpIdx, rts := range checkpoints {
		prevRts := hlc.Timestamp{}
		if cpIdx > 0 {
			prevRts = checkpoints[cpIdx-1]
		}
		expected := filterKVsInTimestampRange(sortedAll, prevRts, rts)
		got := delivered[cpIdx]
		require.Len(t, got, len(expected), "checkpoint %d (rts=%s)", cpIdx, rts)
		for i := range expected {
			e, g := expected[i], got[i]
			require.True(t, e.Value.Timestamp.Equal(g.Value.Timestamp),
				"checkpoint %d event %d: timestamp expected %s got %s", cpIdx, i, e.Value.Timestamp, g.Value.Timestamp)
			require.Equal(t, e.Key, g.Key, "checkpoint %d event %d: key", cpIdx, i)
			require.Equal(t, e.Value.RawBytes, g.Value.RawBytes, "checkpoint %d event %d: value", cpIdx, i)
		}
	}
}
