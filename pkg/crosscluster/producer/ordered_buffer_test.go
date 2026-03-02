// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// makeOrderedBufferConfigTB creates a config with in-memory temp storage for
// tests or benchmarks.
func makeOrderedBufferConfigTB(
	tb testing.TB, flushByteSizeThreshold int64,
) (OrderedBufferConfig, func()) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(ctx, base.DefaultTestTempStorageConfig(st), nil)
	if err != nil {
		tb.Fatal(err)
	}
	cfg := OrderedBufferConfig{
		settings:               st,
		streamID:               streampb.StreamID(1),
		tempStorage:            tempEngine,
		flushByteSizeThreshold: flushByteSizeThreshold,
	}
	return cfg, func() { tempEngine.Close() }
}

// makeOrderedBufferConfig creates a config with in-memory temp storage for tests.
func makeOrderedBufferConfig(
	t *testing.T, flushByteSizeThreshold int64,
) (OrderedBufferConfig, func()) {
	return makeOrderedBufferConfigTB(t, flushByteSizeThreshold)
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

func makeDelRange(startKey, endKey string, wall int64) *kvpb.RangeFeedDeleteRange {
	return &kvpb.RangeFeedDeleteRange{
		Span:      roachpb.Span{Key: roachpb.Key(startKey), EndKey: roachpb.Key(endKey)},
		Timestamp: hlc.Timestamp{WallTime: wall},
	}
}

func ts(wall int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: wall}
}

// getAllEventsFromDisk calls GetEventsFromDisk in a loop until iterExhausted and
// returns all events (KVs and DelRanges) in (ts, key) order.
func getAllEventsFromDisk(
	t *testing.T, b *OrderedBuffer, ctx context.Context, resolvedTs hlc.Timestamp,
) []kvpb.RangeFeedEvent {
	var out []kvpb.RangeFeedEvent
	for {
		events, iterExhausted, err := b.GetEventsFromDisk(ctx, resolvedTs)
		require.NoError(t, err)
		out = append(out, events...)
		if iterExhausted {
			break
		}
	}
	return out
}

// eventTsAndKey returns (timestamp, key) for the event (Val or DeleteRange).
func eventTsAndKey(e kvpb.RangeFeedEvent) (hlc.Timestamp, roachpb.Key) {
	if e.Val != nil {
		return e.Val.Value.Timestamp, e.Val.Key
	}
	return e.DeleteRange.Timestamp, e.DeleteRange.Span.Key
}

// isEventsSorted returns whether events are in (ts asc, key asc) order.
func isEventsSorted(events []kvpb.RangeFeedEvent) bool {
	for i := 0; i < len(events)-1; i++ {
		tsA, keyA := eventTsAndKey(events[i])
		tsB, keyB := eventTsAndKey(events[i+1])
		cmp := tsA.Compare(tsB)
		if cmp > 0 || (cmp == 0 && bytes.Compare(keyA, keyB) > 0) {
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

// filterDelRangesInTimestampRange returns DelRanges with after < ts <= atOrBefore
// (slice must already be sorted by (ts, key)).
func filterDelRangesInTimestampRange(
	dels []*kvpb.RangeFeedDeleteRange, after, atOrBefore hlc.Timestamp,
) []*kvpb.RangeFeedDeleteRange {
	var out []*kvpb.RangeFeedDeleteRange
	for _, d := range dels {
		if d.Timestamp.Compare(after) > 0 && d.Timestamp.Compare(atOrBefore) <= 0 {
			out = append(out, d)
		}
	}
	return out
}

// eventsInTimestampRange merges KVs and DelRanges (both sorted by (ts, key)),
// filters to after < ts <= atOrBefore, and returns []RangeFeedEvent in (ts, key) order.
func eventsInTimestampRange(
	sortedKVs []*kvpb.RangeFeedValue,
	sortedDelRanges []*kvpb.RangeFeedDeleteRange,
	after, atOrBefore hlc.Timestamp,
) []kvpb.RangeFeedEvent {
	kvs := filterKVsInTimestampRange(sortedKVs, after, atOrBefore)
	dels := filterDelRangesInTimestampRange(sortedDelRanges, after, atOrBefore)
	var out []kvpb.RangeFeedEvent
	for i, j := 0, 0; i < len(kvs) || j < len(dels); {
		if i >= len(kvs) {
			out = append(out, kvpb.RangeFeedEvent{DeleteRange: dels[j]})
			j++
			continue
		}
		if j >= len(dels) {
			out = append(out, kvpb.RangeFeedEvent{Val: kvs[i]})
			i++
			continue
		}
		kvTs, kvKey := kvs[i].Value.Timestamp, kvs[i].Key
		drTs, drKey := dels[j].Timestamp, dels[j].Span.Key
		cmpTs := kvTs.Compare(drTs)
		cmpKey := bytes.Compare(kvKey, drKey)
		if cmpTs < 0 || (cmpTs == 0 && cmpKey < 0) {
			out = append(out, kvpb.RangeFeedEvent{Val: kvs[i]})
			i++
		} else {
			out = append(out, kvpb.RangeFeedEvent{DeleteRange: dels[j]})
			j++
		}
	}
	return out
}

// decodeTimestampKeyWithKey is a test helper that decodes both timestamp and key.
func decodeTimestampKeyWithKey(encoded []byte) (hlc.Timestamp, roachpb.Key, error) {
	if len(encoded) < 12 {
		return hlc.Timestamp{}, nil, errors.Newf("key too short: got %d bytes", len(encoded))
	}
	if len(encoded) == 12 {
		return hlc.Timestamp{}, nil, errors.Newf("encoded key missing rangefeed key suffix (timestamp only)")
	}
	ts := hlc.Timestamp{
		WallTime: int64(binary.BigEndian.Uint64(encoded[0:8])),
		Logical:  int32(binary.BigEndian.Uint32(encoded[8:12])),
	}
	return ts, encoded[12:], nil
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
		decTs, decKey, err := decodeTimestampKeyWithKey(enc)
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
		decTs, decKey, err := decodeTimestampKeyWithKey(enc)
		require.NoError(t, err)
		require.True(t, pairs[i].ts.Equal(decTs))
		require.True(t, pairs[i].key.Equal(decKey))
	}
}

func TestDecodeTimestampKeyTooShort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, err := decodeTimestampKey([]byte("short"))
	require.Error(t, err)
}

func TestDecodeTimestampKeyEmptySuffix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Timestamp prefix only — invalid because disk keys would collide per timestamp.
	enc := encodeTimestampKey(ts(1), nil)
	require.Len(t, enc, 12)
	_, err := decodeTimestampKey(enc)
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
	require.NoError(t, buf.AddDelRange(ctx, makeDelRange("a", "b", 3)))
	require.Equal(t, 3, buf.Len())

	// No flush yet (byte threshold not reached), so disk map not created.
	events, _, err := buf.GetEventsFromDisk(ctx, ts(10))
	require.NoError(t, err)
	require.Empty(t, events)
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
	require.NoError(t, buf.AddDelRange(ctx, makeDelRange("x", "z", 15)))

	// resolvedTs < minTsInBuffer, so no flush.
	err := buf.FlushToDisk(ctx, ts(5))
	require.NoError(t, err)
	require.Equal(t, 3, buf.Len(), "buffer should be unchanged")

	// Nothing on disk.
	events, _, err := buf.GetEventsFromDisk(ctx, ts(5))
	require.NoError(t, err)
	require.Empty(t, events)
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
	require.NoError(t, buf.AddDelRange(ctx, makeDelRange("k0", "k1", 5)))
	require.NoError(t, buf.Add(ctx, makeKV("k1", 20, "v2")))
	require.NoError(t, buf.Add(ctx, makeKV("k1", 30, "v3")))

	err := buf.FlushToDisk(ctx, ts(20))
	require.NoError(t, err)
	require.Equal(t, 0, buf.Len())

	// Events should be in (ts asc, key asc) order; DelRange at (5, k0) then KVs.
	events := getAllEventsFromDisk(t, buf, ctx, ts(20))
	require.Len(t, events, 4)
	require.True(t, isEventsSorted(events), "events not in (ts, key) order")

	// Verify DelRange (first event).
	require.NotNil(t, events[0].DeleteRange, "event 0")
	require.Equal(t, ts(5), events[0].DeleteRange.Timestamp)
	require.Equal(t, roachpb.Key("k0"), events[0].DeleteRange.Span.Key)
	require.Equal(t, roachpb.Key("k1"), events[0].DeleteRange.Span.EndKey)
	expectedKVs := []struct {
		ts  int64
		key string
	}{
		{10, "k1"},
		{20, "k1"},
		{20, "k2"},
	}
	// Verify KVs (rest of events).
	for i := 1; i < len(events); i++ {
		exp := expectedKVs[i-1]
		require.NotNil(t, events[i].Val, "event %d", i)
		require.Equal(t, ts(exp.ts), events[i].Val.Value.Timestamp, "event %d timestamp", i)
		require.Equal(t, roachpb.Key(exp.key), events[i].Val.Key, "event %d key", i)
	}
}

// TestOrderedBufferByteSizeFlush verifies that MaybeFlushToDisk flushes once
// the buffer's key+value byte count reaches the configured threshold.
func TestOrderedBufferByteSizeFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Calculate the actual byte size of the 4 events we'll add.
	testEvents := []kvpb.RangeFeedEvent{
		{Val: makeKV("k1", 1, "ab")},
		{Val: makeKV("k2", 2, "abc")},
		{Val: makeKV("k3", 3, "abcde")},
		{DeleteRange: makeDelRange("x", "y", 4)},
	}
	totalBytes := 0
	for _, e := range testEvents {
		totalBytes += e.Size()
	}

	cfg, cleanup := makeOrderedBufferConfig(t, int64(totalBytes))
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	require.NoError(t, buf.Add(ctx, testEvents[0].Val))
	require.NoError(t, buf.Add(ctx, testEvents[1].Val))
	require.Equal(t, 2, buf.Len())
	require.NoError(t, buf.Add(ctx, testEvents[2].Val))
	require.NoError(t, buf.AddDelRange(ctx, testEvents[3].DeleteRange))
	require.Equal(t, 0, buf.Len())

	events := getAllEventsFromDisk(t, buf, ctx, ts(100))
	require.Len(t, events, 4)
	require.True(t, isEventsSorted(events))
}

// TestGetKVsFromDiskByteSizeBound verifies that GetKVsFromDisk
// returns batches bounded by flushByteSizeThreshold (key+value bytes, same as flush).
func TestGetMVCCValuesFromDiskByteSizeBound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Build test events and calculate their actual byte sizes.
	var testEvents []kvpb.RangeFeedEvent
	for i := 1; i <= 5; i++ {
		val := fmt.Sprintf("%05d", i)
		testEvents = append(testEvents, kvpb.RangeFeedEvent{
			Val: makeKV(fmt.Sprintf("key%d", i), int64(i*10), val),
		})
	}

	// Calculate size of first event and set threshold to return 2 events per batch.
	eventSize := testEvents[0].Size()
	threshold := int64(eventSize*2 - 1)

	cfg, cleanup := makeOrderedBufferConfig(t, threshold)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	for i := 0; i < 5; i++ {
		require.NoError(t, buf.Add(ctx, testEvents[i].Val))
	}
	require.NoError(t, buf.FlushToDisk(ctx, ts(100)))

	// With threshold set to 2*eventSize-1, we should get batches of 2, 2, 1.
	expectedBatchSizes := []int{2, 2, 1}
	var allEvents []kvpb.RangeFeedEvent
	for iter, expectedLen := range expectedBatchSizes {
		events, iterExhausted, err := buf.GetEventsFromDisk(ctx, ts(100))
		require.NoError(t, err)
		require.Len(t, events, expectedLen, "iteration %d", iter)
		allEvents = append(allEvents, events...)
		if iter < len(expectedBatchSizes)-1 {
			require.False(t, iterExhausted, "iteration %d: expected more data", iter)
		}
	}
	// Iterator only knows it's exhausted after trying to read past the last event.
	events, iterExhausted, err := buf.GetEventsFromDisk(ctx, ts(100))
	require.NoError(t, err)
	require.Len(t, events, 0, "expected no more events")
	require.True(t, iterExhausted, "expected iterator exhausted")

	require.True(t, isEventsSorted(allEvents), "events across all iterations not sorted")
	require.Len(t, allEvents, 5, "expected 5 KVs")
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
	const numDelRanges = 15
	const numCheckpoints = 8

	streamKVs, _ := replicationtestutils.GenerateRandomKVSequence(rng, numKVs,
		replicationtestutils.StreamSeqOptions{MaxTxnSize: 5, KeyPrefix: roachpb.Key("/a/")})
	resolvedWalls := replicationtestutils.GenerateResolvedTimestamps(rng, streamKVs, numCheckpoints)
	resolvedTs := make([]hlc.Timestamp, len(resolvedWalls))
	for i, w := range resolvedWalls {
		resolvedTs[i] = hlc.Timestamp{WallTime: w}
	}

	// Build DelRanges with keys and timestamps in the same general range as streamKVs.
	var maxWall int64
	for _, skv := range streamKVs {
		if skv.WallTime > maxWall {
			maxWall = skv.WallTime
		}
	}
	allDelRanges := make([]*kvpb.RangeFeedDeleteRange, numDelRanges)
	for i := 0; i < numDelRanges; i++ {
		wall := rng.Int63n(maxWall + 1)
		// Use /a/ prefix to sort with KVs.
		start := roachpb.Key(fmt.Sprintf("/a/del%d", rng.Intn(100)))
		end := start.PrefixEnd()
		allDelRanges[i] = &kvpb.RangeFeedDeleteRange{
			Span:      roachpb.Span{Key: start, EndKey: end},
			Timestamp: hlc.Timestamp{WallTime: wall},
		}
	}

	// Shuffle KVs and DelRanges together so we add in random order.
	type addItem struct {
		kv  *kvpb.RangeFeedValue
		del *kvpb.RangeFeedDeleteRange
	}
	items := make([]addItem, 0, numKVs+numDelRanges)
	for _, skv := range streamKVs {
		val := skv.Value
		if len(val) == 0 {
			val = []byte(fmt.Sprintf("val%d", rng.Intn(1000)))
		}
		items = append(items, addItem{kv: &kvpb.RangeFeedValue{
			Key:   skv.Key,
			Value: roachpb.Value{RawBytes: val, Timestamp: hlc.Timestamp{WallTime: skv.WallTime}},
		}})
	}
	for _, d := range allDelRanges {
		items = append(items, addItem{del: d})
	}
	rng.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })

	cfg, cleanup := makeOrderedBufferConfig(t, 1<<20)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	for _, it := range items {
		if it.kv != nil {
			require.NoError(t, buf.Add(ctx, it.kv))
		} else {
			require.NoError(t, buf.AddDelRange(ctx, it.del))
		}
	}

	// Flush all to disk.
	require.NoError(t, buf.FlushToDisk(ctx, hlc.MaxTimestamp))

	// For each checkpoint, read events with ts <= resolvedTs and assert order + bound.
	for _, rts := range resolvedTs {
		events := getAllEventsFromDisk(t, buf, ctx, rts)
		for _, e := range events {
			ts, _ := eventTsAndKey(e)
			require.False(t, ts.Compare(rts) > 0,
				"event ts %s > resolvedTs %s", ts, rts)
		}
		require.True(t, isEventsSorted(events),
			"events not in (ts, key) order for resolvedTs %s", rts)
	}
}

// TestOrderedBufferRandomizedSequenceWithCheckpoints runs a randomized test that
// interleaves Add and FlushToDisk(resolvedTs) to simulate the
// ordered stream handler behavior. GetKVsFromDisk consumes returned keys.
func TestOrderedBufferRandomizedSequenceWithCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	const numKVs = 40
	const numDelRanges = 8
	const numCheckpoints = 5

	streamKVs, _ := replicationtestutils.GenerateRandomKVSequence(rng, numKVs,
		replicationtestutils.StreamSeqOptions{MaxTxnSize: 4, KeyPrefix: roachpb.Key("/a/")})
	checkpointWalls := replicationtestutils.GenerateResolvedTimestamps(rng, streamKVs, numCheckpoints)
	checkpoints := make([]hlc.Timestamp, len(checkpointWalls))
	for i, w := range checkpointWalls {
		checkpoints[i] = hlc.Timestamp{WallTime: w}
	}

	// Build allKVs and allDelRanges for buffer and expected comparison.
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
	var maxWall int64
	for _, skv := range streamKVs {
		if skv.WallTime > maxWall {
			maxWall = skv.WallTime
		}
	}
	allDelRanges := make([]*kvpb.RangeFeedDeleteRange, numDelRanges)
	for i := 0; i < numDelRanges; i++ {
		wall := rng.Int63n(maxWall + 1)
		start := roachpb.Key(fmt.Sprintf("/a/dr%d", rng.Intn(50)))
		end := start.PrefixEnd()
		allDelRanges[i] = &kvpb.RangeFeedDeleteRange{
			Span:      roachpb.Span{Key: start, EndKey: end},
			Timestamp: hlc.Timestamp{WallTime: wall},
		}
	}

	// Interleave KVs and DelRanges in random order.
	type addItem struct {
		kv  *kvpb.RangeFeedValue
		del *kvpb.RangeFeedDeleteRange
	}
	items := make([]addItem, 0, len(allKVs)+len(allDelRanges))
	for _, v := range allKVs {
		items = append(items, addItem{kv: v})
	}
	for _, d := range allDelRanges {
		items = append(items, addItem{del: d})
	}
	rng.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })

	cfg, cleanup := makeOrderedBufferConfig(t, 1<<20)
	defer cleanup()

	buf := newOrderedBuffer(cfg)
	for _, it := range items {
		if it.kv != nil {
			require.NoError(t, buf.Add(ctx, it.kv))
		} else {
			require.NoError(t, buf.AddDelRange(ctx, it.del))
		}
	}

	// Simulate handler: at each checkpoint, FlushToDisk, then get all up to resolvedTs.
	var delivered [][]kvpb.RangeFeedEvent
	for _, rts := range checkpoints {
		require.NoError(t, buf.FlushToDisk(ctx, rts))
		delivered = append(delivered, getAllEventsFromDisk(t, buf, ctx, rts))
	}

	// Build expected: for each checkpoint, all events (KVs + DelRanges) with ts in (prevRts, rts] in (ts, key) order.
	sortedKVs := make([]*kvpb.RangeFeedValue, len(allKVs))
	copy(sortedKVs, allKVs)
	slices.SortFunc(sortedKVs, func(a, b *kvpb.RangeFeedValue) int {
		cmp := a.Value.Timestamp.Compare(b.Value.Timestamp)
		if cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.Key, b.Key)
	})
	sortedDelRanges := make([]*kvpb.RangeFeedDeleteRange, len(allDelRanges))
	copy(sortedDelRanges, allDelRanges)
	slices.SortFunc(sortedDelRanges, func(a, b *kvpb.RangeFeedDeleteRange) int {
		cmp := a.Timestamp.Compare(b.Timestamp)
		if cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.Span.Key, b.Span.Key)
	})

	for cpIdx, rts := range checkpoints {
		prevRts := hlc.Timestamp{}
		if cpIdx > 0 {
			prevRts = checkpoints[cpIdx-1]
		}
		expected := eventsInTimestampRange(sortedKVs, sortedDelRanges, prevRts, rts)
		got := delivered[cpIdx]
		require.Len(t, got, len(expected), "checkpoint %d (rts=%s)", cpIdx, rts)
		for i := range expected {
			ex := expected[i]
			g := got[i]
			if ex.Val != nil {
				require.NotNil(t, g.Val, "checkpoint %d event %d: expected KV", cpIdx, i)
				require.True(t, ex.Val.Value.Timestamp.Equal(g.Val.Value.Timestamp),
					"checkpoint %d event %d: timestamp", cpIdx, i)
				require.Equal(t, ex.Val.Key, g.Val.Key, "checkpoint %d event %d: key", cpIdx, i)
				require.Equal(t, ex.Val.Value.RawBytes, g.Val.Value.RawBytes, "checkpoint %d event %d: value", cpIdx, i)
			} else {
				require.NotNil(t, g.DeleteRange, "checkpoint %d event %d: expected DelRange", cpIdx, i)
				require.True(t, ex.DeleteRange.Timestamp.Equal(g.DeleteRange.Timestamp),
					"checkpoint %d event %d: DelRange timestamp", cpIdx, i)
				require.Equal(t, ex.DeleteRange.Span.Key, g.DeleteRange.Span.Key, "checkpoint %d event %d: DelRange key", cpIdx, i)
				require.Equal(t, ex.DeleteRange.Span.EndKey, g.DeleteRange.Span.EndKey, "checkpoint %d event %d: DelRange endKey", cpIdx, i)
			}
		}
	}
}
