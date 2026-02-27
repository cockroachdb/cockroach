// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func makeKVEntry(ts int, key string) *mergeEntry {
	return &mergeEntry{
		peekedKVs: []streampb.StreamEvent_KV{
			{
				KeyValue: roachpb.KeyValue{
					Key: roachpb.Key(key),
					Value: roachpb.Value{
						Timestamp: hlc.Timestamp{WallTime: int64(ts)},
					},
				},
			},
		},
		peekTS: hlc.Timestamp{WallTime: int64(ts)},
	}
}

func makeCheckpointEntry(ts int) *mergeEntry {
	return &mergeEntry{
		peekedCheckpoint: &streampb.StreamEvent_StreamCheckpoint{
			ResolvedSpans: []jobspb.ResolvedSpan{
				{Timestamp: hlc.Timestamp{WallTime: int64(ts)}},
			},
		},
		peekTS: hlc.Timestamp{WallTime: int64(ts)},
	}
}

// TestMergeHeapKVBeforeCheckpoint verifies that the heap orders KVs before
// checkpoints at the same timestamp, ensuring data is always emitted before the
// checkpoint that resolves it.
func TestMergeHeapKVBeforeCheckpoint(t *testing.T) {
	var h mergeHeap

	// Insert entries in an order that would break without the tie-breaking
	// rule: checkpoints and KVs interleaved at multiple timestamps.
	h.push(makeCheckpointEntry(300))
	h.push(makeKVEntry(200, "b"))
	h.push(makeCheckpointEntry(100))
	h.push(makeKVEntry(100, "a"))
	h.push(makeKVEntry(300, "c"))
	h.init()

	type result struct {
		ts   int
		isKV bool
	}
	var results []result
	for h.len() > 0 {
		e := h.pop()
		results = append(results, result{
			ts:   int(e.peekTS.WallTime),
			isKV: e.isKV(),
		})
	}

	expected := []result{
		{ts: 100, isKV: true},
		{ts: 100, isKV: false},
		{ts: 200, isKV: true},
		{ts: 300, isKV: true},
		{ts: 300, isKV: false},
	}
	require.Equal(t, expected, results)
}

// TestMergeHeapIsResolved verifies that isResolved correctly reports whether
// all KVs at a given timestamp have been consumed from the heap.
func TestMergeHeapIsResolved(t *testing.T) {
	var h mergeHeap

	// Empty heap: always resolved.
	require.True(t, h.isResolved(hlc.Timestamp{WallTime: 100}))

	// Heap with a KV at ts=100: ts=100 is not resolved, ts=50 is.
	h.push(makeKVEntry(100, "a"))
	h.init()
	require.False(t, h.isResolved(hlc.Timestamp{WallTime: 100}),
		"KV at same timestamp means not yet resolved")
	require.True(t, h.isResolved(hlc.Timestamp{WallTime: 50}),
		"earlier timestamp is resolved when next KV is later")

	// Replace with a checkpoint at ts=100: ts=100 is resolved.
	h.pop()
	h.push(makeCheckpointEntry(100))
	h.init()
	require.True(t, h.isResolved(hlc.Timestamp{WallTime: 100}),
		"checkpoint at same timestamp means resolved")
	require.False(t, h.isResolved(hlc.Timestamp{WallTime: 200}),
		"timestamp beyond checkpoint is not resolved")
}

// TestMergeEntryAdvanceKVWithinBatch verifies that advanceKV correctly reslices
// to the next KV, updates peekTS, and does not report exhaustion when KVs
// remain in the batch.
func TestMergeEntryAdvanceKVWithinBatch(t *testing.T) {
	entry := &mergeEntry{
		peekedKVs: []streampb.StreamEvent_KV{
			{
				KeyValue: roachpb.KeyValue{
					Key: roachpb.Key("a"),
					Value: roachpb.Value{
						Timestamp: hlc.Timestamp{WallTime: 100},
					},
				},
			},
			{
				KeyValue: roachpb.KeyValue{
					Key: roachpb.Key("b"),
					Value: roachpb.Value{
						Timestamp: hlc.Timestamp{WallTime: 200},
					},
				},
			},
		},
		peekTS: hlc.Timestamp{WallTime: 100},
	}

	require.Equal(t, roachpb.Key("a"), entry.currentKV().KeyValue.Key)

	exhausted, err := entry.advanceKV(t.Context())
	require.NoError(t, err)
	require.False(t, exhausted)
	require.Equal(t, roachpb.Key("b"), entry.currentKV().KeyValue.Key)
	require.Equal(t, hlc.Timestamp{WallTime: 200}, entry.peekTS)
}
