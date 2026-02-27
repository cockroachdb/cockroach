// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"math/rand"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

// randKeyWithPrefix returns a random key within [key, key.EndKey()). It
// appends a 16-byte random suffix (UUID-sized) to make collisions negligible.
func randKeyWithPrefix(rng *rand.Rand, key roachpb.Key) string {
	suffix := make([]byte, 16)
	for i := range suffix {
		suffix[i] = byte(rng.Intn(256))
	}
	return string(key) + string(suffix)
}

// addCheckpoints inserts random checkpoint events into a list of kvEvents.
// Checkpoints are inserted randomly within the list, but one is always
// inserted at the end.
func addCheckpoints(
	rng *rand.Rand, density float64, kvs []kvEvent, coveringSpan roachpb.Span,
) []any {
	start := string(coveringSpan.Key)
	end := string(coveringSpan.EndKey)

	if len(kvs) == 0 {
		return []any{
			checkpoint{start: start, end: end, time: 1},
			closeEvent{},
		}
	}

	maxTS := kvs[0].time
	for _, kv := range kvs[1:] {
		if kv.time > maxTS {
			maxTS = kv.time
		}
	}

	// Iterate backwards through the list to compute the minimum timestamp of any
	// KV after each position. Since checkpoint timestamps are inclusive bounds, a
	// checkpoint inserted after position i must be strictly less than the minimum
	// future KV timestamp (otherwise it would claim to resolve a KV not yet
	// emitted).
	maxCheckpointAt := make([]int, len(kvs))
	minSoFar := maxTS
	for i := len(kvs) - 1; i >= 0; i-- {
		maxCheckpointAt[i] = minSoFar
		if kvs[i].time < minSoFar {
			minSoFar = kvs[i].time
		}
	}

	var events []any
	lastCheckpoint := 0
	for i, kv := range kvs {
		events = append(events, kv)

		if maxCheckpointAt[i] == lastCheckpoint {
			continue
		}

		if density < rng.Float64() {
			continue
		}

		lastCheckpoint = rng.Intn(maxCheckpointAt[i]-lastCheckpoint) + lastCheckpoint
		events = append(events, checkpoint{
			start: start, end: end, time: lastCheckpoint,
		})
	}

	// Always append a final checkpoint at the max KV timestamp. Since
	// checkpoints are inclusive, this resolves all KVs.
	events = append(events, checkpoint{
		start: start, end: end, time: maxTS,
	})
	events = append(events, closeEvent{})
	return events
}

// mergeFeedInputOptions configures how generateMergeFeedInputs builds test
// subscriptions. Zero values for numSubs, density, and maxTxnSize are replaced
// with random defaults.
type mergeFeedInputOptions struct {
	// numSubs is the number of subscriptions to partition events across. If
	// zero, a random value in [1, 10] is used.
	numSubs int
	// density controls how frequently checkpoints are inserted between KV
	// events. If zero, a random value in (0, 1] is used.
	density float64
	// maxTxnSize is the upper bound (exclusive) on the number of KVs sharing a
	// single timestamp (simulating a transaction). If zero, defaults to 3.
	maxTxnSize int
}

// generateMergeFeedInputs creates randomized MergeFeed inputs and the expected
// output order.
func generateMergeFeedInputs(
	t testing.TB, rng *rand.Rand, numKVs int, opts mergeFeedInputOptions,
) (subs []streamclient.Subscription, coveringSpan roachpb.Span, expected []kvEvent) {
	t.Helper()

	if opts.numSubs == 0 {
		opts.numSubs = 1 + rng.Intn(10)
	}
	if opts.density == 0 {
		opts.density = rng.Float64()
	}
	if opts.maxTxnSize == 0 {
		opts.maxTxnSize = 3
	}

	prefix := roachpb.Key("/a/")
	coveringSpan = roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}

	// Generate KVs in groups sharing the same timestamp (simulating
	// transactions). Group sizes are random up to maxTxnSize, capped by the
	// remaining KV budget. The 16-byte random suffix in randKeyWithPrefix
	// makes key collisions negligible.
	var allKVs []kvEvent
	ts := 1
	for len(allKVs) < numKVs {
		kvsLeft := numKVs - len(allKVs)
		maxGroup := min(kvsLeft, opts.maxTxnSize)
		groupSize := 1 + rng.Intn(maxGroup)
		for range groupSize {
			allKVs = append(allKVs, kvEvent{
				key:  randKeyWithPrefix(rng, prefix),
				time: ts,
			})
		}
		ts++
	}
	expected = slices.Clone(allKVs)
	// Sort expected by (time, key) to establish a canonical order for
	// comparison, since intra-timestamp ordering across subscriptions is
	// non-deterministic.
	slices.SortFunc(expected, kvEvent.Compare)

	shuffled := slices.Clone(allKVs)
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	subKVs := make([][]kvEvent, opts.numSubs)
	for _, kv := range shuffled {
		idx := rng.Intn(opts.numSubs)
		subKVs[idx] = append(subKVs[idx], kv)
	}

	// Build each subscription's event list with randomly inserted checkpoints,
	// then wrap in OrderedFeed.
	subs = make([]streamclient.Subscription, opts.numSubs)
	for i := 0; i < opts.numSubs; i++ {
		events := addCheckpoints(rng, opts.density, subKVs[i], coveringSpan)

		sub := makeTestSubscription(events)
		frontier, err := span.MakeFrontier(coveringSpan)
		require.NoError(t, err)
		orderedFeed, err := NewOrderedFeed(sub, frontier)
		require.NoError(t, err)
		subs[i] = orderedFeed
	}
	return subs, coveringSpan, expected
}

func TestMergeFeedRandomized(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	numKVs := 1000
	subs, coveringSpan, expected := generateMergeFeedInputs(
		t, rng, numKVs, mergeFeedInputOptions{maxTxnSize: 256})

	feed := NewMergeFeed(subs, coveringSpan, 128)

	var received []kvEvent
	var lastTime int

	// lastBatchMaxTS tracks the maximum timestamp of the most recently
	// received KV batch. Used to verify that no transaction is split across
	// batches: the first KV in a new batch must have a timestamp strictly
	// greater than the previous batch's max.
	lastBatchMaxTS := 0

	synctest.Test(t, func(t *testing.T) {
		go func() {
			err := feed.Subscribe(context.Background())
			require.NoError(t, err)
		}()
		go func() {
			for ev := range feed.Events() {
				switch ev.Type() {
				case crosscluster.KVEvent:
					kvs := ev.GetKVs()

					// The first KV's timestamp must be strictly greater than
					// the previous batch's max timestamp. This ensures that no
					// transaction is split across batches.
					firstTS := int(kvs[0].KeyValue.Value.Timestamp.WallTime)
					require.Greater(t, firstTS, lastBatchMaxTS,
						"first KV timestamp in new batch must exceed previous "+
							"batch max (transaction must not span batches)")

					batchMaxTS := 0
					var prevTS int
					for i, kv := range kvs {
						kvTime := int(kv.KeyValue.Value.Timestamp.WallTime)
						if kvTime > batchMaxTS {
							batchMaxTS = kvTime
						}
						// KVs within a batch must be sorted by timestamp
						// (non-decreasing).
						if i > 0 {
							require.GreaterOrEqual(t, kvTime, prevTS,
								"KVs within a batch must be sorted by timestamp")
						}
						prevTS = kvTime
						received = append(received, kvEvent{
							key:  string(kv.KeyValue.Key),
							time: kvTime,
						})
					}
					lastBatchMaxTS = batchMaxTS
					require.GreaterOrEqual(t, batchMaxTS, lastTime,
						"batch max timestamps must be monotonically non-decreasing")
					lastTime = batchMaxTS
				case crosscluster.CheckpointEvent:
					cp := ev.GetCheckpoint()
					cpTime := int(cp.ResolvedSpans[0].Timestamp.WallTime)
					require.GreaterOrEqual(t, cpTime, lastTime,
						"checkpoint timestamps must be monotonically non-decreasing")
					lastTime = cpTime
				}
			}
		}()
		synctest.Wait()
	})

	// Sort received by (time, key) to match expected's canonical order,
	// since intra-timestamp ordering across subscriptions is
	// non-deterministic.
	slices.SortFunc(received, kvEvent.Compare)
	require.Equal(t, expected, received,
		"output KV order does not match expected timestamp order")
	maxExpectedTS := expected[len(expected)-1].time
	require.GreaterOrEqual(t, lastTime, maxExpectedTS,
		"final event should be at or beyond the max KV timestamp")
}
