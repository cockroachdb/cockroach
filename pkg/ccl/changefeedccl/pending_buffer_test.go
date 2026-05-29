// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func ts(n int) hlc.Timestamp {
	return hlc.Timestamp{WallTime: int64(n)}
}

func mkEvent(key, val string, ts hlc.Timestamp) *rowEvent {
	return &rowEvent{key: []byte(key), val: []byte(val), mvcc: ts}
}

func assertEvents(t *testing.T, expected []*rowEvent, actual []*rowEvent) {
	require.Equal(t, len(expected), len(actual))
	for i, exp := range expected {
		act := actual[i]
		require.Equal(t, exp.key, act.key)
		require.Equal(t, exp.val, act.val)
	}
}

// mustReceive blocks up to one second waiting for ch to fire and
// returns the received value, failing the test on timeout.
func mustReceive[T any](t *testing.T, ch <-chan T, msg string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", msg)
		var zero T
		return zero
	}
}

// mustBlock waits 200ms and fails the test if ch fires in that window.
// The check is best-effort: a goroutine that hasn't yet parked will
// look "blocked" even if it would return immediately, but in practice
// 200ms is plenty for the operations we exercise.
func mustBlock[T any](t *testing.T, ch <-chan T, msg string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s: unexpectedly received %v", msg, v)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestPendingBufferBatchSequence asserts the exact events returned
// by each batch across scenarios that exercise FIFO ordering, key
// requeueing, and partial-batch handling.
func TestPendingBufferBatchSequence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	mk := func(key, val string) *rowEvent {
		return mkEvent(key, val, ts(0))
	}

	tests := []struct {
		name    string
		inputs  []*rowEvent
		batches [][]*rowEvent
	}{
		{
			name:    "round trip 4 distinct keys",
			inputs:  []*rowEvent{mk("a", "1"), mk("b", "1"), mk("c", "1"), mk("d", "1")},
			batches: [][]*rowEvent{{mk("a", "1"), mk("b", "1"), mk("c", "1"), mk("d", "1")}},
		},
		{
			// Fifth event would exceed maxMessages=4; it stays pending
			// and drains in batch2 once batch1 completes.
			name:   "limit caps cross-key batch",
			inputs: []*rowEvent{mk("a", "1"), mk("b", "1"), mk("c", "1"), mk("d", "1"), mk("e", "1")},
			batches: [][]*rowEvent{
				{mk("a", "1"), mk("b", "1"), mk("c", "1"), mk("d", "1")},
				{mk("e", "1")},
			},
		},
		{
			// Single key with 5 events: first 4 fit, fifth comes through
			// in batch2 after completeBatch re-pushes A's tail.
			name: "limit caps per-key batch",
			inputs: []*rowEvent{
				mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4"), mk("a", "5"),
			},
			batches: [][]*rowEvent{
				{mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4")},
				{mk("a", "5")},
			},
		},
		{
			// A is oldest, so its whole FIFO drains before we move on to
			// B; C and D never make it into batch1 and come through in
			// batch2.
			name: "fills batch by oldest key",
			inputs: []*rowEvent{
				mk("a", "1"), mk("b", "1"), mk("c", "1"), mk("d", "1"), mk("a", "2"), mk("a", "3"),
			},
			batches: [][]*rowEvent{
				{mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("b", "1")},
				{mk("c", "1"), mk("d", "1")},
			},
		},
		{
			// Same-key overflow: 8 events for a single key produce two
			// back-to-back full batches once completeBatch releases the
			// key between them.
			name: "same key partial requeue",
			inputs: []*rowEvent{
				mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4"),
				mk("a", "5"), mk("a", "6"), mk("a", "7"), mk("a", "8"),
			},
			batches: [][]*rowEvent{
				{mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4")},
				{mk("a", "5"), mk("a", "6"), mk("a", "7"), mk("a", "8")},
			},
		},
		{
			// A's tail re-enters the heap at its original arrival slot
			// (seq=6, between B/C and D/E), so batch2 is B, C, A, A and
			// D/E come through in batch3.
			name: "requeue lands at arrival slot",
			inputs: []*rowEvent{
				mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4"),
				mk("b", "1"), mk("c", "1"),
				mk("a", "5"), mk("a", "6"),
				mk("d", "1"), mk("e", "1"),
			},
			batches: [][]*rowEvent{
				{mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4")},
				{mk("b", "1"), mk("c", "1"), mk("a", "5"), mk("a", "6")},
				{mk("d", "1"), mk("e", "1")},
			},
		},
		{
			// A's tail arrived AFTER B/C/D/E, so on re-push its seq sits
			// behind them. Batch2 drains B, C, D, E and A's tail comes
			// through in batch3.
			name: "requeue lands behind newer keys",
			inputs: []*rowEvent{
				mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4"),
				mk("b", "1"), mk("c", "1"), mk("d", "1"), mk("e", "1"),
				mk("a", "5"), mk("a", "6"),
			},
			batches: [][]*rowEvent{
				{mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4")},
				{mk("b", "1"), mk("c", "1"), mk("d", "1"), mk("e", "1")},
				{mk("a", "5"), mk("a", "6")},
			},
		},
		{
			// A's FIFO is exactly drained by batch1 (no tail), so
			// completeBatch must NOT re-push A. Batch2 has only B/C/D
			// available and returns a partial 3-event batch.
			name: "fully drained key not requeued",
			inputs: []*rowEvent{
				mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4"),
				mk("b", "1"), mk("c", "1"), mk("d", "1"),
			},
			batches: [][]*rowEvent{
				{mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4")},
				{mk("b", "1"), mk("c", "1"), mk("d", "1")},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
			defer b.close()
			for _, ev := range tc.inputs {
				require.NoError(t, b.addRow(ctx, ev))
			}
			for _, expected := range tc.batches {
				batch, err := b.getBatch(ctx)
				require.NoError(t, err)
				assertEvents(t, expected, batch.events)
				b.completeBatch(batch)
			}
		})
	}
}

// TestPendingBufferGetBatchBlocksUntilAddRow verifies that getBatch
// blocks on an empty buffer and unblocks once an event arrives.
func TestPendingBufferGetBatchBlocksUntilAddRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
	defer b.close()

	batchCh := make(chan *pendingBatch, 1)
	go func() {
		batch, err := b.getBatch(ctx)
		if err != nil {
			return
		}
		batchCh <- batch
	}()

	mustBlock(t, batchCh, "getBatch on empty buffer")

	ev := mkEvent("a", "1", ts(0))
	require.NoError(t, b.addRow(ctx, ev))

	batch := mustReceive(t, batchCh, "getBatch after addRow")
	assertEvents(t, []*rowEvent{ev}, batch.events)
}

// TestPendingBufferGetBatchExcludesInflightKey verifies that getBatch
// does not return a key already held by an outstanding batch.
func TestPendingBufferGetBatchExcludesInflightKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
	defer b.close()

	evA1 := mkEvent("a", "1", ts(0))
	require.NoError(t, b.addRow(ctx, evA1))

	batch, err := b.getBatch(ctx)
	require.NoError(t, err)
	assertEvents(t, []*rowEvent{evA1}, batch.events)

	evA2 := mkEvent("a", "2", ts(1))
	require.NoError(t, b.addRow(ctx, evA2))

	batchCh := make(chan *pendingBatch, 1)
	go func() {
		batch, err := b.getBatch(ctx)
		if err != nil {
			return
		}
		batchCh <- batch
	}()

	mustBlock(t, batchCh, "getBatch while same-key batch was inflight")
}

// TestPendingBufferCloseSentinel verifies that close wakes parked
// getBatch and that subsequent getBatch/addRow calls return
// errPendingBufferClosed.
func TestPendingBufferCloseSentinel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)

	errCh := make(chan error, 1)
	go func() {
		_, err := b.getBatch(ctx)
		errCh <- err
	}()

	mustBlock(t, errCh, "getBatch before close")

	b.close()

	err := mustReceive(t, errCh, "getBatch after close")
	require.ErrorIs(t, err, errPendingBufferClosed)

	err = b.addRow(ctx, mkEvent("a", "1", ts(0)))
	require.ErrorIs(t, err, errPendingBufferClosed)

	_, err = b.getBatch(ctx)
	require.ErrorIs(t, err, errPendingBufferClosed)
}

// TestPendingBufferDrainOnEmpty verifies that drain returns
// immediately on an empty buffer.
func TestPendingBufferDrainOnEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
	defer b.close()

	doneCh := make(chan error, 1)
	go func() { doneCh <- b.drain(ctx) }()
	require.NoError(t, mustReceive(t, doneCh, "drain on empty buffer"))
}

// TestPendingBufferCloseDuringDrain verifies that close while drain
// is parked causes drain to return errPendingBufferClosed rather than
// success, since a closed buffer cannot guarantee pre-drain events
// flushed.
func TestPendingBufferCloseDuringDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)

	// Add a row and pull it into an inflight batch so drain has
	// something to wait on. We intentionally don't completeBatch.
	require.NoError(t, b.addRow(ctx, mkEvent("a", "1", ts(0))))
	_, err := b.getBatch(ctx)
	require.NoError(t, err)

	drainCh := make(chan error, 1)
	go func() { drainCh <- b.drain(ctx) }()
	mustBlock(t, drainCh, "drain with inflight batch")

	b.close()
	err = mustReceive(t, drainCh, "drain after close")
	require.ErrorIs(t, err, errPendingBufferClosed)
}

// TestPendingBufferAddRowDuringDrainDoesNotBlock verifies that
// addRow succeeds while drain is in progress and does not delay
// drain.
func TestPendingBufferAddRowDuringDrainDoesNotBlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
	defer b.close()

	// Park drain by leaving a batch inflight.
	require.NoError(t, b.addRow(ctx, mkEvent("a", "1", ts(0))))
	batch, err := b.getBatch(ctx)
	require.NoError(t, err)

	drainCh := make(chan error, 1)
	go func() { drainCh <- b.drain(ctx) }()
	mustBlock(t, drainCh, "drain with inflight batch")

	// addRow for a post-drain key should not block on drain.
	require.NoError(t, b.addRow(ctx, mkEvent("b", "1", ts(0))))

	// The post-drain addRow does not affect drain's predicate, so drain
	// stays blocked on the pre-drain inflight batch.
	mustBlock(t, drainCh, "drain after post-drain addRow")

	// Releasing the pre-drain inflight batch unblocks drain.
	b.completeBatch(batch)
	require.NoError(t, mustReceive(t, drainCh, "drain after completeBatch"))
}

// TestPendingBufferDrain verifies that drain blocks until every
// event added before the drain call has been released.
func TestPendingBufferDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
	defer b.close()

	mk := func(key, val string) *rowEvent {
		return mkEvent(key, val, ts(0))
	}

	inputs := []*rowEvent{
		mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4"),
		mk("b", "1"), mk("c", "1"), mk("d", "1"), mk("e", "1"),
		mk("a", "5"), mk("a", "6"),
	}
	for _, ev := range inputs {
		require.NoError(t, b.addRow(ctx, ev))
	}

	batch1, err := b.getBatch(ctx)
	require.NoError(t, err)
	assertEvents(t, []*rowEvent{mk("a", "1"), mk("a", "2"), mk("a", "3"), mk("a", "4")}, batch1.events)
	b.completeBatch(batch1)

	batch2, err := b.getBatch(ctx)
	require.NoError(t, err)
	assertEvents(t, []*rowEvent{mk("b", "1"), mk("c", "1"), mk("d", "1"), mk("e", "1")}, batch2.events)
	b.completeBatch(batch2)

	drainCh := make(chan error, 1)
	go func() { drainCh <- b.drain(ctx) }()
	mustBlock(t, drainCh, "drain with A's tail still pending")

	batch3, err := b.getBatch(ctx)
	require.NoError(t, err)
	assertEvents(t, []*rowEvent{mk("a", "5"), mk("a", "6")}, batch3.events)
	mustBlock(t, drainCh, "drain with batch3 inflight")

	b.completeBatch(batch3)
	require.NoError(t, mustReceive(t, drainCh, "drain after batch3 completes"))
}

// TestPendingBufferCompleteBatchAfterClose verifies that
// completeBatch after close is a safe no-op.
func TestPendingBufferCompleteBatchAfterClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)

	require.NoError(t, b.addRow(ctx, mkEvent("a", "1", ts(0))))
	batch, err := b.getBatch(ctx)
	require.NoError(t, err)

	b.close()
	require.NotPanics(t, func() { b.completeBatch(batch) })
}

// TestPendingBufferCompleteBatchStaleReleasePanics verifies that
// double-completeBatch panics rather than silently no-op'ing, which
// would let a stale call erroneously release a fresh batch's
// inflight hold.
func TestPendingBufferCompleteBatchStaleReleasePanics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
	defer b.close()

	require.NoError(t, b.addRow(ctx, mkEvent("a", "1", ts(0))))
	batch1, err := b.getBatch(ctx)
	require.NoError(t, err)
	b.completeBatch(batch1)

	require.NoError(t, b.addRow(ctx, mkEvent("a", "2", ts(0))))
	_, err = b.getBatch(ctx)
	require.NoError(t, err)

	require.Panics(t, func() { b.completeBatch(batch1) })
}

// TestPendingBufferContextCancel verifies that a single ctx
// cancellation wakes every operation parked on the cond.
func TestPendingBufferContextCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
	defer b.close()

	setupCtx := context.Background()
	require.NoError(t, b.addRow(setupCtx, mkEvent("a", "1", ts(0))))
	batch, err := b.getBatch(setupCtx)
	require.NoError(t, err)
	defer b.completeBatch(batch)

	ctx, cancel := context.WithCancel(setupCtx)

	getBatchCh := make(chan error, 1)
	go func() {
		_, err := b.getBatch(ctx)
		getBatchCh <- err
	}()
	mustBlock(t, getBatchCh, "getBatch on empty heap")

	drainCh := make(chan error, 1)
	go func() { drainCh <- b.drain(ctx) }()
	mustBlock(t, drainCh, "drain with inflight batch")

	cancel()
	require.ErrorIs(t, mustReceive(t, getBatchCh, "getBatch after ctx cancel"), context.Canceled)
	require.ErrorIs(t, mustReceive(t, drainCh, "drain after ctx cancel"), context.Canceled)
}

// TestPendingBufferParallelStress runs producers and consumers in
// parallel and asserts that every added event is delivered exactly
// once and that no two outstanding batches share a key.
func TestPendingBufferParallelStress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const (
		numKeys              = 10
		numProducers         = 10
		numRoundsPerProducer = 10
		numConsumers         = 5
		batchLimit           = 5
	)
	const totalEvents = numProducers * numRoundsPerProducer * numKeys

	b := newPendingBuffer(pendingBufferConfig{maxMessages: batchLimit}, nil /* topic */)

	var inflightMu syncutil.Mutex
	inflight := make(map[string]int)

	var seen atomic.Int64
	var producersWg, consumersWg sync.WaitGroup

	// Run as a defer so that on any failure path consumer goroutines
	// still get unblocked (they're parked on getBatch until close).
	defer func() {
		b.close()
		consumersWg.Wait()
	}()

	for w := 0; w < numConsumers; w++ {
		consumersWg.Add(1)
		go func(workerID int) {
			defer consumersWg.Done()
			for {
				batch, err := b.getBatch(ctx)
				if err != nil {
					return
				}

				// A batch can legitimately contain the same key multiple
				// times (per-key FIFO). The invariant is across BATCHES:
				// no key appears in two outstanding batches at once.
				// Dedupe before touching the shadow map.
				batchKeys := make(map[string]struct{}, len(batch.events))
				for _, ev := range batch.events {
					batchKeys[string(ev.key)] = struct{}{}
				}

				inflightMu.Lock()
				for k := range batchKeys {
					prev, present := inflight[k]
					require.Falsef(t, present,
						"worker %d got key %q already held by worker %d", workerID, k, prev)
					inflight[k] = workerID
				}
				inflightMu.Unlock()

				seen.Add(int64(len(batch.events)))

				// Release from the shadow map BEFORE completeBatch so that
				// once the buffer releases the keys, another worker that
				// pulls them next will see them absent from the map.
				inflightMu.Lock()
				for k := range batchKeys {
					delete(inflight, k)
				}
				inflightMu.Unlock()
				b.completeBatch(batch)
			}
		}(w)
	}

	for p := 0; p < numProducers; p++ {
		producersWg.Add(1)
		go func() {
			defer producersWg.Done()
			for r := 0; r < numRoundsPerProducer; r++ {
				for k := 0; k < numKeys; k++ {
					require.NoError(t, b.addRow(ctx, mkEvent(strconv.Itoa(k), "v", ts(0))))
				}
			}
		}()
	}

	producersWg.Wait()
	require.NoError(t, b.drain(ctx))
	require.Equal(t, int64(totalEvents), seen.Load())
}
