// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// TestPendingBufferBatchSequence drives a series of addRow calls
// followed by a sequence of getBatch calls. Between consecutive
// batches the runner calls completeBatch so that inflight keys are
// released and (if they still have pending events) re-enter the heap
// at their original arrival slot. Each subcase asserts the exact
// events returned by every batch in the sequence.
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
// blocks while the buffer is empty and returns promptly once a row is
// added.
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

// TestPendingBufferGetBatchExcludesInflightKey verifies that a key
// held by an outstanding batch is excluded from subsequent batches: a
// concurrent getBatch must block while the only pending events are
// for already-inflight keys.
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

// TestPendingBufferCloseSentinel verifies that close wakes a getBatch
// already parked in cond.Wait with errPendingBufferClosed, that addRow
// calls made after close return the sentinel, and that getBatch on an
// already-closed buffer also returns the sentinel without blocking.
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

// TestPendingBufferDrainOnEmpty verifies that drain on an empty
// buffer with no inflight batches returns immediately.
func TestPendingBufferDrainOnEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
	defer b.close()

	doneCh := make(chan error, 1)
	go func() { doneCh <- b.drain() }()
	require.NoError(t, mustReceive(t, doneCh, "drain on empty buffer"))
}

// TestPendingBufferCompleteBatchAfterClose verifies that completeBatch
// called after close is a no-op (releases events/allocs, does not
// panic, does not touch the heap).
//
// TODO(#170203): "doesn't panic" is a weak assertion. A meaningful
// "releases allocs" check requires rowEvent/Alloc plumbing and a way
// to observe release count (e.g. a test-only release callback or an
// instrumented pool) — separate production work.
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
