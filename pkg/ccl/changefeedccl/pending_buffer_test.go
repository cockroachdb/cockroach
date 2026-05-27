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

// TODO(#170203): add a parallelism/stress test under --race once
// completeBatch is implemented.
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

// TestPendingBufferGetBatch covers the addRow -> getBatch path: events
// come back in oldest-first order, capped by the configured batch
// limit. Same-key and distinct-key inputs are exercised together.
func TestPendingBufferGetBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	evA := mkEvent("a", "1", ts(0))
	evB := mkEvent("b", "1", ts(1))
	evC := mkEvent("c", "1", ts(2))
	evD := mkEvent("d", "1", ts(3))
	evE := mkEvent("e", "1", ts(4))

	evA1 := mkEvent("a", "1", ts(0))
	evA2 := mkEvent("a", "2", ts(1))
	evA3 := mkEvent("a", "3", ts(2))
	evA4 := mkEvent("a", "4", ts(3))
	evA5 := mkEvent("a", "5", ts(4))

	tests := []struct {
		name     string
		inputs   []*rowEvent
		expected []*rowEvent
	}{
		{
			name:     "round trip 4 distinct keys",
			inputs:   []*rowEvent{evA, evB, evC, evD},
			expected: []*rowEvent{evA, evB, evC, evD},
		},
		{
			name:     "limit caps cross-key batch",
			inputs:   []*rowEvent{evA, evB, evC, evD, evE},
			expected: []*rowEvent{evA, evB, evC, evD},
		},
		{
			name:     "limit caps per-key batch",
			inputs:   []*rowEvent{evA1, evA2, evA3, evA4, evA5},
			expected: []*rowEvent{evA1, evA2, evA3, evA4},
		},
		{
			name:     "fills batch by oldest key",
			inputs:   []*rowEvent{evA1, evB, evC, evD, evA2, evA3},
			expected: []*rowEvent{evA1, evA2, evA3, evB},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newPendingBuffer(pendingBufferConfig{maxMessages: 4}, nil /* topic */)
			defer b.close()
			for _, ev := range tc.inputs {
				require.NoError(t, b.addRow(ctx, ev))
			}
			batch, err := b.getBatch(ctx)
			require.NoError(t, err)
			assertEvents(t, tc.expected, batch.events)
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

	select {
	case <-batchCh:
		t.Fatal("getBatch returned on empty buffer")
	case <-time.After(200 * time.Millisecond):
	}

	ev := mkEvent("a", "1", ts(0))
	require.NoError(t, b.addRow(ctx, ev))

	select {
	case batch := <-batchCh:
		assertEvents(t, []*rowEvent{ev}, batch.events)
	case <-time.After(time.Second):
		t.Fatal("getBatch did not return after addRow")
	}
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

	select {
	case <-batchCh:
		t.Fatal("getBatch returned while same-key batch was inflight")
	case <-time.After(200 * time.Millisecond):
	}
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

	select {
	case err := <-errCh:
		t.Fatalf("getBatch returned before close: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	b.close()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, errPendingBufferClosed)
	case <-time.After(time.Second):
		t.Fatal("getBatch did not return after close")
	}

	err := b.addRow(ctx, mkEvent("a", "1", ts(0)))
	require.ErrorIs(t, err, errPendingBufferClosed)

	_, err = b.getBatch(ctx)
	require.ErrorIs(t, err, errPendingBufferClosed)
}
