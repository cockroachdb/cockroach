// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcbatcher

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TODO(#170203): add a parallelism/stress test under --race once
// completeBatch is implemented.

type testEvent struct {
	key, val []byte
}

func mkEvent(key, val string) *testEvent {
	return &testEvent{key: []byte(key), val: []byte(val)}
}

func newTestBuffer(cfg Config[*testEvent]) *Buffer[*testEvent] {
	cfg.Key = func(ev *testEvent) []byte { return ev.key }
	return New(cfg)
}

func assertEvents(t *testing.T, expected []*testEvent, actual []*testEvent) {
	require.Equal(t, len(expected), len(actual))
	for i, exp := range expected {
		act := actual[i]
		require.Equal(t, exp.key, act.key)
		require.Equal(t, exp.val, act.val)
	}
}

// TestBufferNewValidatesConfig verifies that New panics on a Config
// that would otherwise deadlock or panic later.
func TestBufferNewValidatesConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	validKey := func(ev *testEvent) []byte { return ev.key }
	tests := []struct {
		name string
		cfg  Config[*testEvent]
	}{
		{name: "zero MaxMessages", cfg: Config[*testEvent]{Key: validKey}},
		{name: "negative MaxMessages", cfg: Config[*testEvent]{MaxMessages: -1, Key: validKey}},
		{name: "nil Key", cfg: Config[*testEvent]{MaxMessages: 4}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Panics(t, func() { New(tc.cfg) })
		})
	}
}

// TestBufferGetBatch covers the Add -> GetBatch path: events
// come back in oldest-first order, capped by the configured batch
// limit. Same-key and distinct-key inputs are exercised together.
func TestBufferGetBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	evA := mkEvent("a", "1")
	evB := mkEvent("b", "1")
	evC := mkEvent("c", "1")
	evD := mkEvent("d", "1")
	evE := mkEvent("e", "1")

	evA1 := mkEvent("a", "1")
	evA2 := mkEvent("a", "2")
	evA3 := mkEvent("a", "3")
	evA4 := mkEvent("a", "4")
	evA5 := mkEvent("a", "5")

	tests := []struct {
		name     string
		inputs   []*testEvent
		expected []*testEvent
	}{
		{
			name:     "round trip 4 distinct keys",
			inputs:   []*testEvent{evA, evB, evC, evD},
			expected: []*testEvent{evA, evB, evC, evD},
		},
		{
			name:     "limit caps cross-key batch",
			inputs:   []*testEvent{evA, evB, evC, evD, evE},
			expected: []*testEvent{evA, evB, evC, evD},
		},
		{
			name:     "limit caps per-key batch",
			inputs:   []*testEvent{evA1, evA2, evA3, evA4, evA5},
			expected: []*testEvent{evA1, evA2, evA3, evA4},
		},
		{
			name:     "fills batch by oldest key",
			inputs:   []*testEvent{evA1, evB, evC, evD, evA2, evA3},
			expected: []*testEvent{evA1, evA2, evA3, evB},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBuffer(Config[*testEvent]{MaxMessages: 4})
			defer b.Close()
			for _, ev := range tc.inputs {
				require.NoError(t, b.Add(ctx, ev))
			}
			batch, err := b.GetBatch(ctx)
			require.NoError(t, err)
			assertEvents(t, tc.expected, batch.Events)
		})
	}
}

// TestBufferGetBatchBlocksUntilAdd verifies that GetBatch
// blocks while the buffer is empty and returns promptly once a row is
// added.
func TestBufferGetBatchBlocksUntilAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newTestBuffer(Config[*testEvent]{MaxMessages: 4})
	defer b.Close()

	batchCh := make(chan *Batch[*testEvent], 1)
	go func() {
		batch, err := b.GetBatch(ctx)
		if err != nil {
			return
		}
		batchCh <- batch
	}()

	select {
	case <-batchCh:
		t.Fatal("GetBatch returned on empty buffer")
	case <-time.After(200 * time.Millisecond):
	}

	ev := mkEvent("a", "1")
	require.NoError(t, b.Add(ctx, ev))

	select {
	case batch := <-batchCh:
		assertEvents(t, []*testEvent{ev}, batch.Events)
	case <-time.After(time.Second):
		t.Fatal("GetBatch did not return after Add")
	}
}

// TestBufferGetBatchExcludesInflightKey verifies that a key
// held by an outstanding batch is excluded from subsequent batches: a
// concurrent GetBatch must block while the only pending events are
// for already-inflight keys.
func TestBufferGetBatchExcludesInflightKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newTestBuffer(Config[*testEvent]{MaxMessages: 4})
	defer b.Close()

	evA1 := mkEvent("a", "1")
	require.NoError(t, b.Add(ctx, evA1))

	batch, err := b.GetBatch(ctx)
	require.NoError(t, err)
	assertEvents(t, []*testEvent{evA1}, batch.Events)

	evA2 := mkEvent("a", "2")
	require.NoError(t, b.Add(ctx, evA2))

	batchCh := make(chan *Batch[*testEvent], 1)
	go func() {
		batch, err := b.GetBatch(ctx)
		if err != nil {
			return
		}
		batchCh <- batch
	}()

	select {
	case <-batchCh:
		t.Fatal("GetBatch returned while same-key batch was inflight")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestBufferHashCollision exercises the two observable
// consequences of a hash collision: colliding keys jump ahead of older
// distinct keys (FIFO sharing), and a colliding key can be stuck behind
// an inflight batch even when batch capacity is available (over-
// serialization). Otherwise, it keeps updates within a key in order.
func TestBufferHashCollision(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Hash by first byte: all "a*" keys collide; "b1" does not.
	cfg := Config[*testEvent]{
		MaxMessages: 4,
		HashKey:     func(k []byte) uint64 { return uint64(k[0]) },
	}
	b := newTestBuffer(cfg)
	defer b.Close()

	evA1 := mkEvent("a1", "1")
	evB1 := mkEvent("b1", "2")
	evA2 := mkEvent("a2", "3")
	evA3 := mkEvent("a3", "4")
	evA4 := mkEvent("a4", "5")
	evA5 := mkEvent("a5", "6")
	for _, ev := range []*testEvent{evA1, evB1, evA2, evA3, evA4, evA5} {
		require.NoError(t, b.Add(ctx, ev))
	}

	// b1 arrived before a2-a5 but is skipped: the "a*" FIFO drains
	// four events from its head before b1 gets a turn.
	batch, err := b.GetBatch(ctx)
	require.NoError(t, err)
	assertEvents(t, []*testEvent{evA1, evA2, evA3, evA4}, batch.Events)

	// a5 is still pending but the "a*" inflight slot is held, so it
	// cannot ride along with b1 despite having room in the batch.
	batch, err = b.GetBatch(ctx)
	require.NoError(t, err)
	assertEvents(t, []*testEvent{evB1}, batch.Events)
}

// TestBufferCloseSentinel verifies that Close wakes a GetBatch
// already parked in cond.Wait with ErrClosed, that Add
// calls made after Close return the sentinel, and that GetBatch on an
// already-closed buffer also returns the sentinel without blocking.
func TestBufferCloseSentinel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	b := newTestBuffer(Config[*testEvent]{MaxMessages: 4})

	errCh := make(chan error, 1)
	go func() {
		_, err := b.GetBatch(ctx)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("GetBatch returned before Close: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	b.Close()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("GetBatch did not return after Close")
	}

	err := b.Add(ctx, mkEvent("a", "1"))
	require.ErrorIs(t, err, ErrClosed)

	_, err = b.GetBatch(ctx)
	require.ErrorIs(t, err, ErrClosed)
}
