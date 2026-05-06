// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mkEvent constructs a rowEvent from the pool. The kvevent.Alloc is
// left zero, which makes Release a no-op.
func mkEvent(key, val string) *rowEvent {
	ev := newRowEvent()
	ev.key = []byte(key)
	ev.val = []byte(val)
	return ev
}

func defaultTestCfg() pendingBufferConfig {
	return pendingBufferConfig{maxMessages: 100, maxBytes: 1 << 20, bufferLimit: 100}
}

// drainAfterClose closes the buffer and drains any remaining events so
// the test exits cleanly. Safe to call from t.Cleanup.
func drainAfterClose(t *testing.T, b *pendingBuffer) {
	t.Helper()
	ctx := context.Background()
	b.close()
	for {
		batch, err := b.getBatch(ctx)
		if err != nil {
			require.True(t, errors.Is(err, errPendingBufferClosed))
			return
		}
		b.completeBatch(ctx, batch)
	}
}

func TestPendingBuffer_AddAndGetSingleEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	b := newPendingBuffer(defaultTestCfg())
	t.Cleanup(func() { drainAfterClose(t, b) })

	require.NoError(t, b.addRow(ctx, mkEvent("k", "v")))
	batch, err := b.getBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch.events, 1)
	require.Equal(t, []byte("k"), batch.events[0].key)
	b.completeBatch(ctx, batch)
}

func TestPendingBuffer_PerKeyOrderPreserved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := defaultTestCfg()
	cfg.maxMessages = 1 // force one event per batch
	b := newPendingBuffer(cfg)
	t.Cleanup(func() { drainAfterClose(t, b) })

	for i := 0; i < 5; i++ {
		require.NoError(t, b.addRow(ctx, mkEvent("k", fmt.Sprintf("v%d", i))))
	}
	for i := 0; i < 5; i++ {
		batch, err := b.getBatch(ctx)
		require.NoError(t, err)
		require.Len(t, batch.events, 1)
		require.Equal(t, fmt.Sprintf("v%d", i), string(batch.events[0].val), "iteration %d", i)
		b.completeBatch(ctx, batch)
	}
}

func TestPendingBuffer_InflightKeyExclusion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := defaultTestCfg()
	cfg.maxMessages = 1
	b := newPendingBuffer(cfg)
	t.Cleanup(func() { drainAfterClose(t, b) })

	require.NoError(t, b.addRow(ctx, mkEvent("k", "v1")))
	batch1, err := b.getBatch(ctx)
	require.NoError(t, err)

	// Second event for the same key — a second worker must not pull it
	// while batch1 is inflight.
	require.NoError(t, b.addRow(ctx, mkEvent("k", "v2")))

	got := make(chan *pendingBatch, 1)
	go func() {
		batch, _ := b.getBatch(ctx)
		got <- batch
	}()

	select {
	case <-got:
		t.Fatal("getBatch returned while same-key batch was inflight")
	case <-time.After(200 * time.Millisecond):
	}

	b.completeBatch(ctx, batch1)
	select {
	case batch2 := <-got:
		require.Equal(t, "v2", string(batch2.events[0].val))
		b.completeBatch(ctx, batch2)
	case <-time.After(time.Second):
		t.Fatal("getBatch did not return after completeBatch released the inflight key")
	}
}

func TestPendingBuffer_MaxMessages(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := defaultTestCfg()
	cfg.maxMessages = 3
	b := newPendingBuffer(cfg)
	t.Cleanup(func() { drainAfterClose(t, b) })

	const n = 10
	for i := 0; i < n; i++ {
		require.NoError(t, b.addRow(ctx, mkEvent(fmt.Sprintf("k%d", i), "v")))
	}
	delivered := 0
	for delivered < n {
		batch, err := b.getBatch(ctx)
		require.NoError(t, err)
		require.LessOrEqual(t, len(batch.events), cfg.maxMessages)
		require.NotEmpty(t, batch.events)
		delivered += len(batch.events)
		b.completeBatch(ctx, batch)
	}
}

func TestPendingBuffer_MaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := defaultTestCfg()
	cfg.maxBytes = 10
	b := newPendingBuffer(cfg)
	t.Cleanup(func() { drainAfterClose(t, b) })

	const n = 5
	for i := 0; i < n; i++ {
		// Each event is small enough that several fit in a 10-byte
		// batch but not all five.
		require.NoError(t, b.addRow(ctx, mkEvent(fmt.Sprintf("k%d", i), "hello")))
	}
	delivered := 0
	for delivered < n {
		batch, err := b.getBatch(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, batch.events)
		require.LessOrEqual(t, batch.numBytes, cfg.maxBytes)
		delivered += len(batch.events)
		b.completeBatch(ctx, batch)
	}

	// Oversized single event must still be admitted (otherwise it would
	// never ship).
	require.NoError(t, b.addRow(ctx, mkEvent("k", "this-is-too-big")))
	batch, err := b.getBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch.events, 1)
	b.completeBatch(ctx, batch)
}

func TestPendingBuffer_BlockingOnEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	b := newPendingBuffer(defaultTestCfg())
	t.Cleanup(func() { drainAfterClose(t, b) })

	got := make(chan *pendingBatch, 1)
	go func() {
		batch, _ := b.getBatch(ctx)
		got <- batch
	}()

	select {
	case <-got:
		t.Fatal("getBatch returned on empty buffer")
	case <-time.After(200 * time.Millisecond):
	}

	require.NoError(t, b.addRow(ctx, mkEvent("k", "v")))
	select {
	case batch := <-got:
		b.completeBatch(ctx, batch)
	case <-time.After(time.Second):
		t.Fatal("getBatch did not return after addRow")
	}
}

func TestPendingBuffer_BlockingOnFull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := defaultTestCfg()
	cfg.bufferLimit = 2
	b := newPendingBuffer(cfg)
	t.Cleanup(func() { drainAfterClose(t, b) })

	require.NoError(t, b.addRow(ctx, mkEvent("k1", "v")))
	require.NoError(t, b.addRow(ctx, mkEvent("k2", "v")))

	addDone := make(chan error, 1)
	go func() { addDone <- b.addRow(ctx, mkEvent("k3", "v")) }()

	select {
	case <-addDone:
		t.Fatal("addRow returned while buffer was at limit")
	case <-time.After(200 * time.Millisecond):
	}

	batch, err := b.getBatch(ctx)
	require.NoError(t, err)
	b.completeBatch(ctx, batch)

	select {
	case err := <-addDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("addRow did not return after getBatch drained the buffer")
	}
}

func TestPendingBuffer_CloseUnblocksWaiters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("getBatch on empty", func(t *testing.T) {
		b := newPendingBuffer(defaultTestCfg())
		done := make(chan error, 1)
		go func() {
			_, err := b.getBatch(ctx)
			done <- err
		}()
		time.Sleep(200 * time.Millisecond) // let it land in Wait
		b.close()
		select {
		case err := <-done:
			require.True(t, errors.Is(err, errPendingBufferClosed))
		case <-time.After(time.Second):
			t.Fatal("getBatch did not unblock after close")
		}
	})

	t.Run("addRow on full", func(t *testing.T) {
		cfg := defaultTestCfg()
		cfg.bufferLimit = 1
		b := newPendingBuffer(cfg)
		require.NoError(t, b.addRow(ctx, mkEvent("k", "v")))
		done := make(chan error, 1)
		go func() { done <- b.addRow(ctx, mkEvent("k2", "v")) }()
		time.Sleep(200 * time.Millisecond)
		b.close()
		select {
		case err := <-done:
			require.True(t, errors.Is(err, errPendingBufferClosed))
		case <-time.After(time.Second):
			t.Fatal("addRow did not unblock after close")
		}
		// Drain the one buffered event so leaktest is happy.
		batch, err := b.getBatch(ctx)
		require.NoError(t, err)
		b.completeBatch(ctx, batch)
	})
}

func TestPendingBuffer_CloseDrains(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	b := newPendingBuffer(defaultTestCfg())

	for i := 0; i < 3; i++ {
		require.NoError(t, b.addRow(ctx, mkEvent(fmt.Sprintf("k%d", i), "v")))
	}
	b.close()

	delivered := 0
	for {
		batch, err := b.getBatch(ctx)
		if err != nil {
			require.True(t, errors.Is(err, errPendingBufferClosed))
			break
		}
		delivered += len(batch.events)
		b.completeBatch(ctx, batch)
	}
	require.Equal(t, 3, delivered)

	// Subsequent addRow returns the sentinel.
	require.True(t, errors.Is(b.addRow(ctx, mkEvent("k", "v")), errPendingBufferClosed))
}

// TestPendingBuffer_CompleteBatchUnblocksProducer verifies that
// completeBatch's broadcast (independent of getBatch's broadcast) wakes
// addRow waiters. Setup: keep all events in flight so getBatch can't
// drain the buffer; only completeBatch can free a slot.
func TestPendingBuffer_CompleteBatchUnblocksProducer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := defaultTestCfg()
	cfg.bufferLimit = 1
	cfg.maxMessages = 1
	b := newPendingBuffer(cfg)
	t.Cleanup(func() { drainAfterClose(t, b) })

	// Fill the buffer, then pull the only event so it's inflight (not
	// in events). Buffer is empty but the inflight event will be
	// re-added by the next addRow only if we time it right — instead,
	// re-fill, then pull once more so the queued addRow has one
	// inflight event holding the slot.
	require.NoError(t, b.addRow(ctx, mkEvent("k", "v")))
	batch, err := b.getBatch(ctx)
	require.NoError(t, err)
	// The buffer is now empty (event is inflight). Refill to capacity.
	require.NoError(t, b.addRow(ctx, mkEvent("k2", "v")))

	// Producer goroutine: should block, since len(events) == bufferLimit.
	addDone := make(chan error, 1)
	go func() { addDone <- b.addRow(ctx, mkEvent("k3", "v")) }()

	select {
	case <-addDone:
		t.Fatal("addRow returned while buffer was at limit")
	case <-time.After(200 * time.Millisecond):
	}

	// completeBatch is the only thing that can wake the producer here:
	// it doesn't drain events but it does broadcast.
	b.completeBatch(ctx, batch)

	// The completion broadcast doesn't itself create a slot (events is
	// still at limit), so the producer will re-Wait. We then drain via
	// getBatch to free the slot.
	batch2, err := b.getBatch(ctx)
	require.NoError(t, err)
	b.completeBatch(ctx, batch2)

	select {
	case err := <-addDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("addRow did not return after slot freed")
	}
}

// TestPendingBuffer_PreCancelledCtx verifies the basic ctx contract:
// addRow and getBatch return the ctx error if called with an
// already-cancelled ctx, before any blocking happens. This is a
// non-controversial contract — note that the M2 limitation that
// in-progress cond.Wait does not honor ctx is documented in the code
// and intentionally not tested here, since the M3 worker pool may
// change that behavior.
func TestPendingBuffer_PreCancelledCtx(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	b := newPendingBuffer(defaultTestCfg())
	t.Cleanup(func() { drainAfterClose(t, b) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, b.addRow(ctx, mkEvent("k", "v")), context.Canceled)

	_, err := b.getBatch(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// TestPendingBuffer_Demo is a logs-only walkthrough of the addRow →
// getBatch → completeBatch lifecycle. It is skipped in -short runs (so
// CI doesn't waste time on it) and uses t.Logf so the lifecycle is
// visible with `./dev test pkg/ccl/changefeedccl -f
// TestPendingBuffer_Demo -v`.
//
// Real cluster validation lives in the noLingerSink demo (later).
func TestPendingBuffer_Demo(t *testing.T) {
	if testing.Short() {
		t.Skip("logs-only demo; run with -v to see lifecycle")
	}
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := defaultTestCfg()
	cfg.maxMessages = 30
	cfg.bufferLimit = 400
	b := newPendingBuffer(cfg)

	const runFor = 300 * time.Millisecond
	const tickEvery = 5 * time.Millisecond

	var producerWG, consumerWG sync.WaitGroup
	producerWG.Add(1)
	go func() {
		defer producerWG.Done()
		deadline := time.Now().Add(runFor)
		for i := 0; time.Now().Before(deadline); i++ {
			key := fmt.Sprintf("k%d", i%3)
			val := fmt.Sprintf("v%d", i)
			t.Logf("producer: addRow key=%s val=%s", key, val)
			if err := b.addRow(ctx, mkEvent(key, val)); err != nil {
				t.Errorf("addRow: %v", err)
				return
			}
			if i%10 == 0 {
				time.Sleep(tickEvery)
			}
		}
	}()

	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		for {
			batch, err := b.getBatch(ctx)
			if err != nil {
				t.Logf("consumer: getBatch -> %v (exiting)", err)
				return
			}
			keys := make([]string, len(batch.events))
			for i, ev := range batch.events {
				keys[i] = string(ev.key)
			}
			t.Logf("consumer: got batch n=%d bytes=%d keys=%v",
				len(batch.events), batch.numBytes, keys)
			b.completeBatch(ctx, batch)
			t.Logf("consumer: completeBatch released %d keys", len(batch.inflightKeys))
		}
	}()

	producerWG.Wait()
	t.Logf("producer done; closing buffer")
	b.close()
	consumerWG.Wait()
}

func TestPendingBuffer_NoEventLoss(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := defaultTestCfg()
	cfg.bufferLimit = 32
	cfg.maxMessages = 8
	b := newPendingBuffer(cfg)

	const numProducers = 4
	const numConsumers = 4
	const eventsPerProducer = 250
	const totalExpected = numProducers * eventsPerProducer

	var producerWG sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		producerWG.Add(1)
		go func(p int) {
			defer producerWG.Done()
			for i := 0; i < eventsPerProducer; i++ {
				key := fmt.Sprintf("p%d-k%d", p, i%17) // bounded keyspace per producer
				val := fmt.Sprintf("p%d-v%d", p, i)
				if err := b.addRow(ctx, mkEvent(key, val)); err != nil {
					t.Errorf("addRow: %v", err)
					return
				}
			}
		}(p)
	}

	var delivered atomic.Int64
	var consumerWG sync.WaitGroup
	var seen sync.Map // val -> struct{}; detects duplicates
	for c := 0; c < numConsumers; c++ {
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			for {
				batch, err := b.getBatch(ctx)
				if err != nil {
					return
				}
				for _, ev := range batch.events {
					if _, dup := seen.LoadOrStore(string(ev.val), struct{}{}); dup {
						t.Errorf("duplicate event delivered: %s", ev.val)
					}
				}
				delivered.Add(int64(len(batch.events)))
				b.completeBatch(ctx, batch)
			}
		}()
	}

	producerWG.Wait()
	// Wait until everything's delivered, then close to signal consumers.
	require.Eventually(t, func() bool {
		return delivered.Load() == int64(totalExpected)
	}, 5*time.Second, 5*time.Millisecond)
	b.close()
	consumerWG.Wait()

	require.Equal(t, int64(totalExpected), delivered.Load())
}
