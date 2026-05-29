// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nudge

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestBufferPushPop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	buf := MakeBuffer[int]()
	buf.Push(1)
	buf.Push(2)
	buf.Push(3)

	v, ok := buf.Pop()
	require.True(t, ok)
	require.Equal(t, 1, v)

	v, ok = buf.Pop()
	require.True(t, ok)
	require.Equal(t, 2, v)

	v, ok = buf.Pop()
	require.True(t, ok)
	require.Equal(t, 3, v)

	v, ok = buf.Pop()
	require.False(t, ok)
	require.Equal(t, 0, v)
}

func TestBufferChNotification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	buf := MakeBuffer[int]()

	// Push sends a notification.
	buf.Push(10)
	select {
	case <-buf.Ch():
	default:
		t.Fatal("expected notification after Push")
	}

	// Channel is drained; no spurious notification.
	select {
	case <-buf.Ch():
		t.Fatal("unexpected notification")
	default:
	}

	// Pop the earlier Push(10) so we start fresh.
	v, ok := buf.Pop()
	require.True(t, ok)
	require.Equal(t, 10, v)

	// Pop with remaining items re-pings the channel.
	buf.Push(20)
	buf.Push(30)
	// Drain the notification from the pushes.
	select {
	case <-buf.Ch():
	default:
	}

	v, ok = buf.Pop()
	require.True(t, ok)
	require.Equal(t, 20, v)

	// Pop should have re-pinged since 30 is still in the buffer.
	select {
	case <-buf.Ch():
	default:
		t.Fatal("expected re-ping after Pop with remaining items")
	}

	v, ok = buf.Pop()
	require.True(t, ok)
	require.Equal(t, 30, v)

	// No re-ping when buffer is empty after pop.
	select {
	case <-buf.Ch():
		t.Fatal("unexpected notification after draining buffer")
	default:
	}
}

// TestBufferConcurrentSinglePop matches the production usage pattern: pop
// exactly one item per Ch() wake, relying on Pop's re-ping for remaining
// items.
func TestBufferConcurrentSinglePop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := MakeBuffer[int]()
	const numPushers = 4
	const pushesPerGoroutine = 1000

	var wg sync.WaitGroup
	for i := range numPushers {
		wg.Go(func() {
			rng := rand.New(rand.NewSource(int64(i)))
			for j := range pushesPerGoroutine {
				time.Sleep(time.Duration(rng.Int63n(int64(10 * time.Microsecond))))
				buf.Push(i*pushesPerGoroutine + j)
			}
		})
	}

	var popped syncutil.Set[int]
	var popWg sync.WaitGroup
	done := make(chan struct{})
	popWg.Go(func() {
		rng := rand.New(rand.NewSource(99))
		for {
			time.Sleep(time.Duration(rng.Int63n(int64(100 * time.Microsecond))))
			select {
			case <-buf.Ch():
				v, ok := buf.Pop()
				if ok {
					popped.Add(v)
				}
			case <-done:
				for {
					v, ok := buf.Pop()
					if !ok {
						return
					}
					popped.Add(v)
				}
			}
		}
	})

	wg.Wait()
	close(done)
	popWg.Wait()

	for i := range numPushers * pushesPerGoroutine {
		require.True(t, popped.Contains(i), "missing value %d", i)
	}
}
