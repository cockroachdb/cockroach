// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package semaphore

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/require"
)

const maxSleep = 1 * time.Millisecond

func HammerWeighted(t *testing.T, sem *Weighted, n int64, loops int) {
	for i := 0; i < loops; i++ {
		require.Nil(t, sem.AcquireN(context.Background(), n))
		time.Sleep(time.Duration(rand.Int63n(int64(maxSleep/time.Nanosecond))) * time.Nanosecond)
		sem.ReleaseN(n)
	}
}

func TestWeighted(t *testing.T) {
	t.Parallel()

	n := runtime.GOMAXPROCS(0)
	loops := 10000 / n
	sem := NewWeighted(int64(n))
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			HammerWeighted(t, sem, int64(i), loops)
		}()
	}
	wg.Wait()
}

func TestWeightedPanic(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("release of an unacquired weighted semaphore did not panic")
		}
	}()
	w := NewWeighted(1)
	w.ReleaseN(1)
}

func TestWeightedTryAcquire(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sem := NewWeighted(2)
	tries := []bool{}
	require.Nil(t, sem.AcquireN(ctx, 1))
	tries = append(tries, sem.TryAcquireN(1))
	tries = append(tries, sem.TryAcquireN(1))

	sem.ReleaseN(2)

	tries = append(tries, sem.TryAcquireN(1))
	require.Nil(t, sem.AcquireN(ctx, 1))
	tries = append(tries, sem.TryAcquireN(1))

	want := []bool{true, false, true, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d]: got %t, want %t", i, tries[i], want[i])
		}
	}
}

func TestWeightedAcquire(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sem := NewWeighted(2)
	tryAcquire := func(n int64) bool {
		tryCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		return sem.AcquireN(tryCtx, n) == nil
	}

	tries := []bool{}
	require.Nil(t, sem.AcquireN(ctx, 1))
	tries = append(tries, tryAcquire(1))
	tries = append(tries, tryAcquire(1))

	sem.ReleaseN(2)

	tries = append(tries, tryAcquire(1))
	require.Nil(t, sem.AcquireN(ctx, 1))
	tries = append(tries, tryAcquire(1))

	want := []bool{true, false, true, false}
	for i := range tries {
		if tries[i] != want[i] {
			t.Errorf("tries[%d]: got %t, want %t", i, tries[i], want[i])
		}
	}
}

func TestWeightedDoesntBlockIfTooBig(t *testing.T) {
	t.Parallel()

	const n = 2
	sem := NewWeighted(n)
	{
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			require.Equal(t, context.Canceled, sem.AcquireN(ctx, n+1))
		}()
	}

	g, ctx := errgroup.WithContext(context.Background())
	for i := n * 3; i > 0; i-- {
		g.Go(func() error {
			err := sem.AcquireN(ctx, 1)
			if err == nil {
				time.Sleep(1 * time.Millisecond)
				sem.ReleaseN(1)
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		t.Errorf("NewWeighted(%v) failed to AcquireCtx(_, 1) with AcquireCtx(_, %v) pending", n, n+1)
	}
}

// TestLargeAcquireDoesntStarve times out if a large call to Acquire starves.
// Merely returning from the test function indicates success.
func TestLargeAcquireDoesntStarve(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	n := int64(runtime.GOMAXPROCS(0))
	sem := NewWeighted(n)
	running := true

	var wg sync.WaitGroup
	wg.Add(int(n))
	for i := n; i > 0; i-- {
		require.Nil(t, sem.AcquireN(ctx, 1))
		go func() {
			defer func() {
				sem.ReleaseN(1)
				wg.Done()
			}()
			for running {
				time.Sleep(1 * time.Millisecond)
				sem.ReleaseN(1)
				require.Nil(t, sem.AcquireN(ctx, 1))
			}
		}()
	}

	require.Nil(t, sem.AcquireN(ctx, n))
	running = false
	sem.ReleaseN(n)
	wg.Wait()
}

func TestResize(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sem := NewWeighted(10)
	require.Equal(t, int64(10), sem.size)
	require.Equal(t, int64(0), sem.cur)
	require.Equal(t, int64(0), sem.drain)

	require.Nil(t, sem.AcquireN(ctx, 5))
	require.Equal(t, int64(10), sem.size)
	require.Equal(t, int64(5), sem.cur)
	require.Equal(t, int64(0), sem.drain)

	sem.Resize(15)
	require.Equal(t, int64(15), sem.size)
	require.Equal(t, int64(5), sem.cur)
	require.Equal(t, int64(0), sem.drain)

	sem.ReleaseN(5)
	require.Equal(t, int64(15), sem.size)
	require.Equal(t, int64(0), sem.cur)
	require.Equal(t, int64(0), sem.drain)

	require.Nil(t, sem.AcquireN(ctx, 10))
	require.Equal(t, int64(15), sem.size)
	require.Equal(t, int64(10), sem.cur)
	require.Equal(t, int64(0), sem.drain)

	sem.Resize(5)
	require.Equal(t, int64(5), sem.size)
	require.Equal(t, int64(5), sem.cur)
	require.Equal(t, int64(5), sem.drain)

	// Acquisition should block until resized properly.
	allocC := make(chan struct{})
	go func() {
		require.Nil(t, sem.AcquireN(ctx, 10))
		sem.ReleaseN(10)
		close(allocC)
	}()
	checkBlocked := func() {
		select {
		case <-allocC:
			t.Fatal("unexpected acquisition")
		default:
		}
	}
	checkBlocked()

	sem.Resize(2)
	require.Equal(t, int64(2), sem.size)
	require.Equal(t, int64(2), sem.cur)
	require.Equal(t, int64(8), sem.drain)
	checkBlocked()

	sem.Resize(8)
	require.Equal(t, int64(8), sem.size)
	require.Equal(t, int64(8), sem.cur)
	require.Equal(t, int64(2), sem.drain)
	checkBlocked()

	try := sem.TryAcquireN(2)
	require.False(t, try)

	sem.Resize(18)
	require.Equal(t, int64(18), sem.size)
	require.Equal(t, int64(10), sem.cur)
	require.Equal(t, int64(0), sem.drain)
	checkBlocked()

	try = sem.TryAcquireN(2)
	require.True(t, try)
	require.Equal(t, int64(18), sem.size)
	require.Equal(t, int64(12), sem.cur)
	require.Equal(t, int64(0), sem.drain)

	sem.ReleaseN(2)
	require.Equal(t, int64(18), sem.size)
	require.Equal(t, int64(10), sem.cur)
	require.Equal(t, int64(0), sem.drain)

	sem.Resize(20)
	<-allocC
	require.Equal(t, int64(20), sem.size)
	require.Equal(t, int64(10), sem.cur)
	require.Equal(t, int64(0), sem.drain)

	sem.Resize(5)
	require.Equal(t, int64(5), sem.size)
	require.Equal(t, int64(5), sem.cur)
	require.Equal(t, int64(5), sem.drain)

	sem.ReleaseN(10)
	require.Equal(t, int64(5), sem.size)
	require.Equal(t, int64(0), sem.cur)
	require.Equal(t, int64(0), sem.drain)
}
