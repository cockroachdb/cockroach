// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package future_test

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCompletedFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := future.MakeCompletedFuture("future is here")

	require.Equal(t, "future is here", future.MakeAwaitableFuture(f).Get())

	expectErr := errors.New("something bad happened")
	ef := future.MakeCompletedErrorFuture(expectErr)
	require.ErrorIs(t, future.MakeAwaitableFuture(ef).Get(), expectErr)
}

func TestFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := future.Make[int]()

	// More than one WhenReady handler may be added.
	ready1 := make(chan struct{})
	ready2 := make(chan struct{})
	f.WhenReady(func(v int) { close(ready1) })
	f.WhenReady(func(v int) { close(ready2) })
	var fErr future.ErrorFuture

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		f.Set(42)
		fErr.Set(context.Canceled)
	}()

	mustBeReady := func(ch chan struct{}) error {
		soon := 5 * time.Second
		if util.RaceEnabled {
			soon *= 5
		}
		select {
		case <-ch:
			return nil
		case <-time.After(soon):
			return errors.New("channel not ready")
		}
	}

	v, err := future.Wait(context.Background(), f)
	require.NoError(t, err)
	require.Equal(t, 42, v)
	require.NoError(t, mustBeReady(ready1))
	require.NoError(t, mustBeReady(ready2))

	vErr, err := future.Wait(context.Background(), &fErr)
	require.NoError(t, err)
	require.ErrorIs(t, vErr, context.Canceled)
}

func TestThereCanBeOnlyOne(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := future.Make[int]()
	f.Set(1)
	require.Error(t, future.MustSet(f, 42))
}

func TestErrorFuture(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := future.MakeAwaitableFuture(future.Make[error]())

	select {
	case <-f.Done():
		t.Fatal("surprisingly, the future is here")
	default:
	}

	errCh := make(chan error, 1)
	f.WhenReady(func(err error) {
		errCh <- err
	})
	expectErr := errors.Newf("test error")
	f.Set(expectErr)
	require.True(t, errors.Is(<-errCh, expectErr))

	errNow := future.MakeAwaitableFuture(future.MakeCompletedErrorFuture(expectErr))
	require.True(t, errors.Is(errNow.Get(), expectErr))
}

func TestAwaitableFutureRaces(t *testing.T) {
	defer leaktest.AfterTest(t)()

	futures := make([]*future.ErrorFuture, 100)
	for i := range futures {
		futures[i] = &future.ErrorFuture{}
	}

	const numWorkers = 100
	var numWaited int32 = 0 // accessed atomically.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g := ctxgroup.WithContext(ctx)
	for i := 0; i < numWorkers; i++ {
		g.GoCtx(func(ctx context.Context) error {
			for {
				// Pick a future to complete.
				futures[rand.Intn(len(futures))].Set(context.Canceled)

				// Pick a future to wait on.
				f := future.MakeAwaitableFuture(futures[rand.Intn(len(futures))])
				select {
				case <-ctx.Done():
					return nil
				case <-f.Done():
					atomic.AddInt32(&numWaited, 1)
				case <-time.After(100 * time.Millisecond):
					// We might have all threads blocked; so, unblock and complete another
					// future.
				}
			}
		})
	}

	const targetWait = 3000000 // ~10 seconds under race.
	testutils.SucceedsSoon(t, func() error {
		n := atomic.LoadInt32(&numWaited)
		if n >= targetWait {
			return nil
		}
		return errors.Newf("still waiting for %d completed futures; %d so far", targetWait, n)
	})
	cancel()
	_ = g.Wait()
}
