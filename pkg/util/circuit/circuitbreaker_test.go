// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package circuit

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func testLogBridge(t *testing.T) (*redact.StringBuilder, *EventLogger) {
	var allBuf redact.StringBuilder
	return &allBuf, &EventLogger{Log: func(buf redact.StringBuilder) {
		t.Log(buf)
		allBuf.Printf("%s\n", buf)
	}}
}

func TestBreaker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	allBuf, eh := testLogBridge(t)
	var report func(err error)
	var done func()
	b := NewBreaker(Options{
		Name:       "mybreaker",
		ShouldTrip: func(err error) error { return err },
		AsyncProbe: func(_report func(error), _done func()) {
			if report != nil || done != nil {
				t.Error("probe launched twice")
			}
			report = _report
			done = _done
		},
		EventHandler: eh,
	})
	ch, errFn := b.Signal()
	select {
	case <-ch:
		t.Fatalf("channel should be open")
	default:
	}
	// Nobody should call errFn in this situation, but it's fine if they do.
	require.Equal(t, ErrBreakerOpen(), errFn())
	require.NoError(t, b.Err())
	require.Nil(t, report)
	require.Nil(t, done)
	boomErr := errors.New("boom")
	require.True(t, errors.Is(b.Report(boomErr), ErrBreakerOpen()))
	require.NotNil(t, report)
	require.NotNil(t, done)
	select {
	case <-ch:
		require.True(t, errors.Is(errFn(), boomErr))
	default:
		t.Fatal("expected channel to be closed")
	}

	// New signal channel should similarly be closed
	// and report the error.
	ch, errFn = b.Signal()
	select {
	case <-ch:
		require.True(t, errors.Is(errFn(), boomErr))
	default:
		t.Fatal("expected channel to be closed")
	}

	{
		err := b.Err()
		require.True(t, errors.Is(err, ErrBreakerOpen()), "%+v", err)
		// Feeding the output error into the breaker again should not create
		// longer error chains (check pointer equality to verify that nothing
		// changed).
		b.Report(err)
		require.Equal(t, err, b.Err())
		b.Report(errors.Wrap(b.Err(), "more stuff"))
		require.Equal(t, err, b.Err())
	}
	b.Reset()
	require.NoError(t, b.Err())
	// errFn from above should not return nil now, as per the
	// contract on Signal(). However, it has to fall back to
	// ErrBreakerOpen() as boomErr is now wiped from the Breaker.
	select {
	case <-ch:
		require.True(t, errors.Is(errFn(), ErrBreakerOpen()))
	default:
		t.Fatal("expected channel to be closed")
	}

	// Channel is open again with the default fallback error.
	ch, errFn = b.Signal()
	select {
	case <-ch:
		t.Fatal("channel unexpectedly closed")
	default:
		require.True(t, errors.Is(errFn(), ErrBreakerOpen()))
	}

	{
		// The probe reports an error. That error should update that returned by the
		// breaker.
		refErr := errors.New("probe error")
		report(refErr)
		err := b.Err()
		require.True(t, errors.Is(err, refErr), "%+v not marked as %+v", err, refErr)
	}

	{
		// An error is passed to `b.Report`. This is somewhat unusual, after all
		// the point of the breaker is to stop people from trying, but if it
		// happens we update the breaker's error just the same.
		refErr := errors.New("client error")
		b.Report(refErr)
		err := b.Err()
		require.True(t, errors.Is(err, refErr), "%+v not marked as %+v", err, refErr)
	}

	// Can't reset the breaker by calling `b.Report(nil)`. It's the probe's
	// job to reset the breaker.
	b.Report(nil)
	require.Error(t, b.Err())

	{
		// The probe finishes. Breaker is still open, so next time anyone
		// observes that, another probe is launched.
		done()
		report, done = nil, nil
		require.Error(t, b.Err())
		require.NotNil(t, report)
		require.NotNil(t, done)
	}

	{
		// The probe reports success this time. The breaker should reset and
		// stay that way.
		report(nil)
		require.NoError(t, b.Err())
		// The probe finishes.
		done()
		require.NoError(t, b.Err())
		require.NoError(t, b.Err())
	}

	datadriven.RunTest(t,
		filepath.Join("testdata", t.Name()+".txt"),
		func(t *testing.T, d *datadriven.TestData) string {
			return allBuf.String()
		})
}

func TestBreakerRealistic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)
	var intn func(int) int
	{
		rnd, seed := randutil.NewTestRand()
		t.Logf("seed: %d", seed)
		var mu syncutil.Mutex
		intn = func(n int) int {
			mu.Lock()
			defer mu.Unlock()
			return rnd.Intn(n)
		}
	}
	const backoff = 1 * time.Millisecond
	_, eh := testLogBridge(t)
	b := NewBreaker(Options{
		Name: "testbreaker",
		ShouldTrip: func(err error) error {
			return err
		},
		EventHandler: eh,
		AsyncProbe: func(report func(error), done func()) {
			if err := s.RunAsyncTask(ctx, "probe", func(ctx context.Context) {
				defer done()

				for i := 1; ; i++ {
					select {
					case <-time.After(5 * backoff):
						// Unblock the breaker 33% of the time.
						if intn(3) < 1 {
							report(nil /* err */)
							return
						}
						if intn(2) < 1 {
							// Report new error 50% of the time.
							report(errors.Errorf("probe %d failed", i))
						}
					case <-s.ShouldQuiesce():
						return
					}
				}
			}); err != nil {
				report(err)
				done()
			}
		},
	})
	const numOpsPerWorker = 10
	const targetTripResetCycles = 5

	var numTrips int32 // atomic
	worker := func(ctx context.Context, t interface {
		Logf(string, ...interface{})
	}, idx int) error {
		defer t.Logf("w%d: worker done", idx)
		var doneOps int
		for i := 0; doneOps < numOpsPerWorker; i++ {
			runtime.Gosched()
			t.Logf("w%d: attempting op%d (attempt %d)", idx, doneOps, i+1)
			if err := b.Err(); err != nil {
				t.Logf("w%d: breaker open; backing off: %s", idx, err)
				time.Sleep(backoff)
				continue
			}
			// 16.5% chance of handing an error to the breaker, but only a few times
			// (so that the test is guaranteed to finish in a bounded amount of time
			// in practice despite possible parameter changes)
			if intn(6) < 1 && atomic.AddInt32(&numTrips, 1) < targetTripResetCycles {
				err := errors.Errorf("error triggered by w%d/op%d", idx, doneOps)
				t.Logf("injecting error: %s", err)
				b.Report(err)
				continue
			}
			doneOps++
			t.Logf("w%d: op%d finished", idx, doneOps)
			i = 0
		}
		return nil
	}

	const numWorkers = 8
	g := ctxgroup.WithContext(ctx)
	for idx := 0; idx < numWorkers; idx++ {
		idx := idx // copy for the goroutine
		g.GoCtx(func(ctx context.Context) error {
			return s.RunTaskWithErr(ctx, "worker", func(ctx context.Context) error {
				return worker(ctx, t, idx)
			})
		})
	}
	require.NoError(t, g.Wait())
	ch, err := b.Signal()

	// Make sure that at the end of it all, we're not stuck with a broken signal channel.
	tooLong := time.After(10 * time.Second)
	for {
		select {
		case <-ch:
			time.Sleep(time.Millisecond)
		case <-tooLong:
			t.Fatalf("expected breaker to have reset by now: %s", err())
		default:
			return // test done.
		}
	}
}

func ExampleBreaker_Signal() {
	br := NewBreaker(Options{
		Name:       redact.Sprint("Example"),
		ShouldTrip: func(err error) error { return err },
		AsyncProbe: func(_ func(error), done func()) {
			done() // never untrip
		},
		EventHandler: &EventLogger{Log: func(builder redact.StringBuilder) { fmt.Println(builder.String()) }},
	})

	br.Report(errors.New("boom!"))

	ch, err := br.Signal()
	select {
	case <-ch:
		fmt.Println("Signaled with error:", err())
	case <-time.After(10 * time.Second):
		fmt.Println("timed out...")
		return
	}
	_ = br.Report(errors.New("bang!"))
	br.Reset()
	fmt.Println("Err() now returns", br.Err())
	// Output:
	// Example: tripped with error: boom!
	// Signaled with error: boom!
	// Example: now tripped with error: bang! (previously: boom!)
	// Example: breaker reset
	// Err() now returns <nil>
}
