// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package circuit

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testBridge struct {
	*EventLogger
	trip, probeLaunched, probeDone, reset int
}

func (tb *testBridge) OnTrip(breaker *Breaker, prev, cur error) {
	if prev == nil {
		tb.trip++
	}
	tb.EventLogger.OnTrip(breaker, prev, cur)
}

func (tb *testBridge) OnProbeLaunched(breaker *Breaker) {
	tb.probeLaunched++
	tb.EventLogger.OnProbeLaunched(breaker)
}

func (tb *testBridge) OnProbeDone(breaker *Breaker) {
	tb.probeDone++
	tb.EventLogger.OnProbeDone(breaker)
}

func (tb *testBridge) OnReset(breaker *Breaker, prev error) {
	tb.reset++
	tb.EventLogger.OnReset(breaker, prev)
}

// RequireNumTrippedEqualsNumResets verifies that the number of trip events
// equals the number of reset events. This is an invariant that should hold
// while all breakers are healthy.
func (tb *testBridge) RequireNumTrippedEqualsNumResets(t *testing.T) {
	t.Helper()
	require.Equal(t, tb.reset, tb.trip, "got %d resets, but %d trips (%d probes launched)", tb.reset, tb.trip, tb.probeLaunched)
}

func testLogBridge(t *testing.T) (*redact.StringBuilder, *testBridge) {
	var mu syncutil.Mutex
	var allBuf redact.StringBuilder
	el := &EventLogger{Log: func(buf redact.StringBuilder) {
		mu.Lock()
		defer mu.Unlock()
		allBuf.Printf("%s\n", buf)
		t.Log(buf)
	}}
	tlb := &testBridge{EventLogger: el}
	return &allBuf, tlb
}

func TestBreaker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	allBuf, eh := testLogBridge(t)
	defer eh.RequireNumTrippedEqualsNumResets(t)
	var report func(err error)
	var done func()
	br := NewBreaker(Options{
		Name: "mybreaker",
		AsyncProbe: func(_report func(error), _done func()) {
			if report != nil || done != nil {
				t.Error("probe launched twice")
			}
			report = _report
			done = _done
		},
		EventHandler: eh,
	})
	sig := br.Signal()
	select {
	case <-sig.C():
		t.Fatalf("channel should be open")
	default:
	}
	// Nobody should call errFn in this situation, but it's fine if they do.
	require.NoError(t, sig.Err())
	require.NoError(t, br.Signal().Err())
	require.False(t, sig.IsTripped())
	require.False(t, br.Signal().IsTripped())
	require.Nil(t, report)
	require.Nil(t, done)
	boomErr := errors.New("boom")
	br.Report(boomErr)
	require.ErrorIs(t, br.Signal().Err(), boomErr)
	// NB: this can't use ErrorIs because error marks are a cockroachdb/errors'ism.
	require.True(t, errors.Is(br.Signal().Err(), ErrBreakerOpen))
	require.True(t, sig.IsTripped())
	require.True(t, br.Signal().IsTripped())
	require.NotNil(t, report)
	require.NotNil(t, done)
	select {
	case <-sig.C():
		err := sig.Err()
		require.True(t, errors.Is(err, boomErr), "%+v", err)
	default:
		t.Fatal("expected channel to be closed")
	}

	// New signal channel should similarly be closed
	// and report the error.
	sig = br.Signal()
	select {
	case <-sig.C():
		require.True(t, errors.Is(sig.Err(), boomErr))
	default:
		t.Fatal("expected channel to be closed")
	}

	{
		err := br.Signal().Err()
		require.True(t, errors.Is(err, ErrBreakerOpen), "%+v", err)
		// Feeding the output error into the breaker again should not create
		// longer error chains (check pointer equality to verify that nothing
		// changed).
		br.Report(err)
		require.Equal(t, err, br.Signal().Err())
		br.Report(errors.Wrap(br.Signal().Err(), "more stuff"))
		require.Equal(t, err, br.Signal().Err())
	}
	br.Reset()
	require.NoError(t, br.Signal().Err())
	require.False(t, br.Signal().IsTripped())
	// errFn from above should not return nil now, as per the
	// contract on Signal(). However, it has to fall back to
	// ErrBreakerOpen as boomErr is now wiped from the Breaker.
	select {
	case <-sig.C():
		err := sig.Err()
		require.True(t, errors.Is(err, ErrBreakerOpen), "%+v", err)
	default:
		t.Fatal("expected channel to be closed")
	}

	// Channel is open again.
	sig = br.Signal()
	select {
	case <-sig.C():
		t.Fatal("channel unexpectedly closed")
	default:
		require.NoError(t, sig.Err())
	}

	{
		// The probe reports an error. That error should update that returned by the
		// breaker.
		refErr := errors.New("probe error")
		report(refErr)
		err := br.Signal().Err()
		require.True(t, errors.Is(err, refErr), "%+v not marked as %+v", err, refErr)
		require.True(t, br.Signal().IsTripped())
	}

	{
		// An error is passed to `br.Report`. This is somewhat unusual, after all
		// the point of the breaker is to stop people from trying, but if it
		// happens we update the breaker's error just the same.
		refErr := errors.New("client error")
		br.Report(refErr)
		err := br.Signal().Err()
		require.True(t, errors.Is(err, refErr), "%+v not marked as %+v", err, refErr)
		require.True(t, br.Signal().IsTripped())
	}

	// Can't reset the breaker by calling `br.Report(nil)`. It's the probe's
	// job to reset the breaker.
	br.Report(nil)
	require.Error(t, br.Signal().Err())
	require.True(t, br.Signal().IsTripped())

	{
		// The probe finishes. Breaker is still open, so next time anyone
		// observes that, another probe is launched. However, a probe
		// is not launched by IsTripped().
		done()
		report, done = nil, nil
		require.True(t, br.Signal().IsTripped())
		require.Nil(t, report)
		require.Nil(t, done)
		require.Error(t, br.Signal().Err())
		require.NotNil(t, report)
		require.NotNil(t, done)
	}

	{
		// The probe reports success this time. The breaker should reset and
		// stay that way.
		report(nil)
		require.NoError(t, br.Signal().Err())
		require.False(t, br.Signal().IsTripped())
		// The probe finishes.
		done()
		require.NoError(t, br.Signal().Err())
		require.NoError(t, br.Signal().Err())
		require.False(t, br.Signal().IsTripped())
	}

	datadriven.RunTest(t,
		datapathutils.TestDataPath(t, t.Name()+".txt"),
		func(t *testing.T, d *datadriven.TestData) string {
			return allBuf.String()
		})
}

// TestBreakerProbeIsReactive verifies that probes are only launched on Report
// and when a client observes Err() != nil.
func TestBreakerProbeIsReactive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	allBuf, eh := testLogBridge(t)
	defer eh.RequireNumTrippedEqualsNumResets(t)
	var numProbes int32 // atomic
	br := NewBreaker(Options{
		Name: "mybreaker",
		AsyncProbe: func(_report func(error), _done func()) {
			n := atomic.AddInt32(&numProbes, 1)
			_report(errors.Errorf("probe error #%d", n))
			_done()
		},
		EventHandler: eh,
	})
	sig := br.Signal()

	br.Report(errors.New("boom"))

	requireNumProbes := func(t *testing.T, exp int32) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			if act := atomic.LoadInt32(&numProbes); exp != act {
				return errors.Errorf("expected %d probes, found %d", exp, act)
			}
			return nil
		})
	}

	// If the probe ran in a busy loop, we'd quickly race past 1 and this test
	// would be flaky. If the probe never ran, it would fail too.
	time.Sleep(time.Millisecond)
	_ = sig.C() // should not trigger another probe
	time.Sleep(time.Millisecond)
	requireNumProbes(t, 1)

	select {
	case <-sig.C():
	default:
		t.Fatal("Breaker not tripped")
	}

	require.Error(t, sig.Err()) // should trigger probe

	requireNumProbes(t, 2)

	datadriven.RunTest(t,
		datapathutils.TestDataPath(t, t.Name()+".txt"),
		func(t *testing.T, d *datadriven.TestData) string {
			return allBuf.String()
		})
	br.Reset()
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
	defer eh.RequireNumTrippedEqualsNumResets(t)
	br := NewBreaker(Options{
		Name:         "testbreaker",
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
		Errorf(string, ...interface{})
	}, idx int) error {
		defer t.Logf("w%d: worker done", idx)
		var doneOps int
		for i := 0; doneOps < numOpsPerWorker; i++ {
			runtime.Gosched()
			t.Logf("w%d: attempting op%d (attempt %d)", idx, doneOps, i+1)
			sig := br.Signal()
			if err := sig.Err(); err != nil {
				t.Logf("w%d: breaker open; backing off: %s", idx, err)
				time.Sleep(backoff)
				assert.Equal(t, err, sig.Err()) // invariant: non-nil errFn() never changes
				continue
			}
			// 16.5% chance of handing an error to the breaker, but only a few times
			// (so that the test is guaranteed to finish in a bounded amount of time
			// in practice despite possible parameter changes)
			if intn(6) < 1 && atomic.AddInt32(&numTrips, 1) < targetTripResetCycles {
				err := errors.Errorf("error triggered by w%d/op%d", idx, doneOps)
				t.Logf("injecting error: %s", err)
				br.Report(err)
				continue
			}
			select {
			case <-time.After(time.Duration(intn(int(backoff)))):
			case <-sig.C():
				err := sig.Err()
				t.Logf("w%d: breaker open; aborted ongoing operation and backing off: %s", idx, err)
				time.Sleep(backoff)
				assert.Equal(t, err, sig.Err()) // invariant: non-nil errFn() never changes
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
	// The way the test is set up, we should finish with a healthy breaker.
	testutils.SucceedsSoon(t, func() error {
		return br.Signal().Err()
	})
}

func TestBreaker_Probe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, eh := testLogBridge(t)
	defer eh.RequireNumTrippedEqualsNumResets(t)
	var ran bool
	br := NewBreaker(Options{
		Name: "mybreaker",
		AsyncProbe: func(report func(error), done func()) {
			ran = true
			done()
		},
		EventHandler: eh,
	})
	br.Probe()
	testutils.SucceedsSoon(t, func() error {
		if !ran {
			return errors.New("probe did not run")
		}
		return nil
	})
}

func TestTestingSetTripped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, tl := testLogBridge(t)

	// Spawn a never-terminating probe just to prove that we can simulate a
	// breaker trip without communicating with the probe at all.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	br := NewBreaker(Options{
		Name: "test",
		AsyncProbe: func(report func(error), done func()) {
			report(nil)
			<-ctx.Done()
		},
		EventHandler: tl,
	})

	require.NoError(t, br.Signal().Err())
	errBoom := errors.New("boom")
	undo := TestingSetTripped(br, errBoom)
	require.ErrorIs(t, br.Signal().Err(), errBoom)
	undo()
	require.NoError(t, br.Signal().Err())
	require.Zero(t, tl.trip)
}

func BenchmarkBreaker_Signal(b *testing.B) {
	br := NewBreaker(Options{
		Name: redact.Sprint("Breaker"),
		AsyncProbe: func(_ func(error), done func()) {
			done() // never untrip
		},
	})

	// The point of this benchmark is to verify the absence of allocations when
	// calling Signal.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		eac := br.Signal()
		_ = eac.C()
		if err := eac.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func ExampleBreaker_Signal() {
	br := NewBreaker(Options{
		Name: redact.Sprint("Breaker"),
		AsyncProbe: func(_ func(error), done func()) {
			done() // never untrip
		},
		EventHandler: &EventLogger{Log: func(builder redact.StringBuilder) { fmt.Println(builder.String()) }},
	})

	launchWork := func() <-chan time.Time {
		return time.After(time.Nanosecond)
	}

	for i := 0; i < 3; i++ {
		sig := br.Signal()
		if err := sig.Err(); err != nil {
			// Fail-fast - don't even begin work since the breaker indicates that
			// there is a problem.
			fmt.Println(err)
			return // maybe retry later
		}
		workDoneCh := launchWork()
		select {
		case <-workDoneCh:
			fmt.Println("work item", i+1, "done")
		case <-sig.C():
			// Abort work as the breaker is now tripped.
			fmt.Println(sig.Err())
			return // maybe retry later
		}
	}

	// Output:
	// work item 1 done
	// work item 2 done
	// work item 3 done
}
