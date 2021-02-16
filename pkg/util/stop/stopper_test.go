// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stop_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStopper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	running := make(chan struct{})
	waiting := make(chan struct{})
	cleanup := make(chan struct{})
	ctx := context.Background()

	_ = s.RunAsyncTask(ctx, "task", func(context.Context) {
		<-running
	})

	go func() {
		s.Stop(ctx)
		close(waiting)
		<-cleanup
	}()

	<-s.ShouldQuiesce()
	select {
	case <-waiting:
		close(cleanup)
		t.Fatal("expected stopper to have blocked")
	case <-time.After(100 * time.Millisecond):
		// Expected.
	}
	close(running)
	select {
	case <-waiting:
		// Success.
	case <-time.After(time.Second):
		close(cleanup)
		t.Fatal("stopper should have finished waiting")
	}
	close(cleanup)
}

type blockingCloser struct {
	block chan struct{}
}

func newBlockingCloser() *blockingCloser {
	return &blockingCloser{block: make(chan struct{})}
}

func (bc *blockingCloser) Unblock() {
	close(bc.block)
}

func (bc *blockingCloser) Close() {
	<-bc.block
}

func TestStopperIsStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	bc := newBlockingCloser()
	s.AddCloser(bc)
	go s.Stop(context.Background())

	select {
	case <-s.ShouldQuiesce():
	case <-time.After(time.Second):
		t.Fatal("stopper should have finished waiting")
	}
	select {
	case <-s.IsStopped():
		t.Fatal("expected blocked closer to prevent stop")
	case <-time.After(100 * time.Millisecond):
		// Expected.
	}
	bc.Unblock()
	select {
	case <-s.IsStopped():
		// Expected
	case <-time.After(time.Second):
		t.Fatal("stopper should have finished stopping")
	}

	s.Stop(context.Background())
}

func TestStopperMultipleTasks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 3
	s := stop.NewStopper()
	ctx := context.Background()

	for i := 0; i < count; i++ {
		require.NoError(t, s.RunAsyncTask(ctx, "task", func(context.Context) {
			<-s.ShouldQuiesce()
		}))
	}

	done := make(chan struct{})
	go func() {
		s.Stop(ctx)
		close(done)
	}()

	<-done
}

func TestStopperStartFinishTasks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)

	if err := s.RunTask(ctx, "test", func(ctx context.Context) {
		go s.Stop(ctx)

		select {
		case <-s.IsStopped():
			t.Fatal("stopper not fully stopped")
		case <-time.After(100 * time.Millisecond):
			// Expected.
		}
	}); err != nil {
		t.Error(err)
	}
	select {
	case <-s.IsStopped():
		// Success.
	case <-time.After(time.Second):
		t.Fatal("stopper should be ready to stop")
	}
}

// TestStopperQuiesce tests coordinate quiesce with Quiesce.
func TestStopperQuiesce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var stoppers []*stop.Stopper
	for i := 0; i < 3; i++ {
		stoppers = append(stoppers, stop.NewStopper())
	}
	var quiesceDone []chan struct{}
	var runTaskDone []chan struct{}
	ctx := context.Background()

	for _, s := range stoppers {
		thisStopper := s
		qc := make(chan struct{})
		quiesceDone = append(quiesceDone, qc)
		sc := make(chan struct{})
		runTaskDone = append(runTaskDone, sc)
		go func() {
			// Wait until Quiesce() is called.
			<-qc
			err := thisStopper.RunTask(ctx, "inner", func(context.Context) {})
			if !errors.HasType(err, (*roachpb.NodeUnavailableError)(nil)) {
				t.Error(err)
			}
			// Make the stoppers call Stop().
			close(sc)
			<-thisStopper.ShouldQuiesce()
		}()
	}

	done := make(chan struct{})
	go func() {
		for _, s := range stoppers {
			s.Quiesce(ctx)
		}
		// Make the tasks call RunTask().
		for _, qc := range quiesceDone {
			close(qc)
		}

		// Wait until RunTask() is called.
		for _, sc := range runTaskDone {
			<-sc
		}

		for _, s := range stoppers {
			s.Stop(ctx)
			<-s.IsStopped()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Errorf("timed out waiting for stop")
	}
}

type testCloser bool

func (tc *testCloser) Close() {
	*tc = true
}

func TestStopperClosers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	var tc1, tc2 testCloser
	s.AddCloser(&tc1)
	s.AddCloser(&tc2)
	s.Stop(context.Background())
	if !bool(tc1) || !bool(tc2) {
		t.Errorf("expected true & true; got %t & %t", tc1, tc2)
	}
}

func TestStopperCloserConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const trials = 10
	for i := 0; i < trials; i++ {
		s := stop.NewStopper()
		var tc1 testCloser

		// Add Closer and Stop concurrently. There should be
		// no circumstance where the Closer is not called.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			s.AddCloser(&tc1)
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			s.Stop(context.Background())
		}()
		wg.Wait()

		if !tc1 {
			t.Errorf("expected true; got %t", tc1)
		}
	}
}

func TestStopperNumTasks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	defer s.Stop(context.Background())
	var tasks []chan bool
	for i := 0; i < 3; i++ {
		c := make(chan bool)
		tasks = append(tasks, c)
		if err := s.RunAsyncTask(context.Background(), "test", func(_ context.Context) {
			// Wait for channel to close
			<-c
		}); err != nil {
			t.Fatal(err)
		}
		if numTasks := s.NumTasks(); numTasks != i+1 {
			t.Errorf("stopper should have %d running tasks, got %d", i+1, numTasks)
		}
	}
	for i, c := range tasks {
		// Close the channel to let the task proceed.
		close(c)
		expNum := len(tasks[i+1:])
		testutils.SucceedsSoon(t, func() error {
			if nt := s.NumTasks(); nt != expNum {
				return errors.Errorf("%d: stopper should have %d running tasks, got %d", i, expNum, nt)
			}
			return nil
		})
	}
}

// TestStopperRunTaskPanic ensures that a panic handler can recover panicking
// tasks, and that no tasks are leaked when they panic.
func TestStopperRunTaskPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ch := make(chan interface{})
	s := stop.NewStopper(stop.OnPanic(func(v interface{}) {
		ch <- v
	}))
	defer s.Stop(context.Background())
	// If RunTask were not panic-safe, Stop() would deadlock.
	type testFn func()
	explode := func(context.Context) { panic(ch) }
	ctx := context.Background()
	for i, test := range []testFn{
		func() {
			_ = s.RunTask(ctx, "test", explode)
		},
		func() {
			_ = s.RunAsyncTask(ctx, "test", func(ctx context.Context) { explode(ctx) })
		},
		func() {
			_ = s.RunLimitedAsyncTask(
				context.Background(), "test",
				quotapool.NewIntPool("test", 1),
				true, /* wait */
				func(ctx context.Context) { explode(ctx) },
			)
		},
	} {
		go test()
		recovered := <-ch
		if recovered != ch {
			t.Errorf("%d: unexpected recovered value: %+v", i, recovered)
		}
	}
}

func TestStopperWithCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	ctx := context.Background()
	ctx1, _ := s.WithCancelOnQuiesce(ctx)
	ctx3, cancel3 := s.WithCancelOnQuiesce(ctx)

	if err := ctx1.Err(); err != nil {
		t.Fatalf("should not be canceled: %v", err)
	}
	if err := ctx3.Err(); err != nil {
		t.Fatalf("should not be canceled: %v", err)
	}

	cancel3()
	if err := ctx1.Err(); err != nil {
		t.Fatalf("should not be canceled: %v", err)
	}
	if err := ctx3.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("should be canceled: %v", err)
	}

	s.Quiesce(ctx)
	if err := ctx1.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("should be canceled: %v", err)
	}

	s.Stop(ctx)
}

func TestStopperWithCancelConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const trials = 10
	for i := 0; i < trials; i++ {
		s := stop.NewStopper()
		ctx := context.Background()
		var ctx1 context.Context

		// Tie a context to the Stopper and Stop concurrently. There should
		// be no circumstance where either Context is not canceled.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			ctx1, _ = s.WithCancelOnQuiesce(ctx)
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			s.Stop(ctx)
		}()
		wg.Wait()

		if err := ctx1.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("should be canceled: %v", err)
		}
	}
}

func TestStopperShouldQuiesce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	runningTask := make(chan struct{})
	waiting := make(chan struct{})
	cleanup := make(chan struct{})
	ctx := context.Background()

	// Run an asynchronous task. A stopper which has been Stop()ed will not
	// close it's ShouldStop() channel until all tasks have completed. This task
	// will complete when the "runningTask" channel is closed.
	if err := s.RunAsyncTask(ctx, "test", func(_ context.Context) {
		<-runningTask
	}); err != nil {
		t.Fatal(err)
	}

	go func() {
		s.Stop(ctx)
		close(waiting)
		<-cleanup
	}()

	// The ShouldQuiesce() channel should close as soon as the stopper is
	// Stop()ed.
	<-s.ShouldQuiesce()
	// After completing the running task, the ShouldStop() channel should
	// now close.
	close(runningTask)
	select {
	case <-s.IsStopped():
	// Good.
	case <-time.After(10 * time.Second):
		t.Fatal("stopper did not fully stop in time")
	}
	<-waiting
	close(cleanup)
}

func TestStopperRunLimitedAsyncTask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	defer s.Stop(context.Background())

	const maxConcurrency = 5
	const numTasks = maxConcurrency * 3
	sem := quotapool.NewIntPool("test", maxConcurrency)
	taskSignal := make(chan struct{}, maxConcurrency)
	var mu syncutil.Mutex
	concurrency := 0
	peakConcurrency := 0
	var wg sync.WaitGroup

	f := func(_ context.Context) {
		mu.Lock()
		concurrency++
		if concurrency > peakConcurrency {
			peakConcurrency = concurrency
		}
		mu.Unlock()
		<-taskSignal
		mu.Lock()
		concurrency--
		mu.Unlock()
		wg.Done()
	}
	go func() {
		// Loop until the desired peak concurrency has been reached.
		for {
			mu.Lock()
			c := concurrency
			mu.Unlock()
			if c >= maxConcurrency {
				break
			}
			time.Sleep(time.Millisecond)
		}
		// Then let the rest of the async tasks finish quickly.
		for i := 0; i < numTasks; i++ {
			taskSignal <- struct{}{}
		}
	}()

	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		if err := s.RunLimitedAsyncTask(
			context.Background(), "test", sem, true /* wait */, f,
		); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if concurrency != 0 {
		t.Fatalf("expected 0 concurrency at end of test but got %d", concurrency)
	}
	if peakConcurrency != maxConcurrency {
		t.Fatalf("expected peak concurrency %d to equal max concurrency %d",
			peakConcurrency, maxConcurrency)
	}

	sem = quotapool.NewIntPool("test", 1)
	_, err := sem.Acquire(context.Background(), 1)
	require.NoError(t, err)
	err = s.RunLimitedAsyncTask(
		context.Background(), "test", sem, false /* wait */, func(_ context.Context) {
		},
	)
	if !errors.Is(err, stop.ErrThrottled) {
		t.Fatalf("expected %v; got %v", stop.ErrThrottled, err)
	}
}

// This test ensures that if a quotapool has been registered as a Closer for
// the stopper and the stopper is Quiesced then blocked tasks will return
// ErrUnavailable.
func TestStopperRunLimitedAsyncTaskCloser(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)

	sem := quotapool.NewIntPool("test", 1)
	s.AddCloser(sem.Closer("stopper"))
	_, err := sem.Acquire(ctx, 1)
	require.NoError(t, err)
	go func() {
		time.Sleep(time.Millisecond)
		s.Stop(ctx)
	}()
	err = s.RunLimitedAsyncTask(ctx, "foo", sem, true /* wait */, func(context.Context) {})
	require.Equal(t, stop.ErrUnavailable, err)
	<-s.IsStopped()
}

func TestStopperRunLimitedAsyncTaskCancelContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	defer s.Stop(context.Background())

	const maxConcurrency = 5
	sem := quotapool.NewIntPool("test", maxConcurrency)

	// Synchronization channels.
	workersDone := make(chan struct{})
	workerStarted := make(chan struct{})

	var workersRun int32
	var workersCanceled int32

	ctx, cancel := context.WithCancel(context.Background())
	f := func(ctx context.Context) {
		atomic.AddInt32(&workersRun, 1)
		workerStarted <- struct{}{}
		<-ctx.Done()
	}

	// This loop will block when the semaphore is filled.
	if err := s.RunAsyncTask(ctx, "test", func(ctx context.Context) {
		for i := 0; i < maxConcurrency*2; i++ {
			if err := s.RunLimitedAsyncTask(ctx, "test", sem, true, f); err != nil {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				atomic.AddInt32(&workersCanceled, 1)
			}
		}
		close(workersDone)
	}); err != nil {
		t.Fatal(err)
	}

	// Ensure that the semaphore fills up, leaving maxConcurrency workers
	// waiting for context cancelation.
	for i := 0; i < maxConcurrency; i++ {
		<-workerStarted
	}

	// Cancel the context, which should result in all subsequent attempts to
	// queue workers failing.
	cancel()
	<-workersDone

	if a, e := atomic.LoadInt32(&workersRun), int32(maxConcurrency); a != e {
		t.Fatalf("%d workers ran before context close, expected exactly %d", a, e)
	}
	if a, e := atomic.LoadInt32(&workersCanceled), int32(maxConcurrency); a != e {
		t.Fatalf("%d workers canceled after context close, expected exactly %d", a, e)
	}
}

func maybePrint(context.Context) {
	if testing.Verbose() { // This just needs to be complicated enough not to inline.
		fmt.Println("blah")
	}
}

func BenchmarkDirectCall(b *testing.B) {
	defer leaktest.AfterTest(b)()
	s := stop.NewStopper()
	ctx := context.Background()
	defer s.Stop(ctx)
	for i := 0; i < b.N; i++ {
		maybePrint(ctx)
	}
}

func BenchmarkStopper(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)
	for i := 0; i < b.N; i++ {
		if err := s.RunTask(ctx, "test", maybePrint); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkDirectCallPar(b *testing.B) {
	defer leaktest.AfterTest(b)()
	s := stop.NewStopper()
	ctx := context.Background()
	defer s.Stop(ctx)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			maybePrint(ctx)
		}
	})
}

func BenchmarkStopperPar(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.RunTask(ctx, "test", maybePrint); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestCancelInCloser(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)

	// This will call the Closer which will call cancel and should
	// not deadlock.
	_, cancel := s.WithCancelOnQuiesce(ctx)
	s.AddCloser(closerFunc(cancel))
	s.Stop(ctx)
}

// closerFunc implements Closer.
type closerFunc func()

func (cf closerFunc) Close() { cf() }
