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
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	ctx := context.Background()
	defer s.Stop(ctx)
	var tasks []chan bool
	for i := 0; i < 3; i++ {
		c := make(chan bool)
		tasks = append(tasks, c)
		if err := s.RunAsyncTask(ctx, "test", func(_ context.Context) {
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
	s := stop.NewStopper(stop.OnPanic(func(ctx context.Context, v interface{}) {
		log.Infof(ctx, "recovering from panic")
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
			_ = s.RunAsyncTaskEx(
				context.Background(),
				stop.TaskOpts{
					TaskName:   "test",
					Sem:        quotapool.NewIntPool("test", 1),
					WaitForSem: true,
				},
				func(ctx context.Context) { explode(ctx) },
			)
		},
	} {
		t.Run("", func(t *testing.T) {
			go test()
			recovered := <-ch
			if recovered != ch {
				t.Errorf("%d: unexpected recovered value: %+v", i, recovered)
			}
		})
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
		if err := s.RunAsyncTaskEx(
			context.Background(),
			stop.TaskOpts{
				TaskName:   "test",
				Sem:        sem,
				WaitForSem: true,
			},
			f,
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
	err = s.RunAsyncTaskEx(
		context.Background(),
		stop.TaskOpts{
			TaskName:   "test",
			Sem:        sem,
			WaitForSem: false,
		},
		func(_ context.Context) {},
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
	err = s.RunAsyncTaskEx(ctx,
		stop.TaskOpts{
			TaskName:   "foo",
			Sem:        sem,
			WaitForSem: true,
		},
		func(context.Context) {})
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
			if err := s.RunAsyncTaskEx(ctx,
				stop.TaskOpts{
					TaskName:   "test",
					Sem:        sem,
					WaitForSem: true,
				},
				f); err != nil {
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

// Test that task spans are included or not in the parent's recording based on
// the ChildSpan option.
func TestStopperRunAsyncTaskTracing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tr := tracing.NewTracerWithOpt(context.Background(), tracing.WithTracingMode(tracing.TracingModeActiveSpansRegistry))
	s := stop.NewStopper(stop.WithTracer(tr))

	ctx, getRecAndFinish := tracing.ContextWithRecordingSpan(context.Background(), tr, "parent")
	defer getRecAndFinish()
	root := tracing.SpanFromContext(ctx)
	require.NotNil(t, root)
	traceID := root.TraceID()

	// Start two child tasks. Only the one with ChildSpan:true is expected to be
	// present in the parent's recording.
	require.NoError(t, s.RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: "async child different recording",
		SpanOpt:  stop.FollowsFromSpan,
	},
		func(ctx context.Context) {
			log.Event(ctx, "async 1")
		},
	))
	require.NoError(t, s.RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: "async child same trace",
		SpanOpt:  stop.ChildSpan,
	},
		func(ctx context.Context) {
			log.Event(ctx, "async 2")
		},
	))

	{
		errC := make(chan error)
		require.NoError(t, s.RunAsyncTaskEx(ctx, stop.TaskOpts{
			TaskName: "sterile parent",
			SpanOpt:  stop.SterileRootSpan,
		},
			func(ctx context.Context) {
				log.Event(ctx, "async 3")
				sp1 := tracing.SpanFromContext(ctx)
				if sp1 == nil {
					errC <- errors.Errorf("missing span")
					return
				}
				sp2 := tr.StartSpan("child", tracing.WithParent(sp1))
				defer sp2.Finish()
				if sp2.TraceID() == traceID {
					errC <- errors.Errorf("expected different trace")
				}
				close(errC)
			},
		))
		require.NoError(t, <-errC)
	}

	s.Stop(ctx)
	require.NoError(t, tracing.CheckRecordedSpans(getRecAndFinish(), `
		span: parent
			tags: _verbose=1
			span: async child same trace
				tags: _verbose=1
				event: async 2`))
}

// Test that RunAsyncTask creates root spans when the caller doesn't have a
// span.
func TestStopperRunAsyncTaskCreatesRootSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tr := tracing.NewTracer()
	ctx := context.Background()
	s := stop.NewStopper(stop.WithTracer(tr))
	defer s.Stop(ctx)
	c := make(chan *tracing.Span)
	require.NoError(t, s.RunAsyncTask(ctx, "test",
		func(ctx context.Context) {
			c <- tracing.SpanFromContext(ctx)
		},
	))
	require.NotNil(t, <-c)
}

func TestFoo(t *testing.T) {
	r, err := os.Open("weird-pmax.json")
	require.NoError(t, err)
	defer r.Close()

	dec := json.NewDecoder(r)
	m := map[string]interface{}{}
	require.NoError(t, dec.Decode(&m))
	frames := m["response"].(map[string]interface{})["results"].(map[string]interface{})["A"].(map[string]interface{})["frames"].([]interface{})
	var bs buckets
	for _, frame := range frames {
		fields := frame.(map[string]interface{})["schema"].(map[string]interface{})["fields"].([]interface{})
		var leString string
		for _, field := range fields {
			f := field.(map[string]interface{})
			if f["name"] != "Value" {
				continue
			}
			leString = f["labels"].(map[string]interface{})["le"].(string)
		}
		require.NotZero(t, leString)
		tup := frame.(map[string]interface{})["data"].(map[string]interface{})["values"].([]interface{})
		ts, count := int64(tup[0].([]interface{})[0].(float64)), tup[1].([]interface{})[0].(float64)
		le, err := strconv.ParseFloat(leString, 64)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(le), leString) // ensure it round trips
		_ = ts
		bs = append(bs, bucket{
			upperBound: le,
			count:      count,
		})
	}
	sort.Sort(bs)
	if false {
		// This helps a lot - brings the p100 to ~201ms which is the secondmost top bucket.
		bs[len(bs)-1].count += 1.0
	}
	for i, b := range bs {
		s := fmt.Sprint(time.Duration(b.upperBound))
		if math.IsInf(b.upperBound, 1) {
			s = fmt.Sprint(b.upperBound)
		}
		t.Logf("idx %d: <=%s: %f", i, s, b.count)
	}
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999, 0.9999, 1.0} {
		ns := time.Duration(bucketQuantile(t, q, bs))
		t.Logf("p%.3f: %s", q*100, ns)
	}
}

// bucketQuantile calculates the quantile 'q' based on the given buckets. The
// buckets will be sorted by upperBound by this function (i.e. no sorting
// needed before calling this function). The quantile value is interpolated
// assuming a linear distribution within a bucket. However, if the quantile
// falls into the highest bucket, the upper bound of the 2nd highest bucket is
// returned. A natural lower bound of 0 is assumed if the upper bound of the
// lowest bucket is greater 0. In that case, interpolation in the lowest bucket
// happens linearly between 0 and the upper bound of the lowest bucket.
// However, if the lowest bucket has an upper bound less or equal 0, this upper
// bound is returned if the quantile falls into the lowest bucket.
//
// There are a number of special cases (once we have a way to report errors
// happening during evaluations of AST functions, we should report those
// explicitly):
//
// If 'buckets' has 0 observations, NaN is returned.
//
// If 'buckets' has fewer than 2 elements, NaN is returned.
//
// If the highest bucket is not +Inf, NaN is returned.
//
// If q==NaN, NaN is returned.
//
// If q<0, -Inf is returned.
//
// If q>1, +Inf is returned.
func bucketQuantile(t *testing.T, q float64, buckets buckets) float64 {
	if math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sort.Sort(buckets)
	if !math.IsInf(buckets[len(buckets)-1].upperBound, +1) {
		return math.NaN()
	}

	buckets = coalesceBuckets(buckets)
	ensureMonotonic(buckets)

	if len(buckets) < 2 {
		return math.NaN()
	}
	observations := buckets[len(buckets)-1].count
	if observations == 0 {
		return math.NaN()
	}
	rank := q * observations
	t.Logf("q=%f, observations=%f, rank=%f", q, observations, rank)
	b := sort.Search(len(buckets)-1, func(i int) bool { return buckets[i].count >= rank })

	if b == len(buckets)-1 {
		return buckets[len(buckets)-2].upperBound
	}
	if b == 0 && buckets[0].upperBound <= 0 {
		return buckets[0].upperBound
	}
	var (
		bucketStart float64
		bucketEnd   = buckets[b].upperBound
		count       = buckets[b].count
	)
	if b > 0 {
		bucketStart = buckets[b-1].upperBound
		count -= buckets[b-1].count
		rank -= buckets[b-1].count
	}
	return bucketStart + (bucketEnd-bucketStart)*(rank/count)
}

type bucket struct {
	upperBound float64
	count      float64
}

// buckets implements sort.Interface.
type buckets []bucket

func (b buckets) Len() int           { return len(b) }
func (b buckets) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b buckets) Less(i, j int) bool { return b[i].upperBound < b[j].upperBound }

// coalesceBuckets merges buckets with the same upper bound.
//
// The input buckets must be sorted.
func coalesceBuckets(buckets buckets) buckets {
	last := buckets[0]
	i := 0
	for _, b := range buckets[1:] {
		if b.upperBound == last.upperBound {
			last.count += b.count
		} else {
			buckets[i] = last
			last = b
			i++
		}
	}
	buckets[i] = last
	return buckets[:i+1]
}

// The assumption that bucket counts increase monotonically with increasing
// upperBound may be violated during:
//
//   * Recording rule evaluation of histogram_quantile, especially when rate()
//      has been applied to the underlying bucket timeseries.
//   * Evaluation of histogram_quantile computed over federated bucket
//      timeseries, especially when rate() has been applied.
//
// This is because scraped data is not made available to rule evaluation or
// federation atomically, so some buckets are computed with data from the
// most recent scrapes, but the other buckets are missing data from the most
// recent scrape.
//
// Monotonicity is usually guaranteed because if a bucket with upper bound
// u1 has count c1, then any bucket with a higher upper bound u > u1 must
// have counted all c1 observations and perhaps more, so that c  >= c1.
//
// Randomly interspersed partial sampling breaks that guarantee, and rate()
// exacerbates it. Specifically, suppose bucket le=1000 has a count of 10 from
// 4 samples but the bucket with le=2000 has a count of 7 from 3 samples. The
// monotonicity is broken. It is exacerbated by rate() because under normal
// operation, cumulative counting of buckets will cause the bucket counts to
// diverge such that small differences from missing samples are not a problem.
// rate() removes this divergence.)
//
// bucketQuantile depends on that monotonicity to do a binary search for the
// bucket with the φ-quantile count, so breaking the monotonicity
// guarantee causes bucketQuantile() to return undefined (nonsense) results.
//
// As a somewhat hacky solution until ingestion is atomic per scrape, we
// calculate the "envelope" of the histogram buckets, essentially removing
// any decreases in the count between successive buckets.

func ensureMonotonic(buckets buckets) {
	max := buckets[0].count
	for i := 1; i < len(buckets); i++ {
		switch {
		case buckets[i].count > max:
			max = buckets[i].count
		case buckets[i].count < max:
			buckets[i].count = max
		}
	}
}
