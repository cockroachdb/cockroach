// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package stop_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	_ "github.com/cockroachdb/cockroach/util/log" // for flags
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/pkg/errors"
)

func TestStopper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	running := make(chan struct{})
	waiting := make(chan struct{})
	cleanup := make(chan struct{})

	s.RunWorker(func() {
		<-running
	})

	go func() {
		<-s.ShouldStop()
		select {
		case <-waiting:
			t.Fatal("expected stopper to have blocked")
		case <-time.After(1 * time.Millisecond):
			// Expected.
		}
		close(running)
		select {
		case <-waiting:
			// Success.
		case <-time.After(100 * time.Millisecond):
			t.Fatal("stopper should have finished waiting")
		}
		close(cleanup)
	}()

	s.Stop()
	close(waiting)
	<-cleanup
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
	go s.Stop()

	select {
	case <-s.ShouldStop():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stopper should have finished waiting")
	}
	select {
	case <-s.IsStopped():
		t.Fatal("expected blocked closer to prevent stop")
	case <-time.After(1 * time.Millisecond):
		// Expected.
	}
	bc.Unblock()
	select {
	case <-s.IsStopped():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stopper should have finished stopping")
	}
}

func TestStopperMultipleStopees(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const count = 3
	s := stop.NewStopper()

	for i := 0; i < count; i++ {
		s.RunWorker(func() {
			<-s.ShouldStop()
		})
	}

	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()

	<-done
}

func TestStopperStartFinishTasks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()

	if err := s.RunTask(func() {
		go s.Stop()

		select {
		case <-s.ShouldStop():
			t.Fatal("expected stopper to be quiesceing")
		case <-time.After(1 * time.Millisecond):
			// Expected.
		}
	}); err != nil {
		t.Error(err)
	}
	select {
	case <-s.ShouldStop():
		// Success.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stopper should be ready to stop")
	}
}

func TestStopperRunWorker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	s.RunWorker(func() {
		<-s.ShouldStop()
	})
	closer := make(chan struct{})
	go func() {
		s.Stop()
		close(closer)
	}()
	select {
	case <-closer:
		// Success.
	case <-time.After(100 * time.Millisecond):
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

	for _, s := range stoppers {
		thisStopper := s
		qc := make(chan struct{})
		quiesceDone = append(quiesceDone, qc)
		sc := make(chan struct{})
		runTaskDone = append(runTaskDone, sc)
		thisStopper.RunWorker(func() {
			// Wait until Quiesce() is called.
			<-qc
			err := thisStopper.RunTask(func() {})
			if _, ok := err.(*roachpb.NodeUnavailableError); !ok {
				t.Error(err)
			}
			// Make the stoppers call Stop().
			close(sc)
			<-thisStopper.ShouldStop()
		})
	}

	done := make(chan struct{})
	go func() {
		for _, s := range stoppers {
			s.Quiesce()
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
			s.Stop()
			<-s.IsStopped()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
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
	s.Stop()
	if !bool(tc1) || !bool(tc2) {
		t.Errorf("expected true & true; got %t & %t", tc1, tc2)
	}
}

func TestStopperNumTasks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	var tasks []chan bool
	for i := 0; i < 3; i++ {
		c := make(chan bool)
		tasks = append(tasks, c)
		if err := s.RunAsyncTask(func() {
			// Wait for channel to close
			<-c
		}); err != nil {
			t.Fatal(err)
		}
		tm := s.RunningTasks()
		if numTypes, numTasks := len(tm), s.NumTasks(); numTypes != 1 || numTasks != i+1 {
			t.Errorf("stopper should have %d running tasks, got %d / %+v", i+1, numTasks, tm)
		}
		m := s.RunningTasks()
		if len(m) != 1 {
			t.Fatalf("expected exactly one task map entry: %+v", m)
		}
		for _, v := range m {
			if expNum := len(tasks); v != expNum {
				t.Fatalf("%d: expected %d tasks, got %d", i, expNum, v)
			}
		}
	}
	for i, c := range tasks {
		m := s.RunningTasks()
		if len(m) != 1 {
			t.Fatalf("%d: expected exactly one task map entry: %+v", i, m)
		}
		for _, v := range m {
			if expNum := len(tasks[i:]); v != expNum {
				t.Fatalf("%d: expected %d tasks, got %d:\n%s", i, expNum, v, m)
			}
		}
		// Close the channel to let the task proceed.
		close(c)
		expNum := len(tasks[i+1:])
		util.SucceedsSoon(t, func() error {
			if nt := s.NumTasks(); nt != expNum {
				return errors.Errorf("%d: stopper should have %d running tasks, got %d", i, expNum, nt)
			}
			return nil
		})
	}
	// The taskmap should've been cleared out.
	if m := s.RunningTasks(); len(m) != 0 {
		t.Fatalf("task map not empty: %+v", m)
	}
	s.Stop()
}

// TestStopperRunTaskPanic ensures that a panic handler can recover panicking
// tasks, and that no tasks are leaked when they panic.
func TestStopperRunTaskPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ch := make(chan interface{})
	s := stop.NewStopper(stop.OnPanic(func(v interface{}) {
		ch <- v
	}))
	// If RunTask were not panic-safe, Stop() would deadlock.
	type testFn func()
	explode := func() { panic(ch) }
	for i, test := range []testFn{
		func() {
			_ = s.RunTask(explode)
		},
		func() {
			_ = s.RunAsyncTask(explode)
		},
		func() {
			_ = s.RunLimitedAsyncTask(
				make(chan struct{}, 1),
				explode,
			)
		},
		func() {
			s.RunWorker(explode)
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
	ctx := s.WithCancel(context.Background())
	s.Stop()
	if err := ctx.Err(); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestStopperShouldQuiesce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	running := make(chan struct{})
	runningTask := make(chan struct{})
	waiting := make(chan struct{})
	cleanup := make(chan struct{})

	// Run a worker. A call to stopper.Stop() will not close until all workers
	// have completed, and this worker will complete when the "running" channel
	// is closed.
	s.RunWorker(func() {
		<-running
	})
	// Run an asynchronous task. A stopper which has been Stop()ed will not
	// close it's ShouldStop() channel until all tasks have completed. This task
	// will complete when the "runningTask" channel is closed.
	if err := s.RunAsyncTask(func() {
		<-runningTask
	}); err != nil {
		t.Fatal(err)
	}

	go func() {
		// The ShouldQuiesce() channel should close as soon as the stopper is
		// Stop()ed.
		<-s.ShouldQuiesce()
		// However, the ShouldStop() channel should still be blocked because the
		// async task started above is still running, meaning we haven't quiesceed
		// yet.
		select {
		case <-s.ShouldStop():
			t.Fatal("expected ShouldStop() to block until quiesceing complete")
		default:
			// Expected.
		}
		// After completing the running task, the ShouldStop() channel should
		// now close.
		close(runningTask)
		<-s.ShouldStop()
		// However, the working running above prevents the call to Stop() from
		// returning; it blocks until the runner's goroutine is finished. We
		// use the "waiting" channel to detect this.
		select {
		case <-waiting:
			t.Fatal("expected stopper to have blocked")
		default:
			// Expected.
		}
		// Finally, close the "running" channel, which should cause the original
		// call to Stop() to return.
		close(running)
		<-waiting
		close(cleanup)
	}()

	s.Stop()
	close(waiting)
	<-cleanup
}

func TestStopperRunLimitedAsyncTask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := stop.NewStopper()
	defer s.Stop()

	const maxConcurrency = 5
	const duration = 10 * time.Millisecond
	sem := make(chan struct{}, maxConcurrency)
	var mu syncutil.Mutex
	concurrency := 0
	peakConcurrency := 0
	var wg sync.WaitGroup

	f := func() {
		mu.Lock()
		concurrency++
		if concurrency > peakConcurrency {
			peakConcurrency = concurrency
		}
		mu.Unlock()
		time.Sleep(duration)
		mu.Lock()
		concurrency--
		mu.Unlock()
		wg.Done()
	}

	for i := 0; i < maxConcurrency*3; i++ {
		wg.Add(1)
		if err := s.RunLimitedAsyncTask(sem, f); err != nil {
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
}

func maybePrint() {
	if testing.Verbose() { // This just needs to be complicated enough not to inline.
		fmt.Println("blah")
	}
}

func BenchmarkDirectCall(b *testing.B) {
	defer leaktest.AfterTest(b)
	s := stop.NewStopper()
	defer s.Stop()
	for i := 0; i < b.N; i++ {
		maybePrint()
	}
}

func BenchmarkStopper(b *testing.B) {
	defer leaktest.AfterTest(b)
	s := stop.NewStopper()
	defer s.Stop()
	for i := 0; i < b.N; i++ {
		if err := s.RunTask(maybePrint); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkDirectCallPar(b *testing.B) {
	defer leaktest.AfterTest(b)
	s := stop.NewStopper()
	defer s.Stop()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			maybePrint()
		}
	})
}

func BenchmarkStopperPar(b *testing.B) {
	defer leaktest.AfterTest(b)
	s := stop.NewStopper()
	defer s.Stop()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.RunTask(maybePrint); err != nil {
				b.Fatal(err)
			}
		}
	})
}
