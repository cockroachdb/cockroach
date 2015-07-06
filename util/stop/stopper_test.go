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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package stop_test

import (
	"sync"
	"testing"
	"time"

	_ "github.com/cockroachdb/cockroach/util/log" // for flags
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestStopper(t *testing.T) {
	s := stop.NewStopper()
	running := make(chan struct{})
	waiting := make(chan struct{})

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
	}()

	s.Stop()
	close(waiting)
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

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timed out waiting for stop")
	}
}

func TestStopperStartFinishTasks(t *testing.T) {
	s := stop.NewStopper()

	if !s.RunTask(func() {
		go s.Stop()

		select {
		case <-s.ShouldStop():
			t.Fatal("expected stopper to be draining")
		case <-time.After(1 * time.Millisecond):
			// Expected.
		}
	}) {
		t.Error("expected RunTask to succeed")
	}
	select {
	case <-s.ShouldStop():
		// Success.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stopper should be ready to stop")
	}
}

func TestStopperRunWorker(t *testing.T) {
	s := stop.NewStopper()
	s.RunWorker(func() {
		select {
		case <-s.ShouldStop():
			return
		}
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

// TestStopperQuiesce tests coordinate drain with Quiesce.
func TestStopperQuiesce(t *testing.T) {
	var stoppers []*stop.Stopper
	for i := 0; i < 3; i++ {
		stoppers = append(stoppers, stop.NewStopper())
	}
	var quiesceDone []chan struct{}
	var runTaskDone []chan struct{}

	for _, s := range stoppers {
		qc := make(chan struct{})
		quiesceDone = append(quiesceDone, qc)
		sc := make(chan struct{})
		runTaskDone = append(runTaskDone, sc)
		s.RunWorker(func() {
			// Wait until Quiesce() is called.
			<-qc
			if s.RunTask(func() {}) {
				t.Error("expected RunTask to fail")
			}
			// Make the stoppers call Stop().
			close(sc)
			<-s.ShouldStop()
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

		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Errorf("timed out waiting for stop")
	}
}

type testCloser bool

func (tc *testCloser) Close() {
	*tc = true
}

func TestStopperClosers(t *testing.T) {
	s := stop.NewStopper()
	var tc1, tc2 testCloser
	s.AddCloser(&tc1)
	s.AddCloser(&tc2)
	s.Stop()
	if bool(tc1) != true || bool(tc2) != true {
		t.Errorf("expected true & true; got %t & %t", tc1, tc2)
	}
}

func TestStopperNumTasks(t *testing.T) {
	s := stop.NewStopper()

	var tasks []bool
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		tasks = append(tasks, s.RunGoTask(func() {
			time.Sleep(5 * time.Millisecond)
			wg.Done()
		}))
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

	wg.Wait()
	time.Sleep(5 * time.Millisecond)
	// The taskmap should've cleared out.
	if m := s.RunningTasks(); len(m) != 0 {
		t.Fatalf("task map not empty: %+v", m)
	}

	s.Stop()
}
