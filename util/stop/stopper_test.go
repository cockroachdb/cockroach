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

package stop

import (
	"testing"
	"time"
)

func TestStopper(t *testing.T) {
	s := NewStopper()
	s.AddWorker()

	waiting := make(chan struct{})
	go func() {
		<-s.ShouldStop()
		select {
		case <-waiting:
			t.Fatal("expected stopper to have blocked")
		case <-time.After(1 * time.Millisecond):
			// Expected.
		}
		s.SetStopped()
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
	s := NewStopper()
	s.AddWorker()
	bc := newBlockingCloser()
	s.AddCloser(bc)
	go s.Stop()

	select {
	case <-s.ShouldStop():
		s.SetStopped()
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
	s := NewStopper()

	for i := 0; i < count; i++ {
		s.AddWorker()
		go func() {
			<-s.ShouldStop()
			s.SetStopped()
		}()
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
	s := NewStopper()
	s.AddWorker()

	if !s.StartTask() {
		t.Error("expected StartTask to succeed")
	}
	go s.Stop()

	select {
	case <-s.ShouldStop():
		t.Fatal("expected stopper to be draining")
	case <-time.After(1 * time.Millisecond):
		// Expected.
	}
	s.FinishTask()
	select {
	case <-s.ShouldStop():
		// Success.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("stopper should be ready to stop")
	}
	s.SetStopped()
}

func TestStopperRunWorker(t *testing.T) {
	s := NewStopper()
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
	var stoppers []*Stopper
	for i := 0; i < 3; i++ {
		stoppers = append(stoppers, NewStopper())
	}
	var quiesceDone []chan struct{}
	var startTaskDone []chan struct{}

	for i := range stoppers {
		// Create a local copy to avoid data race.
		s := stoppers[i]
		s.AddWorker()
		qc := make(chan struct{})
		quiesceDone = append(quiesceDone, qc)
		sc := make(chan struct{})
		startTaskDone = append(startTaskDone, sc)
		go func() {
			// Wait until Quiesce() is called.
			<-qc
			if s.StartTask() {
				t.Error("expected StartTask to fail")
			}
			// Make the stoppers call Stop().
			close(sc)
			<-s.ShouldStop()
			s.SetStopped()
		}()
	}

	done := make(chan struct{})
	go func() {
		for _, s := range stoppers {
			s.Quiesce()
		}
		// Make the tasks call StartTask().
		for _, qc := range quiesceDone {
			close(qc)
		}

		// Wait until StartTask() is called.
		for _, sc := range startTaskDone {
			<-sc
		}

		for _, s := range stoppers {
			s.Stop()
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
	s := NewStopper()
	var tc1, tc2 testCloser
	s.AddCloser(&tc1)
	s.AddCloser(&tc2)
	s.Stop()
	if bool(tc1) != true || bool(tc2) != true {
		t.Errorf("expected true & true; got %t & %t", tc1, tc2)
	}
}
