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

package util

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestStopper(t *testing.T) {
	s := NewStopper(1)

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

func TestStopperMultipleStopees(t *testing.T) {
	const count = 3
	s := NewStopper(count)

	for i := 0; i < count; i++ {
		go func() {
			<-s.ShouldStop()
			s.SetStopped()
		}()
	}

	var done int64

	go IsTrueWithin(func() bool {
		return atomic.LoadInt64(&done) > 0
	}, 10*time.Millisecond)

	s.Stop()
	atomic.AddInt64(&done, 1)
}
