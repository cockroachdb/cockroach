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

import "sync"

// A Stopper provides a channel-based mechanism to stop a running
// goroutine. Stopping occurs in two phases: the first is the request
// to stop. The second is the confirmation by the goroutine that it
// has stopped. Multiple goroutines can be stopped using the same
// Stopper instance.
type Stopper struct {
	stopper chan struct{}
	wg      sync.WaitGroup
}

// NewStopper returns an instance of Stopper. Count specifies how
// many goroutines this stopper will stop.
func NewStopper(count int) *Stopper {
	s := &Stopper{
		stopper: make(chan struct{}),
	}
	s.wg.Add(count)
	return s
}

// Stop signals the waiting goroutine to stop and then waits
// for it to confirm it has stopped (it does this by calling
// SetStopped).
func (s *Stopper) Stop() {
	close(s.stopper)
	s.wg.Wait()
}

// ShouldStop returns a channel which will be closed when Stop() has
// been invoked. SetStopped() should be called to confirm.
func (s *Stopper) ShouldStop() <-chan struct{} {
	return s.stopper
}

// SetStopped should be called after the ShouldStop() channel has
// been closed to confirm the goroutine has stopped.
func (s *Stopper) SetStopped() {
	s.wg.Done()
}
