// Copyright 2016 The Cockroach Authors.
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
// Author: Daniel Harrison (dan@cockroachlabs.com)

package syncutil

import (
	"sync"

	"github.com/pkg/errors"
)

// WaitGroupWithError is a sync.WaitGroup that also tracks the errors that are
// generated in associated asynchronous work.
//
// Example usage:
// var wg WaitGroupWithError
// for _ := range bar {
//   wg.Add(1)
//   go func() {
//     wg.Done(maybeError())
//   }
// }
// err := wg.Wait()
type WaitGroupWithError struct {
	wg sync.WaitGroup

	mu struct {
		Mutex
		firstErr error
		numErrs  int
	}
}

// Add adds delta, which may be negative, to the WaitGroup counter. If the
// counter becomes zero, all goroutines blocked on Wait are released. If the
// counter goes negative, Add panics.
func (wg *WaitGroupWithError) Add(delta int) {
	wg.wg.Add(delta)
}

// Wait blocks until the WaitGroup counter is zero.
// It returns a summary of the errors that were passed to Done(err) calls (if
// any).
func (wg *WaitGroupWithError) Wait() error {
	wg.wg.Wait()
	// Locking no longer required at this point; no more concurrent Done() calls.
	if wg.mu.numErrs == 1 {
		return wg.mu.firstErr
	} else if wg.mu.numErrs > 0 {
		return errors.Wrapf(wg.mu.firstErr, "first of %d errors", wg.mu.numErrs)
	}
	return nil
}

// Done decrements the WaitGroup counter and records the error if non-nil.
func (wg *WaitGroupWithError) Done(err error) {
	if err != nil {
		wg.mu.Lock()
		if wg.mu.firstErr == nil {
			wg.mu.firstErr = err
		}
		wg.mu.numErrs++
		wg.mu.Unlock()
	}

	wg.wg.Done()
}
