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
// Author: Dan Harrison (daniel.harrison@gmail.com)

package util

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
// wg.Wait()
// return wg.FirstError()
type WaitGroupWithError struct {
	wg sync.WaitGroup

	mu struct {
		syncutil.Mutex
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
func (wg *WaitGroupWithError) Wait() {
	wg.wg.Wait()
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

// FirstError returns the first error that was passed to Done (wrapped in a
// count of how many total errors there were) or nil if there were no errors.
func (wg *WaitGroupWithError) FirstError() error {
	wg.mu.Lock()
	defer wg.mu.Unlock()
	return errors.Wrapf(wg.mu.firstErr, "first of %d errors", wg.mu.numErrs)
}
