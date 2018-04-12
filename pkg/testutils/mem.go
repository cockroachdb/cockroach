// Copyright 2018 The Cockroach Authors.
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

package testutils

import (
	"errors"
	"runtime"
	"testing"
)

// TestNoMallocs repeatedly invokes the given function until it results in zero
// memory allocations. It uses SucceedsSoon to repeatedly iterate in order to
// combat flakiness, as there might be initial "warmup" allocations, or possibly
// background memory allocations that cause issues. The test completes
// successfully as soon as at least one run results in 0 allocations.
func TestNoMallocs(t testing.TB, fn func() error) {
	SucceedsSoon(t, func() error {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		before := mem.Mallocs

		if err := fn(); err != nil {
			return err
		}

		runtime.ReadMemStats(&mem)
		if mem.Mallocs-before == 0 {
			return nil
		}

		return errors.New("memory was allocated during function invocation")
	})
}
