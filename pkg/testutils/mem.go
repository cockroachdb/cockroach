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
	"runtime"
)

// TestNoMallocs returns true if calling the given function results in zero
// memory allocations. It iterates up to 100 times to combat flakiness, as
// there might be initial "warmup" allocations, or possibly background memory
// allocations that cause issues. The test completes successfully as soon as at
// least one run results in 0 allocations.
func TestNoMallocs(fn func()) bool {
	for i := 0; i < 100; i++ {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		before := mem.Mallocs

		fn()

		runtime.ReadMemStats(&mem)
		if mem.Mallocs-before == 0 {
			return true
		}
	}
	return false
}
