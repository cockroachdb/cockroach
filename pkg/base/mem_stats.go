// Copyright 2019 The Cockroach Authors.
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

package base

import (
	"runtime"
	"time"
)

// GoMemStats represents information from the Go allocator.
// All units are bytes.
type GoMemStats struct {
	// MemStats is a pointer because it's a large struct. We want GoMemStats to be
	// passed by value, but not this inner field.
	*runtime.MemStats
	// Collected is the timestamp at which these values were collected.
	Collected time.Time
}

// MakeGoMemStats creates a GoMemStats.
func MakeGoMemStats() GoMemStats {
	return GoMemStats{MemStats: new(runtime.MemStats)}
}

// GoInUse returns the amount of virtual memory currently allocated from the
// OS by the Go parts of the process (i.e. no CGo) for heap, stacks, etc.
//
// Note that memory wasted because of fragmentation (i.e. HeapInUse - HeapAlloc)
// is counted as "in use" by this method.
func (ms *GoMemStats) GoInUse() uint64 {
	// NOTE(andrei): It may seem weird that we're only subtracting HeapReleased
	// from Sys (which refers to more than just the heap), but there are no other
	// XReleased fields. My understanding/assumption is that non-heap spans that
	// become unused are transfered to the heap, and from there they'll be
	// released and accounted for as HeapReleased.
	return ms.Sys - ms.HeapReleased
}
