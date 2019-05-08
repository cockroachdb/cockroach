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

package base

import "time"

// GoMemStats represents information from the Go allocator.
// All units are bytes.
type GoMemStats struct {
	// GoAllocated is  bytes of allocated heap objects.
	GoAllocated uint64
	// GoIdle is space allocated from the OS but not used.
	GoIdle uint64
	// GoTotal is approximately GoAllocated + GoIdle (it also includes stacks and
	// other things not included in GoAllocated).
	GoTotal uint64

	// Collected is the timestamp at which these values were collected.
	Collected time.Time
}

// MemStats groups HeapStats and an rss measurement (rss referring to the whole
// process, beyond the Go parts).
type MemStats struct {
	Go       GoMemStats
	RSSBytes uint64
}
