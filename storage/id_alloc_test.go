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

package storage

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/storage/engine"
)

// TestIDAllocator creates an ID allocator which allocates from
// the Raft ID generator system key in blocks of 10 with a minimum
// ID value of 2 and then starts up 10 goroutines each allocating
// 10 IDs. All goroutines deposit the allocated IDs into a final
// channel, which is queried at the end to ensure that all IDs
// from 2 to 101 are present.
func TestIDAllocator(t *testing.T) {
	store, _ := createTestStore(false, t)
	allocd := make(chan int, 100)
	idAlloc := NewIDAllocator(engine.KeyRaftIDGenerator, store.db, 2, 10)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				allocd <- int(idAlloc.Allocate())
			}
		}()
	}

	// Verify all IDs accounted for.
	ids := make([]int, 100)
	for i := 0; i < 100; i++ {
		ids[i] = <-allocd
	}
	sort.Ints(ids)
	for i := 0; i < 100; i++ {
		if ids[i] != i+2 {
			t.Error("expected \"%d\"th ID to be %d; got %d", i, i+2, ids[i])
		}
	}

	// Verify no leftover IDs.
	select {
	case id := <-allocd:
		t.Error("there appear to be leftover IDs, starting with %d", id)
	default:
		// Expected; noop.
	}
}
