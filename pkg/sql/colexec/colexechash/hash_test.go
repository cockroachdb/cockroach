// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexechash

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestHashingDoesNotAllocate ensures that our use of the noescape hack to make
// sure hashing with unsafe.Pointer doesn't allocate still works correctly.
func TestHashingDoesNotAllocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var sum uintptr
	foundAllocations := 0
	for i := 0; i < 10; i++ {
		// Sometimes, Go allocates somewhere else. To make this test not flaky,
		// let's just make sure that at least one of the rounds of this loop doesn't
		// allocate at all.
		s := &runtime.MemStats{}
		runtime.ReadMemStats(s)
		numAlloc := s.TotalAlloc
		i := 10
		x := memhash64(noescape(unsafe.Pointer(&i)), 0)
		runtime.ReadMemStats(s)

		if numAlloc != s.TotalAlloc {
			foundAllocations++
		}
		sum += x
	}
	if foundAllocations == 10 {
		// Uhoh, we allocated every single time. This probably means we regressed,
		// and our hash function allocates.
		t.Fatalf("memhash64(noescape(&i)) allocated at least once")
	}
	t.Log(sum)
}
