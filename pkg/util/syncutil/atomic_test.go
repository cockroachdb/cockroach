// Copyright 2017 The Cockroach Authors.
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

package syncutil

import "testing"

const magic64 = 0xdeddeadbeefbeef

// Tests of correct behavior, without contention.
// The loop over power-of-two values is meant to
// ensure that the operations apply to the full word size.
// The struct fields x.before and x.after check that the
// operations do not extend past the full word size.
//
// Adapted from https://golang.org/src/sync/atomic/atomic_test.go
func TestAtomicFloat64(t *testing.T) {
	var x struct {
		before AtomicFloat64
		i      AtomicFloat64
		after  AtomicFloat64
	}
	x.before = magic64
	x.after = magic64
	for delta := uint64(1); delta+delta > delta; delta += delta {
		e := float64(delta)
		StoreFloat64(&x.i, e)
		a := LoadFloat64(&x.i)
		if a != e {
			t.Fatalf("stored=%f got=%f", e, a)
		}
	}
	if x.before != magic64 || x.after != magic64 {
		t.Fatalf("wrong magic: %#x _ %#x != %#x _ %#x", x.before, x.after, uint64(magic64), uint64(magic64))
	}
}
