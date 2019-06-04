// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
