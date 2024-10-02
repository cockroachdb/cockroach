// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syncutil

import (
	"math"
	"testing"
)

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
	x.before.val.Store(magic64)
	x.after.val.Store(magic64)
	for delta := float64(1); delta+delta > delta; delta += delta {
		e := delta
		x.i.Store(e)
		a := x.i.Load()
		if a != e {
			t.Fatalf("stored=%f got=%f", e, a)
		}
	}
	if x.before.val.Load() != magic64 || x.after.val.Load() != magic64 {
		t.Fatalf("wrong magic: %#x _ %#x != %#x _ %#x", x.before.val.Load(), x.after.val.Load(), uint64(magic64), uint64(magic64))
	}
}

// TestAtomicStoreFloat64IfHigher is also adapted from https://golang.org/src/sync/atomic/atomic_test.go
func TestAtomicStoreFloat64IfHigher(t *testing.T) {
	var x struct {
		before AtomicFloat64
		i      AtomicFloat64
		after  AtomicFloat64
	}
	x.before.val.Store(magic64)
	x.after.val.Store(magic64)

	// Roughly half the time we will have to store a larger value.
	x.i.Store(math.MaxFloat64 / math.Pow(2, 500))
	for delta := float64(1); delta+delta > delta; delta += delta {
		e := delta
		cur := x.i.Load()
		shouldStore := e > cur
		x.i.StoreIfHigher(e)
		afterStore := x.i.Load()
		if shouldStore && e != afterStore {
			t.Fatalf("should store: expected=%f got=%f", e, afterStore)
		}
		if !shouldStore && cur != afterStore {
			t.Fatalf("should not store: expected=%f got=%f", cur, afterStore)
		}
		x.i.Store(math.MaxFloat64 / math.Pow(2, 500))
	}
	if x.before.val.Load() != magic64 || x.after.val.Load() != magic64 {
		t.Fatalf("wrong magic: %#x _ %#x != %#x _ %#x", x.before.val.Load(), x.after.val.Load(), uint64(magic64), uint64(magic64))
	}
}

// TestAtomicAddFloat64 is also adapted from https://golang.org/src/sync/atomic/atomic_test.go
func TestAtomicAddFloat64(t *testing.T) {
	var x struct {
		before AtomicFloat64
		i      AtomicFloat64
		after  AtomicFloat64
	}
	x.before.val.Store(magic64)
	x.after.val.Store(magic64)
	j := x.i.Load()
	for delta := float64(1); delta+delta > delta; delta += delta {
		x.i.Add(delta)
		j += delta
		got := x.i.Load()
		if j != x.i.Load() {
			t.Fatalf("expected=%f got=%f", j, got)
		}
	}
	if x.before.val.Load() != magic64 || x.after.val.Load() != magic64 {
		t.Fatalf("wrong magic: %#x _ %#x != %#x _ %#x", x.before.val.Load(), x.after.val.Load(), uint64(magic64), uint64(magic64))
	}
}
