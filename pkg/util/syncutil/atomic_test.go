// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syncutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
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
	x.before = magic64
	x.after = magic64
	for delta := float64(1); delta+delta > delta; delta += delta {
		e := delta
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

// TestAtomicStoreFloat64IfHigher is also adapted from https://golang.org/src/sync/atomic/atomic_test.go
func TestAtomicStoreFloat64IfHigher(t *testing.T) {
	var x struct {
		before AtomicFloat64
		i      AtomicFloat64
		after  AtomicFloat64
	}
	x.before = magic64
	x.after = magic64

	// Roughly half the time we will have to store a larger value.
	StoreFloat64(&x.i, math.MaxFloat64/math.Pow(2, 500))
	for delta := float64(1); delta+delta > delta; delta += delta {
		e := delta
		cur := LoadFloat64(&x.i)
		shouldStore := e > cur
		StoreFloat64IfHigher(&x.i, e)
		afterStore := LoadFloat64(&x.i)
		if shouldStore && e != afterStore {
			t.Fatalf("should store: expected=%f got=%f", e, afterStore)
		}
		if !shouldStore && cur != afterStore {
			t.Fatalf("should not store: expected=%f got=%f", cur, afterStore)
		}
		StoreFloat64(&x.i, math.MaxFloat64/math.Pow(2, 500))
	}
	if x.before != magic64 || x.after != magic64 {
		t.Fatalf("wrong magic: %#x _ %#x != %#x _ %#x", x.before, x.after, uint64(magic64), uint64(magic64))
	}
}

// TestAtomicAddFloat64 is also adapted from https://golang.org/src/sync/atomic/atomic_test.go
func TestAtomicAddFloat64(t *testing.T) {
	var x struct {
		before AtomicFloat64
		i      AtomicFloat64
		after  AtomicFloat64
	}
	x.before = magic64
	x.after = magic64
	j := LoadFloat64(&x.i)
	for delta := float64(1); delta+delta > delta; delta += delta {
		AddFloat64(&x.i, delta)
		j += delta
		got := LoadFloat64(&x.i)
		if j != LoadFloat64(&x.i) {
			t.Fatalf("expected=%f got=%f", j, got)
		}
	}
	if x.before != magic64 || x.after != magic64 {
		t.Fatalf("wrong magic: %#x _ %#x != %#x _ %#x", x.before, x.after, uint64(magic64), uint64(magic64))
	}
}

func TestAtomicBool(t *testing.T) {
	var x AtomicBool
	x.Set(true)
	require.Equal(t, x.Get(), true)
	require.Equal(t, x.Swap(false), true)
	require.Equal(t, x.Get(), false)
}
