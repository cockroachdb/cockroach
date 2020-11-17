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

func TestAtomicBool(t *testing.T) {
	var x AtomicBool
	x.Set(true)
	require.Equal(t, x.Get(), true)
	require.Equal(t, x.Swap(false), true)
	require.Equal(t, x.Get(), false)
}
