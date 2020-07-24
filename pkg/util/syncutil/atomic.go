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
	"sync/atomic"
)

// AtomicFloat64 mimics the atomic types in the sync/atomic standard library,
// but for the float64 type. If you'd like to implement additional methods,
// consider checking out the expvar Float type for guidance:
// https://golang.org/src/expvar/expvar.go?s=2188:2222#L69
type AtomicFloat64 uint64

// StoreFloat64 atomically stores a float64 value into the provided address.
func StoreFloat64(addr *AtomicFloat64, val float64) {
	atomic.StoreUint64((*uint64)(addr), math.Float64bits(val))
}

// LoadFloat64 atomically loads tha float64 value from the provided address.
func LoadFloat64(addr *AtomicFloat64) (val float64) {
	return math.Float64frombits(atomic.LoadUint64((*uint64)(addr)))
}

// AtomicBool mimics an atomic boolean.
type AtomicBool uint32

// Set atomically sets the boolean.
func (b *AtomicBool) Set(v bool) {
	s := uint32(0)
	if v {
		s = 1
	}
	atomic.StoreUint32((*uint32)(b), s)
}

// Get atomically gets the boolean.
func (b *AtomicBool) Get() bool {
	return atomic.LoadUint32((*uint32)(b)) != 0
}

// Swap atomically swaps the value.
func (b *AtomicBool) Swap(v bool) bool {
	wanted := uint32(0)
	if v {
		wanted = 1
	}
	return atomic.SwapUint32((*uint32)(b), wanted) != 0
}

// AtomicString gives you atomic-style APIs for string.
type AtomicString struct {
	s atomic.Value
}

// Set atomically sets str as new value.
func (s *AtomicString) Set(val string) {
	s.s.Store(val)
}

// Get atomically returns the current value.
func (s *AtomicString) Get() string {
	val := s.s.Load()
	if val == nil {
		return ""
	}
	return val.(string)
}
