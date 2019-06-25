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
