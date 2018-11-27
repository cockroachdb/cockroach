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
