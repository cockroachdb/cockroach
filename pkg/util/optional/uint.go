// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optional

import (
	"fmt"
	"math"
)

// MakeUint returns an Uint with a set value. The maximum value that can be
// represented is (MaxUint64-1).
func MakeUint(value uint64) Uint {
	if value == math.MaxUint64 {
		return Uint{
			ValuePlusOne: value,
		}
	}
	return Uint{
		ValuePlusOne: value + 1,
	}
}

// HasValue returns true if a value was set.
func (i Uint) HasValue() bool {
	return i.ValuePlusOne != 0
}

// Value returns the current value, or 0 if HasValue() is false.
func (i Uint) Value() uint64 {
	if !i.HasValue() {
		return 0
	}
	return i.ValuePlusOne - 1
}

// String returns the value or "<unset>".
func (i Uint) String() string {
	if !i.HasValue() {
		return "<unset>"
	}
	return fmt.Sprintf("%d", i.Value())
}

// Clear the value.
func (i *Uint) Clear() {
	*i = Uint{}
}

// Set the value.
func (i *Uint) Set(value uint64) {
	*i = MakeUint(value)
}

// Add modifies the value by adding a delta.
func (i *Uint) Add(delta int64) {
	*i = MakeUint(uint64(int64(i.Value()) + delta))
}

// MaybeAdd adds the given value, if it is set. Does nothing if other is not
// set.
func (i *Uint) MaybeAdd(other Uint) {
	if other.HasValue() {
		*i = MakeUint(i.Value() + other.Value())
	}
}
