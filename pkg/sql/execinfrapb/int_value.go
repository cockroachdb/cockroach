// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import "fmt"

// IntValue stores an optional unsigned integer value, used in the ComponentStats
// message.
//
// The underlying value is the logical value plus 1, so that zero remains the
// special case of having no value.
type IntValue uint64

// MakeIntValue returns an IntValue with a set value.
func MakeIntValue(value uint64) IntValue {
	return IntValue(value + 1)
}

// HasValue returns true if a value was set.
func (i IntValue) HasValue() bool {
	return i != 0
}

// Value returns the current value, or 0 if HasValue() is false.
func (i IntValue) Value() uint64 {
	if i == 0 {
		return 0
	}
	return uint64(i - 1)
}

func (i IntValue) String() string {
	if i.HasValue() {
		return "<unset>"
	}
	return fmt.Sprintf("%d", i.Value())
}

// Clear the value.
func (i *IntValue) Clear() {
	*i = 0
}

// Set the value.
func (i *IntValue) Set(value uint64) {
	*i = MakeIntValue(value)
}

// Add modifies the value by adding a delta.
func (i *IntValue) Add(delta int64) {
	*i = MakeIntValue(uint64(int64(i.Value()) + delta))
}
