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

import "time"

// MakeTimeValue returns an Duration with a set value.
func MakeTimeValue(value time.Duration) Duration {
	return Duration{ValuePlusOne: value + 1}
}

// HasValue returns true if a value was set.
func (d Duration) HasValue() bool {
	return d.ValuePlusOne != 0
}

// Value returns the current value, or 0 if HasValue() is false.
func (d Duration) Value() time.Duration {
	if d.ValuePlusOne == 0 {
		return 0
	}
	return d.ValuePlusOne - 1
}

func (d Duration) String() string {
	if !d.HasValue() {
		return "<unset>"
	}
	return d.Value().String()
}

// Clear the value.
func (d *Duration) Clear() {
	d.ValuePlusOne = 0
}

// Set the value.
func (d *Duration) Set(value time.Duration) {
	*d = MakeTimeValue(value)
}

// Add modifies the value by adding a delta.
func (d *Duration) Add(delta time.Duration) {
	*d = MakeTimeValue(d.Value() + delta)
}

// MaybeAdd adds the given value, if it is set. Does nothing if other is not
// set.
func (d *Duration) MaybeAdd(other Duration) {
	if other.HasValue() {
		*d = MakeTimeValue(d.Value() + other.Value())
	}
}
