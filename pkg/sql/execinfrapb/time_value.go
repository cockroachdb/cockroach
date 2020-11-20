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

import (
	"fmt"
	"time"
)

// MakeTimeValue returns an TimeValue with a set value.
func MakeTimeValue(value time.Duration) TimeValue {
	return TimeValue{RawValue: value + 1}
}

// HasValue returns true if a value was set.
func (t TimeValue) HasValue() bool {
	return t.RawValue != 0
}

// Value returns the current value, or 0 if HasValue() is false.
func (t TimeValue) Value() time.Duration {
	if t.RawValue == 0 {
		return 0
	}
	return t.RawValue - 1
}

func (t TimeValue) String() string {
	if t.HasValue() {
		return "<unset>"
	}
	return fmt.Sprintf("%d", t.Value())
}

// Clear the value.
func (t *TimeValue) Clear() {
	t.RawValue = 0
}

// Set the value.
func (t *TimeValue) Set(value time.Duration) {
	*t = MakeTimeValue(value)
}

// Add modifies the value by adding a delta.
func (t *TimeValue) Add(delta time.Duration) {
	*t = MakeTimeValue(t.Value() + delta)
}
