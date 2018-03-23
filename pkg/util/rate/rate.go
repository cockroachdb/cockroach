// Copyright 2018 The Cockroach Authors.
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

// Package rate provides a variety of utility types and methods for
// describing activities per units of time.
package rate

import (
	"fmt"
	"time"
)

// Rate represents a value per unit of time.  Internally, this is
// normalized to units / second, but prefer using the Per()
// function to convert a Rate back to a numeric type.
type Rate float64

// NewRate constructs a rate of values per time period.
func NewRate(count float64, period time.Duration) Rate {
	if period == time.Second {
		return Rate(count)
	}

	return Rate(count / period.Seconds())
}

// Per converts the Rate to units per specified time period.
func (r Rate) Per(d time.Duration) float64 {
	if d == time.Second {
		return float64(r)
	}
	return float64(r) * (float64(d.Nanoseconds()) / float64(time.Second.Nanoseconds()))
}

// Rate implements the Rater interface, since a Rate can vend itself.
func (r Rate) Rate() Rate {
	return r
}

func (r Rate) String() string {
	return fmt.Sprintf("%.2f per second", r)
}

// Rater describes a type which can provide a Rate.
type Rater interface {
	Rate() Rate
}
