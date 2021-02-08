// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import "fmt"

// epsilon is the minimum value Selectivity can hold, since it cannot be 0.
const epsilon = 1e-10

// Selectivity is a value is within the range of [epsilon, 1.0] representing
// the estimated fraction of rows that pass a given condition.
type Selectivity struct {
	selectivity float64
}

// ZeroSelectivity is used in cases where selectivity is known to be zero.
var ZeroSelectivity = Selectivity{0}

// OneSelectivity is used in cases where selectivity is known to be one.
var OneSelectivity = Selectivity{1.0}

// MakeSelectivity initializes and validates a float64 to ensure it is in a
// valid range. This method is used for selectivity calculations involving
// other non-selectivity values.
func MakeSelectivity(sel float64) Selectivity {
	return Selectivity{selectivityInRange(sel)}
}

// MakeSelectivityFromFraction calculates selectivity as a fraction of a and b
// if a is less than b and returns OneSelectivity otherwise..
func MakeSelectivityFromFraction(a, b float64) Selectivity {
	if a < b {
		return MakeSelectivity(a / b)
	}
	return OneSelectivity
}

// AsFloat returns the private selectivity field, allowing it to be accessed
// outside of this package.
func (s *Selectivity) AsFloat() float64 {
	return s.selectivity
}

// Multiply finds the product of two selectivities in the valid range and
// modifies the selectivity specified in the receiver to equal the product.
func (s *Selectivity) Multiply(other Selectivity) {
	s.selectivity = selectivityInRange(s.selectivity * other.selectivity)
}

// Add finds the sum of two selectivities in the valid range and modifies
// the selectivity specified in the receiver to equal the sum.
func (s *Selectivity) Add(other Selectivity) {
	s.selectivity = selectivityInRange(s.selectivity + other.selectivity)
}

// Divide finds the quotient of two selectivities in the valid range and
// modifies the selectivity specified in the receiver to equal the quotient.
func (s *Selectivity) Divide(other Selectivity) {
	s.selectivity = selectivityInRange(s.selectivity / other.selectivity)
}

// MinSelectivity returns the smaller value of two selectivities
func MinSelectivity(a, b Selectivity) Selectivity {
	if a.selectivity < b.selectivity {
		return a
	}
	return b
}

// selectivityInRange performs the range check, if the selectivity falls
// outside of the range, this method will return the appropriate min/max value.
func selectivityInRange(sel float64) float64 {
	switch {
	case sel < epsilon:
		return epsilon
	case sel > 1.0:
		return 1.0
	default:
		return sel
	}
}

func (s Selectivity) String() string {
	return fmt.Sprintf("%f", s.selectivity)
}
