package props

// epsilon is the minimum value Selectivity can hold, since it cannot be 0.
const epsilon = 1e-10

// Selectivity stores selectivity as a float64, while having custom methods for
// performing operations and checking that the value is within the range of
// [epsilon, 1.0].
type Selectivity float64

// MakeSelectivity initializes and validates a float64 to ensure it is in a
// valid range. This method is used for selectivity calculations involving
// other non-selectivity values.
func MakeSelectivity(sel float64) Selectivity {
	return Selectivity(sel).SelectivityInRange()
}

// Multiply is a custom method for multiplying two selectivities and returns
// the product in the valid range.
func (s Selectivity) Multiply(other Selectivity) Selectivity {
	s *= other
	return s.SelectivityInRange()
}

// Add is a custom method for adding two selectivities and returns the sum in
// the valid range.
func (s Selectivity) Add(other Selectivity) Selectivity {
	s += other
	return  s.SelectivityInRange()
}

// SelectivityInRange performs the range check, if the selectivity falls
// outside of the range, this method will return the appropriate min/max value.
func (s Selectivity) SelectivityInRange() Selectivity {
	switch {
	case s < epsilon:
		return epsilon
	case s > 1.0:
		return 1.0
	default:
		return s
	}
}
