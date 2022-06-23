package internal

// IsSameSignAndNonZero checks if both a and b are positive or negative.
func IsSameSignAndNonZero(a, b float64) bool {
	if a == 0 || b == 0 {
		return false
	}
	return (a < 0 && b < 0) || (a > 0 && b > 0)
}

// Min finds the minimum of the 4 parameters
func Min(v1, v2, v3, v4 float64) float64 {
	min := v1
	if v2 < min {
		min = v2
	}
	if v3 < min {
		min = v3
	}
	if v4 < min {
		min = v4
	}
	return min
}
