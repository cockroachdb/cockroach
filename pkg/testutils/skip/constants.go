// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package skip

// ClampMetamorphicConstantUnderDuress ensures that the given integer constant
// with metamorphic testing range is at least the given minimum value, when the
// process is running under duress.
func ClampMetamorphicConstantUnderDuress(val, min int) int {
	if Duress() && val < min {
		return min
	}
	return val
}
