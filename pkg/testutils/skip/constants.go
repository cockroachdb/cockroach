// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package skip

// ClampMetamorphicConstantUnderStress ensures that the given integer constant
// with metamorphic testing range is at least the given minimum value, when the
// process is running under stress.
func ClampMetamorphicConstantUnderStress(val, min int) int {
	if Stress() && val < min {
		return min
	}
	return val
}
