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

package arith

import "math"

// AddWithOverflow returns a+b. If ok is false, a+b overflowed.
func AddWithOverflow(a, b int64) (r int64, ok bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

// MulHalfPositiveWithOverflow returns a*b. b must be positive. If ok
// is false, a*b overflowed.
func MulHalfPositiveWithOverflow(a, b int64) (r int64, ok bool) {
	if a >= 0 {
		if a > math.MaxInt64/b {
			return 0, false
		}
	} else {
		if a < math.MinInt64/b {
			return 0, false
		}
	}
	return a * b, true
}
