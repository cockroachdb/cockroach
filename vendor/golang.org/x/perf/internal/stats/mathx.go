// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import "math"

// mathSign returns the sign of x: -1 if x < 0, 0 if x == 0, 1 if x > 0.
// If x is NaN, it returns NaN.
func mathSign(x float64) float64 {
	if x == 0 {
		return 0
	} else if x < 0 {
		return -1
	} else if x > 0 {
		return 1
	}
	return nan
}

const smallFactLimit = 20 // 20! => 62 bits
var smallFact [smallFactLimit + 1]int64

func init() {
	smallFact[0] = 1
	fact := int64(1)
	for n := int64(1); n <= smallFactLimit; n++ {
		fact *= n
		smallFact[n] = fact
	}
}

// mathChoose returns the binomial coefficient of n and k.
func mathChoose(n, k int) float64 {
	if k == 0 || k == n {
		return 1
	}
	if k < 0 || n < k {
		return 0
	}
	if n <= smallFactLimit { // Implies k <= smallFactLimit
		// It's faster to do several integer multiplications
		// than it is to do an extra integer division.
		// Remarkably, this is also faster than pre-computing
		// Pascal's triangle (presumably because this is very
		// cache efficient).
		numer := int64(1)
		for n1 := int64(n - (k - 1)); n1 <= int64(n); n1++ {
			numer *= n1
		}
		denom := smallFact[k]
		return float64(numer / denom)
	}

	return math.Exp(lchoose(n, k))
}

// mathLchoose returns math.Log(mathChoose(n, k)).
func mathLchoose(n, k int) float64 {
	if k == 0 || k == n {
		return 0
	}
	if k < 0 || n < k {
		return math.NaN()
	}
	return lchoose(n, k)
}

func lchoose(n, k int) float64 {
	a, _ := math.Lgamma(float64(n + 1))
	b, _ := math.Lgamma(float64(k + 1))
	c, _ := math.Lgamma(float64(n - k + 1))
	return a - b - c
}
