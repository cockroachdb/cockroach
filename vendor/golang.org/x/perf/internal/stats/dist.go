// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import "math/rand"

// A DistCommon is a statistical distribution. DistCommon is a base
// interface provided by both continuous and discrete distributions.
type DistCommon interface {
	// CDF returns the cumulative probability Pr[X <= x].
	//
	// For continuous distributions, the CDF is the integral of
	// the PDF from -inf to x.
	//
	// For discrete distributions, the CDF is the sum of the PMF
	// at all defined points from -inf to x, inclusive. Note that
	// the CDF of a discrete distribution is defined for the whole
	// real line (unlike the PMF) but has discontinuities where
	// the PMF is non-zero.
	//
	// The CDF is a monotonically increasing function and has a
	// domain of all real numbers. If the distribution has bounded
	// support, it has a range of [0, 1]; otherwise it has a range
	// of (0, 1). Finally, CDF(-inf)==0 and CDF(inf)==1.
	CDF(x float64) float64

	// Bounds returns reasonable bounds for this distribution's
	// PDF/PMF and CDF. The total weight outside of these bounds
	// should be approximately 0.
	//
	// For a discrete distribution, both bounds are integer
	// multiples of Step().
	//
	// If this distribution has finite support, it returns exact
	// bounds l, h such that CDF(l')=0 for all l' < l and
	// CDF(h')=1 for all h' >= h.
	Bounds() (float64, float64)
}

// A Dist is a continuous statistical distribution.
type Dist interface {
	DistCommon

	// PDF returns the value of the probability density function
	// of this distribution at x.
	PDF(x float64) float64
}

// A DiscreteDist is a discrete statistical distribution.
//
// Most discrete distributions are defined only at integral values of
// the random variable. However, some are defined at other intervals,
// so this interface takes a float64 value for the random variable.
// The probability mass function rounds down to the nearest defined
// point. Note that float64 values can exactly represent integer
// values between ±2**53, so this generally shouldn't be an issue for
// integer-valued distributions (likewise, for half-integer-valued
// distributions, float64 can exactly represent all values between
// ±2**52).
type DiscreteDist interface {
	DistCommon

	// PMF returns the value of the probability mass function
	// Pr[X = x'], where x' is x rounded down to the nearest
	// defined point on the distribution.
	//
	// Note for implementers: for integer-valued distributions,
	// round x using int(math.Floor(x)). Do not use int(x), since
	// that truncates toward zero (unless all x <= 0 are handled
	// the same).
	PMF(x float64) float64

	// Step returns s, where the distribution is defined for sℕ.
	Step() float64
}

// TODO: Add a Support method for finite support distributions? Or
// maybe just another return value from Bounds indicating that the
// bounds are exact?

// TODO: Plot method to return a pre-configured Plot object with
// reasonable bounds and an integral function? Have to distinguish
// PDF/CDF/InvCDF. Three methods? Argument?
//
// Doesn't have to be a method of Dist. Could be just a function that
// takes a Dist and uses Bounds.

// InvCDF returns the inverse CDF function of the given distribution
// (also known as the quantile function or the percent point
// function). This is a function f such that f(dist.CDF(x)) == x. If
// dist.CDF is only weakly monotonic (that it, there are intervals
// over which it is constant) and y > 0, f returns the smallest x that
// satisfies this condition. In general, the inverse CDF is not
// well-defined for y==0, but for convenience if y==0, f returns the
// largest x that satisfies this condition. For distributions with
// infinite support both the largest and smallest x are -Inf; however,
// for distributions with finite support, this is the lower bound of
// the support.
//
// If y < 0 or y > 1, f returns NaN.
//
// If dist implements InvCDF(float64) float64, this returns that
// method. Otherwise, it returns a function that uses a generic
// numerical method to construct the inverse CDF at y by finding x
// such that dist.CDF(x) == y. This may have poor precision around
// points of discontinuity, including f(0) and f(1).
func InvCDF(dist DistCommon) func(y float64) (x float64) {
	type invCDF interface {
		InvCDF(float64) float64
	}
	if dist, ok := dist.(invCDF); ok {
		return dist.InvCDF
	}

	// Otherwise, use a numerical algorithm.
	//
	// TODO: For discrete distributions, use the step size to
	// inform this computation.
	return func(y float64) (x float64) {
		const almostInf = 1e100
		const xtol = 1e-16

		if y < 0 || y > 1 {
			return nan
		} else if y == 0 {
			l, _ := dist.Bounds()
			if dist.CDF(l) == 0 {
				// Finite support
				return l
			} else {
				// Infinite support
				return -inf
			}
		} else if y == 1 {
			_, h := dist.Bounds()
			if dist.CDF(h) == 1 {
				// Finite support
				return h
			} else {
				// Infinite support
				return inf
			}
		}

		// Find loX, hiX for which cdf(loX) < y <= cdf(hiX).
		var loX, loY, hiX, hiY float64
		x1, y1 := 0.0, dist.CDF(0)
		xdelta := 1.0
		if y1 < y {
			hiX, hiY = x1, y1
			for hiY < y && hiX != inf {
				loX, loY, hiX = hiX, hiY, hiX+xdelta
				hiY = dist.CDF(hiX)
				xdelta *= 2
			}
		} else {
			loX, loY = x1, y1
			for y <= loY && loX != -inf {
				hiX, hiY, loX = loX, loY, loX-xdelta
				loY = dist.CDF(loX)
				xdelta *= 2
			}
		}
		if loX == -inf {
			return loX
		} else if hiX == inf {
			return hiX
		}

		// Use bisection on the interval to find the smallest
		// x at which cdf(x) <= y.
		_, x = bisectBool(func(x float64) bool {
			return dist.CDF(x) < y
		}, loX, hiX, xtol)
		return
	}
}

// Rand returns a random number generator that draws from the given
// distribution. The returned generator takes an optional source of
// randomness; if this is nil, it uses the default global source.
//
// If dist implements Rand(*rand.Rand) float64, Rand returns that
// method. Otherwise, it returns a generic generator based on dist's
// inverse CDF (which may in turn use an efficient implementation or a
// generic numerical implementation; see InvCDF).
func Rand(dist DistCommon) func(*rand.Rand) float64 {
	type distRand interface {
		Rand(*rand.Rand) float64
	}
	if dist, ok := dist.(distRand); ok {
		return dist.Rand
	}

	// Otherwise, use a generic algorithm.
	inv := InvCDF(dist)
	return func(r *rand.Rand) float64 {
		var y float64
		for y == 0 {
			if r == nil {
				y = rand.Float64()
			} else {
				y = r.Float64()
			}
		}
		return inv(y)
	}
}
