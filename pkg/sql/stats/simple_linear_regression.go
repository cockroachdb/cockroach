// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import "github.com/cockroachdb/errors"

// float64SimpleLinearRegression fits a simple linear model y = α + βx to a set
// of observations of x and y, and uses this model to predict ŷₙ given an
// xₙ. The R² (goodness of fit) measurement of the model is also returned.
func float64SimpleLinearRegression(x, y []float64, xₙ float64) (yₙ, r2 float64) {
	// We use the ordinary least squares method to find the best fitting α̂ and β̂
	// for linear model y = α + βx. This allows us to solve for α̂ and β̂ directly:
	//
	//   α̂ = y̅ - β̂x̅
	//
	//       ∑ (xᵢ - x̅) (yᵢ - y̅)
	//   β̂ = ---------------------
	//           ∑ (xᵢ - x̅)²
	//
	//            ∑ (yᵢ - ŷᵢ)²
	//   R² = 1 - ------------
	//            ∑ (yᵢ - y̅)²
	//
	// where:
	// * all ∑ are from i=0 to i=n-1
	// * x̅ = (1/n) ∑ xᵢ
	// * y̅ = (1/n) ∑ yᵢ
	// * ŷᵢ = α̂ + β̂xᵢ
	//
	// See https://en.wikipedia.org/wiki/Simple_linear_regression and
	// https://en.wikipedia.org/wiki/Coefficient_of_determination for
	// background.
	//
	// Note that this uses a naive two-pass algorithm. We expect there to be less
	// than a dozen observations, so this is fine.
	// TODO(michae2): Switch to a one-pass algorithm based on Welford-Knuth, see:
	// https://www.johndcook.com/blog/running_regression/

	if len(x) != len(y) {
		panic(errors.AssertionFailedf("x and y had different numbers of observations"))
	}
	if len(x) == 0 {
		panic(errors.AssertionFailedf("zero observations"))
	}

	n := len(x)

	// Calculate x̅ and y̅ (means of x and y).
	var Σx, Σy float64
	for i := 0; i < n; i++ {
		Σx += x[i]
		Σy += y[i]
	}
	xM, yM := Σx/float64(n), Σy/float64(n)

	// Solve for α̂ and β̂.
	var Σxx, Σxy, Σyy float64
	for i := 0; i < n; i++ {
		xD, yD := x[i]-xM, y[i]-yM
		Σxx += xD * xD
		Σxy += xD * yD
		Σyy += yD * yD
	}
	var α, β float64
	// Σxx could be zero if our x's are all the same. In this case Σxy is also
	// zero, and more generally y is not dependent on x, so β̂ is zero.
	if Σxx != 0 {
		β = Σxy / Σxx
	}
	α = yM - β*xM

	// Now use the model to calculate R² and predict ŷₙ given xₙ.
	var Σεε float64
	for i := 0; i < n; i++ {
		yHat := α + β*x[i]
		ε := y[i] - yHat
		Σεε += ε * ε
	}
	yₙ = α + β*xₙ
	// Σyy could be zero if our y's are all the same. In this case we will have a
	// perfect fit and Σεε will also be zero.
	if Σyy == 0 {
		r2 = 1
	} else {
		r2 = 1 - Σεε/Σyy
	}
	return yₙ, r2
}

// quantileSimpleLinearRegression fits a simple linear model y(t) = α(t) + β(t)x
// to a set of observations of x and y(t), and uses this model to predict ŷₙ(t)
// given an xₙ, where y(t), α(t), β(t), and ŷₙ(t) are all quantile functions.
// The R² (goodness of fit) measurement of the model is also returned. The
// returned quantile function yₙ could be malformed, so callers will likely want
// to call yₙ.fixMalformed() afterward.
func quantileSimpleLinearRegression(
	x []float64, y []quantile, xₙ float64,
) (yₙ quantile, r2 float64) {
	// We use the ordinary least squares method to find the best fitting α̂(t) and
	// β̂(t) for linear model y(t) = α(t) + β(t)x. This allows us to solve for α̂(t)
	// and β̂(t) directly. The equations are similar to those used in scalar
	// ordinary least squares, but with definite integrals in a few places to turn
	// quantile functions into scalars:
	//
	//   α̂(t) = y̅(t) - β̂(t)x̅
	//
	//          ∑ (xᵢ - x̅) (yᵢ(t) - y̅(t))
	//   β̂(t) = -------------------------
	//                ∑ (xᵢ - x̅)²
	//
	//            ∑ ∫ (yᵢ(t) - ŷᵢ(t))² dt
	//   R² = 1 - -----------------------
	//            ∑ ∫ (yᵢ(t) - y̅(t))² dt
	//
	// where:
	// * all ∑ are from i=0 to i=n-1
	// * all ∫ are definite integrals w.r.t. t from 0 to 1
	// * x̅ = (1/n) ∑ xᵢ
	// * y̅(t) = (1/n) ∑ yᵢ(t)
	// * ŷᵢ(t) = α̂(t) + β̂(t)xᵢ
	//
	// This approach comes from "Ordinary Least Squares for Histogram Data Based
	// on Wasserstein Distance" by Verde and Irpino, 2010. We use a slightly
	// different model than Verde and Irpino to accommodate scalar x's, but the
	// method of using quantile functions and Wasserstein Distance is the same.
	//
	// Wasserstein distance (a.k.a. "earth mover's distance") is a way of
	// measuring the difference between two probability distributions. Here we're
	// specifically using the 2nd Wasserstein distance over 1-dimensional
	// distributions (W₂), which is defined as:
	//
	//                      1
	//   W₂[q₁(t), q₂(t)] = ∫ (q₁(t) - q₂(t))² dt
	//                      0
	//
	// where q₁(t) and q₂(t) are quantile functions of the distributions. In terms
	// of our quantiles, this Wasserstein distance is calculated as:
	//
	//   W₂[q₁(t), q₂(t)] = q₁.sub(q₂).integrateSquared()
	//
	// See the paper, https://en.wikipedia.org/wiki/Wasserstein_metric and also
	// https://en.wikipedia.org/wiki/Earth_mover%27s_distance for some background.
	//
	// Note that this uses a naive two-pass algorithm. We expect there to be less
	// than a dozen observations, so this is fine.
	// TODO(michae2): Switch to a one-pass algorithm based on Welford-Knuth, see:
	// https://www.johndcook.com/blog/running_regression/

	if len(x) != len(y) {
		panic(errors.AssertionFailedf("x and y had different numbers of observations"))
	}
	if len(x) == 0 {
		panic(errors.AssertionFailedf("zero observations"))
	}

	n := len(x)

	// Calculate x̅ and y̅(t) (means of x and y).
	var Σx float64
	Σy := zeroQuantile
	for i := 0; i < n; i++ {
		Σx += x[i]
		Σy = Σy.add(y[i])
	}
	xM, yM := Σx/float64(n), Σy.mult(1/float64(n))

	// Solve for α̂(t) and β̂(t).
	var Σxx, Σyy float64
	Σxy := zeroQuantile
	for i := 0; i < n; i++ {
		xD, yD := x[i]-xM, y[i].sub(yM)
		Σxx += xD * xD
		Σxy = Σxy.add(yD.mult(xD))
		Σyy += yD.integrateSquared()
	}
	var α, β quantile
	// Σxx could be zero if our x's are all the same. In this case Σxy is the zero
	// quantile, and more generally y(t) is not dependent on x, so β̂(t) is the
	// zero quantile.
	β = zeroQuantile
	if Σxx != 0 {
		β = Σxy.mult(1 / Σxx)
	}
	α = yM.sub(β.mult(xM))
	// Note that at this point α̂(t) and β̂(t) could be malformed quantiles. This is
	// probably fine, because they represent changes to distributions, rather than
	// distributions themselves. We'll use them as they are.

	// Now use the model to calculate R² and predict ŷₙ(t) given xₙ.
	var Σεε float64
	for i := 0; i < n; i++ {
		yHat := α.add(β.mult(x[i]))
		ε := y[i].sub(yHat)
		Σεε += ε.integrateSquared()
	}
	yₙ = α.add(β.mult(xₙ))
	// Σyy could be zero if our y(t)'s are all the same. In this case we will have a
	// perfect fit, and Σεε will also be zero.
	if Σyy == 0 {
		r2 = 1
	} else {
		r2 = 1 - Σεε/Σyy
	}
	return yₙ, r2
}
