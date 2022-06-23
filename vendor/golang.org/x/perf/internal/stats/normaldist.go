// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"math"
	"math/rand"
)

// NormalDist is a normal (Gaussian) distribution with mean Mu and
// standard deviation Sigma.
type NormalDist struct {
	Mu, Sigma float64
}

// StdNormal is the standard normal distribution (Mu = 0, Sigma = 1)
var StdNormal = NormalDist{0, 1}

// 1/sqrt(2 * pi)
const invSqrt2Pi = 0.39894228040143267793994605993438186847585863116493465766592583

func (n NormalDist) PDF(x float64) float64 {
	z := x - n.Mu
	return math.Exp(-z*z/(2*n.Sigma*n.Sigma)) * invSqrt2Pi / n.Sigma
}

func (n NormalDist) pdfEach(xs []float64) []float64 {
	res := make([]float64, len(xs))
	if n.Mu == 0 && n.Sigma == 1 {
		// Standard normal fast path
		for i, x := range xs {
			res[i] = math.Exp(-x*x/2) * invSqrt2Pi
		}
	} else {
		a := -1 / (2 * n.Sigma * n.Sigma)
		b := invSqrt2Pi / n.Sigma
		for i, x := range xs {
			z := x - n.Mu
			res[i] = math.Exp(z*z*a) * b
		}
	}
	return res
}

func (n NormalDist) CDF(x float64) float64 {
	return math.Erfc(-(x-n.Mu)/(n.Sigma*math.Sqrt2)) / 2
}

func (n NormalDist) cdfEach(xs []float64) []float64 {
	res := make([]float64, len(xs))
	a := 1 / (n.Sigma * math.Sqrt2)
	for i, x := range xs {
		res[i] = math.Erfc(-(x-n.Mu)*a) / 2
	}
	return res
}

func (n NormalDist) InvCDF(p float64) (x float64) {
	// This is based on Peter John Acklam's inverse normal CDF
	// algorithm: http://home.online.no/~pjacklam/notes/invnorm/
	const (
		a1 = -3.969683028665376e+01
		a2 = 2.209460984245205e+02
		a3 = -2.759285104469687e+02
		a4 = 1.383577518672690e+02
		a5 = -3.066479806614716e+01
		a6 = 2.506628277459239e+00

		b1 = -5.447609879822406e+01
		b2 = 1.615858368580409e+02
		b3 = -1.556989798598866e+02
		b4 = 6.680131188771972e+01
		b5 = -1.328068155288572e+01

		c1 = -7.784894002430293e-03
		c2 = -3.223964580411365e-01
		c3 = -2.400758277161838e+00
		c4 = -2.549732539343734e+00
		c5 = 4.374664141464968e+00
		c6 = 2.938163982698783e+00

		d1 = 7.784695709041462e-03
		d2 = 3.224671290700398e-01
		d3 = 2.445134137142996e+00
		d4 = 3.754408661907416e+00

		plow  = 0.02425
		phigh = 1 - plow
	)

	if p < 0 || p > 1 {
		return nan
	} else if p == 0 {
		return -inf
	} else if p == 1 {
		return inf
	}

	if p < plow {
		// Rational approximation for lower region.
		q := math.Sqrt(-2 * math.Log(p))
		x = (((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	} else if phigh < p {
		// Rational approximation for upper region.
		q := math.Sqrt(-2 * math.Log(1-p))
		x = -(((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	} else {
		// Rational approximation for central region.
		q := p - 0.5
		r := q * q
		x = (((((a1*r+a2)*r+a3)*r+a4)*r+a5)*r + a6) * q /
			(((((b1*r+b2)*r+b3)*r+b4)*r+b5)*r + 1)
	}

	// Refine approximation.
	e := 0.5*math.Erfc(-x/math.Sqrt2) - p
	u := e * math.Sqrt(2*math.Pi) * math.Exp(x*x/2)
	x = x - u/(1+x*u/2)

	// Adjust from standard normal.
	return x*n.Sigma + n.Mu
}

func (n NormalDist) Rand(r *rand.Rand) float64 {
	var x float64
	if r == nil {
		x = rand.NormFloat64()
	} else {
		x = r.NormFloat64()
	}
	return x*n.Sigma + n.Mu
}

func (n NormalDist) Bounds() (float64, float64) {
	const stddevs = 3
	return n.Mu - stddevs*n.Sigma, n.Mu + stddevs*n.Sigma
}
