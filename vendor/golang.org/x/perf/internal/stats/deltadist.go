// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

// DeltaDist is the Dirac delta function, centered at T, with total
// area 1.
//
// The CDF of the Dirac delta function is the Heaviside step function,
// centered at T. Specifically, f(T) == 1.
type DeltaDist struct {
	T float64
}

func (d DeltaDist) PDF(x float64) float64 {
	if x == d.T {
		return inf
	}
	return 0
}

func (d DeltaDist) pdfEach(xs []float64) []float64 {
	res := make([]float64, len(xs))
	for i, x := range xs {
		if x == d.T {
			res[i] = inf
		}
	}
	return res
}

func (d DeltaDist) CDF(x float64) float64 {
	if x >= d.T {
		return 1
	}
	return 0
}

func (d DeltaDist) cdfEach(xs []float64) []float64 {
	res := make([]float64, len(xs))
	for i, x := range xs {
		res[i] = d.CDF(x)
	}
	return res
}

func (d DeltaDist) InvCDF(y float64) float64 {
	if y < 0 || y > 1 {
		return nan
	}
	return d.T
}

func (d DeltaDist) Bounds() (float64, float64) {
	return d.T - 1, d.T + 1
}
