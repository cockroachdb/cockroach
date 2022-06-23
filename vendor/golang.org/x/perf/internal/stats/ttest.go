// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package stats

import (
	"errors"
	"math"
)

// A TTestResult is the result of a t-test.
type TTestResult struct {
	// N1 and N2 are the sizes of the input samples. For a
	// one-sample t-test, N2 is 0.
	N1, N2 int

	// T is the value of the t-statistic for this t-test.
	T float64

	// DoF is the degrees of freedom for this t-test.
	DoF float64

	// AltHypothesis specifies the alternative hypothesis tested
	// by this test against the null hypothesis that there is no
	// difference in the means of the samples.
	AltHypothesis LocationHypothesis

	// P is p-value for this t-test for the given null hypothesis.
	P float64
}

func newTTestResult(n1, n2 int, t, dof float64, alt LocationHypothesis) *TTestResult {
	dist := TDist{dof}
	var p float64
	switch alt {
	case LocationDiffers:
		p = 2 * (1 - dist.CDF(math.Abs(t)))
	case LocationLess:
		p = dist.CDF(t)
	case LocationGreater:
		p = 1 - dist.CDF(t)
	}
	return &TTestResult{N1: n1, N2: n2, T: t, DoF: dof, AltHypothesis: alt, P: p}
}

// A TTestSample is a sample that can be used for a one or two sample
// t-test.
type TTestSample interface {
	Weight() float64
	Mean() float64
	Variance() float64
}

var (
	ErrSampleSize        = errors.New("sample is too small")
	ErrZeroVariance      = errors.New("sample has zero variance")
	ErrMismatchedSamples = errors.New("samples have different lengths")
)

// TwoSampleTTest performs a two-sample (unpaired) Student's t-test on
// samples x1 and x2. This is a test of the null hypothesis that x1
// and x2 are drawn from populations with equal means. It assumes x1
// and x2 are independent samples, that the distributions have equal
// variance, and that the populations are normally distributed.
func TwoSampleTTest(x1, x2 TTestSample, alt LocationHypothesis) (*TTestResult, error) {
	n1, n2 := x1.Weight(), x2.Weight()
	if n1 == 0 || n2 == 0 {
		return nil, ErrSampleSize
	}
	v1, v2 := x1.Variance(), x2.Variance()
	if v1 == 0 && v2 == 0 {
		return nil, ErrZeroVariance
	}

	dof := n1 + n2 - 2
	v12 := ((n1-1)*v1 + (n2-1)*v2) / dof
	t := (x1.Mean() - x2.Mean()) / math.Sqrt(v12*(1/n1+1/n2))
	return newTTestResult(int(n1), int(n2), t, dof, alt), nil
}

// TwoSampleWelchTTest performs a two-sample (unpaired) Welch's t-test
// on samples x1 and x2. This is like TwoSampleTTest, but does not
// assume the distributions have equal variance.
func TwoSampleWelchTTest(x1, x2 TTestSample, alt LocationHypothesis) (*TTestResult, error) {
	n1, n2 := x1.Weight(), x2.Weight()
	if n1 <= 1 || n2 <= 1 {
		// TODO: Can we still do this with n == 1?
		return nil, ErrSampleSize
	}
	v1, v2 := x1.Variance(), x2.Variance()
	if v1 == 0 && v2 == 0 {
		return nil, ErrZeroVariance
	}

	dof := math.Pow(v1/n1+v2/n2, 2) /
		(math.Pow(v1/n1, 2)/(n1-1) + math.Pow(v2/n2, 2)/(n2-1))
	s := math.Sqrt(v1/n1 + v2/n2)
	t := (x1.Mean() - x2.Mean()) / s
	return newTTestResult(int(n1), int(n2), t, dof, alt), nil
}

// PairedTTest performs a two-sample paired t-test on samples x1 and
// x2. If μ0 is non-zero, this tests if the average of the difference
// is significantly different from μ0. If x1 and x2 are identical,
// this returns nil.
func PairedTTest(x1, x2 []float64, μ0 float64, alt LocationHypothesis) (*TTestResult, error) {
	if len(x1) != len(x2) {
		return nil, ErrMismatchedSamples
	}
	if len(x1) <= 1 {
		// TODO: Can we still do this with n == 1?
		return nil, ErrSampleSize
	}

	dof := float64(len(x1) - 1)

	diff := make([]float64, len(x1))
	for i := range x1 {
		diff[i] = x1[i] - x2[i]
	}
	sd := StdDev(diff)
	if sd == 0 {
		// TODO: Can we still do the test?
		return nil, ErrZeroVariance
	}
	t := (Mean(diff) - μ0) * math.Sqrt(float64(len(x1))) / sd
	return newTTestResult(len(x1), len(x2), t, dof, alt), nil
}

// OneSampleTTest performs a one-sample t-test on sample x. This tests
// the null hypothesis that the population mean is equal to μ0. This
// assumes the distribution of the population of sample means is
// normal.
func OneSampleTTest(x TTestSample, μ0 float64, alt LocationHypothesis) (*TTestResult, error) {
	n, v := x.Weight(), x.Variance()
	if n == 0 {
		return nil, ErrSampleSize
	}
	if v == 0 {
		// TODO: Can we still do the test?
		return nil, ErrZeroVariance
	}
	dof := n - 1
	t := (x.Mean() - μ0) * math.Sqrt(n) / math.Sqrt(v)
	return newTTestResult(int(n), 0, t, dof, alt), nil
}
