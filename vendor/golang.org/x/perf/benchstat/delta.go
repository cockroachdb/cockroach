// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Significance tests.

package benchstat

import (
	"errors"

	"golang.org/x/perf/internal/stats"
)

// A DeltaTest compares the old and new metrics and returns the
// expected probability that they are drawn from the same distribution.
//
// If a probability cannot be computed, the DeltaTest returns an
// error explaining why. Common errors include ErrSamplesEqual
// (all samples are equal), ErrSampleSize (there aren't enough samples),
// and ErrZeroVariance (the sample has zero variance).
//
// As a special case, the missing test NoDeltaTest returns -1, nil.
type DeltaTest func(old, new *Metrics) (float64, error)

// Errors returned by DeltaTest.
var (
	ErrSamplesEqual = errors.New("all equal")
	ErrSampleSize   = errors.New("too few samples")
	ErrZeroVariance = errors.New("zero variance")
)

// NoDeltaTest applies no delta test; it returns -1, nil.
func NoDeltaTest(old, new *Metrics) (pval float64, err error) {
	return -1, nil
}

// TTest is a DeltaTest using the two-sample Welch t-test.
func TTest(old, new *Metrics) (pval float64, err error) {
	t, err := stats.TwoSampleWelchTTest(stats.Sample{Xs: old.RValues}, stats.Sample{Xs: new.RValues}, stats.LocationDiffers)
	if err != nil {
		return -1, convertErr(err)
	}
	return t.P, nil
}

// UTest is a DeltaTest using the Mann-Whitney U test.
func UTest(old, new *Metrics) (pval float64, err error) {
	u, err := stats.MannWhitneyUTest(old.RValues, new.RValues, stats.LocationDiffers)
	if err != nil {
		return -1, convertErr(err)
	}
	return u.P, nil
}

// convertErr converts from the stats package's internal errors
// to errors exported by this package and expected from
// a DeltaTest.
// Using different errors makes it possible for clients to use
// package benchstat without access to the internal stats package,
// and it also gives us a chance to use shorter error messages.
func convertErr(err error) error {
	switch err {
	case stats.ErrZeroVariance:
		return ErrZeroVariance
	case stats.ErrSampleSize:
		return ErrSampleSize
	case stats.ErrSamplesEqual:
		return ErrSamplesEqual
	}
	return err
}
