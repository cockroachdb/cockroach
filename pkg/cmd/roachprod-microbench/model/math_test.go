// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/perf/benchfmt"
	"golang.org/x/perf/benchseries"
)

// TestCalculateConfidenceInterval tests the `calculateConfidenceInterval`
// function, which. The test compares the result of the benchseries method with
// the internal method.
func TestCalculateConfidenceInterval(t *testing.T) {
	testCases := []struct {
		name      string
		oldValues []float64
		newValues []float64
	}{
		{
			name:      "identical values",
			oldValues: []float64{100, 100, 100, 100, 100},
			newValues: []float64{100, 100, 100, 100, 100},
		},
		{
			name:      "ten percent increase",
			oldValues: []float64{100, 100, 100, 100, 100},
			newValues: []float64{110, 110, 110, 110, 110},
		},
		{
			name:      "variable values",
			oldValues: []float64{95, 98, 100, 102, 105},
			newValues: []float64{105, 108, 110, 112, 115},
		},
		{
			name:      "high variance",
			oldValues: []float64{80, 90, 100, 110, 120},
			newValues: []float64{90, 100, 110, 120, 130},
		},
		{
			name:      "large dataset",
			oldValues: []float64{95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105},
			newValues: []float64{105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115},
		},
		{
			name:      "high variance large dataset",
			oldValues: []float64{80, 85, 90, 95, 100, 105, 110, 115, 120, 125, 130},
			newValues: []float64{90, 95, 100, 105, 110, 115, 120, 125, 130, 135, 140},
		},
		{
			name:      "small differences",
			oldValues: []float64{100, 100.1, 99.9, 100.2, 99.8, 100.3, 99.7, 100.4, 99.6},
			newValues: []float64{100.5, 100.6, 100.4, 100.7, 100.3, 100.8, 100.2, 100.9, 100.1},
		},
		{
			name:      "negative values",
			oldValues: []float64{-100, -90, -80, -70, -60, -50, -40, -30},
			newValues: []float64{-90, -80, -70, -60, -50, -40, -30, -20},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Calculate confidence interval using the benchseries method
			opts := benchseries.DefaultBuilderOptions()
			opts.Experiment = "run-stamp"
			opts.Compare = "cockroach"
			opts.Numerator = "experiment"
			opts.Denominator = "baseline"
			builder, err := benchseries.NewBuilder(opts)
			require.NoError(t, err)

			const testTimestamp = "2006-01-02T15:04:05.999Z"
			oldValues := make([]benchfmt.Value, len(tc.oldValues))
			for i, v := range tc.oldValues {
				oldValues[i] = benchfmt.Value{Value: v, Unit: "ns/op"}
			}
			oldResult := &benchfmt.Result{
				Name:   benchfmt.Name("Test"),
				Values: oldValues,
				Config: []benchfmt.Config{
					{Key: "run-stamp", Value: []byte(testTimestamp)},
					{Key: "cockroach", Value: []byte("baseline")},
				},
			}
			newValues := make([]benchfmt.Value, len(tc.newValues))
			for i, v := range tc.newValues {
				newValues[i] = benchfmt.Value{Value: v, Unit: "ns/op"}
			}
			newResult := &benchfmt.Result{
				Name:   benchfmt.Name("Test"),
				Values: newValues,
				Config: []benchfmt.Config{
					{Key: "run-stamp", Value: []byte(testTimestamp)},
					{Key: "experiment-commit-time", Value: []byte(testTimestamp)},
					{Key: "cockroach", Value: []byte("experiment")},
				},
			}
			builder.Add(oldResult)
			builder.Add(newResult)
			comparisons, err := builder.AllComparisonSeries(nil, benchseries.DUPE_REPLACE)
			if err != nil {
				t.Fatal(err)
			}
			var oldCI *benchseries.ComparisonSummary
			for _, cs := range comparisons {
				cs.AddSummaries(0.95, 1000)
				for idx := range cs.Benchmarks {
					oldCI = cs.Summaries[0][idx]
					break
				}
			}

			// Calculate confidence interval using the internal method.
			newCI := calculateConfidenceInterval(tc.newValues, tc.oldValues)

			// Assert that the two methods give the same result, within a small tolerance.
			// The tolerance is due to the randomization in the bootstrap.
			const tolerance = 0.05
			require.InDelta(t, oldCI.Low, newCI.Low, tolerance, "confidence interval lows differ")
			require.InDelta(t, oldCI.High, newCI.High, tolerance, "confidence interval highs differ")
		})
	}
}
