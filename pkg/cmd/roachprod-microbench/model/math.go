// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package model

import (
	"math"
	"math/rand"
	"sort"
)

const resampleCount = 1000
const confidence = 0.95

// calculateConfidenceInterval calculates the confidence interval for the ratio
// of two sets of values. The confidence interval is calculated using a
// bootstrap method.
func calculateConfidenceInterval(newValues, oldValues []float64) ConfidenceInterval {
	rng := rand.New(rand.NewSource(hash(newValues) + hash(oldValues)))
	ratios := make([]float64, 0, resampleCount)
	resNew := make([]float64, len(newValues))
	resOld := make([]float64, len(oldValues))
	for range resampleCount {
		resample(rng, newValues, resNew)
		sort.Float64s(resNew)
		resample(rng, oldValues, resOld)
		sort.Float64s(resOld)

		medOld := median(resOld)
		// Skip if the old median is 0 to avoid division by zero.
		if medOld != 0 {
			ratios = append(ratios, median(resNew)/medOld)
		}
	}
	if len(ratios) == 0 {
		return ConfidenceInterval{}
	}
	sort.Float64s(ratios)
	alpha := (1.0 - confidence) / 2.0
	lowerIndex := int(math.Floor(float64(len(ratios)) * alpha))
	upperIndex := int(math.Floor(float64(len(ratios)) * (1.0 - alpha)))
	return ConfidenceInterval{
		Low:    ratios[lowerIndex],
		High:   ratios[upperIndex],
		Center: median(ratios),
	}
}

// resample samples a slice of values with replacement.
func resample(r *rand.Rand, src, dest []float64) {
	length := len(src)
	for i := range dest {
		dest[i] = src[r.Intn(length)]
	}
}

// hash returns an arbitrary hash of the given values.
func hash(data []float64) int64 {
	var hashValue int64
	for _, d := range data {
		hashValue += (int64)(math.Float64bits(d))
	}
	return hashValue
}

// median returns the median of a sorted slice of values.
func median(values []float64) float64 {
	length := len(values)
	if length%2 == 0 {
		return (values[length/2] + values[length/2-1]) / 2
	}
	return values[length/2]
}
