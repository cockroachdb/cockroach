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
	ratios := make([]float64, resampleCount)
	resNew := make([]float64, len(newValues))
	resOld := make([]float64, len(oldValues))
	for i := 0; i < resampleCount; i++ {
		resample(rng, newValues, resNew)
		resample(rng, oldValues, resOld)
		medOld := median(resOld)
		if medOld == 0 {
			n := median(resNew)
			if n >= 0 {
				ratios[i] = n + 1
			} else {
				ratios[i] = n - 1
			}
		} else {
			ratios[i] = median(resNew) / medOld
		}
	}
	sort.Float64s(ratios)
	p := (1 - confidence) / 2
	return ConfidenceInterval{
		Low:    percentile(ratios, p),
		High:   percentile(ratios, 1-p),
		Center: median(ratios),
	}
}

// resample samples a slice of values with replacement.
func resample(r *rand.Rand, src, dest []float64) {
	l := len(src)
	for i := range dest {
		dest[i] = src[r.Intn(l)]
	}
	sort.Float64s(dest)
}

// hash returns an arbitrary hash of the given values.
func hash(a []float64) int64 {
	var h int64
	for _, x := range a {
		h += (int64)(math.Float64bits(x))
	}
	return h
}

// percentile returns the value at the given percentile in a sorted slice of
// values.
func percentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	if percentile == 0 {
		return values[0]
	}
	length := len(values)
	if percentile == 1 {
		return values[length-1]
	}
	position := float64(length) * percentile
	index := int(position)
	fraction := position - float64(index)
	result := values[index]
	if fraction > 0 && index+1 < len(values) {
		result = (result * (1 - fraction)) + (values[index+1] * fraction)
	}
	return result
}

// median returns the median of a sorted slice of values.
func median(values []float64) float64 {
	length := len(values)
	if length&1 == 1 {
		return values[length/2]
	}
	return (values[length/2] + values[length/2-1]) / 2
}
