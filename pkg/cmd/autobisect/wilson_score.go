// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"math"
)

func wilsonBounds(hits, observations int, conf float64) (float64, float64) {
	// https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Wilson_score_interval
	// p^ = hits/samples
	// z = confidence level (e.g. 0.95)
	// n = observations
	n := float64(observations)
	phat := float64(hits) / n
	probit := 1 - (1-conf)/2      // 1 - error/2
	z, ok := map[float64]float64{ // probit -> z
		0.975: 1.96,
	}[probit]
	if !ok {
		panic(fmt.Sprintf("probit %f not found in table", probit)) // you can add it
	}

	bounds := make([]float64, 2)
	for i, sign := range []float64{-1, 1} {
		bounds[i] = (phat + z*z/(2*n)) + sign*z*math.Sqrt((phat*(1-phat)+z*z/(4*n))/n) // denominator
		bounds[i] /= 1 + z*z/n
		if math.IsNaN(bounds[i]) || math.IsInf(bounds[i], 0) {
			// Lower bound falls back to zero, upper bound falls back to 1.
			bounds[i] = math.Max(sign, 0)
		}
	}
	return bounds[0], bounds[1]
}
