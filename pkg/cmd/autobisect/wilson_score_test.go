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
	"math"
	"math/rand"
	"testing"
)

func neededRunsForLikelyRepro(failPLowerBound float64, confidence float64) int {
	return int(math.Log(1-confidence) / math.Log(1-failPLowerBound))
}

func TestWilsonScore(t *testing.T) {
	realP := 0.002
	var hits int
	for observations := 0; observations < 100000; observations++ {
		if rand.Float64() < realP {
			hits++
		}
		lo, hi := wilsonBounds(hits, observations, 0.95)
		t.Logf("[%5f, %5f] @ (obs=%d, hits=%d)", lo, hi, observations, hits)
		if hits > 0 {
			const conf = 0.999
			t.Logf("would need N=%d runs to make P(no failure in N runs) < %f", neededRunsForLikelyRepro(lo, conf), conf)
		}
	}
}

// P: probability of not seeing a failure in N observations: (1-p)^N
// P = k  <=>     (1-p)^N = k   <=>   N log(1-p) = log(k)  <=> N = log(k)/log(1-p)

func TestNeededRunsForLikelyRepro(t *testing.T) {
	hits := 1
	observations := 2898
	lo, _ := wilsonBounds(hits, observations, 0.95)
	t.Log(neededRunsForLikelyRepro(lo, 0.99))
}
