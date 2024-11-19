// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import "math/rand"

// randBool randomly picks between true and false.
func randBool(randSource *rand.Rand) bool {
	return randSource.Intn(2) == 0
}

// randBetweenMinMaxExclusive randomly selects a number∈[min, max).
func randBetweenMinMaxExclusive(randSource *rand.Rand, min int, max int) int {
	return randSource.Intn(max-min) + min
}

// randIndex randomly selects an index given the length of an array ∈[0,
// lenOfArr).
func randIndex(randSource *rand.Rand, lenOfArr int) int {
	return randBetweenMinMaxExclusive(randSource, 0, lenOfArr)
}
