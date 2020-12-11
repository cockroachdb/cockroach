// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fuzzystrmatch

// LevenshteinDistanceWithCost calculates the Levenshtein distance between
// source and target. The distance is calculated using the given cost metrics
// for each of the three possible edit operations.
// Adapted from the 'Iterative with two matrix rows' approach within
// https://en.wikipedia.org/wiki/Levenshtein_distance. This approach provides us
// with O(n^2) time complexity and O(n) space complexity.
func LevenshteinDistanceWithCost(source, target string, insCost, delCost, subCost int) int {
	if source == target {
		return 0
	}
	// Converting to a slice of runes allows us to count multi-byte characters
	// properly.
	s, t := []rune(source), []rune(target)
	lenS, lenT := len(s), len(t)
	if lenS == 0 {
		return lenT * insCost
	}
	if lenT == 0 {
		return lenS * delCost
	}
	// The algorithm is equivalent to building up an NxM matrix of the Levenshtein
	// distances between prefixes of source and target. However, instead of
	// maintaining the entire matrix in memory, we will only keep two rows of the
	// matrix at any given time.
	rowA, rowB := make([]int, lenT+1), make([]int, lenT+1)
	// The first row is the cost to edit the first prefix of source (empty string)
	// to become each prefix of target.
	for i := 0; i <= lenT; i++ {
		rowA[i] = i * insCost
	}
	for i := 0; i < lenS; i++ {
		// The first column is the cost to edit every prefix of source to become the
		// first prefix of target (empty string). Which is equivalent to the cost of
		// deleting every character in each prefix of source.
		rowB[0] = (i + 1) * delCost
		for j := 0; j < lenT; j++ {
			deletionCost := rowA[j+1] + delCost
			insertionCost := rowB[j] + insCost
			var substitutionCost int
			if s[i] == t[j] {
				substitutionCost = rowA[j]
			} else {
				substitutionCost = rowA[j] + subCost
			}
			rowB[j+1] = min3(deletionCost, insertionCost, substitutionCost)
		}
		rowA, rowB = rowB, rowA
	}
	return rowA[lenT]
}

// LevenshteinDistance is the standard Levenshtein distance where the cost of
// insertion, deletion and substitution are all one.
func LevenshteinDistance(source, target string) int {
	return LevenshteinDistanceWithCost(source, target, 1, 1, 1)
}

func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
	} else if b < c {
		return b
	}
	return c
}
