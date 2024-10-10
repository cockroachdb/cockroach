// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

// LevenshteinDistanceWithCostAndThreshold calculates the Levenshtein distance
// between source and target. The distance is calculated using the given cost
// metrics for each of the three possible edit operations. The distance is
// calculated using the given threshold. If the distance is greater than the
// threshold, the function returns value greater than the threshold.
// Adapted from the 'Iterative with two matrix rows' approach within
// https://en.wikipedia.org/wiki/Levenshtein_distance. This approach provides us
// with O(n^2) time complexity and O(n) space complexity.
func computeLevenshteinDistance(source, target string, insCost, delCost, subCost int, threshold *int) int {
	if source == target {
		return 0
	}
	// Converting to a slice of runes allows us to count multi-byte characters
	// properly.
	s, t := []rune(source), []rune(target)
	lenS, lenT := len(s), len(t)
	if lenS == 0 {
		if threshold != nil && *threshold < lenT*insCost {
			return *threshold + 1
		}
		return lenT * insCost
	}
	if lenT == 0 {
		if threshold != nil && *threshold < lenS*delCost {
			return *threshold + 1
		}
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

	if threshold != nil && *threshold < rowA[lenT] {
		return *threshold + 1
	}
	return rowA[lenT]
}

// LevenshteinDistanceWithCostAndThreshold calculates the Levenshtein distance
// between source and target. The distance is calculated using the given cost
// metrics for each of the three possible edit operations. The distance is
// calculated using the given threshold. If the distance is greater than the
// threshold, the function returns value greater than the threshold.
func LevenshteinDistanceWithCostAndThreshold(source, target string, insCost, delCost, subCost int, threshold int) int {
	return computeLevenshteinDistance(source, target, insCost, delCost, subCost, &threshold)
}

// LevenshteinDistanceWithCost calculates the Levenshtein distance between
// source and target. The distance is calculated using the given cost metrics
// for each of the three possible edit operations.
func LevenshteinDistanceWithCost(source, target string, insCost, delCost, subCost int) int {
	return computeLevenshteinDistance(source, target, insCost, delCost, subCost, nil)
}

// LevenshteinDistance is the standard Levenshtein distance where the cost of
// insertion, deletion and substitution are all one. The distance is
// calculated using the given threshold. If the distance is greater than the
// threshold, the function returns value greater than the threshold.
func LevenshteinDistanceWithThreshold(source, target string, threshold int) int {
	return computeLevenshteinDistance(source, target, 1, 1, 1, &threshold)
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
