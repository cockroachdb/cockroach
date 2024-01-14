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

// LevenshteinLessEqualDistance is the standard Levenshtein distance where the cost of
// insertion, deletion and substitution are all one and we care about distances less than
// or equal to maxDist.
func LevenshteinLessEqualDistance(source, target string, maxD int) int {
	return LevenshteinLessEqualDistanceWithCost(source, target, 1, 1, 1, maxD)
}

// LevenshteinLessEqualDistanceWithCost is the Levenshtein distance between the source and
// the target with the given costs of insertion, deletion and substitution and we care about
// distances less than or equal to maxDist. This approach is adapted from the
// 'Iterative with two matrix rows' approach within https://en.wikipedia.org/wiki/Levenshtein_distance.
// A couple of optimizations have been added for tightening the bounds and for handling
// impossibly tight bound case where the result is definitely greater than given maxDist.
func LevenshteinLessEqualDistanceWithCost(
	source, target string, insCost, delCost, subCost, maxDist int,
) int {
	// Converting to a slice of runes allows us to count multi-byte characters
	// properly.
	s, t := []rune(source), []rune(target)
	lenS, lenT := len(s), len(t)

	startColumn := 0
	stopColumn := lenS + 1
	var netInserts int

	// If maxDist >= 0, determine whether the bound is impossibly tight.  If so,
	// return maxDist + 1 immediately.  Otherwise, determine whether it's tight
	// enough to limit the computation we must perform.  If so, figure out
	// initial stop column.
	if maxDist >= 0 {
		netInserts = lenT - lenS
		minTheoDist := netInserts * insCost
		if netInserts < 0 {
			minTheoDist = -netInserts * delCost
		}
		if minTheoDist > maxDist {
			return maxDist + 1
		}

		subCost = min2(subCost, insCost+delCost)
		maxTheoDist := minTheoDist + subCost*min2(lenS, lenT)
		if maxDist >= maxTheoDist {
			maxDist = -1
		} else if insCost+delCost > 0 {
			// Figure out how much of the first row of the notional matrix we
			// need to fill in.  If the string is growing, the theoretical
			// minimum distance already incorporates the cost of deleting the
			// number of characters necessary to make the two strings equal in
			// length.  Each additional deletion forces another insertion, so
			// the best-case total cost increases by insCost + delCost. If the
			// string is shrinking, the minimum theoretical cost assumes no
			// excess deletions; that is, we're starting no further right than
			// column n - m.  If we do start further right, the best-case
			// total cost increases by insCost + delCost for each move right.
			slackDist := maxDist - minTheoDist
			bestColumn := 0
			if netInserts < 0 {
				bestColumn = -netInserts
			}
			stopColumn = bestColumn + (slackDist / (insCost + delCost)) + 1
			if stopColumn > lenS {
				stopColumn = lenS + 1
			}
		}
	}

	// One more cell for initialization column and row.
	lenS++
	lenT++

	// Initialize previous and current rows of notional array.
	prev, curr := make([]int, lenS), make([]int, lenS)

	// To transform the first i characters of s into the first 0 characters of
	// t, we must perform i deletions.
	for i := startColumn; i < stopColumn; i++ {
		prev[i] = i * delCost
	}

	// Loop through rows of the notional array.
	for i := 1; i < lenT; i++ {
		// In the best case, values percolate down the diagonal unchanged, so
		// we must increment stopColumn unless it's already on the right end
		// of the array.  The inner loop will read prev[stopColumn], so we
		// have to initialize it even though it shouldn't affect the result.
		if stopColumn < lenS {
			prev[stopColumn] = maxDist + 1
			stopColumn++
		}

		// The main loop fills in curr, but curr[0] needs a special case: to
		// transform the first 0 characters of s into the first j characters
		// of t, we must perform j insertions.  However, if startColumn > 0,
		// this special case does not apply.
		j := 1
		if startColumn == 0 {
			curr[0] = i * insCost
		} else {
			j = startColumn
		}

		for ; j < stopColumn; j++ {
			// Calculate costs for insertion, deletion, and substitution.
			ins := prev[j] + insCost
			del := curr[j-1] + delCost
			var sub int
			if t[i-1] == s[j-1] {
				sub = prev[j-1]
			} else {
				sub = prev[j-1] + subCost
			}

			// Take the one with minimum cost.
			curr[j] = min3(del, ins, sub)
		}

		// Swap current row with previous row.
		prev, curr = curr, prev

		// This chunk of code represents a significant performance hit if used
		// in the case where there is no maxDist bound.  This is probably not
		// because the maxDist >= 0 test itself is expensive, but rather because
		// the possibility of needing to execute this code prevents tight
		// optimization of the loop as a whole.
		if maxDist >= 0 {
			// The "zero point" is the column of the current row where the
			// remaining portions of the strings are of equal length.  There
			// are (n - 1) characters in the target string, of which j have
			// been transformed.  There are (m - 1) characters in the source
			// string, so we want to find the value for zp where (n - 1) - j =
			// (m - 1) - zp.
			zp := i - (lenT - lenS)

			// Check whether the stop column can slide left.
			for stopColumn > 0 {
				ii := stopColumn - 1
				netInserts = ii - zp

				oc := -netInserts * delCost
				if netInserts > 0 {
					oc = netInserts * insCost
				}

				if prev[ii]+oc <= maxDist {
					break
				}

				stopColumn--
			}

			// Check whether the start column can slide right.
			for startColumn < stopColumn {
				netInserts = startColumn - zp

				oc := -netInserts * delCost
				if netInserts > 0 {
					oc = -netInserts * insCost
				}
				if prev[startColumn]+oc <= maxDist {
					break
				}

				// We'll never again update these values, so we must make sure
				// there's nothing here that could confuse any future
				// iteration of the outer loop.
				prev[startColumn] = maxDist + 1
				curr[startColumn] = maxDist + 1
				startColumn++
			}

			// If they cross, we're going to exceed the bound.
			if startColumn >= stopColumn {
				return maxDist + 1
			}
		}
	}

	// Because the final value was swapped from the previous row to the
	// current row, that's where we'll find it.
	return prev[lenS-1]
}

func min2(a, b int) int {
	if a < b {
		return a
	}
	return b
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
