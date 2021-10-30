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

func levenshtein(source, target string, insCost, delCost, subCost, maxDistance int) int {
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

	startColumn := 0
	endColumn := lenS + 1

	if maxDistance >= 0 {

		netInserts := lenT - lenS
		minTheoD := netInserts * insCost

		if netInserts < 0 {
			minTheoD = -netInserts * delCost
		}
		if minTheoD > maxDistance {
			return maxDistance + 1
		}
		if insCost+delCost < subCost {
			subCost = insCost + delCost
		}
		maxTheoD := minTheoD + subCost*min2(lenS, lenT)
		if maxDistance >= maxTheoD {
			maxDistance = -1
		} else if insCost+delCost > 0 {
			slackD := maxDistance - minTheoD
			bestColumn := 0
			if netInserts < 0 {
				bestColumn = -netInserts
			}
			endColumn = bestColumn + (slackD / (insCost + delCost)) + 1
			if endColumn > lenS {
				endColumn = lenS + 1
			}
		}
	}

	lenS++
	lenT++

	// The algorithm is equivalent to building up an NxM matrix of the Levenshtein
	// distances between prefixes of source and target. However, instead of
	// maintaining the entire matrix in memory, we will only keep two rows of the
	// matrix at any given time.
	prev, curr := make([]int, lenS), make([]int, lenS)

	// The first column is the cost to edit every prefix of source to become the
	// first prefix of target (empty string). Which is equivalent to the cost of
	// deleting every character in each prefix of source.
	for i := 0; i < endColumn; i++ {
		prev[i] = i * delCost
	}

	// Loop over the remainder of the target string.
	for i := 1; i < lenT; i++ {
		j := 1
		if maxDistance >= 0 {
			// Slide the window
			if endColumn < lenS {
				prev[endColumn] = maxDistance + 1
				endColumn++
			}

			// We need to fill curr[0] only at the first iteration
			if startColumn == 0 {
				curr[0] = i * insCost
			} else {
				j = startColumn
			}
		} else {
			curr[0] = i * insCost
		}

		for ; j < endColumn; j++ {
			// The cost of transforming source[0:j] into target[0:i]
			insertionCost := prev[j] + insCost
			deletionCost := curr[j-1] + delCost
			var substitutionCost int
			if t[i-1] == s[j-1] {
				substitutionCost = prev[j-1]
			} else {
				substitutionCost = prev[j-1] + subCost
			}
			curr[j] = min3(deletionCost, insertionCost, substitutionCost)
		}

		// Swap the current and previous rows.
		prev, curr = curr, prev

		// If a max_d is specified, we need to check if we've exceeded it.
		if maxDistance >= 0 {
			zp := i - (lenT - lenS)

			// Check if we can slide the window to the left
			for endColumn > 0 {
				ii := endColumn - 1
				netInserts := ii - zp
				oc := -netInserts * delCost
				if netInserts > 0 {
					oc = netInserts * insCost
				}
				if prev[ii]+oc <= maxDistance {
					break
				}
				endColumn--
			}

			// Check if we can slide the window to the right
			for startColumn < endColumn {
				netInserts := startColumn - zp
				oc := -netInserts * delCost
				if netInserts > 0 {
					oc = netInserts * insCost
				}
				if prev[startColumn]+oc <= maxDistance {
					break
				}
				prev[startColumn] = maxDistance + 1
				curr[startColumn] = maxDistance + 1
				if startColumn != 0 {
					source += string(s[startColumn-1])
				}
				startColumn++
			}

			// If the two windows cross, we are done.
			if startColumn >= endColumn {
				return maxDistance + 1
			}
		}
	}
	return prev[lenS-1]
}

// LevenshteinDistanceWithCost calculates the Levenshtein distance between
// source and target. The distance is calculated using the given cost metrics
// for each of the three possible edit operations.
// Adapted from the 'Iterative with two matrix rows' approach within
// https://en.wikipedia.org/wiki/Levenshtein_distance. This approach provides us
// with O(n^2) time complexity and O(n) space complexity.
func LevenshteinDistanceWithCost(source, target string, insCost, delCost, subCost int) int {
	return levenshtein(source, target, insCost, delCost, subCost, -1)

}

// LevenshteinDistance is the standard Levenshtein distance where the cost of
// insertion, deletion and substitution are all one.
func LevenshteinDistance(source, target string) int {
	return LevenshteinDistanceWithCost(source, target, 1, 1, 1)
}

// LevenshteinLessEqualWithCost is an accelerated version of levenshtein function for low values
// of distances. If the actual distance is less or equal than the maximum distance allowed (maxDistance),
// the function returns the actual distance. Otherwise, it returns the maximum distance allowed plus 1.
func LevenshteinLessEqualWithCost(source, target string, insCost, delCost, subCost, maxDistance int) int {
	return levenshtein(source, target, insCost, delCost, subCost, maxDistance)
}

// LevenshteinLessEqual is the less equal distance where the cost of
// insertion, deletion and substitution are all one.
func LevenshteinLessEqual(source, target string, maxDistance int) int {
	return levenshtein(source, target, 1, 1, 1, maxDistance)
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

func min2(a, b int) int {
	if a < b {
		return a
	}
	return b
}
