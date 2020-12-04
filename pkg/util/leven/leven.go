package leven

// Distance calculates the Levenshtein distance between source and target
// Adapted from the 'Iterative with two matrix rows' approach within
// https://en.wikipedia.org/wiki/Levenshtein_distance. This approach provides us
// with O(n^2) time complexity and O(n) space complexity.
func Distance(source, target string) int {
	if len(source) == 0 {
		return len(target)
	}
	if len(target) == 0 {
		return len(source)
	}
	if source == target {
		return 0
	}
	// Converting to a slice of runes allows us to count multi-byte characters
	// properly.
	s, t := []rune(source), []rune(target)
	lenS, lenT := len(s), len(t)
	// The algorithm is equivalent to building up an NxM matrix of the Levenshtein
	// distances between prefixes of source and target. However, instead of
	// maintaining the entire matrix in memory, we will only keep two rows of the
	// matrix at any given time.
	rowA, rowB := make([]int, lenT+1), make([]int, lenT+1)
	for i := 0; i <= lenT; i++ {
		rowA[i] = i
	}
	for i := 0; i < lenS; i++ {
		rowB[0] = i + 1
		for j := 0; j < lenT; j++ {
			deletionCost := rowA[j+1] + 1
			insertionCost := rowB[j] + 1
			var substitutionCost int
			if s[i] == t[j] {
				substitutionCost = rowA[j]
			} else {
				substitutionCost = rowA[j] + 1
			}
			rowB[j+1] = min3(deletionCost, insertionCost, substitutionCost)
		}
		rowA, rowB = rowB, rowA
	}
	return rowA[lenT]
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
