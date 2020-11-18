package leven

// Distance calculates the Levenshtein distance between s and t
// Adapted from https://en.wikipedia.org/wiki/Levenshtein_distance#Computing_Levenshtein_distance
func Distance(s, t string) int {
	lenS, lenT := len(s), len(t)
	a, b := make([]int, lenT+1), make([]int, lenT+1)
	for i := 0; i <= lenT; i++ {
		a[i] = i
	}
	for i := 0; i < lenS; i++ {
		b[0] = i + 1
		for j := 0; j < lenT; j++ {
			deletionCost := a[j+1] + 1
			insertionCost := b[j] + 1
			var substitutionCost int
			if s[i] == t[j] {
				substitutionCost = a[j]
			} else {
				substitutionCost = a[j] + 1
			}
			b[j+1] = min3(deletionCost, insertionCost, substitutionCost)
		}
		a, b = b, a
	}
	return a[lenT]
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
