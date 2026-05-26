// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package trigram

import (
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MakeTrigrams returns the downcased, sorted and de-duplicated trigrams for an
// input string. Non-alphanumeric characters (calculated via unicode's Letter
// and Number designations) are treated as word boundaries.
// Words are separately trigrammed. If pad is true, the string will be padded
// with 2 spaces at the front and 1 at the back, producing 3 extra trigrams.
func MakeTrigrams(s string, pad bool) []string {
	if len(s) == 0 {
		return nil
	}

	// Downcase the initial string.
	s = strings.ToLower(s)

	// Approximately pre-size as if the string is all 1 big word.
	output := make([]string, 0, len(s)+2)

	start := -1
	oneByteCharsOnly := true
	// Loop through the input string searching for word boundaries. For each found
	// word, generate trigrams and add to the output list. The start and end
	// variables are used to track the beginning and end of the current word
	// throughout the loop.
	// This loop would be more ergonomic with strings.FieldsFunc, but doing so
	// costs twice the allocations.
	for end, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
			// Non-word char.
			if start < 0 {
				// Keep going until we find a word char to start the run.
				continue
			}
			// We found a word span. Reset the span and handle it below.
		} else {
			// Word char.
			if start < 0 {
				start = end
			}
			if oneByteCharsOnly && r >= utf8.RuneSelf {
				oneByteCharsOnly = false
			}
			continue
		}

		output = generateTrigrams(output, s[start:end], pad, oneByteCharsOnly)
		oneByteCharsOnly = true
		start = -1
	}
	if start >= 0 {
		// Collect final word.
		output = generateTrigrams(output, s[start:], pad, oneByteCharsOnly)
	}

	if len(output) == 0 {
		return nil
	}

	// Sort the array and deduplicate.
	sort.Strings(output)

	// Then distinct: (wouldn't it be nice if Go had generics?)
	lastUniqueIdx := 0
	for i := 1; i < len(output); i++ {
		if output[i] != output[lastUniqueIdx] {
			// We found a unique entry, at index i. The last unique entry in the array
			// was at lastUniqueIdx, so set the entry after that one to our new unique
			// entry, and bump lastUniqueIdx for the next loop iteration.
			lastUniqueIdx++
			output[lastUniqueIdx] = output[i]
		}
	}
	output = output[:lastUniqueIdx+1]

	return output
}

func generateTrigrams(appendTo []string, word string, pad bool, onlyOneByteChars bool) []string {
	if pad {
		var sb strings.Builder
		sb.Grow(len(word) + 3)
		sb.WriteString("  ")
		sb.WriteString(word)
		sb.WriteByte(' ')
		word = sb.String()
	}
	if onlyOneByteChars {
		// Fast path for words that have no wide characters.
		// If not padding, n will be less than 0, so we'll leave the loop as
		// desired, since words less than length 3 have no trigrams.
		n := len(word) - 2
		for i := 0; i < n; i++ {
			appendTo = append(appendTo, word[i:i+3])
		}
	} else {
		// There are some wide characters, so we need to assemble trigrams
		// in a more careful way than just taking 3-byte windows: we have to
		// decode each code point to find its width so we can make
		// windows of 3 codepoints.
		//
		// Note that this behavior differs from Postgres: Postgres computes
		// a hash of the 3 codepoint windows and takes the first 3 bytes of
		// the hash as the trigram. This is due to limitations in Postgres
		// and is a dubious way of computing a trigram.
		// Our method should provide fewer false positives, but note that
		// users shouldn't see any differences due to this change.
		nFound := 0
		charWidths := []int{0, 0}
		for i, w := 0, 0; i < len(word); i += w {
			_, w = utf8.DecodeRuneInString(word[i:])
			if nFound < 2 {
				charWidths[nFound] = w
				nFound += 1
				continue
			}
			// Now that we've found our first 2 widths, we can begin assembling the
			// trigrams.
			appendTo = append(appendTo, word[i-charWidths[0]-charWidths[1]:i+w])
			charWidths[0], charWidths[1] = charWidths[1], w
		}
	}
	return appendTo
}

// Similarity returns a trigram similarity measure between two strings. 1.0
// means the trigrams are identical, 0.0 means no trigrams were shared.
func Similarity(l string, r string) float64 {
	lTrigrams, rTrigrams := MakeTrigrams(l, true /* pad */), MakeTrigrams(r, true /* pad */)

	if len(lTrigrams) == 0 || len(rTrigrams) == 0 {
		return 0
	}

	// To calculate the similarity, we count the number of shared trigrams
	// in the strings, and divide by the number of non-shared trigrams.
	// See the CALCSML macro in Postgres contrib/pg_trgm/trgm.h.

	i, j := 0, 0
	nShared := 0
	for i < len(lTrigrams) && j < len(rTrigrams) {
		lTrigram, rTrigram := lTrigrams[i], rTrigrams[j]
		if lTrigram < rTrigram {
			i++
		} else if lTrigram > rTrigram {
			j++
		} else {
			nShared++
			i++
			j++
		}
	}
	shared := float64(nShared)
	return shared / (float64(len(lTrigrams)+len(rTrigrams)) - shared)
}
