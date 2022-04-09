// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package trigram

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Trigrams are calculated per word. Words are made up of alphanumeric
// characters. Note that this doesn't include _, so we can't just use \w.
var alphaNumRe = regexp.MustCompile("[a-zA-Z0-9]+")

// MakeTrigrams returns the downcased, sorted and de-duplicated trigrams for an
// input string. Non-alphanumeric characters are treated as word boundaries.
// Words are separately trigrammed. If pad is true, the string will be padded
// with 2 spaces at the front and 1 at the back, producing 3 extra trigrams.
func MakeTrigrams(s string, pad bool) []string {
	if len(s) == 0 {
		return nil
	}

	// Downcase the initial string.
	s = strings.ToLower(s)

	// Find words.
	wordSpans := alphaNumRe.FindAllStringIndex(s, -1)

	// Approximately pre-size as if the string is all 1 big word.
	output := make([]string, 0, len(s))

	for _, span := range wordSpans {
		word := s[span[0]:span[1]]
		if pad {
			word = fmt.Sprintf("  %s ", word)
		}
		// If not padding, n will be less than 0, so we'll leave the loop as
		// desired, since words less than length 3 have no trigrams.
		n := len(word) - 2
		for i := 0; i < n; i++ {
			output = append(output, word[i:i+3])
		}
	}

	if len(output) == 0 {
		return output
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

// Similarity returns a trigram similarity measure between two strings. 1.0
// means the trigrams are identical, 0.0 means no trigrams were shared.
func Similarity(l string, r string) float64 {
	lTrigrams, rTrigrams := MakeTrigrams(l, true /* pad */), MakeTrigrams(r, true /* pad */)

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
