// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package trigram

import (
	"slices"
	"sort"
	"strings"
	"sync"
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

// Trigram index keys are built from strings.ToLower of the indexed text (see
// MakeTrigrams). Case-insensitive matching (ILIKE) does not compare lower-cased
// text, though: it compares unicode.ToUpper of both sides, or uses simple case
// folding (unicode.SimpleFold) in its regular-expression path. Some runes that
// ILIKE treats as equal have different lower-case forms. For example, 'ς' and
// 'σ' both upper-case to 'Σ', but ToLower('Σ') is 'σ', so the row "ΛΟΓΟΣ" is
// indexed under the trigram "γοσ" while the pattern '%λογος%' produces the
// trigram "γος". Other examples are 'ı'/'i' (both upper-case to 'I'), 'ſ'/'s',
// and 'µ' (micro sign)/'μ'.
//
// To find every row that may match a case-insensitive pattern, a probe must
// therefore look up each spelling of a pattern trigram that an ILIKE-equal row
// could have been indexed under. CaseInsensitiveVariants computes those
// spellings.

// caseClass describes the set of runes that case-insensitive matching may
// treat as equal to each other.
type caseClass struct {
	// lowers holds the distinct strings.ToLower forms of the runes in the
	// class, in ascending order. It always has at least two elements.
	lowers []rune
	// hasNonWordLower is true if the lower-case form of some rune in the class
	// is neither a letter nor a number. MakeTrigrams treats such a rune as a
	// word boundary, so a row containing it has no trigram covering that
	// position at all.
	hasNonWordLower bool
}

var caseClasses struct {
	once sync.Once
	// byLower maps each lower-case rune to its class. Only runes whose class
	// has more than one lower-case form are present; every other rune can
	// only be indexed under its own lower-case form.
	byLower map[rune]*caseClass
}

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

// buildCaseClasses computes the equivalence classes of runes that are
// connected by unicode.ToUpper, unicode.ToLower, or unicode.SimpleFold, and
// records the classes that have more than one lower-case form. Using the
// union of all three relations over-approximates both the ToUpper comparison
// and the (?i) regular expressions used to evaluate ILIKE, which is what the
// index probe needs: an over-approximation only costs extra lookups, while an
// under-approximation loses rows.
func buildCaseClasses() map[rune]*caseClass {
	parent := make(map[rune]rune)
	var find func(r rune) rune
	find = func(r rune) rune {
		p, ok := parent[r]
		if !ok {
			parent[r] = r
			return r
		}
		if p == r {
			return r
		}
		root := find(p)
		parent[r] = root
		return root
	}
	union := func(a, b rune) {
		if a == b {
			return
		}
		if ra, rb := find(a), find(b); ra != rb {
			parent[ra] = rb
		}
	}
	// Every rune with a non-trivial case mapping is in unicode.CaseRanges. A
	// few runes outside of it (e.g. 'ß') only fold to other runes, and they are
	// reached by walking the SimpleFold orbits of runes in unicode.CaseRanges
	// (here, 'ẞ'). TestCaseClassesCoverAllRunes checks this against every rune.
	for _, cr := range unicode.CaseRanges {
		for r := rune(cr.Lo); r <= rune(cr.Hi); r++ {
			union(r, unicode.ToUpper(r))
			union(r, unicode.ToLower(r))
			for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
				union(r, f)
			}
		}
	}

	members := make(map[rune][]rune)
	for r := range parent {
		root := find(r)
		members[root] = append(members[root], r)
	}
	byLower := make(map[rune]*caseClass)
	for _, rs := range members {
		var c caseClass
		for _, r := range rs {
			l := unicode.ToLower(r)
			if !slices.Contains(c.lowers, l) {
				c.lowers = append(c.lowers, l)
			}
			if !isWordRune(l) {
				c.hasNonWordLower = true
			}
		}
		if len(c.lowers) < 2 {
			continue
		}
		slices.Sort(c.lowers)
		for _, l := range c.lowers {
			byLower[l] = &c
		}
	}
	return byLower
}

func lookupCaseClass(r rune) *caseClass {
	caseClasses.once.Do(func() {
		caseClasses.byLower = buildCaseClasses()
	})
	return caseClasses.byLower[r]
}

// CaseInsensitiveVariants returns every trigram under which a row may be
// indexed if the row's text matches the trigram t case-insensitively. t must be
// a trigram produced by MakeTrigrams, i.e. already lower-cased. The result
// always contains t itself and is sorted. For example:
//
//	"abc" -> ["abc"]
//	"γος" -> ["γος", "γοσ"]
//	"sis" -> ["sis", "siſ", "sıs", "sıſ", "ſis", "ſiſ", "ſıs", "ſıſ"]
//
// ok is false if some rune in t is case-insensitively equal to a rune that
// MakeTrigrams treats as a word boundary (e.g. 'ι' and U+0345 COMBINING GREEK
// YPOGEGRAMMENI). A matching row then need not contain any trigram at that
// position, so t must not be required in an index probe.
func CaseInsensitiveVariants(t string) (variants []string, ok bool) {
	variants = []string{""}
	for _, r := range t {
		c := lookupCaseClass(r)
		if c == nil {
			for i := range variants {
				variants[i] += string(r)
			}
			continue
		}
		if c.hasNonWordLower {
			return nil, false
		}
		next := make([]string, 0, len(variants)*len(c.lowers))
		for _, v := range variants {
			for _, l := range c.lowers {
				var sb strings.Builder
				sb.Grow(len(v) + utf8.RuneLen(l))
				sb.WriteString(v)
				sb.WriteRune(l)
				next = append(next, sb.String())
			}
		}
		variants = next
	}
	slices.Sort(variants)
	return variants, true
}
