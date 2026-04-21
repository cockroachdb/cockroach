// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tokenizer

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// bertPreTokenize splits text into words using the BERT BasicTokenizer
// algorithm. If doLower is true, the text is lowercased and accents are
// stripped before splitting. The result is a slice of words ready for
// WordPiece subword tokenization.
func bertPreTokenize(text string, doLower bool) []string {
	if doLower {
		text = toLowerAndStripAccents(text)
	}
	text = tokenizeCJKChars(text)
	words := splitOnWhitespace(text)
	words = splitOnPunctuation(words)
	return words
}

// toLowerAndStripAccents lowercases the input and removes combining
// accent marks. It applies NFD decomposition to separate base characters
// from their combining marks, then removes all characters in the Mn
// (Mark, Nonspacing) Unicode category.
func toLowerAndStripAccents(text string) string {
	text = strings.ToLower(text)
	// NFD decomposition splits accented characters into base + combining mark.
	text = norm.NFD.String(text)
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		if !unicode.Is(unicode.Mn, r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// isCJKChar returns true if r is a CJK Unified Ideograph or in one of
// the extended CJK ranges where each character should be treated as its
// own word.
func isCJKChar(r rune) bool {
	return (r >= 0x4E00 && r <= 0x9FFF) ||
		(r >= 0x3400 && r <= 0x4DBF) ||
		(r >= 0x20000 && r <= 0x2A6DF) ||
		(r >= 0x2A700 && r <= 0x2B73F) ||
		(r >= 0x2B740 && r <= 0x2B81F) ||
		(r >= 0x2B820 && r <= 0x2CEAF) ||
		(r >= 0xF900 && r <= 0xFAFF) ||
		(r >= 0x2F800 && r <= 0x2FA1F)
}

// tokenizeCJKChars inserts spaces around CJK characters so that each
// character is treated as a separate token.
func tokenizeCJKChars(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		if isCJKChar(r) {
			b.WriteRune(' ')
			b.WriteRune(r)
			b.WriteRune(' ')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// splitOnWhitespace splits text on any Unicode whitespace character,
// discarding empty strings.
func splitOnWhitespace(text string) []string {
	return strings.Fields(text)
}

// isBertPunctuation returns true if r is considered punctuation by the
// BERT tokenizer. This matches HuggingFace's _is_punctuation: ASCII
// ranges 33-47, 58-64, 91-96, 123-126, plus unicode.IsPunct for
// non-ASCII characters.
func isBertPunctuation(r rune) bool {
	if (r >= 33 && r <= 47) || (r >= 58 && r <= 64) ||
		(r >= 91 && r <= 96) || (r >= 123 && r <= 126) {
		return true
	}
	return unicode.IsPunct(r)
}

// splitOnPunctuation splits each word in words so that each punctuation
// character becomes its own token. Non-punctuation runs are kept as a
// single token.
func splitOnPunctuation(words []string) []string {
	var result []string
	for _, word := range words {
		var current strings.Builder
		for _, r := range word {
			if isBertPunctuation(r) {
				if current.Len() > 0 {
					result = append(result, current.String())
					current.Reset()
				}
				result = append(result, string(r))
			} else {
				current.WriteRune(r)
			}
		}
		if current.Len() > 0 {
			result = append(result, current.String())
		}
	}
	return result
}
