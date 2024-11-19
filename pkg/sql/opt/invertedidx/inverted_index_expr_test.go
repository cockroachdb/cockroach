// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package invertedidx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimilarityTrigramsToScan(t *testing.T) {
	s := func(s ...string) []string { return s }

	testCases := []struct {
		s         string
		threshold float64
		expected  []string
	}{
		// Empty string.
		{"", 0.3, nil},
		{"", 1, nil},

		// Threshold of 0.
		{"", 0, nil},
		{"foo", 0, nil},
		{"foobar", 0, nil},

		// Remove 0 trigrams.
		{"a", 0.3, s("  a", " a ")},
		{"ab", 0.3, s("  a", " ab", "ab ")},

		// Remove 1 trigram.
		{"abc", 0.3, s(" ab", "abc", "bc ")},
		{"áǎǝ", 0.3, s(" áǎ", "áǎǝ", "ǎǝ ")},
		{"abcd", 0.3, s(" ab", "abc", "bcd", "cd ")},
		{"abcde", 0.3, s(" ab", "abc", "bcd", "cde", "de ")},

		// Remove 2 trigrams.
		{"abcdef", 0.3, s("abc", "bcd", "cde", "def", "ef ")},
		{"abcdefg", 0.3, s("abc", "bcd", "cde", "def", "efg", "fg ")},
		{"abcdefgh", 0.3, s("abc", "bcd", "cde", "def", "efg", "fgh", "gh ")},
		{"abcdefghi", 0.3, s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hi ")},

		// Remove 3 trigrams.
		{"abcdefghij", 0.3, s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij")},
		{"ábcdǝfghij", 0.3, s("bcd", "cdǝ", "dǝf", "fgh", "ghi", "hij", "ábc", "ǝfg")},
		{"abcdefghij", 0.3, s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij")},
		{"abcdefghijk", 0.3, s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij", "ijk")},
		{
			"abcdefghijkl",
			0.3,
			s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij", "ijk", "jkl"),
		},
		{
			// This test case proves that the trigram with the trailing space is
			// removed when it is not the last trigram in the original slice.
			"abcdefghijal",
			0.3,
			s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij", "ija", "jal"),
		},

		// Remove 4+ trigrams.
		{
			"abcdefghijklm",
			0.3,
			s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij", "ijk", "jkl"),
		},
		{
			"abcdefghijkam",
			0.3,
			s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij", "ijk", "kam"),
		},
		{
			"abcdefghijklmnop",
			0.3,
			s("abc", "bcd", "cde", "def", "efg", "fgh", "ghi", "hij", "ijk", "jkl", "klm", "lmn"),
		},

		// Handle strings with spaces.
		{
			"hello world",
			0.3,
			s(" wo", "ell", "hel", "ld ", "llo", "lo ", "orl", "rld", "wor"),
		},

		// A threshold of 1 requires scanning only one trigram.
		{"foobar", 1, s("oob")},

		// A lower threshold requires scanning more trigrams.
		{"foo", 0.2, s("  f", " fo", "foo", "oo ")},
		{"foobar", 0.2, s(" fo", "ar ", "bar", "foo", "oba", "oob")},

		// A higher threshold requires scanning fewer trigrams.
		{"foo", 0.8, s("foo")},
		{"foobar", 0.8, s("bar", "oob")},
	}
	for _, tc := range testCases {
		res := similarityTrigramsToScan(tc.s, tc.threshold)
		require.Equal(t, tc.expected, res)
	}
}
