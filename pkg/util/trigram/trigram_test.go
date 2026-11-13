// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package trigram

import (
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeTrigrams(t *testing.T) {
	for _, tc := range []struct {
		s            string
		wantPadded   []string
		wantUnpadded []string
	}{
		// Test short strings.
		{"", nil, nil},
		{"a", []string{"  a", " a "}, nil},
		{"1", []string{"  1", " 1 "}, nil},
		{"ab", []string{"  a", " ab", "ab "}, nil},
		{"ab ", []string{"  a", " ab", "ab "}, nil},
		{"%ab", []string{"  a", " ab", "ab "}, nil},
		// Test non-alphanum removal.
		{".,!@#$%^&*()[]{}-_ ", nil, nil},
		// Test de-duping and sorting of trigrams.
		{"abaaba",
			[]string{"  a", " ab", "aab", "aba", "ba ", "baa"},
			[]string{"aab", "aba", "baa"}},
		{"bcaabc",
			[]string{"  b", " bc", "aab", "abc", "bc ", "bca", "caa"},
			[]string{"aab", "abc", "bca", "caa"}},
		{"Приветhi",
			[]string{"  п", " пр", "hi ", "вет", "етh", "иве", "при", "рив", "тhi"},
			[]string{"вет", "етh", "иве", "при", "рив", "тhi"},
		},
	} {
		padded := MakeTrigrams(tc.s, true)
		unpadded := MakeTrigrams(tc.s, false)
		assert.Equal(t, tc.wantPadded, padded)
		assert.Equal(t, tc.wantUnpadded, unpadded)
	}
}

func TestSimilarity(t *testing.T) {
	for _, tc := range []struct {
		l    string
		r    string
		want float64
	}{
		// Empty cases.
		{"", "", 0},
		{"a", "", 0},
		{"blahblah blah", "", 0},
		{"", "a", 0},
		{"", "blahblah blah", 0},

		// Non-alpha.
		{"_-%#@($", "_-%#@($", 0},
		{"_-%#@($a", "a_-%#@($", 1},
		{"_-%#@($a", "a_-%#@($a", 1},

		{"a", "a", 1},
		{"ab", "ab", 1},
		{"ab", "ba", 0},
		{"baba", "abab", 0.25},
		{"trigram", "triglam", 0.4545},
		{"trigram", "trigam", 0.5},
		{"trigram", "trigarm", 0.3333},
		{"tritritritri", "tritritritri", 1},
		{"tritritritri", "tritritritrn", 0.625},
		{"tritritri", "tritritrn", 0.625},
		{"tritritri", "tritrntri", 0.6666},
		{"tritri", "tritrn", 0.625},
		{"tritri", "trntri", 0.4444},
	} {
		assert.InDelta(t, tc.want, Similarity(tc.l, tc.r), 0.0001, "for %s %% %s", tc.l, tc.r)
	}
}

func BenchmarkSimilarity(b *testing.B) {
	for _, t := range []struct {
		x string
		y string
	}{
		{"trigram", "trigarm"},
		{"Приветhi", "Привeтho"},
	} {
		b.Run(t.x+t.y, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = Similarity(t.x, t.y)
			}
		})
	}
}

func TestCaseInsensitiveVariants(t *testing.T) {
	for _, tc := range []struct {
		name             string
		trigram          string
		expectedVariants []string
		expectedOK       bool
	}{
		{name: "no case-ambiguous runes", trigram: "abc",
			expectedVariants: []string{"abc"}, expectedOK: true},
		{name: "kelvin sign lower-cases to k", trigram: "kkk",
			expectedVariants: []string{"kkk"}, expectedOK: true},
		{name: "final sigma", trigram: "γος",
			expectedVariants: []string{"γος", "γοσ"}, expectedOK: true},
		{name: "non-final sigma", trigram: "γοσ",
			expectedVariants: []string{"γος", "γοσ"}, expectedOK: true},
		{name: "dotted and dotless i", trigram: "pıx",
			expectedVariants: []string{"pix", "pıx"}, expectedOK: true},
		{name: "micro sign", trigram: "µgr",
			expectedVariants: []string{"µgr", "μgr"}, expectedOK: true},
		{name: "every rune ambiguous", trigram: "sis",
			expectedVariants: []string{
				"sis", "siſ", "sıs", "sıſ", "ſis", "ſiſ", "ſıs", "ſıſ",
			}, expectedOK: true},
		{name: "iota folds to a combining mark", trigram: "αιβ", expectedOK: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			variants, ok := CaseInsensitiveVariants(tc.trigram)
			require.Equal(t, tc.expectedOK, ok)
			require.Equal(t, tc.expectedVariants, variants)
		})
	}
}

// TestCaseClassesCoverAllRunes verifies that buildCaseClasses, which only
// scans unicode.CaseRanges, accounts for every rune in Unicode: whenever a rune
// is case-insensitively equal to a rune with a different lower-case form, both
// lower-case forms must be in the same class.
func TestCaseClassesCoverAllRunes(t *testing.T) {
	byLower := buildCaseClasses()
	for r := rune(0); r <= unicode.MaxRune; r++ {
		l := unicode.ToLower(r)
		for _, other := range []rune{unicode.ToUpper(r), unicode.SimpleFold(r)} {
			otherLower := unicode.ToLower(other)
			if otherLower == l {
				continue
			}
			c := byLower[l]
			require.NotNil(t, c, "rune %U", r)
			require.Contains(t, c.lowers, otherLower, "rune %U", r)
		}
	}
}
