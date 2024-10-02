// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package trigram

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
