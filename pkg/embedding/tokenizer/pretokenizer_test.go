// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tokenizer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToLowerAndStripAccents(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"lowercase", "Hello World", "hello world"},
		{"accent_e", "caf\u00e9", "cafe"},
		{"accent_u", "na\u00efve", "naive"},
		{"umlaut", "\u00fcber", "uber"},
		{"no_change", "hello", "hello"},
		{"empty", "", ""},
		{"mixed", "R\u00e9sum\u00e9 Caf\u00e9", "resume cafe"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toLowerAndStripAccents(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsCJKChar(t *testing.T) {
	require.True(t, isCJKChar('\u4e2d')) // 中
	require.True(t, isCJKChar('\u6587')) // 文
	require.False(t, isCJKChar('A'))
	require.False(t, isCJKChar('1'))
}

func TestTokenizeCJKChars(t *testing.T) {
	// Each CJK character should be surrounded by spaces.
	result := tokenizeCJKChars("hello\u4e2d\u6587world")
	require.Equal(t, "hello \u4e2d  \u6587 world", result)
}

func TestIsBertPunctuation(t *testing.T) {
	// ASCII punctuation ranges.
	require.True(t, isBertPunctuation('!'))
	require.True(t, isBertPunctuation(','))
	require.True(t, isBertPunctuation('.'))
	require.True(t, isBertPunctuation('?'))
	require.True(t, isBertPunctuation(':'))
	require.True(t, isBertPunctuation(';'))
	require.True(t, isBertPunctuation('['))
	require.True(t, isBertPunctuation('{'))
	require.True(t, isBertPunctuation('~'))

	// Non-punctuation.
	require.False(t, isBertPunctuation('a'))
	require.False(t, isBertPunctuation('0'))
	require.False(t, isBertPunctuation(' '))
}

func TestSplitOnPunctuation(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{"no_punct", []string{"hello"}, []string{"hello"}},
		{"trailing", []string{"hello,"}, []string{"hello", ","}},
		{"multiple", []string{"hello,world!"}, []string{"hello", ",", "world", "!"}},
		{"only_punct", []string{"..."}, []string{".", ".", "."}},
		{"mixed", []string{"a.b", "c"}, []string{"a", ".", "b", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitOnPunctuation(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBertPreTokenize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		doLower  bool
		expected []string
	}{
		{
			"basic_lowercase",
			"Hello World",
			true,
			[]string{"hello", "world"},
		},
		{
			"with_punctuation",
			"Hello, world!",
			true,
			[]string{"hello", ",", "world", "!"},
		},
		{
			"accents",
			"Caf\u00e9 na\u00efve",
			true,
			[]string{"cafe", "naive"},
		},
		{
			"no_lowercase",
			"Hello World",
			false,
			[]string{"Hello", "World"},
		},
		{
			"cjk_mixed",
			"hello\u4e2d\u6587",
			true,
			[]string{"hello", "\u4e2d", "\u6587"},
		},
		{
			"empty",
			"",
			true,
			nil,
		},
		{
			"whitespace_only",
			"   \t\n  ",
			true,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bertPreTokenize(tt.input, tt.doLower)
			require.Equal(t, tt.expected, result)
		})
	}
}
