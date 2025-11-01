// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSingleRune(t *testing.T) {
	tests := []struct {
		s        string
		expected rune
		err      bool
	}{
		{"a", 'a', false},
		{"", 0, false},
		{"🐛"[:1], 0, true},
		{"aa", 'a', true},
	}
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			got, err := GetSingleRune(tc.s)
			if (err != nil) != tc.err {
				t.Fatalf("got unexpected err: %v", err)
			}
			if tc.expected != got {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestToLowerSingleByte(t *testing.T) {
	testCases := []struct {
		from     byte
		expected byte
	}{
		{'a', 'a'},
		{'A', 'a'},
		{'c', 'c'},
		{'C', 'c'},
		{'Z', 'z'},
		{'1', '1'},
		{'\n', '\n'},
	}

	for _, tc := range testCases {
		t.Run(string(tc.from), func(t *testing.T) {
			ret := ToLowerSingleByte(tc.from)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestTruncateString(t *testing.T) {
	testCases := []struct {
		s string
		// res stores the expected results for maxRunes=0,1,2,3,etc.
		res []string
	}{
		{"", []string{"", ""}},
		{"abcd", []string{"", "a", "ab", "abc", "abcd", "abcd", "abcd"}},
		{"🐛🏠", []string{"", "🐛", "🐛🏠", "🐛🏠", "🐛🏠"}},
		{"a🐛b🏠c", []string{"", "a", "a🐛", "a🐛b", "a🐛b🏠", "a🐛b🏠c", "a🐛b🏠c"}},
		{
			// Test with an invalid UTF-8 sequence.
			"\xf0\x90\x28\xbc",
			[]string{"", "\xf0", "\xf0\x90", "\xf0\x90\x28", "\xf0\x90\x28\xbc", "\xf0\x90\x28\xbc"},
		},
	}

	for _, tc := range testCases {
		for i := range tc.res {
			if r := TruncateString(tc.s, i); r != tc.res[i] {
				t.Errorf("TruncateString(\"%q\", %d) = \"%q\"; expected \"%q\"", tc.s, i, r, tc.res[i])
			}
		}
	}
}

func TestRemoveTrailingSpaces(t *testing.T) {
	for _, tc := range []struct{ input, expected string }{
		{
			input:    "",
			expected: "\n",
		},
		{
			input:    "line 1  \nline 2   \nline 3 \n\n",
			expected: "line 1\nline 2\nline 3\n",
		},
		{
			input:    " line 1  \nline 2   \nline 3  ",
			expected: " line 1\nline 2\nline 3\n",
		},
		{
			input:    "line 1\n\n  \nline 2   \nline 3",
			expected: "line 1\n\n\nline 2\nline 3\n",
		},
	} {
		output := RemoveTrailingSpaces(tc.input)
		if output != tc.expected {
			t.Errorf("expected:\n%s\ngot:\n%s", tc.expected, output)
		}
	}
}

func TestStringListBuilder(t *testing.T) {
	var buf bytes.Buffer
	var b StringListBuilder
	expect := func(exp string) {
		t.Helper()
		if buf.String() != exp {
			t.Errorf("expected `%s`, got `%s`", exp, buf.String())
		}
		buf.Reset()
	}

	b = MakeStringListBuilder("(", ",", ")")
	b.Finish(&buf)
	expect("")

	b = MakeStringListBuilder("(", ",", ")")
	b.Add(&buf, "one")
	b.Finish(&buf)
	expect("(one)")

	b = MakeStringListBuilder("[", ", ", "]")
	b.Add(&buf, "one")
	b.Addf(&buf, "%s", "two")
	b.Finish(&buf)
	expect("[one, two]")
}

func TestCollapseRepeatedRune(t *testing.T) {
	type testCase struct {
		input    string
		target   rune
		expected string
	}

	tests := []testCase{
		{"", 'a', ""},
		{"", '∀', ""},
		{"xyz", 'a', "xyz"},
		{"aaa", 'a', "a"},
		{"%abc", '%', "%abc"},
		{"%%abc", '%', "%abc"},
		{"abc%", '%', "abc%"},
		{"abc%%", '%', "abc%"},
		{"%%abc%%", '%', "%abc%"},
		{"%%%a%%%b%%%c%%%", '%', "%a%b%c%"},
		{"∀∀∀∀", '∀', "∀"},
		{"∀∀∀∀a∀b∀∀∀c∀∀∀d∀∀∀∀∀", '∀', "∀a∀b∀c∀d∀"},
		{"%test%%%%", '%', "%test%"},
		{"%%%test%%%%", '%', "%test%"},
		{"%%%test1%%%test2%%%test3%%%", '%', "%test1%test2%test3%"},
		{"I work on ddddddifferent characters", 'd', "I work on different characters"},
		{"%%%%%%%%tèʂt%", '%', "%tèʂt%"},
		{"%a%b%%c%%d", '%', "%a%b%c%d"},
		{"🐛🐛", '🐛', "🐛"},
	}

	for _, test := range tests {
		if result := CollapseRepeatedRune(test.input, test.target); result != test.expected {
			t.Errorf("%q: expected %q but got %q", test.input, test.expected, result)
		}
	}
}

func BenchmarkCollapseRepeatedRune(b *testing.B) {
	type testCase struct {
		name  string
		input string
	}
	testCases := []testCase{
		{"no-target", "abcedfghijklmnopqrstuvwxyz"},
		{"no-dupe", "abce%dfghijklmno%pqrstuvwx%yz"},
		{"start-dupe", "%%%abce%dfghijklmno%pqrstuvwx%yz%"},
		{"end-dupe", "abce%dfghijklmno%pqrstuvwx%yz%%%"},
		{"multiple-dupe", "%%%abce%%%%dfghijklmno%pqrstuvwx%%%%yz%%%"},
	}
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_ = CollapseRepeatedRune(tc.input, '%')
			}
		})
	}
}
