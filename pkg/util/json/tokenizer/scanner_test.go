// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This is a fork of pkg/json package.

// Copyright (c) 2020, Dave Cheney <dave@cheney.net>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//   - Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//   - Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package tokenizer

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScannerNext(t *testing.T) {
	tests := []struct {
		in     string
		tokens []string
	}{
		{in: `""`, tokens: []string{`""`}},
		{in: `"a"`, tokens: []string{`"a"`}},
		{in: ` "a" `, tokens: []string{`"a"`}},
		{in: `"\""`, tokens: []string{`"""`}},
		{in: `1`, tokens: []string{`1`}},
		{in: `-1234567.8e+90`, tokens: []string{`-1234567.8e+90`}},
		{in: `{}`, tokens: []string{`{`, `}`}},
		{in: `[]`, tokens: []string{`[`, `]`}},
		{in: `[{}, {}]`, tokens: []string{`[`, `{`, `}`, `,`, `{`, `}`, `]`}},
		{in: `{"a": 0}`, tokens: []string{`{`, `"a"`, `:`, `0`, `}`}},
		{in: `{"a": []}`, tokens: []string{`{`, `"a"`, `:`, `[`, `]`, `}`}},
		{in: `[10]`, tokens: []string{`[`, `10`, `]`}},
		{in: `[{"a": 1,"b": 123.456, "c": null, "d": [1, -2, "three", true, false, ""]}]`,
			tokens: []string{`[`,
				`{`,
				`"a"`, `:`, `1`, `,`,
				`"b"`, `:`, `123.456`, `,`,
				`"c"`, `:`, `null`, `,`,
				`"d"`, `:`, `[`,
				`1`, `,`, `-2`, `,`, `"three"`, `,`, `true`, `,`, `false`, `,`, `""`,
				`]`,
				`}`,
				`]`,
			},
		},
		{in: `{"x": "va\\\\ue", "y": "value y"}`, tokens: []string{
			`{`, `"x"`, `:`, `"va\\ue"`, `,`, `"y"`, `:`, `"value y"`, `}`,
		}},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			scanner := Scanner{data: []byte(tc.in)}
			for n, want := range tc.tokens {
				got := scanner.Next()
				if string(got) != want {
					t.Fatalf("%v: expected: %v, got: %v", n+1, want, string(got))
				}
			}
			last := scanner.Next()
			if len(last) > 0 {
				t.Fatalf("expected: %q, got: %q", "", string(last))
			}
			require.False(t, scanner.More())
		})
	}
}

func TestParseString(t *testing.T) {
	testParseString(t, `""`, `""`)
	testParseString(t, `"" `, `""`)
	testParseString(t, `"\""`, `"""`)
	testParseString(t, `"\\\\\\\\\t"`, `"\\\\	"`)
	testParseString(t, `"\\b"`, `"\b"`)
	// Regression test: unterminated escape code.
	testParseString(t, `"\u"`, ``)
	testParseString(t, `"\u2"`, ``)
	testParseString(t, `"\u23"`, ``)
	testParseString(t, `"\u234"`, ``)
	testParseString(t, `"\u2345"`, `"⍅"`)
	testParseString(t, `"\u2z45"`, ``)
}

func testParseString(t *testing.T, json, want string) {
	t.Helper()
	scanner := Scanner{data: []byte(json)}
	got := scanner.Next()
	if string(got) != want {
		t.Fatalf("expected: %q, got: %q", want, got)
	}
}

func TestParseNumber(t *testing.T) {
	testParseNumber(t, `1`)
	testParseNumber(t, `12.0004`)
	testParseNumber(t, `1.7734`)
	testParseNumber(t, `15`)
	testParseNumber(t, `-42`)
	testParseNumber(t, `-1.7734`)
	testParseNumber(t, `1.0e+28`)
	testParseNumber(t, `-1.0e+28`)
	testParseNumber(t, `1.0e-28`)
	testParseNumber(t, `-1.0e-28`)
	testParseNumber(t, `-18.3872`)
	testParseNumber(t, `-2.1`)
	testParseNumber(t, `-1234567.891011121314`)
}

func testParseNumber(t *testing.T, tc string) {
	t.Helper()
	scanner := Scanner{data: []byte(tc)}
	got := scanner.Next()
	if string(got) != tc {
		t.Fatalf("expected: %q, got: %q", tc, got)
	}
}

func TestScanner(t *testing.T) {
	for _, tc := range inputs {
		r := fixture(t, tc.path)
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(tc.path, func(t *testing.T) {
			sc := Scanner{data: data}
			n := 0
			for len(sc.Next()) > 0 {
				n++
			}
			if n != tc.alltokens {
				t.Fatalf("expected %v tokens, got %v", tc.alltokens, n)
			}
		})
	}
}
