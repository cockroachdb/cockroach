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
)

func TestDecoderNextToken(t *testing.T) {
	tests := []struct {
		json   string
		tokens []string
	}{
		{json: `"a"`, tokens: []string{`"a"`}},
		{json: `1`, tokens: []string{`1`}},
		{json: `{}`, tokens: []string{`{`, `}`}},
		{json: `[]`, tokens: []string{`[`, `]`}},
		{json: `[[[[[[{"true":true}]]]]]]`, tokens: []string{`[`, `[`, `[`, `[`, `[`, `[`, `{`, `"true"`, `true`, `}`, `]`, `]`, `]`, `]`, `]`, `]`}},
		{json: `[{}, {}]`, tokens: []string{`[`, `{`, `}`, `{`, `}`, `]`}},
		{json: `{"a": 0}`, tokens: []string{`{`, `"a"`, `0`, `}`}},
		{json: `{"a": []}`, tokens: []string{`{`, `"a"`, `[`, `]`, `}`}},
		{json: `{"a":{}, "b":{}}`, tokens: []string{`{`, `"a"`, `{`, `}`, `"b"`, `{`, `}`, `}`}},
		{json: `[10]`, tokens: []string{`[`, `10`, `]`}},
		{json: `""`, tokens: []string{`""`}},
		{json: `[{}]`, tokens: []string{`[`, `{`, `}`, `]`}},
		{json: `[{"a": [{}]}]`, tokens: []string{`[`, `{`, `"a"`, `[`, `{`, `}`, `]`, `}`, `]`}},
		{json: `[{"a": 1,"b": 123.456, "c": null, "d": [1, -2, "three", true, false, ""]}]`,
			tokens: []string{`[`,
				`{`,
				`"a"`, `1`,
				`"b"`, `123.456`,
				`"c"`, `null`,
				`"d"`, `[`,
				`1`, `-2`, `"three"`, `true`, `false`, `""`,
				`]`,
				`}`,
				`]`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.json, func(t *testing.T) {
			dec := MakeDecoder([]byte(tc.json))
			for n, want := range tc.tokens {
				got, err := dec.NextToken()
				if string(got) != want {
					t.Fatalf("%v: expected: %q, got: %q, %v", n+1, want, string(got), err)
				}
				t.Logf("token: %q, stack: %v", got, dec.stack)
			}
			last, err := dec.NextToken()
			if len(last) > 0 {
				t.Fatalf("expected: %q, got: %q, %v", "", string(last), err)
			}
			if err != io.EOF {
				t.Fatalf("expected: %q, got: %q, %v", "", string(last), err)
			}
		})
	}
}

func TestDecoderInvalidJSON(t *testing.T) {
	tests := []struct {
		json string
	}{
		{json: `[`},
		{json: `{"":2`},
		{json: `[[[[]]]`},
		{json: `{"`},
		{json: `{"":` + "\n" + `}`},
		{json: `{{"key": 1}: 2}}`},
		{json: `{1: 1}`},
		// {json: `"\6"`},
		{json: `[[],[], [[]],�[[]]]`},
		{json: `+`},
		{json: `,`},
		// {json: `00`},
		// {json: `1a`},
		{json: `1.e1`},
		{json: `{"a":"b":"c"}`},
		{json: `{"test"::"input"}`},
		{json: `e1`},
		{json: `-.1e-1`},
		{json: `123.`},
		{json: `--123`},
		{json: `.1`},
		{json: `0.1e`},
		// fuzz testing
		// {json: "\"\x00outC: .| >\x185\x014\x80\x00\x01n" +
		//	"E4255425067\x014\x80\x00\x01.242" +
		//	"55425.E420679586036\xef" +
		//	"\xbf9586036�\""},
	}

	for _, tc := range tests {
		t.Run(tc.json, func(t *testing.T) {
			dec := MakeDecoder([]byte(tc.json))
			var err error
			for {
				_, err = dec.NextToken()
				if err != nil {
					break
				}
			}
			if err == io.EOF {
				t.Fatalf("expected err, got: %v", err)
			}
		})
	}
}
