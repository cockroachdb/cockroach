// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package parser

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

func TestJSONOrdering(t *testing.T) {
	// We test here that every element in order sorts before every one that comes
	// after it, and is equal to itself.
	sources := []string{
		`null`,
		`"a"`,
		`"aa"`,
		`"b"`,
		`"bb"`,
		`1`,
		`2`,
		`100`,
		`false`,
		`true`,
		// In Postgres's sorting rules, the empty array comes before everything (even null),
		// so this is a departure.
		// Shorter arrays sort before longer arrays (this is the same as in Postgres).
		`[]`,
		`[1]`,
		`[2]`,
		`[1, 2]`,
		`[1, 3]`,
		// Objects with fewer keys come before objects with more keys.
		`{}`,
		`{"a": 1}`,
		`{"a": 2}`,
		// In Postgres, keys which are shorter sort before keys which are longer. This
		// is not true for us (right now). TODO(justin): unclear if it should be.
		`{"aa": 1}`,
		`{"b": 1}`,
		`{"b": 2}`,
		// Objects are compared key-1, value-1, key-2, value-2, ...
		`{"a": 2, "c": 3}`,
		`{"a": 3, "b": 3}`,
	}
	jsons := make([]Datum, len(sources))
	for i := range sources {
		j, err := ParseDJSON(sources[i])
		if err != nil {
			t.Fatal(err)
		}
		jsons[i] = j
	}
	ctx := NewTestingEvalContext()
	for i := range jsons {
		if jsons[i].Compare(ctx, jsons[i]) != 0 {
			t.Errorf("%s not equal to itself", jsons[i])
		}
		for j := i + 1; j < len(jsons); j++ {
			if jsons[i].Compare(ctx, jsons[j]) != -1 {
				t.Errorf("expected %s < %s", jsons[i], jsons[j])
			}
			if jsons[j].Compare(ctx, jsons[i]) != 1 {
				t.Errorf("expected %s > %s", jsons[j], jsons[i])
			}
		}
	}
}

func TestJSONRoundTrip(t *testing.T) {
	testCases := []string{
		`1`,
		`-5`,
		`1.00`,
		`1.00000000000`,
		`100000000000000000000000000000000000000000`,
		`1.3e100`,
		`true`,
		` true `,
		`
        
        true
        `,
		`false`,
		`"hello"`,
		`"hel\"\n\r\tlo"`,
		`[1, true, "three"]`,
		`[1, 1.0, 1.00, 1.000, 1.0000, 1.00000]`,
		`[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]`,
		`"日本語"`,
		`["日本語"]`,
		`{"日本語": "日本語"}`,
		`{"a": "b"}`,
		`{"a\nb": "a"}`,
		`{"a": "b", "b": "c"}`,
		`{"a": "b", "b": 1}`,
		`{"a": "b", "b": 1, "c": [1, 2, {"a": 3}]}`,
		// We don't expect to get any invalid UTF8, but check one anyway. We could do
		// a validation that the input is valid UTF8, but that seems wasteful when it
		// should be checked higher up.
		string([]byte{'"', 0xa7, '"'}),
	}
	ctx := NewTestingEvalContext()
	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			j, err := ParseDJSON(tc)
			if err != nil {
				t.Fatal(err)
			}
			s := j.String()

			j2, err := ParseDJSON(s)
			if err != nil {
				t.Fatalf("error while parsing %v: %s", s, err)
			}
			if j.Compare(ctx, j2) != 0 {
				t.Fatalf("%v should equal %v", tc, s)
			}
			s2 := j2.String()
			if s != s2 {
				t.Fatalf("%v should equal %v", s, s2)
			}
		})
	}
}

func TestJSONErrors(t *testing.T) {
	testCases := []struct {
		input string
		msg   string
	}{
		{`true false`, `trailing characters after JSON document`},
		{`trues`, `trailing characters after JSON document`},
		{`1 2 3`, `trailing characters after JSON document`},
		// Here the decoder just grabs the 0 and leaves the 1. JSON numbers can't have
		// leading 0s.
		{`01`, `trailing characters after JSON document`},
		{`{foo: 1}`, `invalid character 'f' looking for beginning of object key string`},
		{`{'foo': 1}`, `invalid character '\'' looking for beginning of object key string`},
		{`{"foo": 01}`, `invalid character '1' after object key:value pair`},
		{`{`, `unexpected EOF`},
		{`"\v"`, `invalid character 'v' in string escape code`},
		{`"\x00"`, `invalid character 'x' in string escape code`},
		{string([]byte{'"', '\n', '"'}), `invalid character`},
		{string([]byte{'"', 8, '"'}), `invalid character`},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			j, err := ParseDJSON(tc.input)
			if err == nil {
				t.Fatalf("expected parsing '%v' to error with '%s', but no error occurred and parsed as %s", tc.input, tc.msg, j.String())
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Fatalf("expected error message to be '%s', but was '%s'", tc.msg, err.Error())
			}
			if _, ok := err.(*pgerror.Error); !ok {
				t.Fatalf("expected parsing '%s' to be a pgerror", tc.input)
			}
		})
	}
}

func TestJSONSize(t *testing.T) {
	testCases := []struct {
		input string
		size  uintptr
	}{
		// These numbers aren't set in stone, they're just here to make sure this
		// function makes some amount of sense.
		{`true`, 120},
		{`false`, 120},
		{`null`, 120},
		{`"hello"`, 125},
		{`["hello","goodbye"]`, 372},
		{`{"a":"b"}`, 258},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			j, err := ParseDJSON(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if j.Size() != tc.size {
				t.Fatalf("expected %v to have size %d, but had size %d", j, tc.size, j.Size())
			}
		})
	}
}
