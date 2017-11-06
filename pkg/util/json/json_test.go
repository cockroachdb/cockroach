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

package json

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"bytes"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
	jsons := make([]JSON, len(sources))
	for i := range sources {
		j, err := ParseJSON(sources[i])
		if err != nil {
			t.Fatal(err)
		}
		jsons[i] = j
	}
	for i := range jsons {
		if jsons[i].Compare(jsons[i]) != 0 {
			t.Errorf("%s not equal to itself", jsons[i])
		}
		for j := i + 1; j < len(jsons); j++ {
			if jsons[i].Compare(jsons[j]) != -1 {
				t.Errorf("expected %s < %s", jsons[i], jsons[j])
			}
			if jsons[j].Compare(jsons[i]) != 1 {
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
		`"🤔"`,
		`{"🤔": "🤔"}`,
		// We don't expect to get any invalid UTF8, but check one anyway. We could do
		// a validation that the input is valid UTF8, but that seems wasteful when it
		// should be checked higher up.
		string([]byte{'"', 0xa7, '"'}),
	}
	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			j, err := ParseJSON(tc)
			if err != nil {
				t.Fatal(err)
			}
			s := j.String()

			j2, err := ParseJSON(s)
			if err != nil {
				t.Fatalf("error while parsing %v: %s", s, err)
			}
			if j.Compare(j2) != 0 {
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
			j, err := ParseJSON(tc.input)
			if err == nil {
				t.Fatalf("expected parsing '%v' to error with '%s', but no error occurred and parsed as %s", tc.input, tc.msg, j)
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
		{`true`, 0},
		{`false`, 0},
		{`null`, 0},
		{`"hello"`, 21},
		{`["hello","goodbye"]`, 76},
		{`{"a":"b"}`, 66},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			j, err := ParseJSON(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if j.Size() != tc.size {
				t.Fatalf("expected %v to have size %d, but had size %d", j, tc.size, j.Size())
			}
		})
	}
}

func TestMakeJSON(t *testing.T) {
	testCases := []struct {
		input    interface{}
		expected string
	}{
		// These tests are limited because they only really need to exercise the
		// parts not exercised by the ParseJSON tests - that is, those involving
		// int/int64/float64.
		{json.Number("1.00"), "1.00"},
		{1, "1"},
		{int64(1), "1"},
		{1.4, "1.4"},
		{map[string]interface{}{"foo": 4}, `{"foo": 4}`},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result, err := MakeJSON(tc.input)
			if err != nil {
				t.Fatal(err)
			}

			expectedResult, err := ParseJSON(tc.expected)
			if err != nil {
				t.Fatal(err)
			}

			if result.Compare(expectedResult) != 0 {
				t.Fatalf("expected %v to equal %v", result, expectedResult)
			}
		})
	}
}

func jsonTestShorthand(s string) JSON {
	j, _ := ParseJSON(s)
	return j
}

func TestJSONFetch(t *testing.T) {
	// Shorthand for tests.
	json := jsonTestShorthand
	cases := map[string][]struct {
		key      string
		expected JSON
	}{
		`{}`: {
			{``, nil},
			{`foo`, nil},
		},
		`{"foo": 1, "bar": "baz"}`: {
			{``, nil},
			{`foo`, json(`1`)},
			{`bar`, json(`"baz"`)},
			{`baz`, nil},
		},
		`["a"]`: {{``, nil}, {`0`, nil}, {`a`, nil}},
		`"a"`:   {{``, nil}, {`0`, nil}, {`a`, nil}},
		`1`:     {{``, nil}, {`0`, nil}, {`a`, nil}},
		`true`:  {{``, nil}, {`0`, nil}, {`a`, nil}},
	}

	for k, tests := range cases {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(k+`->`+tc.key, func(t *testing.T) {
				result := left.FetchValKey(tc.key)
				if result == nil || tc.expected == nil {
					if result == tc.expected {
						return
					}
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
				if result.Compare(tc.expected) != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

func TestJSONFetchIdx(t *testing.T) {
	json := jsonTestShorthand
	cases := map[string][]struct {
		idx      int
		expected JSON
	}{
		`{}`: {{1, nil}, {2, nil}},
		`{"foo": 1, "1": "baz"}`: {{0, nil}, {1, nil}, {2, nil}},
		`[]`: {{-1, nil}, {0, nil}, {1, nil}},
		`["a", "b", "c"]`: {
			// Negative indices count from the back.
			{-4, nil},
			{-3, json(`"a"`)},
			{-2, json(`"b"`)},
			{-1, json(`"c"`)},
			{0, json(`"a"`)},
			{1, json(`"b"`)},
			{2, json(`"c"`)},
			{3, nil},
		},
	}

	for k, tests := range cases {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s->%d", k, tc.idx), func(t *testing.T) {
				result := left.FetchValIdx(tc.idx)
				if result == nil || tc.expected == nil {
					if result == tc.expected {
						return
					}
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
				if result.Compare(tc.expected) != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

func TestJSONFetchPath(t *testing.T) {
	json := jsonTestShorthand
	cases := map[string][]struct {
		path     []string
		expected JSON
	}{
		`{}`: {
			{[]string{`a`}, nil},
		},
		`{"foo": "bar"}`: {
			{[]string{`foo`}, json(`"bar"`)},
			{[]string{`goo`}, nil},
		},
		`{"foo": {"bar": "baz"}}`: {
			{[]string{`foo`}, json(`{"bar": "baz"}`)},
			{[]string{`foo`, `bar`}, json(`"baz"`)},
			{[]string{`foo`, `baz`}, nil},
		},
		`{"foo": [1, 2, {"bar": "baz"}]}`: {
			{[]string{`foo`}, json(`[1, 2, {"bar": "baz"}]`)},
			{[]string{`foo`, `0`}, json(`1`)},
			{[]string{`foo`, `1`}, json(`2`)},
			{[]string{`foo`, `2`}, json(`{"bar": "baz"}`)},
			{[]string{`foo`, `2`, "bar"}, json(`"baz"`)},
			{[]string{`foo`, `-1`, "bar"}, json(`"baz"`)},
			{[]string{`foo`, `3`}, nil},
			{[]string{`foo`, `3`, "bar"}, nil},
		},
		`[1, 2, [1, 2, [1, 2]]]`: {
			{[]string{`"foo"`}, nil},
			{[]string{`0`}, json(`1`)},
			{[]string{`1`}, json(`2`)},
			{[]string{`0`, `0`}, nil},
			{[]string{`2`}, json(`[1, 2, [1, 2]]`)},
			{[]string{`2`, `0`}, json(`1`)},
			{[]string{`2`, `1`}, json(`2`)},
			{[]string{`2`, `2`}, json(`[1, 2]`)},
			{[]string{`2`, `2`, `0`}, json(`1`)},
			{[]string{`2`, `2`, `1`}, json(`2`)},
			{[]string{`-1`, `-1`, `-1`}, json(`2`)},
		},
	}

	for k, tests := range cases {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s#>%v", k, tc.path), func(t *testing.T) {
				result := FetchPath(left, tc.path)
				if result == nil || tc.expected == nil {
					if result == tc.expected {
						return
					}
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
				if result.Compare(tc.expected) != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

func TestJSONRemoveKey(t *testing.T) {
	json := jsonTestShorthand
	cases := map[string][]struct {
		key      string
		expected JSON
		errMsg   string
	}{
		`{}`: {
			{key: ``, expected: json(`{}`)},
			{key: `foo`, expected: json(`{}`)},
		},
		`{"foo": 1, "bar": "baz"}`: {
			{key: ``, expected: json(`{"foo": 1, "bar": "baz"}`)},
			{key: `foo`, expected: json(`{"bar": "baz"}`)},
			{key: `bar`, expected: json(`{"foo": 1}`)},
			{key: `baz`, expected: json(`{"foo": 1, "bar": "baz"}`)},
		},
		// Deleting a string key from an array never has any effect.
		`["a", "b", "c"]`: {
			{key: ``, expected: json(`["a", "b", "c"]`)},
			{key: `foo`, expected: json(`["a", "b", "c"]`)},
			{key: `0`, expected: json(`["a", "b", "c"]`)},
			{key: `1`, expected: json(`["a", "b", "c"]`)},
			{key: `-1`, expected: json(`["a", "b", "c"]`)},
		},
		`5`:     {{key: `a`, errMsg: "cannot delete from scalar"}},
		`"b"`:   {{key: `a`, errMsg: "cannot delete from scalar"}},
		`true`:  {{key: `a`, errMsg: "cannot delete from scalar"}},
		`false`: {{key: `a`, errMsg: "cannot delete from scalar"}},
		`null`:  {{key: `a`, errMsg: "cannot delete from scalar"}},
	}

	for k, tests := range cases {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(k+`-`+tc.key, func(t *testing.T) {
				result, err := left.RemoveKey(tc.key)
				if tc.errMsg != "" {
					if err == nil {
						t.Fatal("expected error")
					} else if !strings.Contains(err.Error(), tc.errMsg) {
						t.Fatalf(`expected error message "%s" to contain "%s"`, err.Error(), tc.errMsg)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if result.Compare(tc.expected) != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

func TestJSONRemoveIndex(t *testing.T) {
	json := jsonTestShorthand
	cases := map[string][]struct {
		idx      int
		expected JSON
		errMsg   string
	}{
		`{"foo": 1, "bar": "baz"}`: {
			{idx: 0, errMsg: "cannot delete from object using integer"},
		},
		`["a", "b", "c"]`: {
			{idx: -4, expected: json(`["a", "b", "c"]`)},
			{idx: -3, expected: json(`["b", "c"]`)},
			{idx: -2, expected: json(`["a", "c"]`)},
			{idx: -1, expected: json(`["a", "b"]`)},
			{idx: 0, expected: json(`["b", "c"]`)},
			{idx: 1, expected: json(`["a", "c"]`)},
			{idx: 2, expected: json(`["a", "b"]`)},
			{idx: 3, expected: json(`["a", "b", "c"]`)},
		},
		`[]`: {
			{idx: -1, expected: json(`[]`)},
			{idx: 0, expected: json(`[]`)},
			{idx: 1, expected: json(`[]`)},
		},
		`5`:     {{idx: 0, errMsg: "cannot delete from scalar"}},
		`"b"`:   {{idx: 0, errMsg: "cannot delete from scalar"}},
		`true`:  {{idx: 0, errMsg: "cannot delete from scalar"}},
		`false`: {{idx: 0, errMsg: "cannot delete from scalar"}},
		`null`:  {{idx: 0, errMsg: "cannot delete from scalar"}},
	}

	for k, tests := range cases {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s-%d", k, tc.idx), func(t *testing.T) {
				result, err := left.RemoveIndex(tc.idx)
				if tc.errMsg != "" {
					if err == nil {
						t.Fatal("expected error")
					} else if !strings.Contains(err.Error(), tc.errMsg) {
						t.Fatalf(`expected error message "%s" to contain "%s"`, err.Error(), tc.errMsg)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if result.Compare(tc.expected) != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

type testCaseJSON struct {
	value  JSON
	expEnc [][]byte
}

func makeJson(str string) JSON {
	var json, _ = ParseJSON(str)
	return json
}

func getApdEnconding(num float64) *apd.Decimal {
	dec := apd.Decimal{}
	dec.SetFloat64(num)
	return &dec
}

func TestEncodeDecodeJSON(t *testing.T) {
	testCases := []testCaseJSON{
		{makeJson(`{"a":"b"}`), [][]byte{bytes.Join([][]byte{encoding.EncodeNotNullAscending(nil),
			encoding.EncodeStringAscending(nil, "a"), encoding.EncodeStringAscending(nil, "b")}, nil)}},
		{makeJson(`["a", "b"]`), [][]byte{append(encoding.EncodeArrayAscending(nil),
			encoding.EncodeStringAscending(nil, "a")...), append(encoding.EncodeArrayAscending(nil),
			encoding.EncodeStringAscending(nil, "b")...)}},
		{makeJson(`null`), [][]byte{encoding.EncodeNullAscending(nil)}},
		{makeJson(`false`), [][]byte{encoding.EncodeFalseAscending(nil)}},
		{makeJson(`true`), [][]byte{encoding.EncodeTrueAscending(nil)}},
		{makeJson(`1.23`), [][]byte{encoding.EncodeDecimalAscending(nil, getApdEnconding(1.23))}},
		{makeJson(`"a"`), [][]byte{encoding.EncodeStringAscending(nil, "a")}},
		{makeJson(`["c", {"a":"b"}]`), [][]byte{append(encoding.EncodeArrayAscending(nil),
			encoding.EncodeStringAscending(nil, "c")...),
			bytes.Join([][]byte{encoding.EncodeArrayAscending(nil), encoding.EncodeNotNullAscending(nil),
				encoding.EncodeStringAscending(nil, "a"), encoding.EncodeStringAscending(nil, "b")}, nil)}},
	}

	for _, c := range testCases {
		enc := c.value.EncodeForIndex()
		for j, path := range enc {
			if !bytes.Equal(path, c.expEnc[j]) {
				t.Errorf("unexpected encoding mismatch for %v. expected [%#v], got [%#v]",
					c.value, c.expEnc[j], path)
			}
		}

	}
}
