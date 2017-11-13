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
	"math/rand"
	"strings"
	"testing"

	"bytes"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		for j := i + 1; j < len(jsons); j++ {
			a := jsons[i]
			b := jsons[j]
			c, err := a.Compare(b)
			if err != nil {
				t.Fatal(err)
			}
			if c != -1 {
				t.Errorf("expected %s < %s", a, b)
			}

			c, err = b.Compare(a)
			if err != nil {
				t.Fatal(err)
			}
			if c != 1 {
				t.Errorf("expected %s > %s", a, b)
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
		`"\u0000"`,
		`"\u0001"`,
		`"\u0002"`,
		`"\u0003"`,
		`"\u0004"`,
		`"\u0005"`,
		`"\u0006"`,
		`"\u0007"`,
		`"\u0008"`,
		`"\t"`,
		`"\n"`,
		`"\u000b"`,
		`"\u000c"`,
		`"\r"`,
		`"\u000e"`,
		`"\u000f"`,
		`"\u0010"`,
		`"\u0011"`,
		`"\u0012"`,
		`"\u0013"`,
		`"\u0014"`,
		`"\u0015"`,
		`"\u0016"`,
		`"\u0017"`,
		`"\u0018"`,
		`"\u0019"`,
		`"\u001a"`,
		`"\u001b"`,
		`"\u001c"`,
		`"\u001d"`,
		`"\u001e"`,
		`"\u001f"`,
		`"\uffff"`,
		`"\\"`,
		`"\""`,
		// We don't expect to get any invalid UTF8, but check one anyway. We could do
		// a validation that the input is valid UTF8, but that seems wasteful when it
		// should be checked higher up.
		string([]byte{'"', 0xa7, '"'}),
	}
	for _, tc := range testCases {
		j, err := ParseJSON(tc)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(tc, func(t *testing.T) {
			s := j.String()

			j2, err := ParseJSON(s)
			if err != nil {
				t.Fatalf("error while parsing %v: %s", s, err)
			}
			c, err := j.Compare(j2)
			if err != nil {
				t.Fatal(err)
			}
			if c != 0 {
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

			c, err := result.Compare(expectedResult)
			if err != nil {
				t.Fatal(err)
			}
			if c != 0 {
				t.Fatalf("expected %v to equal %v", result, expectedResult)
			}
		})
	}
}

func jsonTestShorthand(s string) JSON {
	j, err := ParseJSON(s)
	if err != nil {
		panic(fmt.Sprintf("bad json: %s", s))
	}
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
		`{"foo": [1, 2, 3], "bar": {"a": "b"}}`: {
			{``, nil},
			{`foo`, json(`[1, 2, 3]`)},
			{`bar`, json(`{"a": "b"}`)},
			{`baz`, nil},
		},
		`["a"]`: {{``, nil}, {`0`, nil}, {`a`, nil}},
		`"a"`:   {{``, nil}, {`0`, nil}, {`a`, nil}},
		`1`:     {{``, nil}, {`0`, nil}, {`a`, nil}},
		`true`:  {{``, nil}, {`0`, nil}, {`a`, nil}},
	}

	for k, tests := range cases {
		j, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(k+`->`+tc.key, func(t *testing.T) {
				result, err := j.FetchValKey(tc.key)
				if err != nil {
					t.Fatal(err)
				}
				if result == nil || tc.expected == nil {
					if result == tc.expected {
						return
					}
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
				c, err := result.Compare(tc.expected)
				if err != nil {
					t.Fatal(err)
				}
				if c != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

func TestJSONRandomFetch(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for i := 0; i < 1000; i++ {
		// We want a big object to trigger the binary search behavior in FetchValKey.
		j, err := Random(1000, rng)
		if err != nil {
			t.Fatal(err)
		}
		if obj, ok := j.(jsonObject); ok {
			// Pick a key:
			idx := rand.Intn(len(obj))
			result, err := obj.FetchValKey(string(obj[idx].k))
			if err != nil {
				t.Fatal(err)
			}
			c, err := result.Compare(obj[idx].v)
			if err != nil {
				t.Fatal(err)
			}
			if c != 0 {
				t.Fatalf("%s: expected fetching %s to give %s got %s", obj, obj[idx].k, obj[idx].v, result)
			}
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
		`[1, 2, {"foo":"bar"}]`: {
			{0, json(`1`)},
			{1, json(`2`)},
			{2, json(`{"foo":"bar"}`)},
		},
	}

	for k, tests := range cases {
		j, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s->%d", k, tc.idx), func(t *testing.T) {
				result, err := j.FetchValIdx(tc.idx)
				if err != nil {
					t.Fatal(err)
				}
				if result == nil || tc.expected == nil {
					if result == tc.expected {
						return
					}
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
				c, err := result.Compare(tc.expected)
				if err != nil {
					t.Fatal(err)
				}
				if c != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

func TestJSONExists(t *testing.T) {
	cases := map[string][]struct {
		key    string
		exists bool
	}{
		`{}`: {
			{``, false},
			{`foo`, false},
		},
		`{"foo": 1, "bar": "baz"}`: {
			{``, false},
			{`foo`, true},
			{`bar`, true},
			{`baz`, false},
		},
		`{"foo": [1, 2, 3], "bar": {"a": "b"}}`: {
			{``, false},
			{`foo`, true},
			{`bar`, true},
			{`baz`, false},
		},
		`["a"]`: {{``, false}, {`0`, false}, {`a`, true}},
		`"a"`:   {{``, false}, {`0`, false}, {`a`, false}},
		`1`:     {{``, false}, {`0`, false}, {`a`, false}},
		`true`:  {{``, false}, {`0`, false}, {`a`, false}},
	}

	for k, tests := range cases {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(k+` ? `+tc.key, func(t *testing.T) {
				result, err := left.Exists(tc.key)
				if err != nil {
					t.Fatal(err)
				}
				if result != tc.exists {
					if tc.exists {
						t.Fatalf("expected %s ? %s", left, tc.key)
					}
					t.Fatalf("expected %s to NOT ? %s", left, tc.key)
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
				result, err := FetchPath(left, tc.path)
				if err != nil {
					t.Fatal(err)
				}
				if result == nil || tc.expected == nil {
					if result == tc.expected {
						return
					}
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
				c, err := result.Compare(tc.expected)
				if err != nil {
					t.Fatal(err)
				}
				if c != 0 {
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
		j, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(k+`-`+tc.key, func(t *testing.T) {
				result, err := j.RemoveKey(tc.key)
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
				c, err := result.Compare(tc.expected)
				if err != nil {
					t.Fatal(err)
				}
				if c != 0 {
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
		`[{}, {"a":"b"}, {"c":"d"}]`: {
			{idx: 0, expected: json(`[{"a":"b"},{"c":"d"}]`)},
			{idx: 1, expected: json(`[{},{"c":"d"}]`)},
			{idx: 2, expected: json(`[{},{"a":"b"}]`)},
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
		j, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s-%d", k, tc.idx), func(t *testing.T) {
				result, err := j.RemoveIndex(tc.idx)
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
				c, err := result.Compare(tc.expected)
				if err != nil {
					t.Fatal(err)
				}
				if c != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

func getApdEncoding(num float64) *apd.Decimal {
	dec := &apd.Decimal{}
	dec, _ = dec.SetFloat64(num)
	return dec
}

func arraySeparator(b []byte) []byte {
	return encoding.EncodeArrayAscending(b)
}

func keyPrefix(b []byte) []byte {
	return encoding.EncodeNotNullAscending(b)
}

func TestEncodeDecodeJSONInvertedIndex(t *testing.T) {
	bytePrefix := make([]byte, 1, 100)
	bytePrefix[0] = 0x0f

	testCases := []struct {
		value  string
		expEnc [][]byte
	}{
		{`{"a":"b"}`, [][]byte{bytes.Join([][]byte{keyPrefix(bytePrefix),
			encoding.EncodeStringAscending(nil, "a"), encoding.EncodeStringAscending(nil, "b")}, nil)}},
		{`["a", "b"]`, [][]byte{bytes.Join([][]byte{arraySeparator(bytePrefix),
			encoding.EncodeStringAscending(nil, "a")}, nil),
			bytes.Join([][]byte{arraySeparator(bytePrefix),
				encoding.EncodeStringAscending(nil, "b")}, nil)}},
		{`null`, [][]byte{encoding.EncodeNullAscending(bytePrefix)}},
		{`false`, [][]byte{encoding.EncodeFalseAscending(bytePrefix)}},
		{`true`, [][]byte{encoding.EncodeTrueAscending(bytePrefix)}},
		{`1.23`, [][]byte{encoding.EncodeDecimalAscending(bytePrefix, getApdEncoding(1.23))}},
		{`"a"`, [][]byte{encoding.EncodeStringAscending(bytePrefix, "a")}},
		{`["c", {"a":"b"}]`, [][]byte{bytes.Join([][]byte{arraySeparator(bytePrefix),
			encoding.EncodeStringAscending(nil, "c")}, nil),
			bytes.Join([][]byte{arraySeparator(bytePrefix),
				keyPrefix(nil),
				encoding.EncodeStringAscending(nil, "a"),
				encoding.EncodeStringAscending(nil, "b")}, nil)}},
		{`["c", {"a":["c","d"]}]`, [][]byte{bytes.Join([][]byte{
			arraySeparator(bytePrefix),
			encoding.EncodeStringAscending(nil, "c")}, nil),

			bytes.Join([][]byte{arraySeparator(bytePrefix),
				keyPrefix(nil),
				encoding.EncodeStringAscending(nil, "a"),
				arraySeparator(nil),
				encoding.EncodeStringAscending(nil, "c")}, nil),

			bytes.Join([][]byte{arraySeparator(bytePrefix),
				keyPrefix(nil),
				encoding.EncodeStringAscending(nil, "a"),
				arraySeparator(nil),
				encoding.EncodeStringAscending(nil, "d")}, nil)}},

		{`{"a":"b","e":"f"}`, [][]byte{
			bytes.Join([][]byte{keyPrefix(bytePrefix),
				encoding.EncodeStringAscending(nil, "a"), encoding.EncodeStringAscending(nil, "b")}, nil),

			bytes.Join([][]byte{keyPrefix(bytePrefix),
				encoding.EncodeStringAscending(nil, "e"), encoding.EncodeStringAscending(nil, "f")}, nil),
		}},
	}

	for _, c := range testCases {
		enc, err := jsonTestShorthand(c.value).EncodeInvertedIndexKeys(bytePrefix)
		if err != nil {
			t.Fatal(err)
		}
		for j, path := range enc {
			if !bytes.Equal(path, c.expEnc[j]) {
				t.Errorf("unexpected encoding mismatch for %v. expected [%#v], got [%#v]",
					c.value, c.expEnc[j], path)
			}
		}
	}
}

func TestJSONRandomRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for i := 0; i < 1000; i++ {
		j, err := Random(20, rng)
		if err != nil {
			t.Fatal(err)
		}
		j2, err := ParseJSON(j.String())
		if err != nil {
			t.Fatal(err)
		}
		c, err := j.Compare(j2)
		if err != nil {
			t.Fatal(err)
		}
		if c != 0 {
			t.Fatalf("%s did not round-trip, got %s", j.String(), j2.String())
		}
	}
}

func TestJSONContains(t *testing.T) {
	cases := map[string][]struct {
		other    string
		expected bool
	}{
		`10`: {
			{`10`, true},
			{`9`, false},
		},
		`[1, 2, 3]`: {
			{`{}`, false},
			{`[]`, true},
			{`[1]`, true},
			{`[1.0]`, true},
			{`[1.00]`, true},
			{`[1, 2]`, true},
			{`[1, 2, 2]`, true},
			{`[2, 1]`, true},
			{`[1, 4]`, false},
			{`[4, 1]`, false},
			{`[4]`, false},
			// This is a unique, special case that only applies to arrays and
			// scalars.
			{`1`, true},
			{`2`, true},
			{`3`, true},
			{`4`, false},
		},
		`[[1, 2], 3]`: {
			{`[1, 2]`, false},
			{`[[1, 2]]`, true},
			{`[[2, 1]]`, true},
			{`[3, [1, 2]]`, true},
			{`[[1, 2], 3]`, true},
			{`[[1], [2], 3]`, true},
			{`[[1], 3]`, true},
			{`[[2]]`, true},
			{`[[3]]`, false},
		},
		`[]`: {
			{`[]`, true},
			{`[1]`, false},
			{`1`, false},
			{`{}`, false},
		},
		`{}`: {
			{`{}`, true},
			{`1`, false},
			{`{"a":"b"}`, false},
			{`{"":""}`, false},
		},
		`[[1, 2], [3, 4]]`: {
			{`[[1, 2]]`, true},
			{`[[3, 4]]`, true},
			{`[[3, 4, 5]]`, false},
			{`[[1, 3]]`, false},
			{`[[2, 4]]`, false},
			{`[1]`, false},
			{`[1, 2]`, false},
		},
		`{"a": "b", "c": "d"}`: {
			{`{}`, true},
			{`{"a": "b"}`, true},
			{`{"c": "d"}`, true},
			{`{"a": "b", "c": "d"}`, true},
			{`{"a": "x"}`, false},
			{`{"c": "x"}`, false},
			{`{"y": "x"}`, false},
			{`{"a": "b", "c": "x"}`, false},
			{`{"a": "x", "c": "y"}`, false},
		},
		`{"a": [1, 2, 3], "c": {"foo": "bar"}}`: {
			{`{}`, true},
			{`{"a": [1, 2, 3], "c": {"foo": "bar"}}`, true},
			{`{"a": [1, 2]}`, true},
			{`{"a": [2, 1]}`, true},
			{`{"a": []}`, true},
			{`{"a": [4]}`, false},
			{`{"a": [3], "c": {}}`, true},
			{`{"a": [4], "c": {}}`, false},
			{`{"a": [3], "c": {"foo": "gup"}}`, false},
		},
		`[{"a": 1}, {"b": 2, "c": 3}, [1], true, false, null, "hello"]`: {
			{`[]`, true},
			{`{}`, false},
			{`{"a": 1}`, false},
			{`[{"a": 1}]`, true},
			{`[{"b": 2}]`, true},
			{`[{"b": 2, "d": 4}]`, false},
			{`[{"b": 2, "c": 3}]`, true},
			{`[{"a": 1}, {"b": 2}, {"c": 3}]`, true},
			{`[{}]`, true},
			{`[true]`, true},
			{`[true, false]`, true},
			{`[false, true]`, true},
			{`[[1]]`, true},
			{`[[]]`, true},
			{`[null, "hello"]`, true},
			{`["hello", "hello", []]`, true},
			{`["hello", {"a": 1}, "hello", []]`, true},
			{`["hello", {"a": 1, "b": 2}, "hello", []]`, false},
		},
		`[{"Ck@P":{"7RZ2\"mZBH":null,"|__v":[1.685483]},"EM%&":{"I{TH":[],"}p@]7sIKC\\$":[]},"f}?#z~":{"#e9>m\"v75'&+":false,"F;,+&r9":{}}},[{}],false,0.498401]`: {
			{`[false,{}]`, true},
		},
	}

	for k, tests := range cases {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s @> %s", k, tc.other), func(t *testing.T) {
				other, err := ParseJSON(tc.other)
				if err != nil {
					t.Fatal(err)
				}
				result, err := Contains(left, other)
				if err != nil {
					t.Fatal(err)
				}

				checkResult := left.(containsTester).slowContains(other)
				if result != checkResult {
					t.Fatal("mismatch between actual contains and slowContains")
				}

				if tc.expected && !result {
					t.Fatalf("expected %s @> %s", left, other)
				} else if !tc.expected && result {
					t.Fatalf("expected %s to not @> %s", left, other)
				}
			})
		}
	}
}

// TestPositiveRandomJSONContains randomly generates a JSON document, generates
// a subdocument of it, then verifies that it matches under contains.
func TestPositiveRandomJSONContains(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for i := 0; i < 1000; i++ {
		j, err := Random(20, rng)
		if err != nil {
			t.Fatal(err)
		}

		subdoc := j.(containsTester).subdocument(true /* isRoot */, rng)

		c, err := Contains(j, subdoc)
		if err != nil {
			t.Fatal(err)
		}
		if !c {
			t.Fatal(fmt.Sprintf("%s should contain %s", j, subdoc))
		}
		if !j.(containsTester).slowContains(subdoc) {
			t.Fatal(fmt.Sprintf("%s should slowContains %s", j, subdoc))
		}
	}
}

// We expect that this almost always results in a non-contained pair..
func TestNegativeRandomJSONContains(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for i := 0; i < 1000; i++ {
		j1, err := Random(20, rng)
		if err != nil {
			t.Fatal(err)
		}

		j2, err := Random(20, rng)
		if err != nil {
			t.Fatal(err)
		}

		realResult, err := Contains(j1, j2)
		if err != nil {
			t.Fatal(err)
		}
		slowResult := j1.(containsTester).slowContains(j2)
		if realResult != slowResult {
			fmt.Println("realResult=", realResult)
			t.Fatal("mismatch for document " + j1.String() + " @> " + j2.String())
		}
	}
}

func BenchmarkFetchKey(b *testing.B) {
	for _, objectSize := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("object size %d", objectSize), func(b *testing.B) {
			keys := make([]string, objectSize)

			obj := make(map[string]interface{})
			for i := 0; i < objectSize; i++ {
				key := fmt.Sprintf("key%d", i)
				keys = append(keys, key)
				obj[key] = i
			}
			j, err := MakeJSON(obj)
			if err != nil {
				b.Fatal(err)
			}

			// TODO(justin): add benchmarks for fetching from still-encoded objects.
			b.Run("fetch key", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					j.FetchValKey(keys[rand.Intn(len(keys))])
				}
			})
		})
	}
}
