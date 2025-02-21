// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/deduplicate"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func eachPair(a, b JSON, f func(a, b JSON)) {
	f(a, a)
	f(a, b)
	f(b, a)
	f(b, b)
}

func eachComparison(a, b JSON, c, d JSON, f func(a, b JSON)) {
	f(a, c)
	f(a, d)
	f(b, c)
	f(b, d)
}

func runDecodedAndEncoded(t *testing.T, testName string, j JSON, f func(t *testing.T, j JSON)) {
	t.Run(testName, func(t *testing.T) {
		f(t, j)
	})
	t.Run(testName+` (encoded)`, func(t *testing.T) {
		encoding, err := EncodeJSON(nil, j)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := newEncodedFromRoot(encoding)
		if err != nil {
			t.Fatal(err)
		}
		f(t, encoded)
	})
}

func TestJSONOrdering(t *testing.T) {
	// We test here that every element in order sorts before every one that comes
	// after it, and is equal to itself.
	sources := []string{
		// In Postgres's sorting rules, the empty array comes before everything,
		// even null.
		`[]`,
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
		// Shorter arrays sort before longer arrays (this is the same as in
		// Postgres).
		`[1]`,
		`[2]`,
		`[1, 2]`,
		`[1, 3]`,
		// Objects with fewer keys come before objects with more keys.
		`{}`,
		`{"a": 1}`,
		`{"a": 2}`,
		// In Postgres, keys which are shorter sort before keys which are
		// longer. This is not true for us (right now).
		// TODO(justin): unclear if it should be.
		`{"aa": 1}`,
		`{"b": 1}`,
		`{"b": 2}`,
		// Objects are compared key-1, value-1, key-2, value-2, ...
		`{"a": 2, "c": 3}`,
		`{"a": 3, "b": 3}`,
	}
	jsons := make([]JSON, len(sources))
	encJSONs := make([]JSON, len(sources))
	for i := range sources {
		j, err := ParseJSON(sources[i])
		if err != nil {
			t.Fatal(err)
		}
		jsons[i] = j
		b, err := EncodeJSON(nil, j)
		if err != nil {
			t.Fatal(err)
		}
		encJSONs[i], err = newEncodedFromRoot(b)
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range jsons {
		eachPair(jsons[i], encJSONs[i], func(a, b JSON) {
			c, err := a.Compare(b)
			if err != nil {
				t.Fatal(err)
			}
			if c != 0 {
				t.Errorf("%s not equal to %s", a, b)
			}
		})
		for j := i + 1; j < len(jsons); j++ {
			eachComparison(jsons[i], encJSONs[i], jsons[j], encJSONs[j], func(a, b JSON) {
				c, err := a.Compare(b)
				if err != nil {
					t.Fatal(err)
				}
				if c != -1 {
					t.Errorf("expected %s < %s", a, b)
				}
			})
			eachComparison(jsons[j], encJSONs[j], jsons[i], encJSONs[i], func(a, b JSON) {
				c, err := a.Compare(b)
				if err != nil {
					t.Fatal(err)
				}
				if c != 1 {
					t.Errorf("expected %s > %s", a, b)
				}
			})
		}
	}
}

// parseJSONImpls lists set of parse json implementation configurations.
// This is done as a slice to ensure stable ordering when iterating.
var parseJSONImpls = []struct {
	name string
	typ  parseJSONImplType
	opts []ParseOption
}{
	{name: "gostd", typ: useStdGoJSON, opts: []ParseOption{WithGoStandardParser()}},
	{name: "lexer", typ: useFastJSONParser, opts: []ParseOption{WithFastJSONParser()}},
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
		`[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]`,
		string([]byte{'"', 0x7f, '"'}), // DEL character does not need escaping.
		string([]byte{'"', 0x7F, '"'}), // DEL character does not need escaping.
	}

	// Add 2 more tests with random JSON containing large strings.
	// We want to make sure that we exercise code that reads blocks
	// of data in memory.
	rng, _ := randutil.NewTestRand()
	testCases = append(testCases,
		jsonString(randomJSONString(rng, randConfig{maxLen: 1 << 13})).String(),
		jsonString(randomJSONString(rng, randConfig{maxLen: 1 << 13, escapeProb: 0.25})).String(),
	)

	for _, impl := range parseJSONImpls {
		t.Run(impl.name, func(t *testing.T) {
			for i, tc := range testCases {
				j, err := ParseJSON(tc, impl.opts...)
				if err != nil {
					t.Fatal(err, tc)
				}
				// We don't include the actual string here because TeamCity doesn't handle
				// test names with emojis in them well.
				runDecodedAndEncoded(t, fmt.Sprintf("%d", i), j, func(t *testing.T, j JSON) {
					s := j.String()
					j2, err := ParseJSON(s, impl.opts...)
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
		})
	}

	for _, impl := range parseJSONImpls {
		t.Run(fmt.Sprintf("invalid utf/%s", impl.name), func(t *testing.T) {
			// We don't expect to get any invalid UTF8, but check one anyway. We could do
			// a validation that the input is valid UTF8, but that seems wasteful when it
			// should be checked higher up.
			invalid := string([]byte{'"', 0xa7, '"'})

			j, err := ParseJSON(invalid, impl.opts...)
			if err != nil {
				t.Fatal(err)
			}

			expect := string([]rune{'"', utf8.RuneError, '"'})
			runDecodedAndEncoded(t, "invalid utf", j, func(t *testing.T, j JSON) {
				j2, err := ParseJSON(j.String())
				if err != nil {
					t.Fatalf("error while parsing %v: %s", j.String(), err)
				}
				require.Equal(t, expect, j2.String())
			})
		})
	}
}

func TestJSONErrors(t *testing.T) {
	type testCaseDef struct {
		input     string
		implName  string
		opts      []ParseOption
		expectErr string
	}
	testCase := func(input, expectErr string, impl parseJSONImplType) testCaseDef {
		return testCaseDef{
			input:     input,
			implName:  parseJSONImpls[impl].name,
			opts:      parseJSONImpls[impl].opts,
			expectErr: expectErr,
		}
	}
	trailingChars := errTrailingCharacters.Error()

	testCases := []testCaseDef{
		testCase(`true false`, trailingChars, useStdGoJSON),
		testCase(`true false`, trailingChars, useFastJSONParser),
		testCase(`trues`, trailingChars, useStdGoJSON),
		testCase(`trues`, trailingChars, useFastJSONParser),
		testCase(`1 2 3`, trailingChars, useStdGoJSON),
		testCase(`1 2 3`, trailingChars, useFastJSONParser),
		testCase(`[1, 2, 3]]`, trailingChars, useStdGoJSON),
		testCase(`[1, 2, 3]]`, trailingChars, useFastJSONParser),
		testCase(`[1, 2, 3]   }   `, trailingChars, useStdGoJSON),
		testCase(`[1, 2, 3]   }   `, trailingChars, useFastJSONParser),
		// Here the decoder just grabs the 0 and leaves the 1. JSON numbers can't have
		// leading 0s.
		testCase(`01`, trailingChars, useStdGoJSON),
		testCase(`01`, trailingChars, useFastJSONParser),
		testCase(`--01`, `invalid character '-' in numeric literal`, useStdGoJSON),
		testCase(`--01`, `invalid JSON token`, useFastJSONParser),
		testCase(`-`, `unexpected EOF`, useStdGoJSON),
		testCase(`-`, `unable to decode JSON`, useFastJSONParser),

		testCase(`{foo: 1}`,
			`invalid character 'f' looking for beginning of object key string`, useStdGoJSON),
		testCase(`{foo: 1}`, `
...|{foo: 1}|...
...|.^......|...: invalid JSON token`, useFastJSONParser),

		testCase(`{'foo': 1}`,
			`invalid character '\\'' looking for beginning of object key string`, useStdGoJSON),
		testCase(`{'foo': 1}`, `
...|{'foo': 1}|...
...|.^.........|...: invalid JSON token`, useFastJSONParser),

		testCase(`{"foo": 01}`,
			`invalid character '1' after object key:value pair`, useStdGoJSON),
		testCase(`{"foo": 01}`, `
...|{"foo": 01}|...
...|.........^.|...: stateObjectComma: expecting comma`, useFastJSONParser),

		testCase(`{`, `unexpected EOF`, useStdGoJSON),
		testCase(`{`, `unable to decode JSON`, useFastJSONParser),

		testCase(`"\v"`, `invalid character 'v' in string escape code`, useStdGoJSON),
		testCase(`"\v"`, `
...|"\v"|...
...|^...|...: invalid string literal token`, useFastJSONParser),

		testCase(`"\x00"`, `invalid character 'x' in string escape code`, useStdGoJSON),
		testCase(`"\x00"`, `
...|"\x00"|...
...|^.....|...: invalid string literal token`, useFastJSONParser),

		testCase(string([]byte{'"', '\n', '"'}), `invalid character`, useStdGoJSON),
		testCase(string([]byte{'"', '\n', '"'}), `while decoding 3 bytes at offset 0`, useFastJSONParser),

		testCase(string([]byte{'"', 8, '"'}), `invalid character`, useStdGoJSON),
		testCase(string([]byte{'"', 8, '"'}), `while decoding 3 bytes at offset 0`, useFastJSONParser),

		testCase(`{"a":["b","c"]}]`, trailingChars, useStdGoJSON),
		testCase(`{"a":["b","c"]}]`, trailingChars, useFastJSONParser),

		testCase(`\u`, `unable to decode JSON: invalid character .* looking for beginning of value`, useStdGoJSON),
		testCase(`\u`, `invalid JSON token`, useFastJSONParser),
		testCase(`\u1`, `unable to decode JSON: invalid character .* looking for beginning of value`, useStdGoJSON),
		testCase(`\u1`, `invalid JSON token`, useFastJSONParser),
		testCase(`\u111z`, `unable to decode JSON: invalid character .* looking for beginning of value`, useStdGoJSON),
		testCase(`\u111z`, `invalid JSON token`, useFastJSONParser),

		// Trailing "," should not be allowed.
		testCase(`[,]`, `invalid character ','`, useStdGoJSON),
		testCase(`[,]`, `unexpected comma`, useFastJSONParser),
		testCase(`[1,]`, `invalid character ']'`, useStdGoJSON),
		testCase(`[1,]`, `unexpected comma`, useFastJSONParser),
		testCase(`[1, [2,]]`, `invalid character ']'`, useStdGoJSON),
		testCase(`[1, [2,]]`, `unexpected comma`, useFastJSONParser),
		testCase(`[1, [2, 3],]`, `invalid character ']'`, useStdGoJSON),
		testCase(`[1, [2, 3],]`, `unexpected comma`, useFastJSONParser),
		testCase(`{"k":,}`, `invalid character ','`, useStdGoJSON),
		testCase(`{"k":,}`, `unexpected object token ","`, useFastJSONParser),
		testCase(`{"k": [1,]}`, `invalid character ']'`, useStdGoJSON),
		testCase(`{"k": [1,]}`, `unexpected comma`, useFastJSONParser),
		testCase(`{"b": false, }`, `invalid character '}' looking for beginning of object key`, useStdGoJSON),
		testCase(`{"b": false, }`, `stateObjectString: missing string key`, useFastJSONParser),
		testCase(`[1, {"a":"b",}]`, `invalid character '}'`, useStdGoJSON),
		testCase(`[1, {"a":"b",}]`, `stateObjectString: missing string key`, useFastJSONParser),
		testCase(`[1, {"a":"b"},]`, `invalid character ']'`, useStdGoJSON),
		testCase(`[1, {"a":"b"},]`, `unexpected comma`, useFastJSONParser),
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/%s", tc.implName, tc.input), func(t *testing.T) {
			j, err := ParseJSON(tc.input, tc.opts...)
			if err == nil {
				t.Fatalf("expected parsing '%v' to error with '%s', but no error occurred and parsed as %s",
					tc.input, tc.expectErr, j)
			}
			require.Regexp(t, tc.expectErr, err)
			if !pgerror.HasCandidateCode(err) {
				t.Fatalf("expected parsing '%s' to provide a pg error code", tc.input)
			}
		})
	}
}

// Not a true fuzz test, but we'll generate some number of random JSON objects,
// with or without errors, and attempt to parse them using both standard and
// fast parsers.
func TestParseJSONFuzz(t *testing.T) {
	const errProb = 0.005         // ~0.5% chance of an error.
	const fuzzTargetErrors = 1000 // ~90k inputs.

	rng, seed := randutil.NewTestRand()
	t.Log("test seed ", seed)
	numInputs := 0
	for numErrors := 0; numErrors < fuzzTargetErrors; {
		inputJson, err := RandGen(rng)
		require.NoError(t, err)

		jsonStr := AsStringWithErrorChance(inputJson, rng, errProb)
		numInputs++

		fastJson, fastErr := ParseJSON(jsonStr, WithFastJSONParser())
		stdJson, stdErr := ParseJSON(jsonStr, WithGoStandardParser())

		if fastErr == nil && stdErr == nil {
			// No errors -- both JSONs must be identical.
			// Note: we can't compare against inputJSON since jsonStr (generated with
			// error probability), sometimes drops array/object elements.
			eq, err := fastJson.Compare(stdJson)
			require.NoError(t, err)
			require.Equal(t, 0, eq, "unequal JSON objects: std<%s> fast<%s>",
				asString(stdJson), asString(fastJson))
		} else {
			numErrors++
			// Both parsers must produce an error.
			require.Errorf(t, fastErr, "expected fast parser error for input: %s", jsonStr)
			require.Errorf(t, stdErr, "expected std parser error for input: %s", jsonStr)
		}
	}

	t.Logf("Executed fuzz test against %d inputs", numInputs)
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
		{`""`, stringHeaderSize},
		{`"hello"`, stringHeaderSize + 5},
		{`[]`, sliceHeaderSize},
		{`[null]`, sliceHeaderSize + jsonInterfaceSize},
		{`[""]`, sliceHeaderSize + jsonInterfaceSize + stringHeaderSize},
		{`[[]]`, sliceHeaderSize + jsonInterfaceSize + sliceHeaderSize},
		{`[{}]`, sliceHeaderSize + jsonInterfaceSize + sliceHeaderSize},
		{`["hello","goodbye"]`, sliceHeaderSize + 2*jsonInterfaceSize + 2*stringHeaderSize + 12},
		{`{}`, sliceHeaderSize},
		{`{"":null}`, sliceHeaderSize + keyValuePairSize},
		{`{"":{}}`, sliceHeaderSize + keyValuePairSize + sliceHeaderSize},
		{`{"a":"b"}`, sliceHeaderSize + keyValuePairSize + 1 + stringHeaderSize + 1},
	}
	largeBuf := make([]byte, 0, 10240)
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			j, err := ParseJSON(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if j.Size() != tc.size {
				t.Fatalf("expected %v to have size %d, but had size %d", j, tc.size, j.Size())
			}

			t.Run("jsonEncodedSize", func(t *testing.T) {
				largeBuf = largeBuf[:0]
				var buf []byte
				var err error

				if buf, err = EncodeJSON(buf, j); err != nil {
					t.Fatal(err)
				}
				if largeBuf, err = EncodeJSON(largeBuf, j); err != nil {
					t.Fatal(err)
				}

				encoded, err := newEncodedFromRoot(buf)
				if err != nil {
					t.Fatal(err)
				}
				encodedFromLargeBuf, err := newEncodedFromRoot(largeBuf)
				if err != nil {
					t.Fatal(err)
				}
				if encodedFromLargeBuf.Size() != encoded.Size() {
					t.Errorf("expected jsonEncoded on a large buf for %v to have size %d, found %d", j, encoded.Size(), encodedFromLargeBuf.Size())
				}
			})
		})
	}
}

func TestJSONLen(t *testing.T) {
	testCases := []struct {
		input string
		len   int
	}{
		{`true`, 0},
		{`false`, 0},
		{`null`, 0},
		{`"a"`, 0},
		{`[]`, 0},
		{`["a","b"]`, 2},
		{`{}`, 0},
		{`{"a":"b"}`, 1},
	}
	for _, tc := range testCases {
		j, err := ParseJSON(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		runDecodedAndEncoded(t, tc.input, j, func(t *testing.T, j JSON) {
			if j.Len() != tc.len {
				t.Fatalf("expected %d, got %d", tc.len, j.Len())
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

func TestArrayBuilderWithCounter(t *testing.T) {
	testCases := []struct {
		input []interface{}
	}{
		{[]interface{}{}},
		{[]interface{}{"a"}},
		{[]interface{}{1, 2, "abc", nil, 1.2}},
		{[]interface{}{"a", "aa", "aaa", "aaaa"}},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("array %v", tc.input), func(t *testing.T) {
			builder := NewArrayBuilderWithCounter()
			for _, e := range tc.input {
				j, err := MakeJSON(e)
				if err != nil {
					t.Fatal(err)
				}
				builder.Add(j)
			}
			result := builder.Build()
			expectedJSON, err := MakeJSON(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			c, err := result.Compare(expectedJSON)
			if err != nil {
				t.Fatal(err)
			}
			if c != 0 {
				t.Fatalf("expected %v to equal %v", result, expectedJSON)
			}
			if builder.Size() != result.Size() {
				t.Fatalf("expected %v to equal %v", builder.Size(), result.Size())
			}
		})
	}
}

func TestNewObjectBuilderWithCounter(t *testing.T) {
	testCases := []struct {
		input    [][]interface{}
		expected JSON
	}{
		{
			input:    [][]interface{}{},
			expected: parseJSON(t, `{}`),
		},
		{
			input:    [][]interface{}{{"key1", "val1"}},
			expected: parseJSON(t, `{"key1": "val1"}`),
		},
		{
			input:    [][]interface{}{{"key1", "val1"}, {"key2", "val2"}},
			expected: parseJSON(t, `{"key1": "val1", "key2": "val2"}`),
		},
		{
			input:    [][]interface{}{{"key1", []interface{}{1, 2, 3, 4}}},
			expected: parseJSON(t, `{"key1": [1, 2, 3, 4]}`),
		},
		{
			input:    [][]interface{}{{"key1", nil}, {"key2", 1}, {"key3", -1.27}, {"key4", "abcd"}},
			expected: parseJSON(t, `{"key1": null, "key2": 1, "key3": -1.27, "key4": "abcd"}`),
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("keys %+v", tc.input), func(t *testing.T) {
			builder := NewObjectBuilderWithCounter()
			for _, pair := range tc.input {
				j, err := MakeJSON(pair[1])
				if err != nil {
					t.Fatal(err)
				}
				builder.Add(pair[0].(string), j)
			}
			result := builder.Build()
			c, err := result.Compare(tc.expected)
			if err != nil {
				t.Fatal(err)
			}
			if c != 0 {
				t.Fatalf("expected %v to equal %v", result, tc.expected)
			}
			if builder.Size() != result.Size() {
				t.Fatalf("expected %v to equal %v", builder.Size(), result.Size())
			}
		})
	}
}

func TestBuildJSONObject(t *testing.T) {
	checkJSONObjectsEqual := func(t *testing.T, expected, found JSON) {
		t.Helper()
		c, err := found.Compare(expected)
		if err != nil {
			t.Fatal(err)
		}
		if c != 0 {
			t.Fatalf("expected %v to equal %v", found, expectError)
		}
	}

	testCases := []struct {
		input []string
	}{
		{[]string{}},
		{[]string{"a"}},
		{[]string{"a", "c", "a", "b", "a"}},
		{[]string{"2", "1", "10", "3", "10", "1"}},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("keys %v", tc.input), func(t *testing.T) {
			m := map[string]interface{}{}
			b := NewObjectBuilder(0)
			for i, k := range tc.input {
				j := FromString(fmt.Sprintf("%d", i))
				m[k] = j
				b.Add(k, j)
			}
			expectedResult, err := MakeJSON(m)
			if err != nil {
				t.Fatal(err)
			}
			result := b.Build()
			checkJSONObjectsEqual(t, expectedResult, result)

			t.Run("fixedKeys", func(t *testing.T) {
				uniqueKeys := func() (keys []string) {
					for k := range m {
						keys = append(keys, k)
					}
					return keys
				}()

				fkb, err := NewFixedKeysObjectBuilder(uniqueKeys)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 5; i++ {
					for k, v := range m {
						if err := fkb.Set(k, v.(JSON)); err != nil {
							t.Fatal(err)
						}
					}
					result, err := fkb.Build()
					if err != nil {
						t.Fatal(err)
					}
					checkJSONObjectsEqual(t, expectedResult, result)
				}
			})
		})
	}
}

func TestBuildFixedKeysJSONObjectErrors(t *testing.T) {
	t.Run("require_unique_keys", func(t *testing.T) {
		_, err := NewFixedKeysObjectBuilder([]string{"a", "b", "c", "a", "d"})
		require.Error(t, err)
	})
	t.Run("requires_all_keys_set", func(t *testing.T) {
		b, err := NewFixedKeysObjectBuilder([]string{"a", "b", "c"})
		require.NoError(t, err)
		require.NoError(t, b.Set("a", jsonNull{}))
		require.NoError(t, b.Set("b", jsonNull{}))
		_, err = b.Build()
		require.Error(t, err)
	})
}

func parseJSON(tb testing.TB, s string) JSON {
	tb.Helper()
	// Pick random implementation to use.
	impl := parseJSONImpls[rand.Intn(len(parseJSONImpls))]
	j, err := ParseJSON(s, impl.opts...)
	require.NoError(tb, err, "using %s implementation to parse `%s`", impl.name, s)
	return j
}

func TestJSONFetch(t *testing.T) {
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
			{`foo`, parseJSON(t, `1`)},
			{`bar`, parseJSON(t, `"baz"`)},
			{`baz`, nil},
		},
		`{"foo": [1, 2, 3], "bar": {"a": "b"}}`: {
			{``, nil},
			{`foo`, parseJSON(t, `[1, 2, 3]`)},
			{`bar`, parseJSON(t, `{"a": "b"}`)},
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
			runDecodedAndEncoded(t, k+`->`+tc.key, left, func(t *testing.T, j JSON) {
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

func TestJSONFetchFromBig(t *testing.T) {
	size := 100

	obj := make(map[string]interface{})
	for i := 0; i < size; i++ {
		obj[fmt.Sprintf("key%d", i)] = i
	}
	j, err := MakeJSON(obj)
	if err != nil {
		t.Fatal(err)
	}

	runDecodedAndEncoded(t, "fetch big", j, func(t *testing.T, j JSON) {
		for i := 0; i < size; i++ {
			result, err := j.FetchValKey(fmt.Sprintf("key%d", i))
			if err != nil {
				t.Fatal(err)
			}

			expected, err := MakeJSON(i)
			if err != nil {
				t.Fatal(err)
			}

			cmp, err := result.Compare(expected)
			if err != nil {
				t.Fatal(err)
			}
			if cmp != 0 {
				t.Errorf("expected %d, got %d", i, result)
			}
		}
	})
}

func TestJSONFetchIdx(t *testing.T) {
	cases := map[string][]struct {
		idx      int
		expected JSON
	}{
		`{}`:                     {{1, nil}, {2, nil}},
		`{"foo": 1, "1": "baz"}`: {{0, nil}, {1, nil}, {2, nil}},
		`[]`:                     {{-1, nil}, {0, nil}, {1, nil}},
		`["a", "b", "c"]`: {
			// Negative indices count from the back.
			{-4, nil},
			{-3, parseJSON(t, `"a"`)},
			{-2, parseJSON(t, `"b"`)},
			{-1, parseJSON(t, `"c"`)},
			{0, parseJSON(t, `"a"`)},
			{1, parseJSON(t, `"b"`)},
			{2, parseJSON(t, `"c"`)},
			{3, nil},
		},
		`[1, 2, {"foo":"bar"}]`: {
			{0, parseJSON(t, `1`)},
			{1, parseJSON(t, `2`)},
			{2, parseJSON(t, `{"foo":"bar"}`)},
		},
		// Trigger the offset-value
		`[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40]`: {
			{0, parseJSON(t, `0`)},
			{1, parseJSON(t, `1`)},
			{2, parseJSON(t, `2`)},
			{10, parseJSON(t, `10`)},
			{35, parseJSON(t, `35`)},
		},
		// Scalar values can be indexed.
		`null`:  {{-2, nil}, {-1, parseJSON(t, `null`)}, {0, parseJSON(t, `null`)}, {1, nil}},
		`true`:  {{-2, nil}, {-1, parseJSON(t, `true`)}, {0, parseJSON(t, `true`)}, {1, nil}},
		`false`: {{-2, nil}, {-1, parseJSON(t, `false`)}, {0, parseJSON(t, `false`)}, {1, nil}},
		`"foo"`: {{-2, nil}, {-1, parseJSON(t, `"foo"`)}, {0, parseJSON(t, `"foo"`)}, {1, nil}},
		`123`:   {{-2, nil}, {-1, parseJSON(t, `123`)}, {0, parseJSON(t, `123`)}, {1, nil}},
	}

	for k, tests := range cases {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			runDecodedAndEncoded(t, fmt.Sprintf("%s->%d", k, tc.idx), left, func(t *testing.T, j JSON) {
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
		`"a"`:   {{``, false}, {`0`, false}, {`a`, true}},
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

			t.Run(k+` ? `+tc.key+` (encoded)`, func(t *testing.T) {
				encoding, err := EncodeJSON(nil, left)
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := newEncodedFromRoot(encoding)
				if err != nil {
					t.Fatal(err)
				}

				result, err := encoded.Exists(tc.key)
				if err != nil {
					t.Fatal(err)
				}
				if result != tc.exists {
					if tc.exists {
						t.Fatalf("expected %s ? %s", encoded, tc.key)
					}
					t.Fatalf("expected %s to NOT ? %s", encoded, tc.key)
				}
			})
		}
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		left, err := Random(20, rng)
		require.NoError(t, err)
		right := randomJSONString(rng, defaultRandConfig)
		require.NoError(t, err)

		var exists bool
		exists, err = left.Exists(right)
		require.NoError(t, err)

		// Test that we get the same result with the encoded form of the JSON.
		b, err := EncodeJSON(nil, left)
		require.NoError(t, err)
		j, err := FromEncoding(b)
		require.NoError(t, err)

		existsEncoded, err := j.Exists(right)
		require.NoError(t, err)

		require.Equal(t, exists, existsEncoded, "expected encoded/non-encoded Exists to match but didn't: %s %s", left, right)
	}
}

func TestJSONStripNulls(t *testing.T) {
	testcases := []struct {
		input       string
		needToStrip bool
		expected    string
	}{
		{`null`, false, `null`},
		{`1`, false, `1`},
		{`"a"`, false, `"a"`},
		{`true`, false, `true`},
		{`false`, false, `false`},
		{`[]`, false, `[]`},
		{`[1, "a", null, true, {"a":1, "b":[null]}]`, false, `[1, "a", null, true, {"a":1, "b":[null]}]`},
		{`[{"a":null}]`, true, `[{}]`},
		{`[[{"a":null}]]`, true, `[[{}]]`},
		{`[null, {"a":null}, {"a":null}]`, true, `[null, {}, {}]`},
		{`[{"a":null}, {"a":null}, null]`, true, `[{}, {}, null]`},
		{`{}`, false, `{}`},
		{`{"a":[null], "b":1, "c":{"a":1}}`, false, `{"a":[null], "b":1, "c":{"a":1}}`},
		{`{"a":null}`, true, `{}`},
		{`{"a":{"a":null}}`, true, `{"a":{}}`},
		{`{"a":[{"a":null}]}`, true, `{"a":[{}]}`},
		{`{"a":[null], "b":null, "c":{"a":null}}`, true, `{"a":[null], "c":{}}`},
		{`{"a":[null], "b":{"a":null}, "c":{"a":null}}`, true, `{"a":[null], "b":{}, "c":{}}`},
		{`{"a":{"a":null}, "b":{"a":null}, "c":[null]}`, true, `{"a":{}, "b":{}, "c":[null]}`},
	}
	for _, tc := range testcases {
		j, err := ParseJSON(tc.input)
		if err != nil {
			t.Fatal(err)
		}
		expectedResult, err := ParseJSON(tc.expected)
		if err != nil {
			t.Fatal(err)
		}
		runDecodedAndEncoded(t, tc.input, j, func(t *testing.T, j JSON) {
			result, needToStrip, err := j.StripNulls()
			if err != nil {
				t.Fatal(err)
			}
			c, err := result.Compare(expectedResult)
			if err != nil {
				t.Fatal(err)
			}
			if needToStrip != tc.needToStrip {
				t.Fatalf("expected %t, got %t", tc.needToStrip, needToStrip)
			}
			if c != 0 {
				t.Fatalf("expected %s, got %s", tc.expected, result)
			}
			// Check whether j is not changed.
			originInput, err := ParseJSON(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if c, err = j.Compare(originInput); err != nil {
				t.Fatal(err)
			}
			if c != 0 {
				t.Fatalf("expected %s, got %s", originInput, j)
			}
		})
	}
}

func TestJSONFetchPath(t *testing.T) {
	cases := map[string][]struct {
		path     []string
		expected JSON
	}{
		`{}`: {
			{[]string{`a`}, nil},
		},
		`{"foo": "bar"}`: {
			{[]string{`foo`}, parseJSON(t, `"bar"`)},
			{[]string{`goo`}, nil},
		},
		`{"foo": {"bar": "baz"}}`: {
			{[]string{`foo`}, parseJSON(t, `{"bar": "baz"}`)},
			{[]string{`foo`, `bar`}, parseJSON(t, `"baz"`)},
			{[]string{`foo`, `baz`}, nil},
		},
		`{"foo": [1, 2, {"bar": "baz"}]}`: {
			{[]string{`foo`}, parseJSON(t, `[1, 2, {"bar": "baz"}]`)},
			{[]string{`foo`, `0`}, parseJSON(t, `1`)},
			{[]string{`foo`, `1`}, parseJSON(t, `2`)},
			{[]string{`foo`, `2`}, parseJSON(t, `{"bar": "baz"}`)},
			{[]string{`foo`, `2`, "bar"}, parseJSON(t, `"baz"`)},
			{[]string{`foo`, `-1`, "bar"}, parseJSON(t, `"baz"`)},
			{[]string{`foo`, `3`}, nil},
			{[]string{`foo`, `3`, "bar"}, nil},
		},
		`[1, 2, [1, 2, [1, 2]]]`: {
			{[]string{`"foo"`}, nil},
			{[]string{`0`}, parseJSON(t, `1`)},
			{[]string{`1`}, parseJSON(t, `2`)},
			{[]string{`0`, `0`}, nil},
			{[]string{`2`}, parseJSON(t, `[1, 2, [1, 2]]`)},
			{[]string{`2`, `0`}, parseJSON(t, `1`)},
			{[]string{`2`, `1`}, parseJSON(t, `2`)},
			{[]string{`2`, `2`}, parseJSON(t, `[1, 2]`)},
			{[]string{`2`, `2`, `0`}, parseJSON(t, `1`)},
			{[]string{`2`, `2`, `1`}, parseJSON(t, `2`)},
			{[]string{`-1`, `-1`, `-1`}, parseJSON(t, `2`)},
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

		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s#>%v (encoded)", k, tc.path), func(t *testing.T) {
				encoding, err := EncodeJSON(nil, left)
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := newEncodedFromRoot(encoding)
				if err != nil {
					t.Fatal(err)
				}
				result, err := FetchPath(encoded, tc.path)
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

func TestJSONDeepSet(t *testing.T) {
	cases := map[string][]struct {
		path          []string
		to            string
		createMissing bool
		expected      string
	}{
		`{}`: {
			{[]string{}, `1`, true, `{}`},
			{[]string{`a`}, `1`, true, `{"a": 1}`},
			{[]string{`a`}, `1`, false, `{}`},
			{[]string{`a`, `b`}, `1`, true, `{}`},
		},
		`{"a": 1}`: {
			{[]string{}, `2`, true, `{"a": 1}`},
			{[]string{`a`}, `2`, true, `{"a": 2}`},
			{[]string{`a`}, `2`, false, `{"a": 2}`},
			{[]string{`a`, `b`}, `2`, true, `{"a": 1}`},
		},
		`{"a": {"b": 1}}`: {
			{[]string{}, `2`, true, `{"a": {"b": 1}}`},
			{[]string{`a`}, `2`, true, `{"a": 2}`},
			{[]string{`a`, `b`}, `2`, true, `{"a": {"b": 2}}`},
			{[]string{`a`, `b`}, `2`, false, `{"a": {"b": 2}}`},
			{[]string{`a`, `c`}, `3`, true, `{"a": {"b": 1, "c": 3}}`},
			{[]string{`a`, `c`}, `3`, false, `{"a": {"b": 1}}`},
			{[]string{`b`}, `2`, true, `{"a": {"b": 1}, "b": 2}`},
			{[]string{`b`}, `2`, false, `{"a": {"b": 1}}`},
		},
		`{"b": 1}`: {
			{[]string{`a`}, `2`, true, `{"a": 2, "b": 1}`},
			{[]string{`b`}, `2`, true, `{"b": 2}`},
			{[]string{`c`}, `2`, true, `{"b": 1, "c": 2}`},
		},
		`[]`: {
			{[]string{`-2`}, `1`, true, `[1]`},
			{[]string{`-1`}, `1`, true, `[1]`},
			{[]string{`0`}, `1`, true, `[1]`},
			{[]string{`1`}, `1`, true, `[1]`},
			{[]string{`2`}, `1`, true, `[1]`},
			{[]string{`-2`}, `1`, false, `[]`},
			{[]string{`-1`}, `1`, false, `[]`},
			{[]string{`0`}, `1`, false, `[]`},
			{[]string{`1`}, `1`, false, `[]`},
			{[]string{`2`}, `1`, false, `[]`},
			{[]string{`2`, `0`}, `1`, true, `[]`},
		},
		`[10]`: {
			{[]string{`-2`}, `1`, true, `[1, 10]`},
			{[]string{`-1`}, `1`, true, `[1]`},
			{[]string{`0`}, `1`, true, `[1]`},
			{[]string{`1`}, `1`, true, `[10, 1]`},
			{[]string{`2`}, `1`, true, `[10, 1]`},
			{[]string{`-2`}, `1`, false, `[10]`},
			{[]string{`-1`}, `1`, false, `[1]`},
			{[]string{`0`}, `1`, false, `[1]`},
			{[]string{`1`}, `1`, false, `[10]`},
			{[]string{`2`}, `1`, false, `[10]`},
		},
		`[10, 20]`: {
			{[]string{`-3`}, `1`, true, `[1, 10, 20]`},
			{[]string{`-2`}, `1`, true, `[1, 20]`},
			{[]string{`-1`}, `1`, true, `[10, 1]`},
			{[]string{`0`}, `1`, true, `[1, 20]`},
			{[]string{`1`}, `1`, true, `[10, 1]`},
			{[]string{`2`}, `1`, true, `[10, 20, 1]`},
			{[]string{`3`}, `1`, true, `[10, 20, 1]`},
			{[]string{`3`, `1`}, `1`, true, `[10, 20]`},
		},
		`[[10], [20, 30], {"a": 1}]`: {
			{[]string{`0`}, `1`, true, `[1, [20, 30], {"a": 1}]`},
			{[]string{`0`, `0`}, `1`, true, `[[1], [20, 30], {"a": 1}]`},
			{[]string{`0`, `1`}, `1`, true, `[[10, 1], [20, 30], {"a": 1}]`},
			{[]string{`1`, `0`}, `1`, true, `[[10], [1, 30], {"a": 1}]`},
			{[]string{`2`, `a`}, `2`, true, `[[10], [20, 30], {"a": 2}]`},
			{[]string{`2`, `b`}, `2`, true, `[[10], [20, 30], {"a": 1, "b": 2}]`},
			{[]string{`2`, `a`, `c`}, `2`, true, `[[10], [20, 30], {"a": 1}]`},
			{[]string{`2`, `b`, `c`}, `2`, true, `[[10], [20, 30], {"a": 1}]`},
		},
		`[{"a": [1]}]`: {
			{[]string{`0`, `a`, `0`}, `2`, true, `[{"a": [2]}]`},
			{[]string{`0`, `a`, `0`, `0`}, `2`, true, `[{"a": [1]}]`},
		},
	}

	for k, tests := range cases {
		j := parseJSON(t, k)
		for _, tc := range tests {
			t.Run(fmt.Sprintf(`set(%s, %s, %s)`, k, tc.path, tc.to), func(t *testing.T) {
				result, err := DeepSet(j, tc.path, parseJSON(t, tc.to), tc.createMissing)
				if err != nil {
					t.Fatal(err)
				}

				cmp, err := result.Compare(parseJSON(t, tc.expected))
				if err != nil {
					t.Fatal(err)
				}

				if cmp != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result)
				}
			})
		}
	}
}

func TestJSONRemoveString(t *testing.T) {
	cases := map[string][]struct {
		key      string
		ok       bool
		expected JSON
		errMsg   string
	}{
		`{}`: {
			{key: ``, ok: false, expected: parseJSON(t, `{}`)},
			{key: `foo`, ok: false, expected: parseJSON(t, `{}`)},
		},
		`{"foo": 1, "bar": "baz"}`: {
			{key: ``, ok: false, expected: parseJSON(t, `{"foo": 1, "bar": "baz"}`)},
			{key: `foo`, ok: true, expected: parseJSON(t, `{"bar": "baz"}`)},
			{key: `bar`, ok: true, expected: parseJSON(t, `{"foo": 1}`)},
			{key: `baz`, ok: false, expected: parseJSON(t, `{"foo": 1, "bar": "baz"}`)},
		},
		// Deleting a string key from an array never has any effect.
		`["a", "b", "c"]`: {
			{key: ``, ok: false, expected: parseJSON(t, `["a", "b", "c"]`)},
			{key: `foo`, ok: false, expected: parseJSON(t, `["a", "b", "c"]`)},
			{key: `0`, ok: false, expected: parseJSON(t, `["a", "b", "c"]`)},
			{key: `1`, ok: false, expected: parseJSON(t, `["a", "b", "c"]`)},
			{key: `-1`, ok: false, expected: parseJSON(t, `["a", "b", "c"]`)},
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
			runDecodedAndEncoded(t, k+`-`+tc.key, left, func(t *testing.T, j JSON) {
				result, ok, err := j.RemoveString(tc.key)
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
				if tc.ok != ok {
					t.Fatalf("expected %t, got %t", tc.ok, ok)
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
	cases := map[string][]struct {
		idx      int
		ok       bool
		expected JSON
		errMsg   string
	}{
		`{"foo": 1, "bar": "baz"}`: {
			{idx: 0, errMsg: "cannot delete from object using integer"},
		},
		`["a", "b", "c"]`: {
			{idx: -4, ok: false, expected: parseJSON(t, `["a", "b", "c"]`)},
			{idx: -3, ok: true, expected: parseJSON(t, `["b", "c"]`)},
			{idx: -2, ok: true, expected: parseJSON(t, `["a", "c"]`)},
			{idx: -1, ok: true, expected: parseJSON(t, `["a", "b"]`)},
			{idx: 0, ok: true, expected: parseJSON(t, `["b", "c"]`)},
			{idx: 1, ok: true, expected: parseJSON(t, `["a", "c"]`)},
			{idx: 2, ok: true, expected: parseJSON(t, `["a", "b"]`)},
			{idx: 3, ok: false, expected: parseJSON(t, `["a", "b", "c"]`)},
		},
		`[{}, {"a":"b"}, {"c":"d"}]`: {
			{idx: 0, ok: true, expected: parseJSON(t, `[{"a":"b"},{"c":"d"}]`)},
			{idx: 1, ok: true, expected: parseJSON(t, `[{},{"c":"d"}]`)},
			{idx: 2, ok: true, expected: parseJSON(t, `[{},{"a":"b"}]`)},
		},
		`[]`: {
			{idx: -1, ok: false, expected: parseJSON(t, `[]`)},
			{idx: 0, ok: false, expected: parseJSON(t, `[]`)},
			{idx: 1, ok: false, expected: parseJSON(t, `[]`)},
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
			runDecodedAndEncoded(t, fmt.Sprintf("%s-%d", k, tc.idx), left, func(t *testing.T, j JSON) {
				result, ok, err := j.RemoveIndex(tc.idx)
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
				if tc.ok != ok {
					t.Fatalf("expected %t, got %t", tc.ok, ok)
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

func TestEncodeDecodeJSONInvertedIndex(t *testing.T) {
	testCases := []struct {
		value  string
		expEnc []string
	}{
		{`{"a":"b"}`, []string{`/"a"/"b"`}},
		{`null`, []string{`/NULL`}},
		{`false`, []string{`/False`}},
		{`true`, []string{`/True`}},
		{`1.23`, []string{`/1.23`}},
		{`"a"`, []string{`/"a"`}},
		{`[]`, []string{`/[]`}},
		{`{}`, []string{`/{}`}},
		{`["c", {"a":"b"}]`, []string{`/Arr/"c"`, `/Arr/"a"/"b"`}},
		{`["c", {"a":["c","d"]}]`, []string{`/Arr/"c"`,
			`/Arr/"a"/Arr/"c"`,
			`/Arr/"a"/Arr/"d"`}},
		{`{"a":[]}`, []string{`/"a"/[]`}},
		{`{"a":[{}]}`, []string{`/"a"/Arr/{}`}},
		{`[[],{}]`, []string{`/Arr/[]`, `/Arr/{}`}},
		{`[["a"]]`, []string{`/Arr/Arr/"a"`}},
	}

	for _, c := range testCases {
		enc, err := EncodeInvertedIndexKeys(nil, parseJSON(t, c.value))
		if err != nil {
			t.Fatal(err)
		}

		for j, path := range enc {
			var buf redact.StringBuilder
			encoding.PrettyPrintValue(&buf, nil, path, "/")
			str := buf.String()
			if str != c.expEnc[j] {
				t.Errorf("unexpected encoding mismatch for %v. expected [%s], got [%s]",
					c.value, c.expEnc[j], str)
			}
		}
	}
}

func getApdEncoding(num float64) *apd.Decimal {
	dec := &apd.Decimal{}
	dec, _ = dec.SetFloat64(num)
	return dec
}

func TestEncodeJSONInvertedIndex(t *testing.T) {
	jsonPrefix := make([]byte, 1, 100)
	jsonPrefix[0] = 0x37

	terminator := make([]byte, 2, 100)
	terminator[0] = 0x00
	terminator[1] = 0x01

	objectSeparator := make([]byte, 2, 100)
	objectSeparator[0] = 0x00
	objectSeparator[1] = 0x02

	arrayPrefix := make([]byte, 2, 100)
	arrayPrefix[0] = 0x00
	arrayPrefix[1] = 0x03

	aEncoding := make([]byte, 1, 100)
	aEncoding[0] = 0x61

	bEncoding := make([]byte, 1, 100)
	bEncoding[0] = 0x62

	emptyObjectEncoding := make([]byte, 1, 100)
	emptyObjectEncoding[0] = 0x39

	emptyArrayEncoding := make([]byte, 1, 100)
	emptyArrayEncoding[0] = 0x38

	testCases := []struct {
		value  string
		expEnc [][]byte
	}{
		{`{"a":"b"}`, [][]byte{bytes.Join([][]byte{jsonPrefix, aEncoding, terminator, encoding.EncodeStringAscending(nil, "b")},
			nil)}},
		{`{"a":{"b":"c"}}`, [][]byte{bytes.Join([][]byte{jsonPrefix, aEncoding, objectSeparator, bEncoding, terminator, encoding.EncodeStringAscending(nil, "c")},
			nil)}},
		{`{"a":{"b":[]}}`, [][]byte{bytes.Join([][]byte{jsonPrefix, aEncoding, objectSeparator, bEncoding, terminator, emptyArrayEncoding},
			nil)}},
		{`{"a":{"b":{}}}`, [][]byte{bytes.Join([][]byte{jsonPrefix, aEncoding, objectSeparator, bEncoding, terminator, emptyObjectEncoding},
			nil)}},
		{`null`, [][]byte{bytes.Join([][]byte{jsonPrefix, terminator, encoding.EncodeNullAscending(nil)},
			nil)}},
		{`false`, [][]byte{bytes.Join([][]byte{jsonPrefix, terminator, encoding.EncodeFalseAscending(nil)},
			nil)}},
		{`true`, [][]byte{bytes.Join([][]byte{jsonPrefix, terminator, encoding.EncodeTrueAscending(nil)},
			nil)}},
		{`1.23`, [][]byte{bytes.Join([][]byte{jsonPrefix, terminator, encoding.EncodeDecimalAscending(nil, getApdEncoding(1.23))},
			nil)}},
		{`"a"`, [][]byte{bytes.Join([][]byte{jsonPrefix, terminator, encoding.EncodeStringAscending(nil, "a")},
			nil)}},
		{`[["a"]]`, [][]byte{bytes.Join([][]byte{jsonPrefix, arrayPrefix, arrayPrefix, terminator, encoding.EncodeStringAscending(nil, "a")},
			nil)}},
		{`{"a":["b","c"]}`, [][]byte{bytes.Join([][]byte{jsonPrefix, aEncoding, objectSeparator, arrayPrefix, terminator, encoding.EncodeStringAscending(nil, "b")}, nil),
			bytes.Join([][]byte{jsonPrefix, aEncoding, objectSeparator, arrayPrefix, terminator, encoding.EncodeStringAscending(nil, "c")}, nil)}},
		{`[]`, [][]byte{bytes.Join([][]byte{jsonPrefix, terminator, emptyArrayEncoding},
			nil)}},
		{`{}`, [][]byte{bytes.Join([][]byte{jsonPrefix, terminator, emptyObjectEncoding},
			nil)}},
		{`[{}, []]`, [][]byte{bytes.Join([][]byte{jsonPrefix, arrayPrefix, terminator, emptyObjectEncoding},
			nil),
			bytes.Join([][]byte{jsonPrefix, arrayPrefix, terminator, emptyArrayEncoding}, nil)}},
		{`[[[]]]`, [][]byte{bytes.Join([][]byte{jsonPrefix, arrayPrefix, arrayPrefix, terminator, emptyArrayEncoding},
			nil)}},
		{`[[{}]]`, [][]byte{bytes.Join([][]byte{jsonPrefix, arrayPrefix, arrayPrefix, terminator, emptyObjectEncoding},
			nil)}},
	}

	for _, c := range testCases {
		enc, err := EncodeInvertedIndexKeys(nil, parseJSON(t, c.value))
		if err != nil {
			t.Fatal(err, c.value)
		}
		// Make sure that the expected encoding slice is sorted, as well as the
		// output of the function under test, because the function under test can
		// reorder the keys it's returning if there are arrays inside.
		enc = deduplicate.ByteSlices(enc)
		c.expEnc = deduplicate.ByteSlices(c.expEnc)
		for j, path := range enc {
			if !bytes.Equal(path, c.expEnc[j]) {
				t.Errorf("unexpected encoding mismatch for %v. expected [%#v], got [%#v]",
					c.value, c.expEnc[j], path)
			}
		}
	}
}

func TestEncodeContainingJSONInvertedIndexSpans(t *testing.T) {
	testCases := []struct {
		indexedValue string
		value        string
		expected     bool
		tight        bool
		unique       bool
	}{
		// This test uses EncodeInvertedIndexKeys and
		// EncodeContainingInvertedIndexSpans to determine whether the first JSON
		// value contains the second. If the indexedValue @> value, expected is
		// true. Otherwise expected is false. If the spans produced for contains
		// are tight, tight is true. Otherwise tight is false.
		//
		// If EncodeContainingInvertedIndexSpans produces spans that are guaranteed not to
		// contain duplicate primary keys, unique is true. Otherwise it is false.
		{`{}`, `{}`, true, true, false},
		{`[]`, `[]`, true, true, false},
		{`[]`, `{}`, false, true, false},
		{`"a"`, `"a"`, true, true, true},
		{`null`, `{}`, false, true, false},
		{`{}`, `true`, false, true, true},
		{`[[], {}]`, `[]`, true, true, false},
		{`[[], {}]`, `{}`, false, true, false}, // Surprising, but matches Postgres' behavior.
		{`[{"a": "a"}, {"a": "a"}]`, `[]`, true, true, false},
		{`[[[["a"]]], [[["a"]]]]`, `[]`, true, true, false},
		{`{}`, `{"a": {}}`, false, true, false},
		{`{"a": 123.123}`, `{}`, true, true, false},
		{`{"a": [{}]}`, `{"a": []}`, true, true, false},
		{`{"a": [{}]}`, `{"a": {}}`, false, true, false},
		{`{"a": [1]}`, `{"a": []}`, true, true, false},
		{`{"a": {"b": "c"}}`, `{"a": {}}`, true, true, false},
		{`{"a": {}}`, `{"a": {"b": true}}`, false, true, true},
		{`[1, 2, 3, 4, "foo"]`, `[1, 2]`, true, true, true},
		{`[1, 2, 3, 4, "foo"]`, `[1, "bar"]`, false, true, true},
		{`{"a": {"b": [1]}}`, `{"a": {"b": [1]}}`, true, true, true},
		{`{"a": {"b": [1, [2]]}}`, `{"a": {"b": [1]}}`, true, true, true},
		{`{"a": "b", "c": "d"}`, `{"a": "b", "c": "d"}`, true, true, true},
		{`{"a": {"b": false}}`, `{"a": {"b": true}}`, false, true, true},
		{`[{"a": {"b": [1, [2]]}}, "d"]`, `[{"a": {"b": [[2]]}}, "d"]`, true, true, true},
		{`["a", "a"]`, `"a"`, true, true, true},
		{`[1, 2, 3, 1]`, `1`, true, true, true},
		{`[1, 2, 3, 1]`, `[1, 1]`, true, true, true},
		{`[true, false, null, 1.23, "a"]`, `"b"`, false, true, true},
		{`{"a": {"b": "c", "d": "e"}, "f": "g"}`, `{"a": {"b": "c"}}`, true, true, true},
		{`{"\u0000\u0001": "b"}`, `{}`, true, true, false},
		{`{"\u0000\u0001": {"\u0000\u0001": "b"}}`, `{"\u0000\u0001": {}}`, true, true, false},
		{`[[1], false, null]`, `[null, []]`, true, true, false},
		{`[[[], {}], false, null]`, `[null, []]`, true, true, false},
		{`[false, null]`, `[null, []]`, false, true, false},
		{`[[], null]`, `[null, []]`, true, true, false},
		{`[{"a": []}, null]`, `[null, []]`, false, true, false},
		{`[{"a": [[]]}, null]`, `[null, []]`, false, true, false},
		{`[{"foo": {"bar": "foobar"}}, true]`, `[true, {}]`, true, true, false},
		{`[{"b": null}, {"bar": "c"}]`, `[{"b": {}}]`, false, true, false},
		{`[[[[{}], [], false], false], [{}]]`, `[[[[]]]]`, true, true, false},
		{`[[[[{}], [], false], false], [{}]]`, `[false]`, false, true, true},
		{`[[{"a": {}, "c": "foo"}, {}], [false]]`, `[[false, {}]]`, false, false, false},
		{`[[1], [2]]`, `[[1, 2]]`, false, false, true},
		{`[[1, 2]]`, `[[1], [2]]`, true, true, true},
		{`{"bar": [["c"]]}`, `{"bar": []}`, true, true, false},
		{`{"c": [{"a": "b"}, []]}`, `{"c": [{}]}`, true, true, false},
		{`[{"bar": {"foo": {}}}, {"a": []}]`, `[{}, {"a": [], "bar": {}}, {}]`, false, false, false},
		{`[{"bar": [1]},{"bar": [2]}]`, `[{"bar": [1, 2]}]`, false, false, true},
		{`[[1], [2]]`, `[[1, 1]]`, true, true, true},
	}

	// runTest checks that evaluating `left @> right` using keys from
	// EncodeInvertedIndexKeys and spans from EncodeContainingInvertedIndexSpans
	// produces the expected result.
	// returns tight=true if the spans from EncodeContainingInvertedIndexSpans
	// were tight, and tight=false otherwise.
	runTest := func(left, right JSON, expected, expectUnique bool) (tight bool) {
		keys, err := EncodeInvertedIndexKeys(nil, left)
		require.NoError(t, err)

		invertedExpr, err := EncodeContainingInvertedIndexSpans(nil, right)
		require.NoError(t, err)

		spanExpr, ok := invertedExpr.(*inverted.SpanExpression)
		if !ok {
			t.Fatalf("invertedExpr %v is not a SpanExpression", invertedExpr)
		}

		if spanExpr.Unique != expectUnique {
			t.Errorf("For %s, expected unique=%v, but got %v", right, expectUnique, spanExpr.Unique)
		}

		actual, err := spanExpr.ContainsKeys(keys)
		require.NoError(t, err)

		// There may be some false positives, so filter those out.
		if actual && !spanExpr.Tight {
			actual, err = Contains(left, right)
			require.NoError(t, err)
		}

		if actual != expected {
			if expected {
				t.Errorf("expected %s to contain %s but it did not", left.String(), right.String())
			} else {
				t.Errorf("expected %s not to contain %s but it did", left.String(), right.String())
			}
		}

		return spanExpr.Tight
	}

	// Run pre-defined test cases from above.
	for _, c := range testCases {
		indexedValue, value := parseJSON(t, c.indexedValue), parseJSON(t, c.value)

		// First check that evaluating `indexedValue @> value` matches the expected
		// result.
		res, err := Contains(indexedValue, value)
		require.NoError(t, err)
		if res != c.expected {
			t.Fatalf(
				"expected value of %s @> %s did not match actual value. Expected: %v. Got: %v",
				c.indexedValue, c.value, c.expected, res,
			)
		}

		// Now check that we get the same result with the inverted index spans.
		tight := runTest(indexedValue, value, c.expected, c.unique)

		// And check that the tightness matches the expected value.
		if tight != c.tight {
			if c.tight {
				t.Errorf("expected spans for %s to be tight but they were not", c.value)
			} else {
				t.Errorf("expected spans for %s not to be tight but they were", c.value)
			}
		}
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		// Generate two random JSONs and evaluate the result of `left @> right`.
		left, err := Random(20, rng)
		require.NoError(t, err)
		right, err := Random(20, rng)
		require.NoError(t, err)

		res, err := Contains(left, right)
		require.NoError(t, err)

		// The spans will not produce duplicate primary keys if all paths end in a
		// scalar (i.e., not an empty object or array).
		paths, err := AllPaths(right)
		require.NoError(t, err)
		hasContainerLeaf := false
		for i := range paths {
			hasContainerLeaf, err = paths[i].HasContainerLeaf()
			require.NoError(t, err)
			if hasContainerLeaf {
				break
			}
		}
		expectUnique := !hasContainerLeaf

		// Now check that we get the same result with the inverted index spans.
		runTest(left, right, res, expectUnique)
	}
}

func TestEncodeContainedJSONInvertedIndexSpans(t *testing.T) {
	testCases := []struct {
		indexedValue string
		value        string
		containsKeys bool
		expected     bool
		unique       bool
	}{
		// This test uses EncodeInvertedIndexKeys and EncodeContainedInvertedIndexSpans
		// to determine if the spans produced from the second JSON value will correctly
		// include or exclude the first value, indicated by containsKeys.
		// Then, if indexedValue <@ value, expected is true.

		// Not all indexedValues included in the spans are contained by the value,
		// so the expression is never tight. Unless the value is a scalar, empty
		// object, or empty array, the expression produced is a union of spans, so
		// unique should be false.

		// First we test that the spans will include expected results, even if
		// they are not necessarily contained by the value.
		{`{}`, `{}`, true, true, true},
		{`[]`, `[]`, true, true, true},
		{`1`, `1`, true, true, true},
		{`"a"`, `"a"`, true, true, true},
		{`null`, `null`, true, true, true},
		{`true`, `true`, true, true, true},
		{`{}`, `[[], {}]`, true, false, false}, // Surprising, but matches Postgres' behavior.
		{`[]`, `[[], {}]`, true, true, false},
		{`[]`, `[{"a": "a"}, {"b": "c"}]`, true, true, false},
		{`[{}]`, `[{"a": "a"}, {"b": "c"}]`, true, true, false},
		{`[{"a": "a"}]`, `[{"a": "a"}, {"b": "c"}]`, true, true, false},
		{`[]`, `[[[["a"]]], [[["a"]]]]`, true, true, false},
		{`{}`, `{"a": 123.123}`, true, true, false},
		{`{"a": []}`, `{"a": [{}]}`, true, true, false},
		{`{"a": []}`, `{"a": [1]}`, true, true, false},
		{`{"a": {}}`, `{"a": {"b": "c"}}`, true, true, false},
		{`[1, 2]`, `[1, 2, 3, 4, "foo"]`, true, true, false},
		{`[1, "bar"]`, `[1, 2, 3, 4, "foo"]`, true, false, false},
		{`{"a": {"b": [1]}}`, `{"a": {"b": [1]}}`, true, true, false},
		{`{"a": {"b": [1]}}`, `{"a": {"b": [1, [2]]}}`, true, true, false},
		{`{"a": {"b": [1, 2]}}`, `{"a": {"b": [1, [2]]}}`, true, false, false},
		{`{"a": "b", "c": "d"}`, `{"a": "b", "c": "d"}`, true, true, false},
		{`"a"`, `["a", "a"]`, true, true, false},
		{`1`, `[1, 2, 3, 1]`, true, true, false},
		{`[1, 3, 3]`, `[1, 2, 3, 1]`, true, true, false},
		{`{}`, `{"\u0000\u0001": "b"}`, true, true, false},
		{`{"\u0000\u0001": {}}`, `{"\u0000\u0001": {"\u0000\u0001": "b"}}`, true, true, false},
		{`[null, []]`, `[[1], false, null]`, true, true, false},
		{`[null, {}]`, `[[[], {}], false, null]`, true, false, false},
		{`[true, {}]`, `[{"foo": {"bar": "foobar"}}, true]`, true, true, false},
		{`{"a": {"b": "c"}}`, `{"a": {"b": "c", "d": "e"}, "f": "g"}`, true, true, false},
		{`[{"a": {"b": [[2]]}}, "d"]`, `[{"a": {"b": [1, [2]]}}, "d"]`, true, true, false},
		{`[null, []]`, `[[], null]`, true, true, false},
		{`[[1, 2]]`, `[[1], [2]]`, true, false, false},
		{`[[1], [2]]`, `[[1, 2]]`, true, true, false},
		{`{"bar": []}`, `{"bar": [["c"]]}`, true, true, false},
		{`{"c": [{}]}`, `{"c": [{"a": "b"}, []]}`, true, true, false},
		{`[{}, {"a": [], "bar": {}}, {}]`, `[{"bar": {"foo": {}}}, {"a": []}]`, true, false, false},
		{`[{"bar": [1, 2]}]`, `[{"bar": [1]}, {"bar": [2]}]`, true, false, false},
		{`[[1, 1]]`, `[[1], [2]]`, true, true, false},
		{`[[[[]]]]`, `[[[[{}], [], false], false], [{}]]`, true, true, false},
		{`{"a": []}`, `{"4@9>eZjMRS": {"b": {}}, "9@B6\\ 3b": [null], "J4u}'6zpjW~": "DF,.W9t$PHZ", "a": [{"-zovTiPCGGV": {"\">&kjO": "c", "+Oyq": []}, "Ac{": null, "a": {}, "foobar": []}], "c": []}`, true, true, false},
		{`{"b": []}`, `{"b": [[{"a": []}, [], 0.6458094342366152]], "c": {"=<74QyuG": [2.3595799519823046, [], false], "R;J]H$T\"\\X": {"S\\PV)>H": {}}}}`, true, true, false},
		{`{"bar": []}`, `{"3o55": {"foo": {}, "foobar": [null]}, "bar": [true, null], "baz": {"bar": [true], "foobar": [[true], true], "}J>r!]_|Xd=": null}}`, true, true, false},
		{`{"b": []}`, `{"'fpR-G": "c", "8tQU": [], "MiL=R:;8A%{o": [], "a": {}, "b": [{}, [{}], 0.3628875092529953, []]}`, true, true, false},
		{`{"b": {}}`, `{"b": {"QOp1#": [], "b": [{}, {"bar": "_p9&"}, {}], "c": [null, []], "m2D": "b"}, "c": [true], "foo": {}}`, true, true, false},
		// Then we test that the spans do not include results that should not be
		// included.
		{`{"a": {}}`, `{}`, false, false, true},
		{`{"a": {}}`, `{"a": [{}]}`, false, false, false},
		{`{"b": "c"}`, `{"a": {"b": "c"}}`, false, false, false},
		{`{"a": [[]]}`, `{"a": [{}]}`, false, false, false},
		{`{"a": {"b": true}}`, `{"a": {}}`, false, false, false},
		{`{"a": {"b": true}}`, `{"a": {"b": false}}`, false, false, false},
		{`{"a": "a"}`, `[{"a": "a"}, {"b": "c"}]`, false, false, false},
		{`{}`, `[{"a": "a"}, {"b": "c"}]`, false, false, false},
		{`"b"`, `[true, false, null, 1.23, "a"]`, false, false, false},
		{`1`, `[[1]]`, false, false, false},
		{`[true]`, `[false, null]`, false, false, false},
		{`[[null]]`, `[{"a": []}, null]`, false, false, false},
		{`[[]]`, `[{"a": [[]]}, null]`, false, false, false},
		{`[{"b": {}}]`, `[{"b": null}, {"bar": "c"}]`, false, false, false},
		{`[false]`, `[[[[{}], [], false], false], [{}]]`, false, false, false},
		{`"foo"`, `[[{"a": {}, "c": "foo"}, {}], [false]]`, false, false, false},
	}

	runTest := func(indexedValue, value JSON, expectContainsKeys, expected, expectUnique bool) {
		keys, err := EncodeInvertedIndexKeys(nil, indexedValue)
		require.NoError(t, err)

		invertedExpr, err := EncodeContainedInvertedIndexSpans(nil, value)
		require.NoError(t, err)

		spanExpr, ok := invertedExpr.(*inverted.SpanExpression)
		if !ok {
			t.Fatalf("invertedExpr %v is not a SpanExpression", invertedExpr)
		}

		// Spans should never be tight for contained by.
		if spanExpr.Tight {
			t.Errorf("For %s, expected tight=false, but got true", value)
		}

		if spanExpr.Unique != expectUnique {
			t.Errorf("For %s, expected unique=%v, but got %v", value, expectUnique, spanExpr.Unique)
		}

		containsKeys, err := spanExpr.ContainsKeys(keys)
		require.NoError(t, err)

		if containsKeys != expectContainsKeys {
			if expectContainsKeys {
				t.Errorf("expected spans of %s to include %s but they did not", value, indexedValue)
			} else {
				t.Errorf("expected spans of %s not to include %s but they did", value, indexedValue)
			}
		}

		// Since the spans are never tight, apply an additional filter to determine
		// if the result is contained.
		actual, err := Contains(value, indexedValue)
		require.NoError(t, err)
		if actual != expected {
			if expected {
				t.Errorf("expected %s to be contained by %s but it was not", indexedValue, value)
			} else {
				t.Errorf("expected %s not to be contained by %s but it was", indexedValue, value)
			}
		}
	}

	// Run pre-defined test cases from above.
	for _, c := range testCases {
		indexedValue, value := parseJSON(t, c.indexedValue), parseJSON(t, c.value)

		// First check that evaluating `indexedValue <@ value` matches the expected
		// result.
		res, err := Contains(value, indexedValue)
		require.NoError(t, err)
		if res != c.expected {
			t.Fatalf(
				"expected value of %s <@ %s did not match actual value. Expected: %v. Got: %v",
				c.indexedValue, c.value, c.expected, res,
			)
		}
		runTest(indexedValue, value, c.containsKeys, c.expected, c.unique)
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		// Generate two random JSONs and evaluate the result of `left <@ right`.
		left, err := Random(20, rng)
		require.NoError(t, err)
		right, err := Random(20, rng)
		require.NoError(t, err)

		// We cannot check for false positives with these tests (due to the fact that
		// the spans are not tight), so we will only test for false negatives.
		isContained, err := Contains(right, left)
		require.NoError(t, err)
		if !isContained {
			continue
		}

		// The spans will not produce duplicate primary keys only if the json is a
		// scalar, empty object, or empty array).
		expectUnique := false
		if right.Len() == 0 {
			expectUnique = true
		}

		// Now check that we get the same result with the inverted index spans.
		runTest(left, right, true, true, expectUnique)
	}
}

func TestEncodeExistsJSONInvertedIndexSpans(t *testing.T) {
	testCases := []struct {
		indexedValue string
		value        string
		expected     bool
	}{
		// This test uses EncodeInvertedIndexKeys and EncodeExistsInvertedIndexSpans
		// to determine if the spans produced from the string key will include or
		// excludes the keys produced by the JSON value, indicated by exists.
		// Then, if indexedValue ? value, expected is true.

		// First we test that the spans will include expected results.
		{`{"a": "a"}`, `a`, true},
		{`{"a": null}`, `a`, true},
		{`{"a": []}`, `a`, true},
		{`{"a": {}}`, `a`, true},
		{`{"a": 3}`, `a`, true},
		{`["a"]`, `a`, true},
		{`["a", "a"]`, `a`, true},
		{`["b", "a"]`, `a`, true},
		{`"a"`, `a`, true},

		// Test negative cases.
		// The number 1 doesn't have key string-1.
		{`1`, `1`, false},
		// Null doesn't have key string-null.
		{`null`, `null`, false},
		// [] doesn't have key string-null.
		{`[]`, `null`, false},
		{`["a"]`, `argh`, false},
		{`["argh"]`, `a`, false},
		{`{}`, `null`, false},
		{`{}`, `a`, false},
		{`{"a":"a"}`, `argh`, false},
		{`{"argh":"a"}`, `a`, false},
	}

	runTest := func(indexedValue JSON, value string, expected bool) {
		keys, err := EncodeInvertedIndexKeys(nil, indexedValue)
		require.NoError(t, err)

		invertedExpr, err := EncodeExistsInvertedIndexSpans(nil, value)
		require.NoError(t, err)

		spanExpr, ok := invertedExpr.(*inverted.SpanExpression)
		if !ok {
			t.Fatalf("invertedExpr %v is not a SpanExpression", invertedExpr)
		}

		// Spans should always be tight for exists.
		if !spanExpr.Tight {
			t.Errorf("For %s, expected tight=true, but got false", value)
		}

		// Spans should never be unique for exists.
		if spanExpr.Unique {
			t.Errorf("For %s, expected unique=false, but got true", value)
		}

		containsKeys, err := spanExpr.ContainsKeys(keys)
		require.NoError(t, err)

		if containsKeys != expected {
			if expected {
				t.Errorf("expected spans of %s to include %s but they did not", value, indexedValue)
			} else {
				t.Errorf("expected spans of %s not to include %s but they did", value, indexedValue)
			}
		}
	}

	// Run pre-defined test cases from above.
	for _, c := range testCases {
		indexedValue, value := parseJSON(t, c.indexedValue), c.value

		// First check that evaluating `indexedValue ? value` matches the expected
		// result.
		actual, err := indexedValue.Exists(value)
		require.NoError(t, err)
		if actual != c.expected {
			t.Fatalf(
				"expected value of %s ? %s did not match actual value. Expected: %v. Got: %v",
				c.indexedValue, c.value, c.expected, actual,
			)
		}
		runTest(indexedValue, value, c.expected)
	}

	// Run a set of randomly generated test cases.
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		// Generate two random JSONs and evaluate the result of `left ? right`.
		left, err := Random(20, rng)
		require.NoError(t, err)
		right := randomJSONString(rng, defaultRandConfig)
		require.NoError(t, err)

		var exists bool
		exists, err = left.Exists(right)
		require.NoError(t, err)

		// Now check that we get the same result with the inverted index spans.
		runTest(left, right, exists)
	}
}

func TestNumInvertedIndexEntries(t *testing.T) {
	testCases := []struct {
		value    string
		expCount int
	}{
		{`1`, 1},
		{`"a"`, 1},
		{`null`, 1},
		{`false`, 1},
		{`true`, 1},
		{`[]`, 1},
		{`{}`, 1},
		{`[[[]]]`, 1},
		{`[[{}]]`, 1},
		{`[{}, []]`, 2},
		{`[1]`, 1},
		{`[1, 2]`, 2},
		{`[1, [1]]`, 2},
		{`[1, 2, 1, 2]`, 2},
		{`[[1, 2], [2, 3]]`, 3},
		{`[{"a": 1, "b": 2}]`, 2},
		{`[{"a": 1, "b": 2}, {"a": 1, "b": 3}]`, 3},
		{`[{"a": [1, 2], "b": 2}, {"a": [1, 3], "b": 3}]`, 5},
	}
	for _, c := range testCases {
		n, err := NumInvertedIndexEntries(parseJSON(t, c.value))
		if err != nil {
			t.Fatal(err)
		}
		if n != c.expCount {
			t.Errorf("unexpected count of index entries for %v. expected %d, got %d",
				c.value, c.expCount, n)
		}
	}
}

const expectError = "<<error>>"

func TestConcat(t *testing.T) {
	cases := map[string][]struct {
		concatWith string
		expected   string
	}{
		`null`: {
			{`null`, `[null, null]`},
			{`[]`, `[null]`},
			{`[1]`, `[null, 1]`},
			{`{}`, expectError},
		},
		`true`: {
			{`null`, `[true, null]`},
			{`[]`, `[true]`},
			{`[1]`, `[true, 1]`},
			{`{}`, expectError},
		},
		`false`: {
			{`null`, `[false, null]`},
			{`[]`, `[false]`},
			{`[1]`, `[false, 1]`},
			{`{}`, expectError},
		},
		`123`: {
			{`null`, `[123, null]`},
			{`[]`, `[123]`},
			{`[1]`, `[123, 1]`},
			{`{}`, expectError},
		},
		`"hello"`: {
			{`null`, `["hello", null]`},
			{`[]`, `["hello"]`},
			{`[1]`, `["hello", 1]`},
			{`{}`, expectError},
		},
		`[1]`: {
			{`null`, `[1, null]`},
			{`1`, `[1, 1]`},
			{`"foo"`, `[1, "foo"]`},
			{`[2]`, `[1, 2]`},
			{`[2, 3]`, `[1, 2, 3]`},
			{`{"a": 1}`, `[1, {"a": 1}]`},
		},
		`[1, 2]`: {
			{`[3]`, `[1, 2, 3]`},
			{`[3, 4]`, `[1, 2, 3, 4]`},
		},
		`{"a": 1}`: {
			{`null`, expectError},
			{`1`, expectError},
			{`"foo"`, expectError},
			{`[2]`, `[{"a": 1}, 2]`},
			{`[2, 3]`, `[{"a": 1}, 2, 3]`},
			{`{"b": 2}`, `{"a": 1, "b": 2}`},
			{`{"a": 2}`, `{"a": 2}`},
		},
		`{"b": 2}`: {
			{`{"a": 1}`, `{"a": 1, "b": 2}`},
		},
		`{"a": 1, "b": 2}`: {
			{`{"b": 3, "c": 4}`, `{"a": 1, "b": 3, "c": 4}`},
		},
	}

	for k, tcs := range cases {
		left := parseJSON(t, k)
		runDecodedAndEncoded(t, k, left, func(t *testing.T, left JSON) {
			for _, tc := range tcs {
				right := parseJSON(t, tc.concatWith)
				runDecodedAndEncoded(t, tc.concatWith, right, func(t *testing.T, right JSON) {
					if tc.expected == expectError {
						result, err := left.Concat(right)
						if err == nil {
							t.Fatalf("expected %s || %s to error, but got %s", left, right, result)
						}
					} else {
						result, err := left.Concat(right)
						if err != nil {
							t.Fatal(err)
						}

						expectedResult := parseJSON(t, tc.expected)

						cmp, err := result.Compare(expectedResult)
						if err != nil {
							t.Fatal(err)
						}

						if cmp != 0 {
							t.Fatalf("expected %v || %v = %v, got %v", left, right, expectedResult, result)
						}
					}
				})
			}
		})
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
			{`{"a": 3}`, false},
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
			{`"hello"`, true},
			{`true`, true},
			{`false`, true},
			{`null`, true},
			{`[1]`, false},
			{`[[1]]`, true},
			{`1`, false},
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

				checkResult := slowContains(left, other)
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
			t.Fatalf("%s should contain %s", j, subdoc)
		}
		if !slowContains(j, subdoc) {
			t.Fatalf("%s should slowContains %s", j, subdoc)
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
		slowResult := slowContains(j1, j2)
		if realResult != slowResult {
			t.Fatal("mismatch for document " + j1.String() + " @> " + j2.String())
		}
	}
}

func TestPretty(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{`true`, `true`},
		{`1`, `1`},
		{`1.0`, `1.0`},
		{`1.00`, `1.00`},
		{`"hello"`, `"hello"`},
		{`["hello"]`,
			`[
    "hello"
]`},
		{`["hello", "world"]`,
			`[
    "hello",
    "world"
]`},
		{`["hello", ["world"]]`,
			`[
    "hello",
    [
        "world"
    ]
]`},
		{`{"a": 1}`,
			`{
    "a": 1
}`},
		{`{"a": 1, "b": 2}`,
			`{
    "a": 1,
    "b": 2
}`},
		{`{"a": 1, "b": {"c": 3}}`,
			`{
    "a": 1,
    "b": {
        "c": 3
    }
}`},
	}

	for _, tc := range cases {
		j := parseJSON(t, tc.input)
		runDecodedAndEncoded(t, tc.input, j, func(t *testing.T, j JSON) {
			pretty, err := Pretty(j)
			if err != nil {
				t.Fatal(err)
			}
			if pretty != tc.expected {
				t.Fatalf("expected:\n%s\ngot:\n%s\n", tc.expected, pretty)
			}
		})
	}
}

func TestHasContainerLeaf(t *testing.T) {
	cases := []struct {
		input    string
		expected bool
	}{
		{`true`, false},
		{`false`, false},
		{`1`, false},
		{`[]`, true},
		{`{}`, true},
		{`{"a": 1}`, false},
		{`{"a": {"b": 3}, "x": "y", "c": []}`, true},
		{`{"a": {"b": 3}, "c": [], "x": "y"}`, true},
		{`{"a": {}}`, true},
		{`{"a": []}`, true},
		{`[]`, true},
		{`[[]]`, true},
		{`[1, 2, 3, []]`, true},
		{`[1, 2, [], 3]`, true},
		{`[[], 1, 2, 3]`, true},
		{`[1, 2, 3]`, false},
		{`[[1, 2, 3]]`, false},
		{`[[1, 2], 3]`, false},
		{`[1, 2, 3, [4]]`, false},
	}

	for _, tc := range cases {
		j := parseJSON(t, tc.input)
		runDecodedAndEncoded(t, tc.input, j, func(t *testing.T, j JSON) {
			result, err := j.HasContainerLeaf()
			if err != nil {
				t.Fatal(err)
			}
			if result != tc.expected {
				t.Fatalf("expected:\n%v\ngot:\n%v\n", tc.expected, result)
			}
		})
	}
}

func BenchmarkBuildJSONObject(b *testing.B) {
	for _, objectSize := range []int{1, 10, 100, 1000, 10000, 100000} {
		keys := make([]string, objectSize)
		for i := 0; i < objectSize; i++ {
			keys[i] = fmt.Sprintf("key%d", i)
		}
		for i := 0; i < objectSize; i++ {
			p := rand.Intn(objectSize-i) + i
			keys[i], keys[p] = keys[p], keys[i]
		}
		b.Run(fmt.Sprintf("object size %d", objectSize), func(b *testing.B) {
			b.Run("from builder", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					builder := NewObjectBuilder(0)
					for i, k := range keys {
						builder.Add(k, FromInt(i))
					}
					_ = builder.Build()
				}
			})

			b.Run("from go map", func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					m := map[string]interface{}{}
					for i, k := range keys {
						m[k] = FromInt(i)
					}
					if _, err := fromMap(m); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkArrayConcat(b *testing.B) {
	for _, arraySize := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("concat two arrays with size %d", arraySize), func(b *testing.B) {
			arr := make([]interface{}, arraySize)
			for i := 0; i < arraySize; i++ {
				arr[i] = i
			}
			left, err := MakeJSON(arr)
			if err != nil {
				b.Fatal(err)
			}
			right, err := MakeJSON(arr)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err = left.Concat(right)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkObjectStripNulls(b *testing.B) {
	for _, objectSize := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("object size %d no need to strip", objectSize), func(b *testing.B) {
			builder := NewObjectBuilder(0)
			for i := 0; i < objectSize; i++ {
				builder.Add(strconv.Itoa(i), FromInt(i))
			}
			obj := builder.Build()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := obj.StripNulls()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("object size %d need to strip", objectSize), func(b *testing.B) {
			builder := NewObjectBuilder(0)
			builder.Add(strconv.Itoa(0), NullJSONValue)
			for i := 1; i < objectSize; i++ {
				builder.Add(strconv.Itoa(i), FromInt(i))
			}
			obj := builder.Build()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := obj.StripNulls()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkFetchFromArray(b *testing.B) {
	for _, arraySize := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("array size %d", arraySize), func(b *testing.B) {
			ary := make([]interface{}, arraySize)
			for i := 0; i < len(ary); i++ {
				ary[i] = i
			}
			j, err := MakeJSON(ary)
			if err != nil {
				b.Fatal(err)
			}

			_, encoding, err := j.encode(nil)
			if err != nil {
				b.Fatal(err)
			}

			encoded, err := newEncodedFromRoot(encoding)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = encoded.FetchValIdx(rand.Intn(arraySize))
			}
		})
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

			b.Run("fetch key", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, _ = j.FetchValKey(keys[rand.Intn(len(keys))])
				}
			})

			encoding, err := EncodeJSON(nil, j)
			if err != nil {
				b.Fatal(err)
			}

			encoded, err := newEncodedFromRoot(encoding)
			if err != nil {
				b.Fatal(err)
			}

			b.Run("fetch key by decoding and then reading", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					// This is a hack to get around the decoded caching.
					encoded.mu.cachedDecoded = nil

					decoded, err := encoded.shallowDecode()
					if err != nil {
						b.Fatal(err)
					}
					_, _ = decoded.FetchValKey(keys[rand.Intn(len(keys))])
				}
			})

			b.Run("fetch key encoded", func(b *testing.B) {
				encoded, err := newEncodedFromRoot(encoding)
				if err != nil {
					b.Fatal(err)
				}
				for i := 0; i < b.N; i++ {
					_, _ = encoded.FetchValKey(keys[rand.Intn(len(keys))])
				}
			})
		})
	}
}

func BenchmarkJSONNumInvertedIndexEntries(b *testing.B) {
	j := parseJSON(b, sampleJSON)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NumInvertedIndexEntries(j)
	}
}

func TestJSONRemovePath(t *testing.T) {
	queryTests := map[string][]struct {
		path     []string
		ok       bool
		expected string
		errMsg   string
	}{
		`{"foo": 1}`: {
			{path: []string{"foa"}, ok: false, expected: `{"foo": 1}`},
			{path: []string{"foo"}, ok: true, expected: `{}`},
			{path: []string{"foz"}, ok: false, expected: `{"foo": 1}`},
			{path: []string{}, ok: false, expected: `{"foo": 1}`},
			{path: []string{"bar"}, ok: false, expected: `{"foo": 1}`},
		},
		`{"foo": {"bar": 1, "baz": 2}}`: {
			{path: []string{}, ok: false, expected: `{"foo": {"bar": 1, "baz": 2}}`},
			{path: []string{"foo"}, ok: true, expected: `{}`},
			{path: []string{"foo", "bar"}, ok: true, expected: `{"foo": {"baz": 2}}`},
			{path: []string{"foo", "bar", "gup"}, ok: false, expected: `{"foo": {"bar": 1, "baz": 2}}`},
			{path: []string{"foo", "baz"}, ok: true, expected: `{"foo": {"bar": 1}}`},
		},
		`{"foo": {"bar": 1, "baz": {"gup": 3}}}`: {
			{path: []string{}, ok: false, expected: `{"foo": {"bar": 1, "baz": {"gup": 3}}}`},
			{path: []string{"foo"}, ok: true, expected: `{}`},
			{path: []string{"foo", "bar"}, ok: true, expected: `{"foo": {"baz": {"gup": 3}}}`},
			{path: []string{"foo", "baz"}, ok: true, expected: `{"foo": {"bar": 1}}`},
			{path: []string{"foo", "baz", "gup"}, ok: true, expected: `{"foo": {"bar": 1, "baz": {}}}`},
		},
		`{"foo": [1, 2, {"bar": 3}]}`: {
			{path: []string{}, ok: false, expected: `{"foo": [1, 2, {"bar": 3}]}`},
			{path: []string{`foo`}, ok: true, expected: `{}`},
			{path: []string{`foo`, `0`}, ok: true, expected: `{"foo": [2, {"bar": 3}]}`},
			{path: []string{`foo`, `1`}, ok: true, expected: `{"foo": [1, {"bar": 3}]}`},
			{path: []string{`foo`, `2`}, ok: true, expected: `{"foo": [1, 2]}`},
			{path: []string{`foo`, `2`, `bar`}, ok: true, expected: `{"foo": [1, 2, {}]}`},
			{path: []string{`foo`, `-3`}, ok: true, expected: `{"foo": [2, {"bar": 3}]}`},
			{path: []string{`foo`, `-2`}, ok: true, expected: `{"foo": [1, {"bar": 3}]}`},
			{path: []string{`foo`, `-1`}, ok: true, expected: `{"foo": [1, 2]}`},
			{path: []string{`foo`, `-1`, `bar`}, ok: true, expected: `{"foo": [1, 2, {}]}`},
		},
		`[[1]]`: {
			{path: []string{"0", "0"}, ok: true, expected: `[[]]`},
		},
		`[1]`: {
			{path: []string{"foo"}, ok: false, expected: "", errMsg: "a path element is not an integer: foo"},
			{path: []string{""}, ok: false, expected: "", errMsg: "a path element is not an integer: "},
			{path: []string{"0"}, ok: true, expected: `[]`},
			{path: []string{"0", "0"}, ok: false, expected: `[1]`},
		},
		`1`: {
			{path: []string{"foo"}, errMsg: "cannot delete path in scalar"},
		},
	}

	for k, tests := range queryTests {
		left, err := ParseJSON(k)
		if err != nil {
			t.Fatal(err)
		}

		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s #- %v", k, tc.path), func(t *testing.T) {
				result, ok, err := left.RemovePath(tc.path)
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
				if tc.ok != ok {
					t.Fatalf("expected %t, got %t", tc.ok, ok)
				}
				cmp, err := result.Compare(parseJSON(t, tc.expected))
				if err != nil {
					t.Fatal(err)
				}
				if cmp != 0 {
					t.Fatalf("expected %s, got %s", tc.expected, result.String())
				}
			})
		}
	}
}

func TestToDecimal(t *testing.T) {
	numericCases := []string{
		"1",
		"1.0",
		"3.14",
		"-3.14",
		"1.000",
		"-0.0",
		"-0.09",
		"0.08",
	}

	nonNumericCases := []string{
		"\"1\"",
		"{}",
		"[]",
		"true",
		"false",
		"null",
	}

	for _, tc := range numericCases {
		t.Run(fmt.Sprintf("numeric - %s", tc), func(t *testing.T) {
			dec1, _, err := apd.NewFromString(tc)
			if err != nil {
				t.Fatal(err)
			}

			json, err := ParseJSON(tc)
			if err != nil {
				t.Fatal(err)
			}

			dec2, ok := json.AsDecimal()
			if !ok {
				t.Fatalf("could not cast %v to decmial", json)
			}

			if dec1.Cmp(dec2) != 0 {
				t.Fatalf("expected %s == %s", dec1.String(), dec2.String())
			}
		})
	}

	for _, tc := range nonNumericCases {
		t.Run(fmt.Sprintf("nonNumeric - %s", tc), func(t *testing.T) {
			json, err := ParseJSON(tc)
			if err != nil {
				t.Fatalf("expected no error")
			}

			dec, ok := json.AsDecimal()
			if dec != nil || ok {
				t.Fatalf("%v should not be a valid decimal", json)
			}
		})
	}
}

func TestAllPaths(t *testing.T) {
	cases := []struct {
		json     string
		expected []string
	}{
		{`{}`, []string{`{}`}},
		{`[]`, []string{`[]`}},
		{`[1, 2, 3]`, []string{`[1]`, `[2]`, `[3]`}},
		{`{"foo": {"bar": [1, 2, 3]}}`, []string{`{"foo": {"bar": [1]}}`, `{"foo": {"bar": [2]}}`, `{"foo": {"bar": [3]}}`}},
		{
			`{"a": [1, 2, true, false], "b": {"ba": 0, "bb": null}, "c": {}}`,
			[]string{`{"a": [1]}`, `{"a": [2]}`, `{"a": [true]}`, `{"a": [false]}`, `{"b": {"ba": 0}}`, `{"b": {"bb": null}}`, `{"c": {}}`},
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("all paths - %d", i), func(t *testing.T) {
			j := parseJSON(t, tc.json)
			paths, err := AllPaths(j)
			if err != nil {
				t.Fatal(err)
			}
			if len(paths) != len(tc.expected) {
				t.Fatalf("expected %d paths, got %d", len(tc.expected), len(paths))
			}
			for k, p := range paths {
				cmp, err := p.Compare(parseJSON(t, tc.expected[k]))
				if err != nil {
					t.Fatal(err)
				}
				if cmp != 0 {
					t.Fatalf("expected %s, got %s", tc.expected[k], p.String())
				}
			}
		})
	}
}

func TestAllPathsWithDepth(t *testing.T) {
	cases := []struct {
		json            string
		depthToExpected map[int][]string
	}{
		{`{}`, map[int][]string{
			-1: {`{}`},
			0:  {`{}`},
			1:  {`{}`},
			2:  {`{}`},
		}},
		{`[]`, map[int][]string{
			-1: {`[]`},
			0:  {`[]`},
			1:  {`[]`},
			2:  {`[]`},
		}},
		{`[1, 2, 3]`, map[int][]string{
			-1: {`[1]`, `[2]`, `[3]`},
			0:  {`[1, 2, 3]`},
			1:  {`[1]`, `[2]`, `[3]`},
			2:  {`[1]`, `[2]`, `[3]`},
		}},
		{`{"foo": {"bar": [1, 2, 3]}}`, map[int][]string{
			-1: {`{"foo": {"bar": [1]}}`, `{"foo": {"bar": [2]}}`, `{"foo": {"bar": [3]}}`},
			0:  {`{"foo": {"bar": [1, 2, 3]}}`},
			1:  {`{"foo": {"bar": [1, 2, 3]}}`},
			2:  {`{"foo": {"bar": [1, 2, 3]}}`},
			3:  {`{"foo": {"bar": [1]}}`, `{"foo": {"bar": [2]}}`, `{"foo": {"bar": [3]}}`},
		}},
		{`{"a": [1, 2, true, false], "b": {"ba": 0, "bb": null}, "c": {}}`, map[int][]string{
			-1: {`{"a": [1]}`, `{"a": [2]}`, `{"a": [true]}`, `{"a": [false]}`, `{"b": {"ba": 0}}`, `{"b": {"bb": null}}`, `{"c": {}}`},
			0:  {`{"a": [1, 2, true, false], "b": {"ba": 0, "bb": null}, "c": {}}`},
			1:  {`{"a": [1, 2, true, false]}`, `{"b": {"ba": 0, "bb": null}}`, `{"c": {}}`},
			2:  {`{"a": [1]}`, `{"a": [2]}`, `{"a": [true]}`, `{"a": [false]}`, `{"b": {"ba": 0}}`, `{"b": {"bb": null}}`, `{"c": {}}`},
		}},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("all paths with depth - %d", i), func(t *testing.T) {
			j := parseJSON(t, tc.json)
			for depth, expected := range tc.depthToExpected {
				paths, err := AllPathsWithDepth(j, depth)
				if err != nil {
					t.Fatal(err)
				}
				if len(paths) != len(expected) {
					t.Fatalf("expected %d paths, got %d", len(expected), len(paths))
				}
				for k, p := range paths {
					cmp, err := p.Compare(parseJSON(t, expected[k]))
					if err != nil {
						t.Fatal(err)
					}
					if cmp != 0 {
						t.Fatalf("expected %s, got %s", expected[k], p.String())
					}
				}
			}
		})
	}
}
