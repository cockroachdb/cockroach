// Copyright 2016 The Cockroach Authors.
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

package builtins

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestCategory(t *testing.T) {
	if expected, actual := categoryString, Builtins["lower"][0].Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryString, Builtins["length"][0].Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryDateAndTime, Builtins["now"][0].Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categorySystemInfo, Builtins["version"][0].Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
}

// TestGenerateUniqueIDOrder verifies the expected ordering of
// GenerateUniqueID.
func TestGenerateUniqueIDOrder(t *testing.T) {
	tests := []tree.DInt{
		GenerateUniqueID(0, 0),
		GenerateUniqueID(1, 0),
		GenerateUniqueID(2<<15, 0),
		GenerateUniqueID(0, 1),
		GenerateUniqueID(0, 10000),
		GenerateUniqueInt(0),
	}
	prev := tests[0]
	for _, tc := range tests[1:] {
		if tc <= prev {
			t.Fatalf("%d > %d", tc, prev)
		}
	}
}

func TestStringToArray(t *testing.T) {
	// s allows us to have a string pointer literal.
	s := func(x string) *string { return &x }
	cases := []struct {
		input    string
		sep      *string
		nullStr  *string
		expected []*string
	}{
		{`abcxdef`, s(`x`), nil, []*string{s(`abc`), s(`def`)}},
		{`xxx`, s(`x`), nil, []*string{s(``), s(``), s(``), s(``)}},
		{`xxx`, s(`xx`), nil, []*string{s(``), s(`x`)}},
		{`abcxdef`, s(``), nil, []*string{s(`abcxdef`)}},
		{`abcxdef`, s(`abcxdef`), nil, []*string{s(``), s(``)}},
		{`abcxdef`, s(`x`), s(`abc`), []*string{nil, s(`def`)}},
		{`abcxdef`, s(`x`), s(`x`), []*string{s(`abc`), s(`def`)}},
		{`abcxdef`, s(`x`), s(``), []*string{s(`abc`), s(`def`)}},
		{``, s(`x`), s(``), []*string{}},
		{``, s(``), s(``), []*string{}},
		{``, s(`x`), nil, []*string{}},
		{``, s(``), nil, []*string{}},
		{`abcxdef`, nil, nil, []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(`abc`), []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(`x`), []*string{s(`a`), s(`b`), s(`c`), nil, s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(``), []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{``, nil, s(``), []*string{}},
		{``, nil, nil, []*string{}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("string_to_array(%q, %q)", tc.input, tc.sep), func(t *testing.T) {
			result, err := stringToArray(tc.input, tc.sep, tc.nullStr)
			if err != nil {
				t.Fatal(err)
			}

			expectedArray := tree.NewDArray(types.String)
			for _, s := range tc.expected {
				datum := tree.DNull
				if s != nil {
					datum = tree.NewDString(*s)
				}
				if err := expectedArray.Append(datum); err != nil {
					t.Fatal(err)
				}
			}

			evalContext := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			if result.Compare(evalContext, expectedArray) != 0 {
				t.Fatalf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestEscapeFormat(t *testing.T) {
	testCases := []struct {
		bytes []byte
		str   string
	}{
		{[]byte{}, ``},
		{[]byte{'a', 'b', 'c'}, `abc`},
		{[]byte{'a', 'b', 'c', 'd'}, `abcd`},
		{[]byte{'a', 'b', 0, 'd'}, `ab\000d`},
		{[]byte{'a', 'b', 0, 0, 'd'}, `ab\000\000d`},
		{[]byte{'a', 'b', 0, 'a', 'b', 'c', 0, 'd'}, `ab\000abc\000d`},
		{[]byte{'a', 'b', 0, 0}, `ab\000\000`},
		{[]byte{'a', 'b', '\\', 'd'}, `ab\\d`},
		{[]byte{'a', 'b', 200, 'd'}, `ab\310d`},
		{[]byte{'a', 'b', 7, 'd'}, "ab\x07d"},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			result := encodeEscape(tc.bytes)
			if result != tc.str {
				t.Fatalf("expected %q, got %q", tc.str, result)
			}

			decodedResult, err := decodeEscape(tc.str)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decodedResult, tc.bytes) {
				t.Fatalf("expected %q, got %#v", tc.bytes, decodedResult)
			}
		})
	}
}

func TestEscapeFormatRandom(t *testing.T) {
	for i := 0; i < 1000; i++ {
		b := make([]byte, rand.Intn(100))
		for j := 0; j < len(b); j++ {
			b[j] = byte(rand.Intn(256))
		}
		str := encodeEscape(b)
		decodedResult, err := decodeEscape(str)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decodedResult, b) {
			t.Fatalf("generated %#v, after round-tripping got %#v", b, decodedResult)
		}
	}
}
