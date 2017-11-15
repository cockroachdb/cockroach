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
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func assertEncodeRoundTrip(t *testing.T, j JSON) {
	encoded, err := EncodeJSON(nil, j)
	if err != nil {
		t.Fatal(err)
	}
	_, decoded, err := DecodeJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if j.Compare(decoded) != 0 {
		t.Fatalf("expected %s, got %s (encoding %v)", j, decoded, encoded)
	}
}

func TestJSONRandomEncodeRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	for i := 0; i < 1000; i++ {
		j, err := Random(20, rng)
		if err != nil {
			t.Fatal(err)
		}

		assertEncodeRoundTrip(t, j)
	}
}

func TestFilesEncodeRoundTrip(t *testing.T) {
	_, fname, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("couldn't get directory")
	}
	dir := filepath.Join(filepath.Dir(fname), "/testdata")
	dirContents, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range dirContents {
		t.Run(tc.Name(), func(t *testing.T) {
			if !strings.HasSuffix(tc.Name(), ".json") {
				return
			}
			path := filepath.Join(dir, tc.Name())
			contents, err := ioutil.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			tc := string(contents)

			j, err := ParseJSON(tc)
			if err != nil {
				t.Fatal(err)
			}

			encoded, err := EncodeJSON(nil, j)
			if err != nil {
				t.Fatal(err)
			}

			_, decoded, err := DecodeJSON(encoded)
			if err != nil {
				t.Fatal(err)
			}

			// We also check with these that they re-stringify properly.
			newStr := decoded.String()
			if newStr != j.String() {
				t.Fatalf("expected %s, got %s", tc, newStr)
			}
		})
	}
}

func TestJSONEncodeRoundTrip(t *testing.T) {
	cases := []string{
		`true`,
		`false`,
		`null`,
		`""`,
		`"🤔"`,
		`"\""`,
		`"\n"`,
		`"hello"`,
		`"goodbye"`,
		`1`,
		`1.00`,
		`100`,
		`-1`,
		`1000000000000000`,
		`100000000000000000000000000000000000`,
		`[]`,
		`["hello"]`,
		`[1]`,
		`[1, 2]`,
		`[1, 2, 3]`,
		`[100000000000000000000000000000000000]`,
		`[1, true, "three"]`,
		`[[1]]`,
		`[[1], [1], [[1]]]`,
		`[[[["hello"]]]]`,
		`{}`,
		`{"a": 1, "b": 2}`,
		`{"b": 1, "a": 2}`,
		`{"a": [1, 2, 3]}`,
		`{"a": [{"b": 2}, {"b": 4}]}`,
		`[1, {"a": 3}, null, {"b": null}]`,
		`{"🤔": "foo"}`,
		`"6U閆崬밺뀫颒myj츥휘:$薈mY햚#rz飏+玭V㭢뾿愴YꖚX亥ᮉ푊\u0006垡㐭룝\"厓ᔧḅ^Sqpv媫\"⤽걒\"˽Ἆ?ꇆ䬔未tv{DV鯀Tἆl凸g\\㈭ĭ즿UH㽤"`,
	}

	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			j, err := ParseJSON(tc)
			if err != nil {
				t.Fatal(err)
			}

			assertEncodeRoundTrip(t, j)
		})
	}
}

// This tests that the stringified version is the same, for testing precision
// is maintained for numbers.  This will not maintain, for example, the
// ordering of object keys.
func TestJSONEncodeStrictRoundTrip(t *testing.T) {
	cases := []string{
		`1`,
		`1.00`,
		`100`,
		`-1`,
		`-1.000000000`,
		`-1000000000`,
		`1.1231231230`,
		`1.1231231230000`,
		`1.1231231230000000`,
	}

	for _, tc := range cases {
		t.Run(tc, func(t *testing.T) {
			j, err := ParseJSON(tc)
			if err != nil {
				t.Fatal(err)
			}

			encoded, err := EncodeJSON(nil, j)
			if err != nil {
				t.Fatal(err)
			}
			_, decoded, err := DecodeJSON(encoded)
			if err != nil {
				t.Fatal(err)
			}

			newStr := decoded.String()
			if newStr != tc {
				t.Fatalf("expected %s, got %s", tc, newStr)
			}
		})
	}
}

func TestJSONEncodeNonRoundTrip(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		// Due to the encoding used by the DECIMAL encoder, these values do not round trip perfectly.
		{`0e+1`, `0`},
		{`0e1`, `0`},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			j, err := ParseJSON(tc.input)
			if err != nil {
				t.Fatal(err)
			}

			encoded, err := EncodeJSON(nil, j)
			if err != nil {
				t.Fatal(err)
			}
			_, decoded, err := DecodeJSON(encoded)
			if err != nil {
				t.Fatal(err)
			}

			newStr := decoded.String()
			if newStr != tc.expected {
				t.Fatalf("expected %s, got %s", tc.expected, newStr)
			}
		})
	}
}

// Taken from Wikipedia's JSON page.
const sampleJSON = `{
  "firstName": "John",
  "lastName": "Smith",
  "isAlive": true,
  "age": 25,
  "address": {
    "streetAddress": "21 2nd Street",
    "city": "New York",
    "state": "NY",
    "postalCode": "10021-3100"
  },
  "phoneNumbers": [
    {
      "type": "home",
      "number": "212 555-1234"
    },
    {
      "type": "office",
      "number": "646 555-4567"
    },
    {
      "type": "mobile",
      "number": "123 456-7890"
    }
  ],
  "children": [],
  "spouse": null
}`

func BenchmarkEncodeJSON(b *testing.B) {
	j := jsonTestShorthand(sampleJSON)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = EncodeJSON(nil, j)
	}
}

func BenchmarkDecodeJSON(b *testing.B) {
	j := jsonTestShorthand(sampleJSON)

	b.ResetTimer()
	bytes, err := EncodeJSON(nil, j)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeJSON(bytes)
	}
}
