// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package json

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var rewriteResultsInTestfiles = flag.Bool(
	"rewrite-results-in-testfiles", false,
	"ignore the expected results and rewrite the test files with the actual results from this "+
		"run. Used to update tests when a change affects many cases; please verify the testfile "+
		"diffs carefully!",
)

func assertEncodeRoundTrip(t *testing.T, j JSON) {
	beforeStr := j.String()
	encoding, err := EncodeJSON(nil, j)
	if err != nil {
		t.Fatal(j, err)
	}
	encoded, err := newEncodedFromRoot(encoding)
	if err != nil {
		t.Fatal(j, err)
	}
	encodedStr := encoded.String()
	_, decoded, err := DecodeJSON(encoding)
	if err != nil {
		t.Fatal(j, err)
	}
	afterStr := decoded.String()

	c, err := j.Compare(decoded)
	if err != nil {
		t.Fatal(j, err)
	}
	if c != 0 {
		t.Fatalf("expected %s, got %s (encoding %v)", j, decoded, encoding)
	}
	if beforeStr != encodedStr {
		t.Fatalf("expected %s, got %s (encoding %v)", beforeStr, encodedStr, encoding)
	}
	if beforeStr != afterStr {
		t.Fatalf("expected %s, got %s (encoding %v)", beforeStr, afterStr, encoding)
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

func TestFilesEncode(t *testing.T) {
	dir := datapathutils.TestDataPath(t, "raw")
	dirContents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	numFilesRan := 0

	for _, tc := range dirContents {
		if !strings.HasSuffix(tc.Name(), ".json") {
			continue
		}
		t.Run(tc.Name(), func(t *testing.T) {
			numFilesRan++
			path := filepath.Join(dir, tc.Name())
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			jsonString := string(contents)

			j, err := ParseJSON(jsonString)
			if err != nil {
				t.Fatal(err)
			}

			encoded, err := EncodeJSON(nil, j)
			if err != nil {
				t.Fatal(err)
			}

			t.Run(`round trip`, func(t *testing.T) {
				_, decoded, err := DecodeJSON(encoded)
				if err != nil {
					t.Fatal(err)
				}

				newStr := decoded.String()
				if newStr != j.String() {
					t.Fatalf("expected %s, got %s", jsonString, newStr)
				}
			})

			// If this test is failing because you changed the encoding of JSON values,
			// rerun with -rewrite-results-in-testfiles.
			t.Run(`explicit encoding`, func(t *testing.T) {
				stringifiedEncoding := fmt.Sprintf("%v", encoded)
				fixtureFilename := datapathutils.TestDataPath(
					t, "encoded", tc.Name()+".bytes")

				if *rewriteResultsInTestfiles {
					err := os.WriteFile(fixtureFilename, []byte(stringifiedEncoding), 0644)
					if err != nil {
						t.Fatal(err)
					}
				}

				expected, err := os.ReadFile(fixtureFilename)
				if err != nil {
					t.Fatal(err)
				}

				if string(expected) != stringifiedEncoding {
					t.Fatalf("expected %s, got %s", string(expected), stringifiedEncoding)
				}
			})
		})
	}

	// Sanity check.
	if numFilesRan == 0 {
		t.Fatal("didn't find any test files!")
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
		`0e1`,
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
		`{"a":"b","c":"d"}`,
	}

	for _, tc := range cases {
		j, err := ParseJSON(tc)
		if err != nil {
			t.Fatal(err)
		}

		assertEncodeRoundTrip(t, j)
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
		`0E+1`,
	}

	for _, tc := range cases {
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
	j := parseJSON(b, sampleJSON)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = EncodeJSON(nil, j)
	}
}

func BenchmarkDecodeJSON(b *testing.B) {
	j := parseJSON(b, sampleJSON)

	b.ResetTimer()
	bytes, err := EncodeJSON(nil, j)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		_, _, _ = DecodeJSON(bytes)
	}
}

func BenchmarkFormatJSON(b *testing.B) {
	j := parseJSON(b, sampleJSON)

	b.Run("decoded", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = j.String()
		}
	})

	b.Run("encoded", func(b *testing.B) {
		encoding, err := EncodeJSON(nil, j)
		if err != nil {
			b.Fatal(err)
		}
		encoded, err := newEncodedFromRoot(encoding)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = encoded.String()
		}
	})
}
