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

package tree

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
)

func TestParseArray(t *testing.T) {
	testData := []struct {
		str      string
		typ      coltypes.T
		expected Datums
	}{
		{`{}`, coltypes.Int, Datums{}},
		{`{1}`, coltypes.Int, Datums{NewDInt(1)}},
		{`{1,2}`, coltypes.Int, Datums{NewDInt(1), NewDInt(2)}},
		{`   { 1    ,  2  }  `, coltypes.Int, Datums{NewDInt(1), NewDInt(2)}},
		{`   { 1    ,
			"2"  }  `, coltypes.Int, Datums{NewDInt(1), NewDInt(2)}},
		{`{1,2,3}`, coltypes.Int, Datums{NewDInt(1), NewDInt(2), NewDInt(3)}},
		{`{"1"}`, coltypes.Int, Datums{NewDInt(1)}},
		{` { "1" , "2"}`, coltypes.Int, Datums{NewDInt(1), NewDInt(2)}},
		{` { "1" , 2}`, coltypes.Int, Datums{NewDInt(1), NewDInt(2)}},
		{`{1,NULL}`, coltypes.Int, Datums{NewDInt(1), DNull}},

		{`{hello}`, coltypes.String, Datums{NewDString(`hello`)}},
		{`{hel
lo}`, coltypes.String, Datums{NewDString(`hel
lo`)}},
		{`{hel,
lo}`, coltypes.String, Datums{NewDString(`hel`), NewDString(`lo`)}},
		{`{hel,lo}`, coltypes.String, Datums{NewDString(`hel`), NewDString(`lo`)}},
		{`{  he llo  }`, coltypes.String, Datums{NewDString(`he llo`)}},
		{"{hello,\u1680world}", coltypes.String, Datums{NewDString(`hello`), NewDString(`world`)}},
		{`{hell\\o}`, coltypes.String, Datums{NewDString(`hell\o`)}},
		{`{"hell\\o"}`, coltypes.String, Datums{NewDString(`hell\o`)}},
		{`{NULL,"NULL"}`, coltypes.String, Datums{DNull, NewDString(`NULL`)}},
		{`{"hello"}`, coltypes.String, Datums{NewDString(`hello`)}},
		{`{" hello "}`, coltypes.String, Datums{NewDString(` hello `)}},
		{`{"hel,lo"}`, coltypes.String, Datums{NewDString(`hel,lo`)}},
		{`{"hel\"lo"}`, coltypes.String, Datums{NewDString(`hel"lo`)}},
		{`{"h\"el\"lo"}`, coltypes.String, Datums{NewDString(`h"el"lo`)}},
		{`{"\"hello"}`, coltypes.String, Datums{NewDString(`"hello`)}},
		{`{"hello\""}`, coltypes.String, Datums{NewDString(`hello"`)}},
		{`{"hel\nlo"}`, coltypes.String, Datums{NewDString(`helnlo`)}},
		{`{"hel\\lo"}`, coltypes.String, Datums{NewDString(`hel\lo`)}},
		{`{"hel\\\"lo"}`, coltypes.String, Datums{NewDString(`hel\"lo`)}},
		{`{"hel\\\\lo"}`, coltypes.String, Datums{NewDString(`hel\\lo`)}},
		{`{"hel\\\\\\lo"}`, coltypes.String, Datums{NewDString(`hel\\\lo`)}},
		{`{"\\"}`, coltypes.String, Datums{NewDString(`\`)}},
		{`{"\\\\"}`, coltypes.String, Datums{NewDString(`\\`)}},
		{`{"\\\\\\"}`, coltypes.String, Datums{NewDString(`\\\`)}},
		{`{"he\,l\}l\{o"}`, coltypes.String, Datums{NewDString(`he,l}l{o`)}},
		// There is no way to input non-printing characters (without having used an escape string previously).
		{`{\\x07}`, coltypes.String, Datums{NewDString(`\x07`)}},
		{`{\x07}`, coltypes.String, Datums{NewDString(`x07`)}},

		{`{日本語}`, coltypes.String, Datums{NewDString(`日本語`)}},

		// This can generate some strings with invalid UTF-8, but this isn't a
		// problem, since the input would have had to be invalid UTF-8 for that to
		// occur.
		{string([]byte{'{', 'a', 200, '}'}), coltypes.String, Datums{NewDString("a\xc8")}},
		{string([]byte{'{', 'a', 200, 'a', '}'}), coltypes.String, Datums{NewDString("a\xc8a")}},
	}
	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			expected := NewDArray(coltypes.CastTargetToDatumType(td.typ))
			for _, d := range td.expected {
				if err := expected.Append(d); err != nil {
					t.Fatal(err)
				}
			}
			evalContext := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			actual, err := ParseDArrayFromString(evalContext, td.str, td.typ)
			if err != nil {
				t.Fatalf("ARRAY %s: got error %s, expected %s", td.str, err.Error(), expected)
			}
			if actual.Compare(evalContext, expected) != 0 {
				t.Fatalf("ARRAY %s: got %s, expected %s", td.str, actual, expected)
			}
		})
	}
}

const randomArrayIterations = 1000
const randomArrayMaxLength = 10
const randomStringMaxLength = 1000

func TestParseArrayRandomParseArray(t *testing.T) {
	for i := 0; i < randomArrayIterations; i++ {
		numElems := rand.Intn(randomArrayMaxLength)
		ary := make([][]byte, numElems)
		for aryIdx := range ary {
			len := rand.Intn(randomStringMaxLength)
			str := make([]byte, len)
			for strIdx := 0; strIdx < len; strIdx++ {
				str[strIdx] = byte(rand.Intn(256))
			}
			ary[aryIdx] = str
		}

		var buf bytes.Buffer
		buf.WriteByte('{')
		for j, b := range ary {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteByte('"')
			// The input format for this doesn't support regular escaping, any
			// character can be preceded by a backslash to encode it directly (this
			// means that there's no printable way to encode non-printing characters,
			// users must use `e` prefixed strings).
			for _, c := range b {
				if c == '"' || c == '\\' || rand.Intn(10) == 0 {
					buf.WriteByte('\\')
				}
				buf.WriteByte(c)
			}
			buf.WriteByte('"')
		}
		buf.WriteByte('}')

		parsed, err := ParseDArrayFromString(
			NewTestingEvalContext(cluster.MakeTestingClusterSettings()), buf.String(), coltypes.String)
		if err != nil {
			t.Fatalf(`got error: "%s" for elem "%s"`, err, buf.String())
		}
		for aryIdx := range ary {
			value := MustBeDString(parsed.Array[aryIdx])
			if string(value) != string(ary[aryIdx]) {
				t.Fatalf(`iteration %d: array "%s", got %#v, expected %#v`, i, buf.String(), value, string(ary[aryIdx]))
			}
		}
	}
}

func TestParseArrayError(t *testing.T) {
	testData := []struct {
		str           string
		typ           coltypes.T
		expectedError string
	}{
		{``, coltypes.Int, "array must be enclosed in { and }"},
		{`1`, coltypes.Int, "array must be enclosed in { and }"},
		{`1,2`, coltypes.Int, "array must be enclosed in { and }"},
		{`{1,2`, coltypes.Int, "malformed array"},
		{`{1,2,`, coltypes.Int, "malformed array"},
		{`{`, coltypes.Int, "malformed array"},
		{`{,}`, coltypes.Int, "malformed array"},
		{`{}{}`, coltypes.Int, "extra text after closing right brace"},
		{`{} {}`, coltypes.Int, "extra text after closing right brace"},
		{`{{}}`, coltypes.Int, "nested arrays not supported"},
		{`{1, {1}}`, coltypes.Int, "nested arrays not supported"},
		{`{hello}`, coltypes.Int, `could not parse "hello" as type int: strconv.ParseInt: parsing "hello": invalid syntax`},
		{`{"hello}`, coltypes.String, `malformed array`},
		// It might be unnecessary to disallow this, but Postgres does.
		{`{he"lo}`, coltypes.String, "malformed array"},

		{string([]byte{200}), coltypes.String, "array must be enclosed in { and }"},
		{string([]byte{'{', 'a', 200}), coltypes.String, "malformed array"},
	}
	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			_, err := ParseDArrayFromString(
				NewTestingEvalContext(cluster.MakeTestingClusterSettings()), td.str, td.typ)
			if err == nil {
				t.Fatalf("expected %#v to error with message %#v", td.str, td.expectedError)
			}
			if err.Error() != td.expectedError {
				t.Fatalf("ARRAY %s: got error %s, expected error %s", td.str, err.Error(), td.expectedError)
			}
		})
	}
}
