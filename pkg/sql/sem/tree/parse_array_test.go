// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var tupleOfTwoInts = types.MakeTuple([]*types.T{types.Int, types.Int})
var tupleOfStringAndInt = types.MakeTuple([]*types.T{types.String, types.Int})

func TestParseArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		str      string
		typ      *types.T
		expected Datums
	}{
		{`{}`, types.Int, Datums{}},
		{`{1}`, types.Int, Datums{NewDInt(1)}},
		{`{1,2}`, types.Int, Datums{NewDInt(1), NewDInt(2)}},
		{`   { 1    ,  2  }  `, types.Int, Datums{NewDInt(1), NewDInt(2)}},
		{`   { 1    ,
			"2"  }  `, types.Int, Datums{NewDInt(1), NewDInt(2)}},
		{`{1,2,3}`, types.Int, Datums{NewDInt(1), NewDInt(2), NewDInt(3)}},
		{`{"1"}`, types.Int, Datums{NewDInt(1)}},
		{` { "1" , "2"}`, types.Int, Datums{NewDInt(1), NewDInt(2)}},
		{` { "1" , 2}`, types.Int, Datums{NewDInt(1), NewDInt(2)}},
		{`{1,NULL}`, types.Int, Datums{NewDInt(1), DNull}},

		{`{hello}`, types.String, Datums{NewDString(`hello`)}},
		{`{hel
lo}`, types.String, Datums{NewDString(`hel
lo`)}},
		{`{hel,
lo}`, types.String, Datums{NewDString(`hel`), NewDString(`lo`)}},
		{`{hel,lo}`, types.String, Datums{NewDString(`hel`), NewDString(`lo`)}},
		{`{  he llo  }`, types.String, Datums{NewDString(`he llo`)}},
		{"{hello,\u1680world}", types.String, Datums{NewDString(`hello`), NewDString(`world`)}},
		{`{hell\\o}`, types.String, Datums{NewDString(`hell\o`)}},
		{`{"hell\\o"}`, types.String, Datums{NewDString(`hell\o`)}},
		{`{NULL,"NULL"}`, types.String, Datums{DNull, NewDString(`NULL`)}},
		{`{"hello"}`, types.String, Datums{NewDString(`hello`)}},
		{`{" hello "}`, types.String, Datums{NewDString(` hello `)}},
		{`{"hel,lo"}`, types.String, Datums{NewDString(`hel,lo`)}},
		{`{"hel\"lo"}`, types.String, Datums{NewDString(`hel"lo`)}},
		{`{"h\"el\"lo"}`, types.String, Datums{NewDString(`h"el"lo`)}},
		{`{"\"hello"}`, types.String, Datums{NewDString(`"hello`)}},
		{`{"hello\""}`, types.String, Datums{NewDString(`hello"`)}},
		{`{"hel\nlo"}`, types.String, Datums{NewDString(`helnlo`)}},
		{`{"hel\\lo"}`, types.String, Datums{NewDString(`hel\lo`)}},
		{`{"hel\\\"lo"}`, types.String, Datums{NewDString(`hel\"lo`)}},
		{`{"hel\\\\lo"}`, types.String, Datums{NewDString(`hel\\lo`)}},
		{`{"hel\\\\\\lo"}`, types.String, Datums{NewDString(`hel\\\lo`)}},
		{`{"\\"}`, types.String, Datums{NewDString(`\`)}},
		{`{"\\\\"}`, types.String, Datums{NewDString(`\\`)}},
		{`{"\\\\\\"}`, types.String, Datums{NewDString(`\\\`)}},
		{`{"he\,l\}l\{o"}`, types.String, Datums{NewDString(`he,l}l{o`)}},
		// There is no way to input non-printing characters (without having used an escape string previously).
		{`{\\x07}`, types.String, Datums{NewDString(`\x07`)}},
		{`{\x07}`, types.String, Datums{NewDString(`x07`)}},

		{`{日本語}`, types.String, Datums{NewDString(`日本語`)}},

		// byte(133) and byte(160) are treated as whitespace to ignore by unicode,
		// but Postgres handles them as normal characters.
		{string([]byte{'{', 133, '}'}), types.String, Datums{NewDString("\x85")}},
		{string([]byte{'{', 160, '}'}), types.String, Datums{NewDString("\xa0")}},

		// This can generate some strings with invalid UTF-8, but this isn't a
		// problem, since the input would have had to be invalid UTF-8 for that to
		// occur.
		{string([]byte{'{', 'a', 200, '}'}), types.String, Datums{NewDString("a\xc8")}},
		{string([]byte{'{', 'a', 200, 'a', '}'}), types.String, Datums{NewDString("a\xc8a")}},

		// Arrays of tuples can also be parsed from string literals.
		{
			`{"(3,4)"}`,
			tupleOfTwoInts,
			Datums{NewDTuple(tupleOfTwoInts, NewDInt(3), NewDInt(4))},
		},
		{
			`{"(3,4)", null}`,
			tupleOfTwoInts,
			Datums{NewDTuple(tupleOfTwoInts, NewDInt(3), NewDInt(4)), DNull},
		},
		{
			`{"(a12',4)"}`,
			tupleOfStringAndInt,
			Datums{NewDTuple(tupleOfStringAndInt, NewDString("a12'"), NewDInt(4))},
		},
		{
			`{"(cat,4)", "(null,0)"}`,
			tupleOfStringAndInt,
			Datums{
				NewDTuple(tupleOfStringAndInt, NewDString("cat"), NewDInt(4)),
				NewDTuple(tupleOfStringAndInt, NewDString("null"), NewDInt(0)),
			},
		},
		{
			`{"(1,2)", "(3,)"}`,
			tupleOfTwoInts,
			Datums{
				NewDTuple(tupleOfTwoInts, NewDInt(1), NewDInt(2)),
				NewDTuple(tupleOfTwoInts, NewDInt(3), DNull),
			},
		},
	}
	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			expected := NewDArray(td.typ)
			for _, d := range td.expected {
				if err := expected.Append(d); err != nil {
					t.Fatal(err)
				}
			}
			actual, _, err := ParseDArrayFromString(nil /* ParseContext */, td.str, td.typ)
			if err != nil {
				t.Fatalf("ARRAY %s: got error %s, expected %s", td.str, err.Error(), expected)
			}
			if cmp, err := actual.Compare(context.Background(), noopUnwrapCompareContext{}, expected); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatalf("ARRAY %s: got %s, expected %s", td.str, actual, expected)
			}
		})
	}
}

type noopUnwrapCompareContext struct {
	CompareContext
}

func (noopUnwrapCompareContext) UnwrapDatum(ctx context.Context, d Datum) Datum { return d }

const randomArrayIterations = 1000
const randomArrayMaxLength = 10
const randomStringMaxLength = 1000

func TestParseArrayRandomParseArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

		parsed, _, err := ParseDArrayFromString(
			nil /* ParseContext */, buf.String(), types.String,
		)
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
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		str           string
		typ           *types.T
		expectedError string
	}{
		{``, types.Int, `could not parse "" as type int[]: array must be enclosed in { and }`},
		{`1`, types.Int, `could not parse "1" as type int[]: array must be enclosed in { and }`},
		{`1,2`, types.Int, `could not parse "1,2" as type int[]: array must be enclosed in { and }`},
		{`{1,2`, types.Int, `could not parse "{1,2" as type int[]: malformed array`},
		{`{1,2,`, types.Int, `could not parse "{1,2," as type int[]: malformed array`},
		{`{`, types.Int, `could not parse "{" as type int[]: malformed array`},
		{`{,}`, types.Int, `could not parse "{,}" as type int[]: malformed array`},
		{`{}{}`, types.Int, `could not parse "{}{}" as type int[]: extra text after closing right brace`},
		{`{} {}`, types.Int, `could not parse "{} {}" as type int[]: extra text after closing right brace`},
		{`{{}}`, types.Int, `could not parse "{{}}" as type int[]: unimplemented: nested arrays not supported`},
		{`{1, {1}}`, types.Int, `could not parse "{1, {1}}" as type int[]: unimplemented: nested arrays not supported`},
		{`{hello}`, types.Int, `could not parse "{hello}" as type int[]: could not parse "hello" as type int: strconv.ParseInt: parsing "hello": invalid syntax`},
		{`{"hello}`, types.String, `could not parse "{\"hello}" as type string[]: malformed array`},
		// It might be unnecessary to disallow this, but Postgres does.
		{`{he"lo}`, types.String, `could not parse "{he\"lo}" as type string[]: malformed array`},

		{string([]byte{200}), types.String, `could not parse "\xc8" as type string[]: array must be enclosed in { and }`},
		{string([]byte{'{', 'a', 200}), types.String, `could not parse "{a\xc8" as type string[]: malformed array`},

		{`{"(1,2)", "(3,4,5)"}`, tupleOfTwoInts, `could not parse "{\"(1,2)\", \"(3,4,5)\"}" as type tuple{int, int}[]: could not parse "(3,4,5)" as type tuple{int, int}: malformed record literal`},
		{`{"(1,2)", "(3,4,)"}`, tupleOfTwoInts, `could not parse "{\"(1,2)\", \"(3,4,)\"}" as type tuple{int, int}[]: could not parse "(3,4,)" as type tuple{int, int}: malformed record literal`},
		{`{"(1,2)", "(3)"}`, tupleOfTwoInts, `could not parse "{\"(1,2)\", \"(3)\"}" as type tuple{int, int}[]: could not parse "(3)" as type tuple{int, int}: malformed record literal`},
		{`{"(1,2)", "(3,4"}`, tupleOfTwoInts, `could not parse "{\"(1,2)\", \"(3,4\"}" as type tuple{int, int}[]: could not parse "(3,4" as type tuple{int, int}: malformed record literal`},
		{`{"(1,2)", "(3,,4)"}`, tupleOfTwoInts, `could not parse "{\"(1,2)\", \"(3,,4)\"}" as type tuple{int, int}[]: could not parse "(3,,4)" as type tuple{int, int}: malformed record literal`},
	}
	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			_, _, err := ParseDArrayFromString(nil /* ParseContext */, td.str, td.typ)
			if err == nil {
				t.Fatalf("expected %#v to error with message %#v", td.str, td.expectedError)
			}
			if err.Error() != td.expectedError {
				t.Fatalf("ARRAY %s: got error %s, expected error %s", td.str, err.Error(), td.expectedError)
			}
		})
	}
}
