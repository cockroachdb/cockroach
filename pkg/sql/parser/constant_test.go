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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package parser

import (
	"go/constant"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd"
)

// TestNumericConstantVerifyAndResolveAvailableTypes verifies that test NumVals will
// all return expected available type sets, and that attempting to resolve the NumVals
// as each of these types will all succeed with an expected Datum result.
func TestNumericConstantVerifyAndResolveAvailableTypes(t *testing.T) {
	wantInt := numValAvailInteger
	wantDecButCanBeInt := numValAvailDecimalNoFraction
	wantDec := numValAvailDecimalWithFraction

	testCases := []struct {
		str   string
		avail []Type
	}{
		{"1", wantInt},
		{"0", wantInt},
		{"-1", wantInt},
		{"9223372036854775807", wantInt},
		{"1.0", wantDecButCanBeInt},
		{"-1234.0000", wantDecButCanBeInt},
		{"1e10", wantDecButCanBeInt},
		{"1E10", wantDecButCanBeInt},
		{"1.1", wantDec},
		{"1e-10", wantDec},
		{"1E-10", wantDec},
		{"-1231.131", wantDec},
		{"876543234567898765436787654321", wantDec},
	}

	for i, test := range testCases {
		tok := token.INT
		if strings.ContainsAny(test.str, ".eE") {
			tok = token.FLOAT
		}
		val := constant.MakeFromLiteral(test.str, tok, 0)
		if val.Kind() == constant.Unknown {
			t.Fatalf("%d: could not parse value string %q", i, test.str)
		}

		// Check available types.
		c := &NumVal{Value: val, OrigString: test.str}
		avail := c.AvailableTypes()
		if !reflect.DeepEqual(avail, test.avail) {
			t.Errorf("%d: expected the available type set %v for %v, found %v",
				i, test.avail, c.Value.ExactString(), avail)
		}

		// Make sure it can be resolved as each of those types.
		for _, availType := range avail {
			if res, err := c.ResolveAsType(&SemaContext{}, availType); err != nil {
				t.Errorf("%d: expected resolving %v as available type %s would succeed, found %v",
					i, c.Value.ExactString(), availType, err)
			} else {
				resErr := func(parsed, resolved interface{}) {
					t.Errorf("%d: expected resolving %v as available type %s would produce a Datum"+
						" with the value %v, found %v",
						i, c, availType, parsed, resolved)
				}
				switch typ := res.(type) {
				case *DInt:
					var i int64
					var err error
					if tok == token.INT {
						if i, err = strconv.ParseInt(test.str, 10, 64); err != nil {
							t.Fatal(err)
						}
					} else {
						var f float64
						if f, err = strconv.ParseFloat(test.str, 64); err != nil {
							t.Fatal(err)
						}
						i = int64(f)
					}
					if resI := int64(*typ); i != resI {
						resErr(i, resI)
					}
				case *DFloat:
					f, err := strconv.ParseFloat(test.str, 64)
					if err != nil {
						t.Fatal(err)
					}
					if resF := float64(*typ); f != resF {
						resErr(f, resF)
					}
				case *DDecimal:
					d := new(apd.Decimal)
					if !strings.ContainsAny(test.str, "eE") {
						if _, _, err := d.SetString(test.str); err != nil {
							t.Fatalf("could not set %q on decimal", test.str)
						}
					} else {
						_, _, err = d.SetString(test.str)
						if err != nil {
							t.Fatal(err)
						}
					}
					resD := &typ.Decimal
					if d.Cmp(resD) != 0 {
						resErr(d, resD)
					}
				}
			}
		}
	}
}

// TestStringConstantVerifyAvailableTypes verifies that test StrVals will all
// return expected available type sets, and that attempting to resolve the StrVals
// as each of these types will either succeed or return a parse error.
func TestStringConstantVerifyAvailableTypes(t *testing.T) {
	wantStringButCanBeAll := strValAvailAllParsable
	wantBytesButCanBeString := strValAvailBytesString
	wantBytes := strValAvailBytes

	testCases := []struct {
		c     *StrVal
		avail []Type
	}{
		{&StrVal{s: "abc 世界", bytesEsc: false}, wantStringButCanBeAll},
		{&StrVal{s: "t", bytesEsc: false}, wantStringButCanBeAll},
		{&StrVal{s: "2010-09-28", bytesEsc: false}, wantStringButCanBeAll},
		{&StrVal{s: "2010-09-28 12:00:00.1", bytesEsc: false}, wantStringButCanBeAll},
		{&StrVal{s: "PT12H2M", bytesEsc: false}, wantStringButCanBeAll},
		{&StrVal{s: "abc 世界", bytesEsc: true}, wantBytesButCanBeString},
		{&StrVal{s: "t", bytesEsc: true}, wantBytesButCanBeString},
		{&StrVal{s: "2010-09-28", bytesEsc: true}, wantBytesButCanBeString},
		{&StrVal{s: "2010-09-28 12:00:00.1", bytesEsc: true}, wantBytesButCanBeString},
		{&StrVal{s: "PT12H2M", bytesEsc: true}, wantBytesButCanBeString},
		{&StrVal{s: string([]byte{0xff, 0xfe, 0xfd}), bytesEsc: true}, wantBytes},
	}

	for i, test := range testCases {
		// Check that the expected available types are returned.
		avail := test.c.AvailableTypes()
		if !reflect.DeepEqual(avail, test.avail) {
			t.Errorf("%d: expected the available type set %v for %+v, found %v",
				i, test.avail, test.c, avail)
		}

		// Make sure it can be resolved as each of those types or throws a parsing error.
		for _, availType := range avail {
			if _, err := test.c.ResolveAsType(&SemaContext{}, availType); err != nil {
				if !strings.Contains(err.Error(), "could not parse") {
					// Parsing errors are permitted for this test, as proper StrVal parsing
					// is tested in TestStringConstantTypeResolution. Any other error should
					// throw a failure.
					t.Errorf("%d: expected resolving %v as available type %s would either succeed"+
						" or throw a parsing error, found %v",
						i, test.c, availType, err)
				}
			}
		}
	}
}

func mustParseDBool(t *testing.T, s string) Datum {
	d, err := ParseDBool(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDDate(t *testing.T, s string) Datum {
	d, err := ParseDDate(s, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDTimestamp(t *testing.T, s string) Datum {
	d, err := ParseDTimestamp(s, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDTimestampTZ(t *testing.T, s string) Datum {
	d, err := ParseDTimestampTZ(s, time.UTC, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func mustParseDInterval(t *testing.T, s string) Datum {
	d, err := ParseDInterval(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

var parseFuncs = map[Type]func(*testing.T, string) Datum{
	TypeString:      func(t *testing.T, s string) Datum { return NewDString(s) },
	TypeBytes:       func(t *testing.T, s string) Datum { return NewDBytes(DBytes(s)) },
	TypeBool:        mustParseDBool,
	TypeDate:        mustParseDDate,
	TypeTimestamp:   mustParseDTimestamp,
	TypeTimestampTZ: mustParseDTimestampTZ,
	TypeInterval:    mustParseDInterval,
}

func typeSet(types ...Type) map[Type]struct{} {
	set := make(map[Type]struct{}, len(types))
	for _, t := range types {
		set[t] = struct{}{}
	}
	return set
}

// TestStringConstantResolveAvailableTypes verifies that test StrVals can all be
// resolved successfully into an expected set of Datum types. The test will make sure
// the correct set of Datum types are resolvable, and that the resolved Datum match
// the expected results which come from running the string literal through a
// corresponding parseFunc (above).
func TestStringConstantResolveAvailableTypes(t *testing.T) {
	testCases := []struct {
		c            *StrVal
		parseOptions map[Type]struct{}
	}{
		{
			c:            &StrVal{s: "abc 世界", bytesEsc: false},
			parseOptions: typeSet(TypeString, TypeBytes),
		},
		{
			c:            &StrVal{s: "true", bytesEsc: false},
			parseOptions: typeSet(TypeString, TypeBytes, TypeBool),
		},
		{
			c:            &StrVal{s: "2010-09-28", bytesEsc: false},
			parseOptions: typeSet(TypeString, TypeBytes, TypeDate, TypeTimestamp, TypeTimestampTZ),
		},
		{
			c:            &StrVal{s: "2010-09-28 12:00:00.1", bytesEsc: false},
			parseOptions: typeSet(TypeString, TypeBytes, TypeTimestamp, TypeTimestampTZ, TypeDate),
		},
		{
			c:            &StrVal{s: "2006-07-08T00:00:00.000000123Z", bytesEsc: false},
			parseOptions: typeSet(TypeString, TypeBytes, TypeTimestamp, TypeTimestampTZ, TypeDate),
		},
		{
			c:            &StrVal{s: "PT12H2M", bytesEsc: false},
			parseOptions: typeSet(TypeString, TypeBytes, TypeInterval),
		},
		{
			c:            &StrVal{s: "abc 世界", bytesEsc: true},
			parseOptions: typeSet(TypeString, TypeBytes),
		},
		{
			c:            &StrVal{s: "true", bytesEsc: true},
			parseOptions: typeSet(TypeString, TypeBytes),
		},
		{
			c:            &StrVal{s: "2010-09-28", bytesEsc: true},
			parseOptions: typeSet(TypeString, TypeBytes),
		},
		{
			c:            &StrVal{s: "2010-09-28 12:00:00.1", bytesEsc: true},
			parseOptions: typeSet(TypeString, TypeBytes),
		},
		{
			c:            &StrVal{s: "PT12H2M", bytesEsc: true},
			parseOptions: typeSet(TypeString, TypeBytes),
		},
		{
			c:            &StrVal{s: string([]byte{0xff, 0xfe, 0xfd}), bytesEsc: true},
			parseOptions: typeSet(TypeBytes),
		},
	}

	for i, test := range testCases {
		parseableCount := 0

		// Make sure it can be resolved as each of those types or throws a parsing error.
		for _, availType := range test.c.AvailableTypes() {
			res, err := test.c.ResolveAsType(&SemaContext{}, availType)
			if err != nil {
				if !strings.Contains(err.Error(), "could not parse") {
					// Parsing errors are permitted for this test, but the number of correctly
					// parseable types will be verified. Any other error should throw a failure.
					t.Errorf("%d: expected resolving %v as available type %s would either succeed"+
						" or throw a parsing error, found %v",
						i, test.c, availType, err)
				}
				continue
			}
			parseableCount++

			if _, isExpected := test.parseOptions[availType]; !isExpected {
				t.Errorf("%d: type %s not expected to be resolvable from the StrVal %v, found %v",
					i, availType, test.c, res)
			} else {
				expectedDatum := parseFuncs[availType](t, test.c.s)
				if res.Compare(&EvalContext{}, expectedDatum) != 0 {
					t.Errorf("%d: type %s expected to be resolved from the StrVal %v to Datum %v"+
						", found %v",
						i, availType, test.c, expectedDatum, res)
				}
			}
		}

		// Make sure the expected number of types can be resolved from the StrVal.
		if expCount := len(test.parseOptions); parseableCount != expCount {
			t.Errorf("%d: expected %d successfully resolvable types for the StrVal %v, found %d",
				i, expCount, test.c, parseableCount)
		}
	}
}

type constantLiteralFoldingTestCase struct {
	expr     string
	expected string
}

func testConstantLiteralFolding(t *testing.T, testData []constantLiteralFoldingTestCase) {
	for _, d := range testData {
		expr, err := ParseExpr(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		rOrig := expr.String()
		r, err := foldConstantLiterals(expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		// Folding again should be a no-op.
		r2, err := foldConstantLiterals(r)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r2.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		// The original expression should be unchanged.
		if rStr := expr.String(); rOrig != rStr {
			t.Fatalf("Original expression `%s` changed to `%s`", rOrig, rStr)
		}
	}
}

func TestFoldNumericConstants(t *testing.T) {
	testConstantLiteralFolding(t, []constantLiteralFoldingTestCase{
		// Unary ops.
		{`+1`, `1`},
		{`+1.2`, `1.2`},
		{`-1`, `-1`},
		{`-1.2`, `-1.2`},
		// Unary ops (int only).
		{`~1`, `-2`},
		{`~1.2`, `~ 1.2`},
		// Binary ops.
		{`1 + 1`, `2`},
		{`1.2 + 2.3`, `3.5`},
		{`1 + 2.3`, `3.3`},
		{`2 - 1`, `1`},
		{`1.2 - 2.3`, `-1.1`},
		{`1 - 2.3`, `-1.3`},
		{`2 * 1`, `2`},
		{`1.2 * 2.3`, `2.76`},
		{`1 * 2.3`, `2.3`},
		{`123456789.987654321 * 987654321`, `1.21933e+17`},
		{`9 / 4`, `2.25`},
		{`9.7 / 4`, `2.425`},
		{`4.72 / 2.36`, `2`},
		{`0 / 0`, `0 / 0`}, // Will be caught during evaluation.
		{`1 / 0`, `1 / 0`}, // Will be caught during evaluation.
		// Binary ops (int only).
		{`9 // 2`, `4`},
		{`-5 // 3`, `-1`},
		{`100 // 17`, `5`},
		{`100.43 // 17.82`, `100.43 // 17.82`}, // Constant folding won't fold numeric modulo.
		{`0 // 0`, `0 // 0`},                   // Will be caught during evaluation.
		{`1 // 0`, `1 // 0`},                   // Will be caught during evaluation.
		{`9 % 2`, `1`},
		{`100 % 17`, `15`},
		{`100.43 % 17.82`, `100.43 % 17.82`}, // Constant folding won't fold numeric modulo.
		{`1 & 3`, `1`},
		{`1.3 & 3.2`, `1.3 & 3.2`}, // Will be caught during type checking.
		{`1 | 2`, `3`},
		{`1.3 | 2.8`, `1.3 | 2.8`}, // Will be caught during type checking.
		{`1 # 3`, `2`},
		{`1.3 # 3.9`, `1.3 # 3.9`}, // Will be caught during type checking.
		{`2 ^ 3`, `2 ^ 3`},         // Constant folding won't fold power.
		{`1.3 ^ 3.9`, `1.3 ^ 3.9`},
		// Shift ops (int only).
		{`1 << 2`, `4`},
		{`1 << -2`, `1 << -2`},                                                     // Should be caught during evaluation.
		{`1 << 9999999999999999999999999999`, `1 << 9999999999999999999999999999`}, // Will be caught during type checking.
		{`1.2 << 2.4`, `1.2 << 2.4`},                                               // Will be caught during type checking.
		{`4 >> 2`, `1`},
		{`4.1 >> 2.9`, `4.1 >> 2.9`}, // Will be caught during type checking.
		// Comparison ops.
		{`4 = 2`, `false`},
		{`4 = 4.0`, `true`},
		{`4.0 = 4`, `true`},
		{`4.9 = 4`, `false`},
		{`4.9 = 4.9`, `true`},
		{`4 != 2`, `true`},
		{`4 != 4.0`, `false`},
		{`4.0 != 4`, `false`},
		{`4.9 != 4`, `true`},
		{`4.9 != 4.9`, `false`},
		{`4 < 2`, `false`},
		{`4 < 4.0`, `false`},
		{`4.0 < 4`, `false`},
		{`4.9 < 4`, `false`},
		{`4.9 < 4.9`, `false`},
		{`4 <= 2`, `false`},
		{`4 <= 4.0`, `true`},
		{`4.0 <= 4`, `true`},
		{`4.9 <= 4`, `false`},
		{`4.9 <= 4.9`, `true`},
		{`4 > 2`, `true`},
		{`4 > 4.0`, `false`},
		{`4.0 > 4`, `false`},
		{`4.9 > 4`, `true`},
		{`4.9 > 4.9`, `false`},
		{`4 >= 2`, `true`},
		{`4 >= 4.0`, `true`},
		{`4.0 >= 4`, `true`},
		{`4.9 >= 4`, `true`},
		{`4.9 >= 4.9`, `true`},
		// With parentheses.
		{`(4)`, `4`},
		{`(((4)))`, `4`},
		{`(((9 / 3) * (1 / 3)))`, `1`},
		{`(((9 / 3) % (1 / 3)))`, `((3 % 0.333333))`},
		{`(1.0) << ((2) + 3 / (1/9))`, `536870912`},
		// With non-constants.
		{`a + 5 * b`, `a + (5 * b)`},
		{`a + 5 + b + 7`, `((a + 5) + b) + 7`},
		{`a + 5 * 2`, `a + 10`},
		{`a * b + 5 / 2`, `(a * b) + 2.5`},
		{`a - b * 5 - 3`, `(a - (b * 5)) - 3`},
		{`a - b + 5 * 3`, `(a - b) + 15`},
	})
}

func TestFoldStringConstants(t *testing.T) {
	testConstantLiteralFolding(t, []constantLiteralFoldingTestCase{
		// Binary ops.
		{`'string' || 'string'`, `'stringstring'`},
		{`'string' || b'bytes'`, `b'stringbytes'`},
		{`b'bytes' || b'bytes'`, `b'bytesbytes'`},
		{`'a' || 'b' || 'c'`, `'abc'`},
		{`'\' || (b'0a' || b'\x0a')`, `b'\\0a\n'`},
		// Comparison ops.
		{`'string' = 'string'`, `true`},
		{`'string' = b'bytes'`, `false`},
		{`'value' = b'value'`, `true`},
		{`b'bytes' = b'bytes'`, `true`},
		{`'string' != 'string'`, `false`},
		{`'string' != b'bytes'`, `true`},
		{`'value' != b'value'`, `false`},
		{`b'bytes' != b'bytes'`, `false`},
		{`'string' < 'string'`, `false`},
		{`'string' < b'bytes'`, `false`},
		{`'value' < b'value'`, `false`},
		{`b'bytes' < b'bytes'`, `false`},
		{`'string' <= 'string'`, `true`},
		{`'string' <= b'bytes'`, `false`},
		{`'value' <= b'value'`, `true`},
		{`b'bytes' <= b'bytes'`, `true`},
		{`'string' > 'string'`, `false`},
		{`'string' > b'bytes'`, `true`},
		{`'value' > b'value'`, `false`},
		{`b'bytes' > b'bytes'`, `false`},
		{`'string' >= 'string'`, `true`},
		{`'string' >= b'bytes'`, `true`},
		{`'value' >= b'value'`, `true`},
		{`b'bytes' >= b'bytes'`, `true`},
		// With parentheses.
		{`('string') || (b'bytes')`, `b'stringbytes'`},
		{`('a') || (('b') || ('c'))`, `'abc'`},
		// With non-constants.
		{`a > 'str' || b`, `a > ('str' || b)`},
		{`a > 'str' || 'ing'`, `a > 'string'`},
	})
}
