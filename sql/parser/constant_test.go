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
	"fmt"
	"go/constant"
	"go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/util/decimal"

	"gopkg.in/inf.v0"
)

func TestNumericConstantAvailableTypes(t *testing.T) {
	wantInt := numValAvailIntFloatDec
	wantDecButCanBeInt := numValAvailDecFloatInt
	wantDec := numValAvailDecFloat

	testCases := []struct {
		str   string
		avail []Datum
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
		c := &NumVal{Value: val}
		avail := c.AvailableTypes()
		if !reflect.DeepEqual(avail, test.avail) {
			t.Errorf("%d: expected the available type set %v for %v, found %v",
				i, test.avail, c.Value.ExactString(), avail)
		}

		// Make sure it can be resolved as each of those types.
		for _, availType := range avail {
			if res, err := c.ResolveAsType(availType); err != nil {
				t.Errorf("%d: expected resolving %v as available type %s would succeed, found %v",
					i, c.Value.ExactString(), availType.Type(), err)
			} else {
				resErr := func(parsed, resolved interface{}) {
					t.Errorf("%d: expected resolving %v as available type %s would produce a Datum with the value %v, found %v",
						i, c, availType.Type(), parsed, resolved)
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
					d := new(inf.Dec)
					if !strings.ContainsAny(test.str, "eE") {
						if _, ok := d.SetString(test.str); !ok {
							t.Fatal(fmt.Sprintf("could not set %q on decimal", test.str))
						}
					} else {
						f, err := strconv.ParseFloat(test.str, 64)
						if err != nil {
							t.Fatal(err)
						}
						decimal.SetFromFloat(d, f)
					}
					if resD := &typ.Dec; d.Cmp(resD) != 0 {
						resErr(d, resD)
					}
				}
			}
		}
	}
}

func TestStringConstantAvailableTypes(t *testing.T) {
	wantStringButCanBeBytes := strValAvailStringBytes
	wantBytesButCanBeString := strValAvailBytesString
	wantBytes := strValAvailBytes

	testCases := []struct {
		c     *StrVal
		avail []Datum
	}{
		{&StrVal{s: "abc 世界", bytesEsc: false}, wantStringButCanBeBytes},
		{&StrVal{s: "abc 世界", bytesEsc: true}, wantBytesButCanBeString},
		{&StrVal{s: string([]byte{0xff, 0xfe, 0xfd}), bytesEsc: true}, wantBytes},
	}

	for i, test := range testCases {
		// Check available types.
		avail := test.c.AvailableTypes()
		if !reflect.DeepEqual(avail, test.avail) {
			t.Errorf("%d: expected the available type set %v for %+v, found %v",
				i, test.avail, test.c, avail)
		}

		// Make sure it can be resolved as each of those types.
		for _, availType := range avail {
			if res, err := test.c.ResolveAsType(availType); err != nil {
				t.Errorf("%d: expected resolving %v as available type %s would succeed, found %v",
					i, test.c, availType.Type(), err)
			} else {
				var resString string
				switch typ := res.(type) {
				case *DString:
					resString = string(*typ)
				case *DBytes:
					resString = string(*typ)
				default:
					panic("unreachable")
				}
				if resString != test.c.s {
					t.Errorf("%d: expected resolving %v as available type %s would produce a Datum with the same value, found %s",
						i, test.c, availType.Type(), resString)
				}
			}
		}
	}
}

func TestFoldNumericConstants(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
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
		{`9 % 2`, `1`},
		{`100 % 17`, `15`},
		{`100.43 % 17.82`, `100.43 % 17.82`}, // Constant folding won't fold float modulo.
		{`1 & 3`, `1`},
		{`1.3 & 3.2`, `1.3 & 3.2`}, // Will be caught during type checking.
		{`1 | 2`, `3`},
		{`1.3 | 2.8`, `1.3 | 2.8`}, // Will be caught during type checking.
		{`1 ^ 3`, `2`},
		{`1.3 ^ 3.9`, `1.3 ^ 3.9`}, // Will be caught during type checking.
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
	}
	for _, d := range testData {
		expr, err := ParseExprTraditional(d.expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		rOrig := expr.String()
		r, err := foldNumericConstants(expr)
		if err != nil {
			t.Fatalf("%s: %v", d.expr, err)
		}
		if s := r.String(); d.expected != s {
			t.Errorf("%s: expected %s, but found %s", d.expr, d.expected, s)
		}
		// Folding again should be a no-op.
		r2, err := foldNumericConstants(r)
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
