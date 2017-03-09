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
// Author: David Eisenstat (eisen@cockroachlabs.com)

package parser

import (
	"fmt"
	"math"
	"testing"
)

func prepareExpr(t *testing.T, datumExpr string) TypedExpr {
	expr, err := ParseExprTraditional(datumExpr)
	if err != nil {
		t.Fatalf("%s: %v", datumExpr, err)
	}
	// Type checking ensures constant folding is performed and type
	// annotations have come into effect.
	typedExpr, err := TypeCheck(expr, nil, TypeAny)
	if err != nil {
		t.Fatalf("%s: %v", datumExpr, err)
	}
	// Normalization ensures that casts are processed.
	ctx := &EvalContext{}
	typedExpr, err = ctx.NormalizeExpr(typedExpr)
	if err != nil {
		t.Fatalf("%s: %v", datumExpr, err)
	}
	return typedExpr
}

func TestDatumOrdering(t *testing.T) {
	const valIsMin = `min`
	const valIsMax = `max`
	const noPrev = ``
	const noNext = ``
	const noMin = ``
	const noMax = ``

	testData := []struct {
		datumExpr string
		prev      string
		next      string
		min       string
		max       string
	}{
		// Integers
		{`1`, `0`, `2`, `-9223372036854775808`, `9223372036854775807`},
		{`-9223372036854775808`, valIsMin, `-9223372036854775807`, `-9223372036854775808`, `9223372036854775807`},
		{`9223372036854775807`, `9223372036854775806`, valIsMax, `-9223372036854775808`, `9223372036854775807`},

		// Boolean
		{`true`, `false`, valIsMax, `false`, `true`},
		{`false`, valIsMin, `true`, `false`, `true`},

		// Floats
		{`3.14:::float`, `3.1399999999999997`, `3.1400000000000006`, `-Inf`, `+Inf`},
		{`9.223372036854776e+18:::float`, `9.223372036854775e+18`, `9.223372036854778e+18`, `-Inf`, `+Inf`},
		{`-(1:::float/0)`, valIsMin, `-1.7976931348623157e+308`, `-Inf`, `+Inf`},
		{`(1:::float/0)`, `1.7976931348623157e+308`, valIsMax, `-Inf`, `+Inf`},
		{`-1.7976931348623157e+308:::float`, `-Inf`, `-1.7976931348623155e+308`, `-Inf`, `+Inf`},
		{`1.7976931348623157e+308:::float`, `1.7976931348623155e+308`, `+Inf`, `-Inf`, `+Inf`},

		// Decimal
		{`1.0:::decimal`, noPrev, noNext, noMin, noMax},

		// Strings and byte arrays
		{`'':::string`, valIsMin, `e'\x00'`, `''`, noMax},
		{`e'\x00'`, noPrev, `e'\x00\x00'`, `''`, noMax},
		{`'abc':::string`, noPrev, `e'abc\x00'`, `''`, noMax},
		{`'':::bytes`, valIsMin, `b'\x00'`, `b''`, noMax},
		{`'abc':::bytes`, noPrev, `b'abc\x00'`, `b''`, noMax},

		// Dates
		{`'2006-01-02':::date`, `'2006-01-01'`, `'2006-01-03'`, noMin, noMax},
		{`'0000-01-01':::date`, `'-0001-12-31'`, `'0000-01-02'`, noMin, noMax},
		{`'4000-01-01':::date`, `'3999-12-31'`, `'4000-01-02'`, noMin, noMax},
		{`'2006-01-02 03:04:05.123123':::timestamp`,
			`'2006-01-02 03:04:05.123122+00:00'`, `'2006-01-02 03:04:05.123124+00:00'`, noMin, noMax},

		// Intervals
		{`'1 day':::interval`, noPrev, noNext,
			`'-9223372036854775808m-9223372036854775808d-2562047h47m16.854775808s'`,
			`'9223372036854775807m9223372036854775807d2562047h47m16.854775807s'`},
		// Max interval: we use Postgres syntax, because Go doesn't accept
		// months/days and ISO8601 doesn't accept nanoseconds.
		{`'9223372036854775807 months 9223372036854775807 days ` +
			`2562047 hours 47 minutes 16 seconds 854775807 nanoseconds':::interval`,
			noPrev, valIsMax,
			`'-9223372036854775808m-9223372036854775808d-2562047h47m16.854775808s'`,
			`'9223372036854775807m9223372036854775807d2562047h47m16.854775807s'`},
		// It's hard to generate a minimum interval! We can't use a
		// negative value inside the string constant, because that's not
		// allowed in a Postgres duration specification. We can't use the
		// full positive value (9223372036854775808) and then negate that
		// after parsing, because that would overflow. So we generate
		// first a slightly smaller absolute value by conversion, negate,
		// then add the missing bits.
		{`-'9223372036854775807 months 9223372036854775807 days ` +
			`2562047 hours 47 minutes 16 seconds':::interval` +
			`-'1 month 1 day 854775808 nanoseconds':::interval`,
			valIsMin, noNext,
			`'-9223372036854775808m-9223372036854775808d-2562047h47m16.854775808s'`,
			`'9223372036854775807m9223372036854775807d2562047h47m16.854775807s'`},

		// NULL
		{`NULL`, valIsMin, valIsMax, `NULL`, `NULL`},

		// Tuples
		{`row()`, valIsMin, valIsMax, `()`, `()`},

		{`row(NULL)`, valIsMin, valIsMax, `(NULL)`, `(NULL)`},

		{`row(true)`, `(false)`, valIsMax, `(false)`, `(true)`},
		{`row(false)`, valIsMin, `(true)`, `(false)`, `(true)`},

		{`row(true, false, false)`, `(false, true, true)`, `(true, false, true)`,
			`(false, false, false)`, `(true, true, true)`},
		{`row(false, true, true)`, `(false, true, false)`, `(true, false, false)`,
			`(false, false, false)`, `(true, true, true)`},

		{`row(0, 0)`, `(0, -1)`, `(0, 1)`,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},

		{`row(0, 9223372036854775807)`,
			`(0, 9223372036854775806)`, `(1, -9223372036854775808)`,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},
		{`row(9223372036854775807, 9223372036854775807)`,
			`(9223372036854775807, 9223372036854775806)`, valIsMax,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},

		{`row(0, 0:::decimal)`, noPrev, noNext, noMin, noMax},
		{`row(0:::decimal, 0)`, `(0, -1)`, `(0, 1)`, noMin, noMax},

		{`row(10, '')`, noPrev, `(10, e'\x00')`,
			`(-9223372036854775808, '')`, noMax},
		{`row(-9223372036854775808, '')`, valIsMin, `(-9223372036854775808, e'\x00')`,
			`(-9223372036854775808, '')`, noMax},
		{`row(-9223372036854775808, 'abc')`, noPrev, `(-9223372036854775808, e'abc\x00')`,
			`(-9223372036854775808, '')`, noMax},

		{`row(10, NULL)`, `(9, NULL)`, `(11, NULL)`,
			`(-9223372036854775808, NULL)`, `(9223372036854775807, NULL)`},
		{`row(NULL, 10)`, `(NULL, 9)`, `(NULL, 11)`,
			`(NULL, -9223372036854775808)`, `(NULL, 9223372036854775807)`},

		{`row(true, NULL, false)`, `(false, NULL, true)`, `(true, NULL, true)`,
			`(false, NULL, false)`, `(true, NULL, true)`},
		{`row(false, NULL, true)`, `(false, NULL, false)`, `(true, NULL, false)`,
			`(false, NULL, false)`, `(true, NULL, true)`},

		{`row(row(true), row(false))`, `((false), (true))`, `((true), (true))`,
			`((false), (false))`, `((true), (true))`},
		{`row(row(false), row(true))`, `((false), (false))`, `((true), (false))`,
			`((false), (false))`, `((true), (true))`},

		// Arrays

		// TODO(nathan) Until we support literals for empty arrays, this
		// is the easiest way to construct one.
		{`current_schemas(false)`, valIsMin, `{NULL}`, `{}`, noMax},

		{`array[NULL]`, noPrev, `{NULL,NULL}`, `{}`, noMax},
		{`array[true]`, noPrev, `{true,NULL}`, `{}`, noMax},

		// Mixed tuple/array datums.
		{`row(ARRAY[true], row(true))`, `({true}, (false))`, `({true,NULL}, (false))`, `({}, (false))`, noMax},
		{`row(row(false), ARRAY[true])`, noPrev, `((false), {true,NULL})`, `((false), {})`, noMax},
	}
	for _, td := range testData {
		expr := prepareExpr(t, td.datumExpr)

		d := expr.(Datum)
		prevVal, hasPrev := d.Prev()
		nextVal, hasNext := d.Next()
		if td.prev == noPrev {
			if hasPrev {
				if !d.IsMin() {
					t.Errorf("%s: value should not have a prev, yet hasPrev true and IsMin() false (expected (!hasPrev || IsMin()))", td.datumExpr)
				}
			}
		} else {
			if !hasPrev && td.prev != valIsMin {
				t.Errorf("%s: hasPrev: got false, expected true", td.datumExpr)
				continue
			}
			isMin := d.IsMin()
			if isMin != (td.prev == valIsMin) {
				t.Errorf("%s: IsMin() %v, expected %v", td.datumExpr, isMin, (td.prev == valIsMin))
				continue
			}
			if !isMin {
				dPrev := fmt.Sprintf("%s", prevVal)
				if dPrev != td.prev {
					t.Errorf("%s: Prev(): got %s, expected %s", td.datumExpr, dPrev, td.prev)
				}
			}
		}
		if td.next == noNext {
			if hasNext {
				if !d.IsMax() {
					t.Errorf("%s: value should not have a next, yet hasNext true and IsMax() false (expected (!hasNext || IsMax()))", td.datumExpr)
				}
			}
		} else {
			if !hasNext && td.next != valIsMax {
				t.Errorf("%s: HasNext(): got false, expected true", td.datumExpr)
				continue
			}
			isMax := d.IsMax()
			if isMax != (td.next == valIsMax) {
				t.Errorf("%s: IsMax() %v, expected %v", td.datumExpr, isMax, (td.next == valIsMax))
				continue
			}
			if !isMax {
				dNext := fmt.Sprintf("%s", nextVal)
				if dNext != td.next {
					t.Errorf("%s: Next(): got %s, expected %s", td.datumExpr, dNext, td.next)
				}
			}
		}

		minVal, hasMin := d.min()
		maxVal, hasMax := d.max()

		if td.min == noMin {
			if hasMin {
				t.Errorf("%s: hasMin true, expected false", td.datumExpr)
			}
		} else {
			dMin := fmt.Sprintf("%s", minVal)
			if dMin != td.min {
				t.Errorf("%s: min(): got %s, expected %s", td.datumExpr, dMin, td.min)
			}
		}
		if td.max == noMax {
			if hasMax {
				t.Errorf("%s: hasMax true, expected false", td.datumExpr)
			}
		} else {
			dMax := fmt.Sprintf("%s", maxVal)
			if dMax != td.max {
				t.Errorf("%s: max(): got %s, expected %s", td.datumExpr, dMax, td.max)
			}
		}
	}
}

func TestDFloatCompare(t *testing.T) {
	values := []Datum{DNull}
	for _, x := range []float64{math.NaN(), math.Inf(-1), -1, 0, 1, math.Inf(1)} {
		values = append(values, NewDFloat(DFloat(x)))
	}
	for i, x := range values {
		for j, y := range values {
			expected := 0
			if i < j {
				expected = -1
			} else if i > j {
				expected = 1
			}
			got := x.Compare(&EvalContext{}, y)
			if got != expected {
				t.Errorf("comparing DFloats %s and %s: expected %d, got %d", x, y, expected, got)
			}
		}
	}
}

// TestParseDIntervalWithField tests that the additional features available
// to ParseDIntervalWithField beyond those in ParseDInterval behave as expected.
func TestParseDIntervalWithField(t *testing.T) {
	testData := []struct {
		str      string
		field    durationField
		expected string
	}{
		// Test cases for raw numbers with fields
		{"5", second, "5s"},
		{"5.8", second, "5.8s"},
		{"5", minute, "5m"},
		{"5.8", minute, "5m"},
		{"5", hour, "5h"},
		{"5.8", hour, "5h"},
		{"5", day, "5 day"},
		{"5.8", day, "5 day"},
		{"5", month, "5 month"},
		{"5.8", month, "5 month"},
		{"5", year, "5 year"},
		{"5.8", year, "5 year"},
		// Test cases for truncation based on fields
		{"1-2 3 4:56:07", second, "1-2 3 4:56:07"},
		{"1-2 3 4:56:07", minute, "1-2 3 4:56:00"},
		{"1-2 3 4:56:07", hour, "1-2 3 4:00:00"},
		{"1-2 3 4:56:07", day, "1-2 3 0:"},
		{"1-2 3 4:56:07", month, "1-2 0 0:"},
		{"1-2 3 4:56:07", year, "1 year"},
	}
	for _, td := range testData {
		actual, err := ParseDIntervalWithField(td.str, td.field)
		if err != nil {
			t.Errorf("unexpected error while parsing INTERVAL %s %d: %s", td.str, td.field, err)
			continue
		}
		expected, err := ParseDInterval(td.expected)
		if err != nil {
			t.Errorf("unexpected error while parsing expected value INTERVAL %s: %s", td.expected, err)
			continue
		}
		if expected.Compare(&EvalContext{}, actual) != 0 {
			t.Errorf("INTERVAL %s %v: got %s, expected %s", td.str, td.field, actual, expected)
		}
	}
}
