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
	typedExpr, err := TypeCheck(expr, nil, NoTypePreference)
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
	testData := []struct {
		datumExpr string
		prev      string
		next      string
		min       string
		max       string
	}{
		// Integers
		{`1`, `0`, `2`, `-9223372036854775808`, `9223372036854775807`},
		{`-9223372036854775808`, `min`, `-9223372036854775807`, `-9223372036854775808`, `9223372036854775807`},
		{`9223372036854775807`, `9223372036854775806`, `max`, `-9223372036854775808`, `9223372036854775807`},

		// Boolean
		{`true`, `false`, `max`, `false`, `true`},
		{`false`, `min`, `true`, `false`, `true`},

		// Floats
		{`3.14:::float`, `3.1399999999999997`, `3.1400000000000006`, `-Inf`, `+Inf`},
		{`9.223372036854776e+18:::float`, `9.223372036854775e+18`, `9.223372036854778e+18`, `-Inf`, `+Inf`},
		{`-(1:::float/0)`, `min`, `-1.7976931348623157e+308`, `-Inf`, `+Inf`},
		{`(1:::float/0)`, `1.7976931348623157e+308`, `max`, `-Inf`, `+Inf`},
		{`-1.7976931348623157e+308:::float`, `-Inf`, `-1.7976931348623155e+308`, `-Inf`, `+Inf`},
		{`1.7976931348623157e+308:::float`, `1.7976931348623155e+308`, `+Inf`, `-Inf`, `+Inf`},

		// Decimal
		{`1.0:::decimal`, ``, ``, ``, ``},

		// Strings and byte arrays
		{`'':::string`, `min`, `e'\x00'`, `''`, ``},
		{`e'\x00'`, ``, `e'\x00\x00'`, `''`, ``},
		{`'abc':::string`, ``, `e'abc\x00'`, `''`, ``},
		{`'':::bytes`, `min`, `b'\x00'`, `b''`, ``},
		{`'abc':::bytes`, ``, `b'abc\x00'`, `b''`, ``},

		// Dates
		{`'2006-01-02':::date`, `2006-01-01`, `2006-01-03`, ``, ``},
		{`'0000-01-01':::date`, `-0001-12-31`, `0000-01-02`, ``, ``},
		{`'4000-01-01':::date`, `3999-12-31`, `4000-01-02`, ``, ``},
		{`'2006-01-02 03:04:05.123123':::timestamp`,
			`2006-01-02 03:04:05.123122+00:00`, `2006-01-02 03:04:05.123124+00:00`, ``, ``},

		// Intervals
		{`'1 day':::interval`, ``, ``,
			`-9223372036854775808m-9223372036854775808d-2562047h47m16.854775808s`,
			`9223372036854775807m9223372036854775807d2562047h47m16.854775807s`},
		// Max interval: we use Postgres syntax, because Go doesn't accept
		// months/days and ISO8601 doesn't accept nanoseconds.
		{`'9223372036854775807 months 9223372036854775807 days` +
			`2562047 hours 47 minutes 16 seconds 854775807 nanoseconds':::interval`,
			``, `max`,
			`-9223372036854775808m-9223372036854775808d-2562047h47m16.854775808s`,
			`9223372036854775807m9223372036854775807d2562047h47m16.854775807s`},
		// It's hard to generate a minimum interval! We can't use a
		// negative value inside the string constant, because that's not
		// allowed in a Postgres duration specification. We can't use the
		// full positive value (9223372036854775808) and then negate that
		// after parsing, because that would overflow. So we generate
		// first a slightly smaller absolute value by conversion, negate,
		// then add the missing bits.
		{`-'9223372036854775807 months 9223372036854775807 days` +
			`2562047 hours 47 minutes 16 seconds':::interval` +
			`-'1 month 1 day 854775808 nanoseconds':::interval`,
			`min`, ``,
			`-9223372036854775808m-9223372036854775808d-2562047h47m16.854775808s`,
			`9223372036854775807m9223372036854775807d2562047h47m16.854775807s`},

		// NULL
		{`NULL`, `min`, `max`, `NULL`, `NULL`},

		// Tuples
		{`row()`, `min`, `max`, `()`, `()`},

		{`row(NULL)`, `min`, `max`, `(NULL)`, `(NULL)`},

		{`row(true)`, `(false)`, `max`, `(false)`, `(true)`},
		{`row(false)`, `min`, `(true)`, `(false)`, `(true)`},

		{`row(0, 0)`, `(0, -1)`, `(0, 1)`,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},

		{`row(0, 9223372036854775807)`,
			`(0, 9223372036854775806)`, `(1, -9223372036854775808)`,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},
		{`row(9223372036854775807, 9223372036854775807)`,
			`(9223372036854775807, 9223372036854775806)`, `max`,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},

		{`row(0, 0:::decimal)`, ``, ``, ``, ``},
		{`row(0:::decimal, 0)`, `(0, -1)`, `(0, 1)`, ``, ``},

		{`row(10, '')`, ``, `(10, e'\x00')`,
			`(-9223372036854775808, '')`, ``},
		{`row(-9223372036854775808, '')`, `min`, `(-9223372036854775808, e'\x00')`,
			`(-9223372036854775808, '')`, ``},
		{`row(-9223372036854775808, 'abc')`, ``, `(-9223372036854775808, e'abc\x00')`,
			`(-9223372036854775808, '')`, ``},

		{`row(10, NULL)`, `(9, NULL)`, `(11, NULL)`,
			`(-9223372036854775808, NULL)`, `(9223372036854775807, NULL)`},
		{`row(NULL, 10)`, `(NULL, 9)`, `(NULL, 11)`,
			`(NULL, -9223372036854775808)`, `(NULL, 9223372036854775807)`},
	}
	for _, td := range testData {
		expr := prepareExpr(t, td.datumExpr)

		d := expr.(Datum)
		if td.prev == "" {
			if d.HasPrev() {
				if !d.IsMin() {
					t.Errorf("%s: HasPrev() true but IsMin() false, expected !HasPrev() or IsMin() true", td.datumExpr)
				}
			}
		} else {
			if !d.HasPrev() && td.prev != "min" {
				t.Errorf("%s: HasPrev(): got false, expected true", td.datumExpr)
				continue
			}
			isMin := d.IsMin()
			if isMin != (td.prev == "min") {
				t.Errorf("%s: IsMin() %v, expected %v", td.datumExpr, isMin, (td.prev == "min"))
				continue
			}
			if !isMin {
				dPrev := d.Prev().String()
				if dPrev != td.prev {
					t.Errorf("%s: Prev(): got %s, expected %s", td.datumExpr, dPrev, td.prev)
				}
			}
		}
		if td.next == "" {
			if d.HasNext() {
				if !d.IsMax() {
					t.Errorf("%s: HasNext() true but IsMax() false, expected (!HasNext() || IsMax())", td.datumExpr)
				}
			}
		} else {
			if !d.HasNext() && td.next != "max" {
				t.Errorf("%s: HasNext(): got false, expected true", td.datumExpr)
				continue
			}
			isMax := d.IsMax()
			if isMax != (td.next == "max") {
				t.Errorf("%s: IsMax() %v, expected %v", td.datumExpr, isMax, (td.next == "max"))
				continue
			}
			if !isMax {
				dNext := d.Next().String()
				if dNext != td.next {
					t.Errorf("%s: Next(): got %s, expected %s", td.datumExpr, dNext, td.next)
				}
			}
		}
		if td.min == "" {
			if d.hasMin() {
				t.Errorf("%s: hasMin() true, expected false", td.datumExpr)
			}
		} else {
			dMin := d.min().String()
			if dMin != td.min {
				t.Errorf("%s: Min(): got %s, expected %s", td.datumExpr, dMin, td.min)
			}
		}
		if td.max == "" {
			if d.hasMax() {
				t.Errorf("%s: hasMax() true, expected false", td.datumExpr)
			}
		} else {
			dMax := d.max().String()
			if dMax != td.max {
				t.Errorf("%s: Max(): got %s, expected %s", td.datumExpr, dMax, td.max)
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
			got := x.Compare(y)
			if got != expected {
				t.Errorf("comparing DFloats %s and %s: expected %d, got %d", x, y, expected, got)
			}
		}
	}
}
