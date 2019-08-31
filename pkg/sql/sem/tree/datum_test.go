// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
)

func prepareExpr(t *testing.T, datumExpr string) tree.TypedExpr {
	expr, err := parser.ParseExpr(datumExpr)
	if err != nil {
		t.Fatalf("%s: %v", datumExpr, err)
	}
	// Type checking ensures constant folding is performed and type
	// annotations have come into effect.
	typedExpr, err := tree.TypeCheck(expr, nil, types.Any)
	if err != nil {
		t.Fatalf("%s: %v", datumExpr, err)
	}
	// Normalization ensures that casts are processed.
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	typedExpr, err = evalCtx.NormalizeExpr(typedExpr)
	if err != nil {
		t.Fatalf("%s: %v", datumExpr, err)
	}
	return typedExpr
}

func TestDatumOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		{`3.14:::float`, `3.1399999999999997`, `3.1400000000000006`, `NaN`, `+Inf`},
		{`9.223372036854776e+18:::float`, `9.223372036854775e+18`, `9.223372036854778e+18`, `NaN`, `+Inf`},
		{`'NaN':::float`, valIsMin, `-Inf`, `NaN`, `+Inf`},
		{`-1.7976931348623157e+308:::float`, `-Inf`, `-1.7976931348623155e+308`, `NaN`, `+Inf`},
		{`1.7976931348623157e+308:::float`, `1.7976931348623155e+308`, `+Inf`, `NaN`, `+Inf`},

		// Decimal
		{`1.0:::decimal`, noPrev, noNext, `NaN`, `Infinity`},

		// Strings and byte arrays
		{`'':::string`, valIsMin, `e'\x00'`, `''`, noMax},
		{`e'\x00'`, noPrev, `e'\x00\x00'`, `''`, noMax},
		{`'abc':::string`, noPrev, `e'abc\x00'`, `''`, noMax},
		{`'':::bytes`, valIsMin, `'\x00'`, `'\x'`, noMax},
		{`'abc':::bytes`, noPrev, `'\x61626300'`, `'\x'`, noMax},

		// Dates
		{`'2006-01-02':::date`, `'2006-01-01'`, `'2006-01-03'`, `'-infinity'`, `'infinity'`},
		{`'0001-01-01':::date`, `'0001-12-31 BC'`, `'0001-01-02'`, `'-infinity'`, `'infinity'`},
		{`'4000-01-01 BC':::date`, `'4001-12-31 BC'`, `'4000-01-02 BC'`, `'-infinity'`, `'infinity'`},
		{`'2006-01-02 03:04:05.123123':::timestamp`,
			`'2006-01-02 03:04:05.123122+00:00'`, `'2006-01-02 03:04:05.123124+00:00'`, noMin, noMax},

		// Times
		{`'00:00:00':::time`, valIsMin, `'00:00:00.000001'`,
			`'00:00:00'`, `'23:59:59.999999'`},
		{`'12:00:00':::time`, `'11:59:59.999999'`, `'12:00:00.000001'`,
			`'00:00:00'`, `'23:59:59.999999'`},
		{`'23:59:59.999999':::time`, `'23:59:59.999998'`, valIsMax,
			`'00:00:00'`, `'23:59:59.999999'`},
		{`'24:00':::time`, `'23:59:59.999999'`, `'00:00:00.000001'`,
			`'00:00:00'`, `'23:59:59.999999'`},

		// Intervals
		{`'1 day':::interval`, noPrev, noNext,
			`'-768614336404564650 years -8 mons -9223372036854775808 days -2562047:47:16.854775'`,
			`'768614336404564650 years 7 mons 9223372036854775807 days 2562047:47:16.854775'`},
		// Max interval: we use Postgres syntax, because Go doesn't accept
		// months/days and ISO8601 doesn't accept nanoseconds.
		{`'9223372036854775807 months 9223372036854775807 days ` +
			`2562047 hours 47 minutes 16 seconds 854775 us':::interval`,
			noPrev, valIsMax,
			`'-768614336404564650 years -8 mons -9223372036854775808 days -2562047:47:16.854775'`,
			`'768614336404564650 years 7 mons 9223372036854775807 days 2562047:47:16.854775'`},
		{`'-9223372036854775808 months -9223372036854775808 days ` +
			`-2562047 h -47 m -16 s -854775 us':::interval`,
			valIsMin, noNext,
			`'-768614336404564650 years -8 mons -9223372036854775808 days -2562047:47:16.854775'`,
			`'768614336404564650 years 7 mons 9223372036854775807 days 2562047:47:16.854775'`},

		// UUIDs
		{`'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid`, `'ffffffff-ffff-ffff-ffff-fffffffffffe'`, valIsMax,
			`'00000000-0000-0000-0000-000000000000'`, `'ffffffff-ffff-ffff-ffff-ffffffffffff'`},
		{`'00000000-0000-0000-0000-000000000000'::uuid`, valIsMin, `'00000000-0000-0000-0000-000000000001'`,
			`'00000000-0000-0000-0000-000000000000'`, `'ffffffff-ffff-ffff-ffff-ffffffffffff'`},
		{`'ffffffff-ffff-ffff-0000-000000000000'::uuid`, `'ffffffff-ffff-fffe-ffff-ffffffffffff'`,
			`'ffffffff-ffff-ffff-0000-000000000001'`, `'00000000-0000-0000-0000-000000000000'`,
			`'ffffffff-ffff-ffff-ffff-ffffffffffff'`},
		{`'00000000-0000-0000-ffff-ffffffffffff'::uuid`, `'00000000-0000-0000-ffff-fffffffffffe'`,
			`'00000000-0000-0001-0000-000000000000'`, `'00000000-0000-0000-0000-000000000000'`,
			`'ffffffff-ffff-ffff-ffff-ffffffffffff'`},

		// INETs
		{`'0.0.0.0'::inet`, `'255.255.255.255/31'`, `'0.0.0.1'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'0.0.0.0/0'::inet`, noPrev, `'0.0.0.1/0'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'192.168.255.255'::inet`, `'192.168.255.254'`, `'192.169.0.0'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'127.0.0.1'::inet`, `'127.0.0.0'`, `'127.0.0.2'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'192.168.0.1/20'::inet`, `'192.168.0.0/20'`, `'192.168.0.2/20'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'192.168.0.0/20'::inet`, `'192.167.255.255/20'`, `'192.168.0.1/20'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'::ffff:1.2.3.4'::inet`, `'::ffff:1.2.3.3'`, `'::ffff:1.2.3.5'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'::0'::inet`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/127'`, `'::1'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'::0/0'::inet`, `'255.255.255.255'`, `'::1/0'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'255.255.255.255/32'::inet`, `'255.255.255.254'`, `'::/0'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'255.255.255.255/16'::inet`, `'255.255.255.254/16'`, `'0.0.0.0/17'`, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},
		{`'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128'::inet`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe'`, noNext, `'0.0.0.0/0'`, `'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'`},

		// NULL
		{`NULL`, valIsMin, valIsMax, `NULL`, `NULL`},

		// Tuples
		{`row()`, valIsMin, valIsMax, `()`, `()`},

		{`(NULL,)`, valIsMin, valIsMax, `(NULL,)`, `(NULL,)`},

		{`(true,)`, `(false,)`, valIsMax, `(false,)`, `(true,)`},
		{`(false,)`, valIsMin, `(true,)`, `(false,)`, `(true,)`},

		{`(true, false, false)`, `(false, true, true)`, `(true, false, true)`,
			`(false, false, false)`, `(true, true, true)`},
		{`(false, true, true)`, `(false, true, false)`, `(true, NULL, NULL)`,
			`(false, false, false)`, `(true, true, true)`},

		{`(0, 0)`, `(0, -1)`, `(0, 1)`,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},

		{`(0, 9223372036854775807)`,
			`(0, 9223372036854775806)`, `(1, NULL)`,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},
		{`(9223372036854775807, 9223372036854775807)`,
			`(9223372036854775807, 9223372036854775806)`, valIsMax,
			`(-9223372036854775808, -9223372036854775808)`,
			`(9223372036854775807, 9223372036854775807)`},

		{`(0, 0:::decimal)`, noPrev, noNext,
			`(-9223372036854775808, NaN)`,
			`(9223372036854775807, Infinity)`},
		{`(0:::decimal, 0)`, `(0, -1)`, `(0, 1)`,
			`(NaN, -9223372036854775808)`,
			`(Infinity, 9223372036854775807)`},

		{`(10, '')`, noPrev, `(10, e'\x00')`,
			`(-9223372036854775808, '')`, noMax},
		{`(-9223372036854775808, '')`, valIsMin, `(-9223372036854775808, e'\x00')`,
			`(-9223372036854775808, '')`, noMax},
		{`(-9223372036854775808, 'abc')`, noPrev, `(-9223372036854775808, e'abc\x00')`,
			`(-9223372036854775808, '')`, noMax},

		{`(10, NULL)`, `(9, NULL)`, `(11, NULL)`,
			`(-9223372036854775808, NULL)`, `(9223372036854775807, NULL)`},
		{`(NULL, 10)`, `(NULL, 9)`, `(NULL, 11)`,
			`(NULL, -9223372036854775808)`, `(NULL, 9223372036854775807)`},

		{`(true, NULL, false)`, `(false, NULL, true)`, `(true, NULL, true)`,
			`(false, NULL, false)`, `(true, NULL, true)`},
		{`(false, NULL, true)`, `(false, NULL, false)`, `(true, NULL, NULL)`,
			`(false, NULL, false)`, `(true, NULL, true)`},

		{`((true,), (false,))`, `((false,), (true,))`, `((true,), (true,))`,
			`((false,), (false,))`, `((true,), (true,))`},
		{`((false,), (true,))`, `((false,), (false,))`, `((true,), NULL)`,
			`((false,), (false,))`, `((true,), (true,))`},

		// Arrays

		{`'{}'::INT[]`, valIsMin, `ARRAY[NULL]`, `ARRAY[]`, noMax},

		{`array[NULL]`, noPrev, `ARRAY[NULL,NULL]`, `ARRAY[]`, noMax},
		{`array[true]`, noPrev, `ARRAY[true,NULL]`, `ARRAY[]`, noMax},

		// Mixed tuple/array datums.
		{`(ARRAY[true], (true,))`, `(ARRAY[true], (false,))`, `(ARRAY[true,NULL], NULL)`,
			`(ARRAY[], (false,))`, noMax},
		{`((false,), ARRAY[true])`, noPrev, `((false,), ARRAY[true,NULL])`,
			`((false,), ARRAY[])`, noMax},
	}
	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for _, td := range testData {
		expr := prepareExpr(t, td.datumExpr)

		d := expr.(tree.Datum)
		prevVal, hasPrev := d.Prev(ctx)
		nextVal, hasNext := d.Next(ctx)
		if td.prev == noPrev {
			if hasPrev {
				if !d.IsMin(ctx) {
					t.Errorf("%s: value should not have a prev, yet hasPrev true and IsMin() false (expected (!hasPrev || IsMin()))", td.datumExpr)
				}
			}
		} else {
			if !hasPrev && td.prev != valIsMin {
				t.Errorf("%s: hasPrev: got false, expected true", td.datumExpr)
				continue
			}
			isMin := d.IsMin(ctx)
			if isMin != (td.prev == valIsMin) {
				t.Errorf("%s: IsMin() %v, expected %v", td.datumExpr, isMin, (td.prev == valIsMin))
				continue
			}
			if !isMin {
				dPrev := prevVal.String()
				if dPrev != td.prev {
					t.Errorf("%s: Prev(): got %s, expected %s", td.datumExpr, dPrev, td.prev)
				}
			}
		}
		if td.next == noNext {
			if hasNext {
				if !d.IsMax(ctx) {
					t.Errorf("%s: value should not have a next, yet hasNext true and IsMax() false (expected (!hasNext || IsMax()))", td.datumExpr)
				}
			}
		} else {
			if !hasNext && td.next != valIsMax {
				t.Errorf("%s: HasNext(): got false, expected true", td.datumExpr)
				continue
			}
			isMax := d.IsMax(ctx)
			if isMax != (td.next == valIsMax) {
				t.Errorf("%s: IsMax() %v, expected %v", td.datumExpr, isMax, (td.next == valIsMax))
				continue
			}
			if !isMax {
				dNext := nextVal.String()
				if dNext != td.next {
					t.Errorf("%s: Next(): got %s, expected %s", td.datumExpr, dNext, td.next)
				}
			}
		}

		minVal, hasMin := d.Min(ctx)
		maxVal, hasMax := d.Max(ctx)

		if td.min == noMin {
			if hasMin {
				t.Errorf("%s: hasMin true, expected false", td.datumExpr)
			}
		} else {
			dMin := minVal.String()
			if dMin != td.min {
				t.Errorf("%s: min(): got %s, expected %s", td.datumExpr, dMin, td.min)
			}
		}
		if td.max == noMax {
			if hasMax {
				t.Errorf("%s: hasMax true, expected false", td.datumExpr)
			}
		} else {
			dMax := maxVal.String()
			if dMax != td.max {
				t.Errorf("%s: max(): got %s, expected %s", td.datumExpr, dMax, td.max)
			}
		}
	}
}

func TestDFloatCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	values := []tree.Datum{tree.DNull}
	for _, x := range []float64{math.NaN(), math.Inf(-1), -1, 0, 1, math.Inf(1)} {
		values = append(values, tree.NewDFloat(tree.DFloat(x)))
	}
	for i, x := range values {
		for j, y := range values {
			expected := 0
			if i < j {
				expected = -1
			} else if i > j {
				expected = 1
			}
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			got := x.Compare(evalCtx, y)
			if got != expected {
				t.Errorf("comparing DFloats %s and %s: expected %d, got %d", x, y, expected, got)
			}
		}
	}
}

// TestParseDIntervalWithField tests that the additional features available
// to tree.ParseDIntervalWithField beyond those in tree.ParseDInterval behave as expected.
func TestParseDIntervalWithField(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		str      string
		field    tree.DurationField
		expected string
	}{
		// Test cases for raw numbers with fields
		{"5", tree.Second, "5s"},
		{"5.8", tree.Second, "5.8s"},
		{"5", tree.Minute, "5m"},
		{"5.8", tree.Minute, "5m"},
		{"5", tree.Hour, "5h"},
		{"5.8", tree.Hour, "5h"},
		{"5", tree.Day, "5 day"},
		{"5.8", tree.Day, "5 day"},
		{"5", tree.Month, "5 month"},
		{"5.8", tree.Month, "5 month"},
		{"5", tree.Year, "5 year"},
		{"5.8", tree.Year, "5 year"},
		// Test cases for truncation based on fields
		{"1-2 3 4:56:07", tree.Second, "1-2 3 4:56:07"},
		{"1-2 3 4:56:07", tree.Minute, "1-2 3 4:56:00"},
		{"1-2 3 4:56:07", tree.Hour, "1-2 3 4:00:00"},
		{"1-2 3 4:56:07", tree.Day, "1-2 3 0:"},
		{"1-2 3 4:56:07", tree.Month, "1-2 0 0:"},
		{"1-2 3 4:56:07", tree.Year, "1 year"},
	}
	for _, td := range testData {
		actual, err := tree.ParseDIntervalWithField(td.str, td.field)
		if err != nil {
			t.Errorf("unexpected error while parsing INTERVAL %s %d: %s", td.str, td.field, err)
			continue
		}
		expected, err := tree.ParseDInterval(td.expected)
		if err != nil {
			t.Errorf("unexpected error while parsing expected value INTERVAL %s: %s", td.expected, err)
			continue
		}
		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer evalCtx.Stop(context.Background())
		if expected.Compare(evalCtx, actual) != 0 {
			t.Errorf("INTERVAL %s %v: got %s, expected %s", td.str, td.field, actual, expected)
		}
	}
}

func TestParseDDate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		str      string
		expected string
	}{
		{"2017-03-03 -01:00:00", "2017-03-03"},
		{"2017-03-03 -1:0:0", "2017-03-03"},
		{"2017-03-03 -01:00", "2017-03-03"},
		{"2017-03-03 -01", "2017-03-03"},
		{"2017-03-03 -010000", "2017-03-03"},
		{"2017-03-03 -0100", "2017-03-03"},
		{"2017-03-03 -1", "2017-03-03"},
		{"2017-03-03", "2017-03-03"},
		{"2017-3-3 -01:00:00", "2017-03-03"},
		{"2017-3-3 -1:0:0", "2017-03-03"},
		{"2017-3-3 -01:00", "2017-03-03"},
		{"2017-3-3 -01", "2017-03-03"},
		{"2017-3-3 -010000", "2017-03-03"},
		{"2017-3-3 -0100", "2017-03-03"},
		{"2017-3-3 -1", "2017-03-03"},
		{"2017-3-3", "2017-03-03"},
	}
	for _, td := range testData {
		actual, err := tree.ParseDDate(nil, td.str)
		if err != nil {
			t.Errorf("unexpected error while parsing DATE %s: %s", td.str, err)
			continue
		}
		expected, err := tree.ParseDDate(nil, td.expected)
		if err != nil {
			t.Errorf("unexpected error while parsing expected value DATE %s: %s", td.expected, err)
			continue
		}
		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer evalCtx.Stop(context.Background())
		if expected.Compare(evalCtx, actual) != 0 {
			t.Errorf("DATE %s: got %s, expected %s", td.str, actual, expected)
		}
	}
}

func TestParseDBool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		str      string
		expected *tree.DBool
		err      bool
	}{
		{str: "t", expected: tree.DBoolTrue},
		{str: "tr", expected: tree.DBoolTrue},
		{str: "tru", expected: tree.DBoolTrue},
		{str: "true", expected: tree.DBoolTrue},
		{str: "tr", expected: tree.DBoolTrue},
		{str: "TRUE", expected: tree.DBoolTrue},
		{str: "tRUe", expected: tree.DBoolTrue},
		{str: "  tRUe    ", expected: tree.DBoolTrue},
		{str: "  tR    ", expected: tree.DBoolTrue},
		{str: "on", expected: tree.DBoolTrue},
		{str: "On", expected: tree.DBoolTrue},
		{str: "oN", expected: tree.DBoolTrue},
		{str: "ON", expected: tree.DBoolTrue},
		{str: "1", expected: tree.DBoolTrue},
		{str: "yes", expected: tree.DBoolTrue},
		{str: "ye", expected: tree.DBoolTrue},
		{str: "y", expected: tree.DBoolTrue},

		{str: "false", expected: tree.DBoolFalse},
		{str: "FALSE", expected: tree.DBoolFalse},
		{str: "fALse", expected: tree.DBoolFalse},
		{str: "f", expected: tree.DBoolFalse},
		{str: "off", expected: tree.DBoolFalse},
		{str: "Off", expected: tree.DBoolFalse},
		{str: "oFF", expected: tree.DBoolFalse},
		{str: "OFF", expected: tree.DBoolFalse},
		{str: "0", expected: tree.DBoolFalse},

		{str: "foo", err: true},
		{str: "tr ue", err: true},
		{str: "o", err: true},
		{str: "", err: true},
		{str: " ", err: true},
		{str: "  ", err: true},
	}

	for _, td := range testData {
		t.Run(td.str, func(t *testing.T) {
			result, err := tree.ParseDBool(td.str)
			if td.err {
				if err == nil {
					t.Fatalf("expected parsing %v to error, got %v", td.str, result)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected parsing %v to be %s, got error: %s", td.str, td.expected, err)
			}
			if *td.expected != *result {
				t.Fatalf("expected parsing %v to be %s, got %s", td.str, td.expected, result)
			}
		})
	}
}

func TestParseDTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Since ParseDTime mostly delegates parsing logic to ParseDTimestamp, we only test a subset of
	// the timestamp test cases.
	testData := []struct {
		str      string
		expected timeofday.TimeOfDay
	}{
		{"04:05:06", timeofday.New(4, 5, 6, 0)},
		{"04:05:06.000001", timeofday.New(4, 5, 6, 1)},
		{"04:05:06-07", timeofday.New(4, 5, 6, 0)},
		{"4:5:6", timeofday.New(4, 5, 6, 0)},
		{"24:00:00", timeofday.Time2400},
		{"24:00:00.000", timeofday.Time2400},
		{"24:00:00.000000", timeofday.Time2400},
	}
	for _, td := range testData {
		actual, err := tree.ParseDTime(nil, td.str)
		if err != nil {
			t.Errorf("unexpected error while parsing TIME %s: %s", td.str, err)
			continue
		}
		if *actual != tree.DTime(td.expected) {
			t.Errorf("TIME %s: got %s, expected %s", td.str, actual, td.expected)
		}
	}
}

func TestParseDTimeError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []string{
		"",
		"foo",
		"01",
	}
	for _, s := range testData {
		actual, _ := tree.ParseDTime(nil, s)
		if actual != nil {
			t.Errorf("TIME %s: got %s, expected error", s, actual)
		}
	}
}

func TestParseDTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		str      string
		expected time.Time
	}{
		{"2001-02-03", time.Date(2001, time.February, 3, 0, 0, 0, 0, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.000001", time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.00001", time.Date(2001, time.February, 3, 4, 5, 6, 10000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.0001", time.Date(2001, time.February, 3, 4, 5, 6, 100000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.001", time.Date(2001, time.February, 3, 4, 5, 6, 1000000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.01", time.Date(2001, time.February, 3, 4, 5, 6, 10000000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.1", time.Date(2001, time.February, 3, 4, 5, 6, 100000000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.12", time.Date(2001, time.February, 3, 4, 5, 6, 120000000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.123", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.1234", time.Date(2001, time.February, 3, 4, 5, 6, 123400000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.12345", time.Date(2001, time.February, 3, 4, 5, 6, 123450000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.123456", time.Date(2001, time.February, 3, 4, 5, 6, 123456000, time.FixedZone("", 0))},
		{"2001-02-03 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000,
			time.FixedZone("", 0))},
		{"2001-02-03 04:05:06-07", time.Date(2001, time.February, 3, 4, 5, 6, 0,
			time.FixedZone("", 0))},
		{"2001-02-03 04:05:06-07:42", time.Date(2001, time.February, 3, 4, 5, 6, 0,
			time.FixedZone("", 0))},
		{"2001-02-03 04:05:06-07:30:09", time.Date(2001, time.February, 3, 4, 5, 6, 0,
			time.FixedZone("", 0))},
		{"2001-02-03 04:05:06+07", time.Date(2001, time.February, 3, 4, 5, 6, 0,
			time.FixedZone("", 0))},
		{"2001-02-03 04:0:06", time.Date(2001, time.February, 3, 4, 0, 6, 0,
			time.FixedZone("", 0))},
		{"2001-02-03 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0,
			time.FixedZone("", 0))},
		{"2001-02-03 4:05:0", time.Date(2001, time.February, 3, 4, 5, 0, 0,
			time.FixedZone("", 0))},
		{"2001-02-03 4:05:0-07:0:00", time.Date(2001, time.February, 3, 4, 5, 0, 0,
			time.FixedZone("", 0))},
		{"2001-02-03 4:0:6 +3:0:0", time.Date(2001, time.February, 3, 4, 0, 6, 0,
			time.FixedZone("", 0))},
	}
	for _, td := range testData {
		actual, err := tree.ParseDTimestamp(nil, td.str, time.Nanosecond)
		if err != nil {
			t.Errorf("unexpected error while parsing TIMESTAMP %s: %s", td.str, err)
			continue
		}
		if !actual.Time.Equal(td.expected) {
			t.Errorf("DATE %s: got %s, expected %s", td.str, actual, td.expected)
		}
	}
}

func TestMakeDJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()
	j1, err := tree.MakeDJSON(1)
	if err != nil {
		t.Fatal(err)
	}
	j2, err := tree.MakeDJSON(2)
	if err != nil {
		t.Fatal(err)
	}
	if j1.Compare(tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings()), j2) != -1 {
		t.Fatal("expected JSON 1 < 2")
	}
}

func TestIsDistinctFrom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		a        string // comma separated list of strings, `NULL` is converted to a NULL
		b        string // same as a
		expected bool
	}{
		{"a", "a", false},
		{"a", "b", true},
		{"b", "b", false},
		{"a,a", "a,a", false},
		{"a,a", "a,b", true},
		{"a,a", "b,a", true},
		{"a,a,a", "a,a,a", false},
		{"a,a,a", "a,a,b", true},
		{"a,a,a", "a,b,a", true},
		{"a,a,a", "a,b,b", true},
		{"a,a,a", "b,a,a", true},
		{"a,a,a", "b,a,b", true},
		{"a,a,a", "b,b,a", true},
		{"a,a,a", "b,b,b", true},
		{"NULL", "NULL", false},
		{"a", "NULL", true},
		{"a,a", "a,NULL", true},
		{"a,a", "NULL,a", true},
		{"a,a", "NULL,NULL", true},
		{"a,NULL", "a,a", true},
		{"a,NULL", "a,NULL", false},
		{"a,NULL", "NULL,a", true},
		{"a,NULL", "NULL,NULL", true},
		{"NULL,a", "a,a", true},
		{"NULL,a", "a,NULL", true},
		{"NULL,a", "NULL,a", false},
		{"NULL,a", "NULL,NULL", true},
		{"NULL,NULL", "a,a", true},
		{"NULL,NULL", "a,NULL", true},
		{"NULL,NULL", "NULL,a", true},
		{"NULL,NULL", "NULL,NULL", false},
		{"a,a,a", "a,a,NULL", true},
		{"a,a,a", "a,NULL,a", true},
		{"a,a,a", "a,NULL,NULL", true},
		{"a,a,a", "NULL,a,a", true},
		{"a,a,a", "NULL,a,NULL", true},
		{"a,a,a", "NULL,NULL,a", true},
		{"a,a,a", "NULL,NULL,NULL", true},
		{"a,NULL,a", "a,a,a", true},
		{"a,NULL,a", "a,a,NULL", true},
		{"a,NULL,a", "a,NULL,a", false},
		{"a,NULL,a", "a,NULL,NULL", true},
		{"a,NULL,a", "NULL,a,a", true},
		{"a,NULL,a", "NULL,a,NULL", true},
		{"a,NULL,a", "NULL,NULL,a", true},
		{"a,NULL,a", "NULL,NULL,NULL", true},
		{"NULL,a,NULL", "a,a,a", true},
		{"NULL,a,NULL", "a,a,NULL", true},
		{"NULL,a,NULL", "a,NULL,a", true},
		{"NULL,a,NULL", "a,NULL,NULL", true},
		{"NULL,a,NULL", "NULL,a,a", true},
		{"NULL,a,NULL", "NULL,a,NULL", false},
		{"NULL,a,NULL", "NULL,NULL,a", true},
		{"NULL,a,NULL", "NULL,NULL,NULL", true},
		{"NULL,NULL,NULL", "a,a,a", true},
		{"NULL,NULL,NULL", "a,a,NULL", true},
		{"NULL,NULL,NULL", "a,NULL,a", true},
		{"NULL,NULL,NULL", "a,NULL,NULL", true},
		{"NULL,NULL,NULL", "NULL,a,a", true},
		{"NULL,NULL,NULL", "NULL,a,NULL", true},
		{"NULL,NULL,NULL", "NULL,NULL,a", true},
		{"NULL,NULL,NULL", "NULL,NULL,NULL", false},
	}
	convert := func(s string) tree.Datums {
		splits := strings.Split(s, ",")
		result := make(tree.Datums, len(splits))
		for i, value := range splits {
			if value == "NULL" {
				result[i] = tree.DNull
				continue
			}
			result[i] = tree.NewDString(value)
		}
		return result
	}
	for _, td := range testData {
		t.Run(fmt.Sprintf("%s to %s", td.a, td.b), func(t *testing.T) {
			datumsA := convert(td.a)
			datumsB := convert(td.b)
			if e, a := td.expected, datumsA.IsDistinctFrom(&tree.EvalContext{}, datumsB); e != a {
				if e {
					t.Errorf("expected %s to be distinct from %s, but got %t", datumsA, datumsB, e)
				} else {
					t.Errorf("expected %s to not be distinct from %s, but got %t", datumsA, datumsB, e)
				}
			}
		})
	}
}

func TestAllTypesAsJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, typ := range types.Scalar {
		d := tree.SampleDatum(typ)
		_, err := tree.AsJSON(d)
		if err != nil {
			t.Errorf("couldn't convert %s to JSON: %s", d, err)
		}
	}
}
