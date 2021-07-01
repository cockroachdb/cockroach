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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareExpr(t *testing.T, datumExpr string) tree.Datum {
	expr, err := parser.ParseExpr(datumExpr)
	if err != nil {
		t.Fatalf("%s: %v", datumExpr, err)
	}
	// Type checking ensures constant folding is performed and type
	// annotations have come into effect.
	ctx := context.Background()
	sema := tree.MakeSemaContext()
	typedExpr, err := tree.TypeCheck(ctx, expr, &sema, types.Any)
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
	d, err := typedExpr.Eval(evalCtx)
	if err != nil {
		t.Fatalf("%s: %v", datumExpr, err)
	}
	return d
}

func TestDatumOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
			`'2006-01-02 03:04:05.123122'`, `'2006-01-02 03:04:05.123124'`, `'-4713-11-24 00:00:00'`, `'294276-12-31 23:59:59.999999'`},

		// Geospatial types
		{`'BOX(1 2,3 4)'::box2d`, noPrev, noNext, noMin, noMax},
		{`'POINT(1.0 1.0)'::geometry`, noPrev, noNext, noMin, noMax},
		{`'POINT(1.0 1.0)'::geography`, noPrev, noNext, noMin, noMax},

		// Times
		{`'00:00:00':::time`, valIsMin, `'00:00:00.000001'`,
			`'00:00:00'`, `'24:00:00'`},
		{`'12:00:00':::time`, `'11:59:59.999999'`, `'12:00:00.000001'`,
			`'00:00:00'`, `'24:00:00'`},
		{`'24:00:00':::time`, `'23:59:59.999999'`, valIsMax, `'00:00:00'`, `'24:00:00'`},

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
		d := prepareExpr(t, td.datumExpr)

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
	defer log.Scope(t).Close(t)
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

// TestParseDIntervalWithTypeMetadata tests that the additional features available
// to tree.ParseDIntervalWithTypeMetadata beyond those in tree.ParseDInterval behave as expected.
func TestParseDIntervalWithTypeMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		second = types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_SECOND,
			},
		}
		minute = types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_MINUTE,
			},
		}
		hour = types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_HOUR,
			},
		}
		day = types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_DAY,
			},
		}
		month = types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_MONTH,
			},
		}
		year = types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_YEAR,
			},
		}
	)

	testData := []struct {
		str      string
		dtype    types.IntervalTypeMetadata
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
		actual, err := tree.ParseDIntervalWithTypeMetadata(td.str, td.dtype)
		if err != nil {
			t.Errorf("unexpected error while parsing INTERVAL %s %#v: %s", td.str, td.dtype, err)
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
			t.Errorf("INTERVAL %s %#v: got %s, expected %s", td.str, td.dtype, actual, expected)
		}
	}
}

func TestParseDDate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := testParseTimeContext(
		time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.FixedZone("foo", -18000)),
	)

	testData := []struct {
		str              string
		expected         string
		expectedDepOnCtx bool
	}{
		{"now", "2001-02-03", true},
		{"today", "2001-02-03", true},
		{"tomorrow", "2001-02-04", true},
		{"yesterday", "2001-02-02", true},
		{"2017-03-03 01:00:00.00000", "2017-03-03", false},
		{"2017-03-03 01:00:00.00000-05", "2017-03-03", false},
		{"2017-03-03 01:00:00.00000+05", "2017-03-03", false},
		{"2017-03-03 -01:00:00", "2017-03-03", false},
		{"2017-03-03 -01:00:00 America/New_York", "2017-03-03", false},
		{"2017-03-03 -1:0:0", "2017-03-03", false},
		{"2017-03-03 -01:00", "2017-03-03", false},
		{"2017-03-03 -01", "2017-03-03", false},
		{"2017-03-03 -010000", "2017-03-03", false},
		{"2017-03-03 -0100", "2017-03-03", false},
		{"2017-03-03 -1", "2017-03-03", false},
		{"2017-03-03", "2017-03-03", false},
		{"2017-3-3 -01:00:00", "2017-03-03", false},
		{"2017-3-3 -1:0:0", "2017-03-03", false},
		{"2017-3-3 -01:00", "2017-03-03", false},
		{"2017-3-3 -01", "2017-03-03", false},
		{"2017-3-3 -010000", "2017-03-03", false},
		{"2017-3-3 -0100", "2017-03-03", false},
		{"2017-3-3 -1", "2017-03-03", false},
		{"2017-3-3", "2017-03-03", false},
	}
	for _, td := range testData {
		actual, depOnCtx, err := tree.ParseDDate(ctx, td.str)
		if err != nil {
			t.Errorf("unexpected error while parsing DATE %s: %s", td.str, err)
			continue
		}
		expected, _, err := tree.ParseDDate(nil, td.expected)
		if err != nil {
			t.Errorf("unexpected error while parsing expected value DATE %s: %s", td.expected, err)
			continue
		}
		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer evalCtx.Stop(context.Background())
		if expected.Compare(evalCtx, actual) != 0 {
			t.Errorf("DATE %s: got %s, expected %s", td.str, actual, expected)
		}
		if td.expectedDepOnCtx != depOnCtx {
			t.Errorf("DATE %s: expected depOnCtx=%v", td.str, td.expectedDepOnCtx)
		}
	}
}

func TestParseDBool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)

	ctx := testParseTimeContext(
		time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.FixedZone("foo", -18000)),
	)
	// Since ParseDTime shares most of the underlying parsing logic to
	// ParseDTimestamp, we only test a subset of the timestamp test cases.
	testData := []struct {
		str              string
		precision        time.Duration
		expected         timeofday.TimeOfDay
		expectedDepOnCtx bool
	}{
		{"now", time.Microsecond, timeofday.New(4, 5, 6, 1), true},
		{" 04:05:06 ", time.Microsecond, timeofday.New(4, 5, 6, 0), false},
		{"04:05:06", time.Microsecond, timeofday.New(4, 5, 6, 0), false},
		{"04:05:06.000001", time.Microsecond, timeofday.New(4, 5, 6, 1), false},
		{"04:05:06.000001+00", time.Microsecond, timeofday.New(4, 5, 6, 1), false},
		{"04:05:06.000001-05", time.Microsecond, timeofday.New(4, 5, 6, 1), false},
		{"04:05:06.000001+05", time.Microsecond, timeofday.New(4, 5, 6, 1), false},
		{"04:05:06.000001", time.Second, timeofday.New(4, 5, 6, 0), false},
		{"04:05:06-07", time.Microsecond, timeofday.New(4, 5, 6, 0), false},
		{"0000-01-01 04:05:06", time.Microsecond, timeofday.New(4, 5, 6, 0), false},
		{"2001-01-01 04:05:06", time.Microsecond, timeofday.New(4, 5, 6, 0), false},
		{"4:5:6", time.Microsecond, timeofday.New(4, 5, 6, 0), false},
		{"24:00:00", time.Microsecond, timeofday.Time2400, false},
		{"24:00:00.000", time.Microsecond, timeofday.Time2400, false},
		{"24:00:00.000000", time.Microsecond, timeofday.Time2400, false},
		{"0000-01-01T24:00:00", time.Microsecond, timeofday.Time2400, false},
		{"0000-01-01T24:00:00.0", time.Microsecond, timeofday.Time2400, false},
		{"0000-01-01 24:00:00", time.Microsecond, timeofday.Time2400, false},
		{"0000-01-01 24:00:00.0", time.Microsecond, timeofday.Time2400, false},
		{" 24:00:00.0", time.Microsecond, timeofday.Time2400, false},
		{" 24:00:00.0  ", time.Microsecond, timeofday.Time2400, false},
	}
	for _, td := range testData {
		actual, depOnCtx, err := tree.ParseDTime(ctx, td.str, td.precision)
		if err != nil {
			t.Errorf("unexpected error while parsing TIME %s: %s", td.str, err)
			continue
		}
		if *actual != tree.DTime(td.expected) {
			t.Errorf("TIME %s: got %s, expected %s", td.str, actual, td.expected)
		}
		if td.expectedDepOnCtx != depOnCtx {
			t.Errorf("TIME %s: expected depOnCtx=%v", td.str, td.expectedDepOnCtx)
		}
	}
}

func TestParseDTimeError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []string{
		"",
		"foo",
		"01",
		"today",
		"yesterday",

		// TODO(radu): these exceptions seem dubious. They work in postgres.
		"24:00:00.000000+00",
		"24:00:00.000000-05",
		"24:00:00.000000+05",
	}
	for _, s := range testData {
		actual, _, _ := tree.ParseDTime(nil, s, time.Microsecond)
		if actual != nil {
			t.Errorf("TIME %s: got %s, expected error", s, actual)
		}
	}
}

func TestParseDTimeTZ(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := testParseTimeContext(
		time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.FixedZone("foo", 18000)),
	)

	mk := func(hour, min, sec, micro int, offset int32) timetz.TimeTZ {
		return timetz.MakeTimeTZ(timeofday.New(hour, min, sec, micro), offset)
	}

	testData := []struct {
		str              string
		precision        time.Duration
		expected         timetz.TimeTZ
		expectedDepOnCtx bool
	}{
		{" 04:05:06 ", time.Microsecond, mk(4, 5, 6, 0, -18000), true},
		{"04:05:06", time.Microsecond, mk(4, 5, 6, 0, -18000), true},
		{"04:05:06.000001", time.Microsecond, mk(4, 5, 6, 1, -18000), true},
		{"04:05:06.000001", time.Second, mk(4, 5, 6, 0, -18000), true},
		{"04:05:06.000001+00", time.Microsecond, mk(4, 5, 6, 1, 0), false},
		{"04:05:06.000001-04", time.Microsecond, mk(4, 5, 6, 1, 4*3600), false},
		{"04:05:06.000001+04", time.Microsecond, mk(4, 5, 6, 1, -4*3600), false},
		{"04:05:06-07", time.Microsecond, mk(4, 5, 6, 0, 7*3600), false},
		{"0000-01-01 04:05:06", time.Microsecond, mk(4, 5, 6, 0, -18000), true},
		{"2001-01-01 04:05:06", time.Microsecond, mk(4, 5, 6, 0, -18000), true},
		{"4:5:6", time.Microsecond, mk(4, 5, 6, 0, -18000), true},
		{"24:00:00", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
		{"24:00:00.000", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
		{"24:00:00.000000", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
		{"24:00:00.000000+00", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, 0), false},
		{"24:00:00.000000-04", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, 4*3600), false},
		{"24:00:00.000000+04", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -4*3600), false},
		{"0000-01-01T24:00:00", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
		{"0000-01-01T24:00:00.0", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
		{"0000-01-01 24:00:00", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
		{"0000-01-01 24:00:00.0", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
		{" 24:00:00.0", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
		{" 24:00:00.0  ", time.Microsecond, timetz.MakeTimeTZ(timeofday.Time2400, -18000), true},
	}
	for _, td := range testData {
		actual, depOnCtx, err := tree.ParseDTimeTZ(ctx, td.str, td.precision)
		if err != nil {
			t.Errorf("unexpected error while parsing TIME %s: %s", td.str, err)
			continue
		}
		exp := tree.DTimeTZ{TimeTZ: td.expected}
		if *actual != exp {
			t.Errorf("TIMETZ %s: got %s, expected %s", td.str, actual, &exp)
		}
		if td.expectedDepOnCtx != depOnCtx {
			t.Errorf("TIME %s: expected depOnCtx=%v", td.str, td.expectedDepOnCtx)
		}
	}
}

func TestParseDTimeTZError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []string{
		"",
		"foo",
		"01",
		"today",
		"yesterday",

		// TODO(radu): this should work.
		"now",
	}
	for _, s := range testData {
		actual, _, _ := tree.ParseDTimeTZ(nil, s, time.Microsecond)
		if actual != nil {
			t.Errorf("TIMETZ %s: got %s, expected error", s, actual)
		}
	}
}

func TestParseDTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := testParseTimeContext(
		time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.FixedZone("foo", -18000)),
	)

	testData := []struct {
		str              string
		expected         time.Time
		expectedDepOnCtx bool
	}{
		{"now", time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.UTC), true},
		{"today", time.Date(2001, time.February, 3, 0, 0, 0, 0, time.UTC), true},
		{"tomorrow", time.Date(2001, time.February, 4, 0, 0, 0, 0, time.UTC), true},
		{"yesterday", time.Date(2001, time.February, 2, 0, 0, 0, 0, time.UTC), true},
		{"2001-02-03", time.Date(2001, time.February, 3, 0, 0, 0, 0, time.UTC), false},
		{"2001-02-03 04:05:06", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.UTC), false},
		{"2001-02-03 04:05:06.000001", time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.UTC), false},
		{"2001-02-03 04:05:06.00001", time.Date(2001, time.February, 3, 4, 5, 6, 10000, time.UTC), false},
		{"2001-02-03 04:05:06.0001", time.Date(2001, time.February, 3, 4, 5, 6, 100000, time.UTC), false},
		{"2001-02-03 04:05:06.001", time.Date(2001, time.February, 3, 4, 5, 6, 1000000, time.UTC), false},
		{"2001-02-03 04:05:06.01", time.Date(2001, time.February, 3, 4, 5, 6, 10000000, time.UTC), false},
		{"2001-02-03 04:05:06.1", time.Date(2001, time.February, 3, 4, 5, 6, 100000000, time.UTC), false},
		{"2001-02-03 04:05:06.12", time.Date(2001, time.February, 3, 4, 5, 6, 120000000, time.UTC), false},
		{"2001-02-03 04:05:06.123", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.UTC), false},
		{"2001-02-03 04:05:06.1234", time.Date(2001, time.February, 3, 4, 5, 6, 123400000, time.UTC), false},
		{"2001-02-03 04:05:06.12345", time.Date(2001, time.February, 3, 4, 5, 6, 123450000, time.UTC), false},
		{"2001-02-03 04:05:06.123456", time.Date(2001, time.February, 3, 4, 5, 6, 123456000, time.UTC), false},
		{"2001-02-03 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.UTC), false},
		{"2001-02-03 04:05:06-07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.UTC), false},
		{"2001-02-03 04:05:06-07:42", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.UTC), false},
		{"2001-02-03 04:05:06-07:30:09", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.UTC), false},
		{"2001-02-03 04:05:06+07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.UTC), false},
		{"2001-02-03 04:0:06", time.Date(2001, time.February, 3, 4, 0, 6, 0, time.UTC), false},
		{"2001-02-03 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0, time.UTC), false},
		{"2001-02-03 4:05:0", time.Date(2001, time.February, 3, 4, 5, 0, 0, time.UTC), false},
		{"2001-02-03 4:05:0-07:0:00", time.Date(2001, time.February, 3, 4, 5, 0, 0, time.UTC), false},
		{"2001-02-03 4:0:6 +3:0:0", time.Date(2001, time.February, 3, 4, 0, 6, 0, time.UTC), false},
		{"12-04-2011 04:05:06.123", time.Date(2011, time.December, 4, 4, 5, 6, 123000000, time.UTC), false},
		{"12-04-2011 04:05", time.Date(2011, time.December, 4, 4, 5, 0, 0, time.UTC), false},
		{"12-04-11 04:05", time.Date(2011, time.December, 4, 4, 5, 0, 0, time.UTC), false},
		{"12-4-11 04:05", time.Date(2011, time.December, 4, 4, 5, 0, 0, time.UTC), false},
		{"12/4/2011 4:0:6 +3:0:0", time.Date(2011, time.December, 4, 4, 0, 6, 0, time.UTC), false},
		{"12/4/11 4:0:6 +3:0:0", time.Date(2011, time.December, 4, 4, 0, 6, 0, time.UTC), false},
		{"2001/02/03 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0, time.UTC), false},
		{"2001/02/03 4:05:0", time.Date(2001, time.February, 3, 4, 5, 0, 0, time.UTC), false},
		{"2001/02/03 4:05:0-07:0:00", time.Date(2001, time.February, 3, 4, 5, 0, 0, time.UTC), false},
		{"2001/02/03 4:0:6 +3:0:0", time.Date(2001, time.February, 3, 4, 0, 6, 0, time.UTC), false},
		{"2001/2/03 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0, time.UTC), false},
		{"2001/02/3 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0, time.UTC), false},
		{"01/02/03 0:0:06", time.Date(2003, time.January, 2, 0, 0, 6, 0, time.UTC), false},
		{"1/2/3 0:0:06", time.Date(2003, time.January, 2, 0, 0, 6, 0, time.UTC), false},
		{"10-08-2019 15:03", time.Date(2019, time.October, 8, 15, 3, 0, 0, time.UTC), false},
		{"10-08-2019 15:03:10", time.Date(2019, time.October, 8, 15, 3, 10, 0, time.UTC), false},
		{"10-08-19 15:03", time.Date(2019, time.October, 8, 15, 3, 0, 0, time.UTC), false},
		{"10-08-19 15:03:10", time.Date(2019, time.October, 8, 15, 3, 10, 0, time.UTC), false},
		{"5-06-19 15:03", time.Date(2019, time.May, 6, 15, 3, 0, 0, time.UTC), false},
		{"05-6-19 15:03", time.Date(2019, time.May, 6, 15, 3, 0, 0, time.UTC), false},
		{"05-06-19 15:03", time.Date(2019, time.May, 6, 15, 3, 0, 0, time.UTC), false},
		{"05-06-2019 15:03", time.Date(2019, time.May, 6, 15, 3, 0, 0, time.UTC), false},
		{"02-03-2001 04:05:06.1", time.Date(2001, time.February, 3, 4, 5, 6, 100000000, time.UTC), false},
		{"02-03-2001 04:05:06.12", time.Date(2001, time.February, 3, 4, 5, 6, 120000000, time.UTC), false},
		{"02-03-2001 04:05:06.123", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.UTC), false},
		{"02-03-2001 04:05:06.1234", time.Date(2001, time.February, 3, 4, 5, 6, 123400000, time.UTC), false},
		{"02-03-2001 04:05:06.12345", time.Date(2001, time.February, 3, 4, 5, 6, 123450000, time.UTC), false},
		{"02-03-2001 04:05:06.123456", time.Date(2001, time.February, 3, 4, 5, 6, 123456000, time.UTC), false},
	}
	for _, td := range testData {
		actual, depOnCtx, err := tree.ParseDTimestamp(ctx, td.str, time.Nanosecond)
		if err != nil {
			t.Errorf("unexpected error while parsing TIMESTAMP %s: %s", td.str, err)
			continue
		}
		if !actual.Time.Equal(td.expected) {
			t.Errorf("TIMESTAMP %s: got %s, expected %s", td.str, actual, td.expected)
		}
		if td.expectedDepOnCtx != depOnCtx {
			t.Errorf("TIMESTAMP %s: expected depOnCtx=%v", td.str, td.expectedDepOnCtx)
		}
	}
}

func TestParseDTimestampTZ(t *testing.T) {
	defer leaktest.AfterTest(t)()

	local := time.FixedZone("foo", -18000)
	ctx := testParseTimeContext(time.Date(2001, time.February, 3, 4, 5, 6, 1000, local))

	testData := []struct {
		str              string
		expected         time.Time
		expectedDepOnCtx bool
	}{
		{"now", time.Date(2001, time.February, 3, 4, 5, 6, 1000, local), true},
		{"today", time.Date(2001, time.February, 3, 0, 0, 0, 0, local), true},
		{"tomorrow", time.Date(2001, time.February, 4, 0, 0, 0, 0, local), true},
		{"yesterday", time.Date(2001, time.February, 2, 0, 0, 0, 0, local), true},
		{"2001-02-03", time.Date(2001, time.February, 3, 0, 0, 0, 0, local), true},
		{"2001-02-03 04:05:06", time.Date(2001, time.February, 3, 4, 5, 6, 0, local), true},
		{"2001-02-03 04:05:06.000001", time.Date(2001, time.February, 3, 4, 5, 6, 1000, local), true},
		{"2001-02-03 04:05:06.00001", time.Date(2001, time.February, 3, 4, 5, 6, 10000, local), true},
		{"2001-02-03 04:05:06.0001", time.Date(2001, time.February, 3, 4, 5, 6, 100000, local), true},
		{"2001-02-03 04:05:06.001", time.Date(2001, time.February, 3, 4, 5, 6, 1000000, local), true},
		{"2001-02-03 04:05:06.01", time.Date(2001, time.February, 3, 4, 5, 6, 10000000, local), true},
		{"2001-02-03 04:05:06.1", time.Date(2001, time.February, 3, 4, 5, 6, 100000000, local), true},
		{"2001-02-03 04:05:06.12", time.Date(2001, time.February, 3, 4, 5, 6, 120000000, local), true},
		{"2001-02-03 04:05:06.123", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, local), true},
		{"2001-02-03 04:05:06.1234", time.Date(2001, time.February, 3, 4, 5, 6, 123400000, local), true},
		{"2001-02-03 04:05:06.12345", time.Date(2001, time.February, 3, 4, 5, 6, 123450000, local), true},
		{"2001-02-03 04:05:06.123456", time.Date(2001, time.February, 3, 4, 5, 6, 123456000, local), true},
		{"2001-02-03 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", -7*3600)), false},
		{"2001-02-03 04:05:06-07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -7*3600)), false},
		{"2001-02-03 04:05:06-07:42", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -7*3600-42*60)), false},
		{"2001-02-03 04:05:06-07:30:09", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -7*3600-30*60-9)), false},
		{"2001-02-03 04:05:06+07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", 7*3600)), false},
		{"2001-02-03 04:0:06", time.Date(2001, time.February, 3, 4, 0, 6, 0, local), true},
		{"2001-02-03 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0, local), true},
		{"2001-02-03 4:05:0", time.Date(2001, time.February, 3, 4, 5, 0, 0, local), true},
		{"2001-02-03 4:05:0-07:0:00", time.Date(2001, time.February, 3, 4, 5, 0, 0, time.FixedZone("", -7*3600)), false},
		{"2001-02-03 4:0:6 +3:0:0", time.Date(2001, time.February, 3, 4, 0, 6, 0, time.FixedZone("", 3*3600)), false},
		{"12-04-2011 04:05:06.123", time.Date(2011, time.December, 4, 4, 5, 6, 123000000, local), true},
		{"12-04-2011 04:05", time.Date(2011, time.December, 4, 4, 5, 0, 0, local), true},
		{"12-04-11 04:05", time.Date(2011, time.December, 4, 4, 5, 0, 0, local), true},
		{"12-4-11 04:05", time.Date(2011, time.December, 4, 4, 5, 0, 0, local), true},
		{"12/4/2011 4:0:6", time.Date(2011, time.December, 4, 4, 0, 6, 0, local), true},
		{"12/4/11 4:0:6", time.Date(2011, time.December, 4, 4, 0, 6, 0, local), true},
		{"2001/02/03 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0, local), true},
		{"2001/02/03 4:05:0", time.Date(2001, time.February, 3, 4, 5, 0, 0, local), true},
		{"2001/02/03 4:0:6", time.Date(2001, time.February, 3, 4, 0, 6, 0, local), true},
		{"2001/2/03 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0, local), true},
		{"2001/02/3 0:0:06", time.Date(2001, time.February, 3, 0, 0, 6, 0, local), true},
		{"01/02/03 0:0:06", time.Date(2003, time.January, 2, 0, 0, 6, 0, local), true},
		{"1/2/3 0:0:06", time.Date(2003, time.January, 2, 0, 0, 6, 0, local), true},
		{"10-08-2019 15:03", time.Date(2019, time.October, 8, 15, 3, 0, 0, local), true},
		{"10-08-2019 15:03:10", time.Date(2019, time.October, 8, 15, 3, 10, 0, local), true},
		{"10-08-19 15:03", time.Date(2019, time.October, 8, 15, 3, 0, 0, local), true},
		{"10-08-19 15:03:10", time.Date(2019, time.October, 8, 15, 3, 10, 0, local), true},
		{"5-06-19 15:03", time.Date(2019, time.May, 6, 15, 3, 0, 0, local), true},
		{"05-6-19 15:03", time.Date(2019, time.May, 6, 15, 3, 0, 0, local), true},
		{"05-06-19 15:03", time.Date(2019, time.May, 6, 15, 3, 0, 0, local), true},
		{"05-06-2019 15:03", time.Date(2019, time.May, 6, 15, 3, 0, 0, local), true},
		{"02-03-2001 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", -7*3600)), false},
		{"02-03-2001 04:05:06-07", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -7*3600)), false},
		{"02-03-2001 04:05:06-07:42", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -7*3600-42*60)), false},
		{"02-03-2001 04:05:06-07:30:09", time.Date(2001, time.February, 3, 4, 5, 6, 0, time.FixedZone("", -7*3600-30*60-9)), false},
	}
	for _, td := range testData {
		actual, depOnCtx, err := tree.ParseDTimestampTZ(ctx, td.str, time.Nanosecond)
		if err != nil {
			t.Errorf("unexpected error while parsing TIMESTAMP %s: %s", td.str, err)
			continue
		}
		if !actual.Time.Equal(td.expected) {
			t.Errorf("TIMESTAMPTZ %s: got %s, expected %s", td.str, actual, td.expected)
		}
		if td.expectedDepOnCtx != depOnCtx {
			t.Errorf("TIMESTAMPTZ %s: expected depOnCtx=%v", td.str, td.expectedDepOnCtx)
		}
	}
}

func TestMakeDJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

func TestDTimeTZ(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := &tree.EvalContext{
		SessionData: &sessiondata.SessionData{
			Location: time.UTC,
		},
	}

	maxTime, depOnCtx, err := tree.ParseDTimeTZ(ctx, "24:00:00-1559", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)
	minTime, depOnCtx, err := tree.ParseDTimeTZ(ctx, "00:00:00+1559", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)

	// These are all the same UTC time equivalents.
	utcTime, depOnCtx, err := tree.ParseDTimeTZ(ctx, "11:14:15+0", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)
	sydneyTime, depOnCtx, err := tree.ParseDTimeTZ(ctx, "21:14:15+10", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)

	// No daylight savings in Hawaii!
	hawaiiZone, err := timeutil.LoadLocation("Pacific/Honolulu")
	require.NoError(t, err)
	hawaiiTime := tree.NewDTimeTZFromLocation(timeofday.New(1, 14, 15, 0), hawaiiZone)

	weirdTimeZone := tree.NewDTimeTZFromOffset(timeofday.New(10, 0, 0, 0), -((5 * 60 * 60) + 30*60 + 15))

	testCases := []struct {
		t           *tree.DTimeTZ
		largerThan  []tree.Datum
		smallerThan []tree.Datum
		equalTo     []tree.Datum
		isMax       bool
		isMin       bool
	}{
		{
			t:           weirdTimeZone,
			largerThan:  []tree.Datum{minTime, tree.DNull},
			smallerThan: []tree.Datum{maxTime},
			equalTo:     []tree.Datum{weirdTimeZone},
			isMax:       false,
			isMin:       false,
		},
		{
			t:           utcTime,
			largerThan:  []tree.Datum{minTime, sydneyTime, tree.DNull},
			smallerThan: []tree.Datum{maxTime, hawaiiTime},
			equalTo:     []tree.Datum{utcTime},
			isMax:       false,
			isMin:       false,
		},
		{
			t:           sydneyTime,
			largerThan:  []tree.Datum{minTime, tree.DNull},
			smallerThan: []tree.Datum{maxTime, utcTime, hawaiiTime},
			equalTo:     []tree.Datum{sydneyTime},
			isMax:       false,
			isMin:       false,
		},
		{
			t:           hawaiiTime,
			largerThan:  []tree.Datum{minTime, utcTime, sydneyTime, tree.DNull},
			smallerThan: []tree.Datum{maxTime},
			equalTo:     []tree.Datum{hawaiiTime},
			isMax:       false,
			isMin:       false,
		},
		{
			t:           minTime,
			largerThan:  []tree.Datum{tree.DNull},
			smallerThan: []tree.Datum{maxTime, utcTime, sydneyTime, hawaiiTime},
			equalTo:     []tree.Datum{minTime},
			isMax:       false,
			isMin:       true,
		},
		{
			t:           maxTime,
			largerThan:  []tree.Datum{minTime, utcTime, sydneyTime, hawaiiTime, tree.DNull},
			smallerThan: []tree.Datum{},
			equalTo:     []tree.Datum{maxTime},
			isMax:       true,
			isMin:       false,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d %s", i, tc.t.String()), func(t *testing.T) {
			var largerThan []tree.Datum
			prev, ok := tc.t.Prev(ctx)
			if !tc.isMin {
				assert.True(t, ok)
				largerThan = append(largerThan, prev)
			} else {
				assert.False(t, ok)
			}
			for _, largerThan := range append(largerThan, tc.largerThan...) {
				assert.Equal(t, 1, tc.t.Compare(ctx, largerThan), "%s > %s", tc.t.String(), largerThan.String())
			}

			var smallerThan []tree.Datum
			next, ok := tc.t.Next(ctx)
			if !tc.isMax {
				assert.True(t, ok)
				smallerThan = append(smallerThan, next)
			} else {
				assert.False(t, ok)
			}
			for _, smallerThan := range append(smallerThan, tc.smallerThan...) {
				assert.Equal(t, -1, tc.t.Compare(ctx, smallerThan), "%s < %s", tc.t.String(), smallerThan.String())
			}

			for _, equalTo := range tc.equalTo {
				assert.Equal(t, 0, tc.t.Compare(ctx, equalTo), "%s = %s", tc.t.String(), equalTo.String())
			}

			assert.Equal(t, tc.isMax, tc.t.IsMax(ctx))
			assert.Equal(t, tc.isMin, tc.t.IsMin(ctx))
		})
	}
}

func TestIsDistinctFrom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)
	for _, typ := range types.Scalar {
		d := tree.SampleDatum(typ)
		_, err := tree.AsJSON(
			d,
			sessiondatapb.DataConversionConfig{},
			time.UTC,
		)
		if err != nil {
			t.Errorf("couldn't convert %s to JSON: %s", d, err)
		}
	}
}

// Test default values of many different datum types.
func TestNewDefaultDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	testCases := []struct {
		t        *types.T
		expected string
	}{
		{t: types.Bool, expected: "false"},
		{t: types.Int, expected: "0:::INT8"},
		{t: types.Int2, expected: "0:::INT8"},
		{t: types.Int4, expected: "0:::INT8"},
		{t: types.Float, expected: "0.0:::FLOAT8"},
		{t: types.Float4, expected: "0.0:::FLOAT8"},
		{t: types.Decimal, expected: "0:::DECIMAL"},
		{t: types.MakeDecimal(10, 5), expected: "0:::DECIMAL"},
		{t: types.Date, expected: "'2000-01-01':::DATE"},
		{t: types.Timestamp, expected: "'0001-01-01 00:00:00':::TIMESTAMP"},
		{t: types.Interval, expected: "'00:00:00':::INTERVAL"},
		{t: types.String, expected: "'':::STRING"},
		{t: types.MakeChar(3), expected: "'':::STRING"},
		{t: types.Bytes, expected: "'\\x':::BYTES"},
		{t: types.TimestampTZ, expected: "'0001-01-01 00:00:00+00:00':::TIMESTAMPTZ"},
		{t: types.MakeCollatedString(types.MakeVarChar(10), "de"), expected: "'' COLLATE de"},
		{t: types.MakeCollatedString(types.VarChar, "en_US"), expected: "'' COLLATE en_US"},
		{t: types.Oid, expected: "26:::OID"},
		{t: types.RegClass, expected: "crdb_internal.create_regclass(2205,'regclass'):::REGCLASS"},
		{t: types.Unknown, expected: "NULL"},
		{t: types.Uuid, expected: "'00000000-0000-0000-0000-000000000000':::UUID"},
		{t: types.MakeArray(types.Int), expected: "ARRAY[]:::INT8[]"},
		{t: types.MakeArray(types.MakeArray(types.String)), expected: "ARRAY[]:::STRING[][]"},
		{t: types.OidVector, expected: "ARRAY[]:::OID[]"},
		{t: types.INet, expected: "'0.0.0.0/0':::INET"},
		{t: types.Time, expected: "'00:00:00':::TIME"},
		{t: types.Jsonb, expected: "'null':::JSONB"},
		{t: types.TimeTZ, expected: "'00:00:00+00:00:00':::TIMETZ"},
		{t: types.MakeTuple([]*types.T{}), expected: "()"},
		{t: types.MakeTuple([]*types.T{types.Int, types.MakeChar(1)}), expected: "(0:::INT8, '':::STRING)"},
		{t: types.MakeTuple([]*types.T{types.OidVector, types.MakeTuple([]*types.T{types.Float})}), expected: "(ARRAY[]:::OID[], (0.0:::FLOAT8,))"},
		{t: types.VarBit, expected: "B''"},
		{t: types.MakeBit(5), expected: "B''"},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d %s", i, tc.t.SQLString()), func(t *testing.T) {
			datum, err := tree.NewDefaultDatum(evalCtx, tc.t)
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			actual := tree.AsStringWithFlags(datum, tree.FmtCheckEquivalence)
			if actual != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, actual)
			}
		})
	}
}

type testParseTimeContext time.Time

var _ tree.ParseTimeContext = testParseTimeContext{}

func (t testParseTimeContext) GetRelativeParseTime() time.Time {
	return time.Time(t)
}

func TestGeospatialSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		wkt      string
		expected uintptr
	}{
		{"SRID=4004;POINT EMPTY", 73},
		{"SRID=4326;LINESTRING(0 0, 10 0)", 125},
	}

	for _, tc := range testCases {
		t.Run(tc.wkt, func(t *testing.T) {
			t.Run("geometry", func(t *testing.T) {
				g, err := tree.ParseDGeometry(tc.wkt)
				require.NoError(t, err)
				require.Equal(t, tc.expected, g.Size())
			})
			t.Run("geography", func(t *testing.T) {
				g, err := tree.ParseDGeography(tc.wkt)
				require.NoError(t, err)
				require.Equal(t, tc.expected, g.Size())
			})
		})
	}
}
