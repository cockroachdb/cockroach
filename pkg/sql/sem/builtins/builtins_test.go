// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCategory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if expected, actual := categoryString, builtins["lower"].props.Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryString, builtins["length"].props.Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categoryDateAndTime, builtins["now"].props.Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := categorySystemInfo, builtins["version"].props.Category; expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
}

// TestGenerateUniqueIDOrder verifies the expected ordering of
// GenerateUniqueID.
func TestGenerateUniqueIDOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []tree.DInt{
		GenerateUniqueID(0, 0),
		GenerateUniqueID(1, 0),
		GenerateUniqueID(2<<15, 0),
		GenerateUniqueID(0, 1),
		GenerateUniqueID(0, 10000),
		GenerateUniqueInt(0),
	}
	prev := tests[0]
	for _, tc := range tests[1:] {
		if tc <= prev {
			t.Fatalf("%d > %d", tc, prev)
		}
	}
}

func TestStringToArrayAndBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// s allows us to have a string pointer literal.
	s := func(x string) *string { return &x }
	fs := func(x *string) string {
		if x != nil {
			return *x
		}
		return "<nil>"
	}
	cases := []struct {
		input    string
		sep      *string
		nullStr  *string
		expected []*string
	}{
		{`abcxdef`, s(`x`), nil, []*string{s(`abc`), s(`def`)}},
		{`xxx`, s(`x`), nil, []*string{s(``), s(``), s(``), s(``)}},
		{`xxx`, s(`xx`), nil, []*string{s(``), s(`x`)}},
		{`abcxdef`, s(``), nil, []*string{s(`abcxdef`)}},
		{`abcxdef`, s(`abcxdef`), nil, []*string{s(``), s(``)}},
		{`abcxdef`, s(`x`), s(`abc`), []*string{nil, s(`def`)}},
		{`abcxdef`, s(`x`), s(`x`), []*string{s(`abc`), s(`def`)}},
		{`abcxdef`, s(`x`), s(``), []*string{s(`abc`), s(`def`)}},
		{``, s(`x`), s(``), []*string{}},
		{``, s(``), s(``), []*string{}},
		{``, s(`x`), nil, []*string{}},
		{``, s(``), nil, []*string{}},
		{`abcxdef`, nil, nil, []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(`abc`), []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(`x`), []*string{s(`a`), s(`b`), s(`c`), nil, s(`d`), s(`e`), s(`f`)}},
		{`abcxdef`, nil, s(``), []*string{s(`a`), s(`b`), s(`c`), s(`x`), s(`d`), s(`e`), s(`f`)}},
		{``, nil, s(``), []*string{}},
		{``, nil, nil, []*string{}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("string_to_array(%q, %q)", tc.input, fs(tc.sep)), func(t *testing.T) {
			result, err := stringToArray(tc.input, tc.sep, tc.nullStr)
			if err != nil {
				t.Fatal(err)
			}

			expectedArray := tree.NewDArray(types.String)
			for _, s := range tc.expected {
				datum := tree.DNull
				if s != nil {
					datum = tree.NewDString(*s)
				}
				if err := expectedArray.Append(datum); err != nil {
					t.Fatal(err)
				}
			}

			evalContext := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			if result.Compare(evalContext, expectedArray) != 0 {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}

			if tc.sep == nil {
				return
			}

			s, err := arrayToString(evalContext, result.(*tree.DArray), *tc.sep, tc.nullStr)
			if err != nil {
				t.Fatal(err)
			}
			if s == tree.DNull {
				t.Errorf("expected not null, found null")
			}

			ds := s.(*tree.DString)
			fmt.Println(ds)
			if string(*ds) != tc.input {
				t.Errorf("original %s, roundtripped %s", tc.input, s)
			}
		})
	}
}

func TestEscapeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		bytes []byte
		str   string
	}{
		{[]byte{}, ``},
		{[]byte{'a', 'b', 'c'}, `abc`},
		{[]byte{'a', 'b', 'c', 'd'}, `abcd`},
		{[]byte{'a', 'b', 0, 'd'}, `ab\000d`},
		{[]byte{'a', 'b', 0, 0, 'd'}, `ab\000\000d`},
		{[]byte{'a', 'b', 0, 'a', 'b', 'c', 0, 'd'}, `ab\000abc\000d`},
		{[]byte{'a', 'b', 0, 0}, `ab\000\000`},
		{[]byte{'a', 'b', '\\', 'd'}, `ab\\d`},
		{[]byte{'a', 'b', 200, 'd'}, `ab\310d`},
		{[]byte{'a', 'b', 7, 'd'}, "ab\x07d"},
	}

	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			result := encodeEscape(tc.bytes)
			if result != tc.str {
				t.Fatalf("expected %q, got %q", tc.str, result)
			}

			decodedResult, err := decodeEscape(tc.str)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decodedResult, tc.bytes) {
				t.Fatalf("expected %q, got %#v", tc.bytes, decodedResult)
			}
		})
	}
}

func TestEscapeFormatRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for i := 0; i < 1000; i++ {
		b := make([]byte, rand.Intn(100))
		for j := 0; j < len(b); j++ {
			b[j] = byte(rand.Intn(256))
		}
		str := encodeEscape(b)
		decodedResult, err := decodeEscape(str)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decodedResult, b) {
			t.Fatalf("generated %#v, after round-tripping got %#v", b, decodedResult)
		}
	}
}

func TestLPadRPad(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		padFn    func(string, int, string) (string, error)
		str      string
		length   int
		fill     string
		expected string
	}{
		{lpad, "abc", 1, "xy", "a"},
		{lpad, "abc", 2, "xy", "ab"},
		{lpad, "abc", 3, "xy", "abc"},
		{lpad, "abc", 5, "xy", "xyabc"},
		{lpad, "abc", 6, "xy", "xyxabc"},
		{lpad, "abc", 7, "xy", "xyxyabc"},
		{lpad, "abc", 1, " ", "a"},
		{lpad, "abc", 2, " ", "ab"},
		{lpad, "abc", 3, " ", "abc"},
		{lpad, "abc", 5, " ", "  abc"},
		{lpad, "Hello, 世界", 9, " ", "Hello, 世界"},
		{lpad, "Hello, 世界", 10, " ", " Hello, 世界"},
		{lpad, "Hello", 8, "世界", "世界世Hello"},
		{lpad, "foo", -1, "世界", ""},
		{rpad, "abc", 1, "xy", "a"},
		{rpad, "abc", 2, "xy", "ab"},
		{rpad, "abc", 3, "xy", "abc"},
		{rpad, "abc", 5, "xy", "abcxy"},
		{rpad, "abc", 6, "xy", "abcxyx"},
		{rpad, "abc", 7, "xy", "abcxyxy"},
		{rpad, "abc", 1, " ", "a"},
		{rpad, "abc", 2, " ", "ab"},
		{rpad, "abc", 3, " ", "abc"},
		{rpad, "abc", 5, " ", "abc  "},
		{rpad, "abc", 5, " ", "abc  "},
		{rpad, "Hello, 世界", 9, " ", "Hello, 世界"},
		{rpad, "Hello, 世界", 10, " ", "Hello, 世界 "},
		{rpad, "Hello", 8, "世界", "Hello世界世"},
		{rpad, "foo", -1, "世界", ""},
	}
	for _, tc := range testCases {
		out, err := tc.padFn(tc.str, tc.length, tc.fill)
		if err != nil {
			t.Errorf("Found err %v, expected nil", err)
		}
		if out != tc.expected {
			t.Errorf("expected %s, found %s", tc.expected, out)
		}
	}
}

func TestExtractTimeSpanFromTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	utcPositiveOffset := time.FixedZone("otan happy time", 60*60*4+30*60)
	utcNegativeOffset := time.FixedZone("otan sad time", -60*60*4-30*60)

	testCases := []struct {
		input    time.Time
		timeSpan string

		expected      tree.DFloat
		expectedError string
	}{
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "timezone", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "timezone_hour", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "timezone_minute", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "millennia", expected: 3},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "century", expected: 21},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "decade", expected: 201},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "year", expected: 2019},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "month", expected: 12},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "day", expected: 11},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "hour", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "minute", expected: 14},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "second", expected: 15.123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "millisecond", expected: 15123.456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "microsecond", expected: 15123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, time.UTC), timeSpan: "epoch", expected: 1.576023255123456e+09},

		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "timezone", expected: 4*60*60 + 30*60},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "timezone_hour", expected: 4},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "timezone_minute", expected: 30},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "millennia", expected: 3},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "century", expected: 21},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "decade", expected: 201},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "year", expected: 2019},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "month", expected: 12},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "day", expected: 11},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "hour", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "minute", expected: 14},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "second", expected: 15.123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "millisecond", expected: 15123.456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "microsecond", expected: 15123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcPositiveOffset), timeSpan: "epoch", expected: 1.576007055123456e+09},

		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "timezone", expected: -4*60*60 - 30*60},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "timezone_hour", expected: -4},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "timezone_minute", expected: -30},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "millennia", expected: 3},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "century", expected: 21},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "decade", expected: 201},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "year", expected: 2019},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "month", expected: 12},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "day", expected: 11},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "hour", expected: 0},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "minute", expected: 14},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "second", expected: 15.123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "millisecond", expected: 15123.456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "microsecond", expected: 15123456},
		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "epoch", expected: 1.576039455123456e+09},

		{input: time.Date(2019, time.December, 11, 0, 14, 15, 123456000, utcNegativeOffset), timeSpan: "it's numberwang!", expectedError: "unsupported timespan: it's numberwang!"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.timeSpan, tc.input.Format(time.RFC3339)), func(t *testing.T) {
			datum, err := extractTimeSpanFromTimestampTZ(nil, tc.input, tc.timeSpan)
			if tc.expectedError != "" {
				assert.EqualError(t, err, tc.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, *(datum.(*tree.DFloat)))
			}
		})
	}
}

func TestExtractTimeSpanFromTimeTZ(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		timeTZString  string
		timeSpan      string
		expected      tree.DFloat
		expectedError string
	}{
		{timeTZString: "11:12:13+01:02", timeSpan: "hour", expected: 11},
		{timeTZString: "11:12:13+01:02", timeSpan: "minute", expected: 12},
		{timeTZString: "11:12:13+01:02", timeSpan: "second", expected: 13},
		{timeTZString: "11:12:13.123456+01:02", timeSpan: "millisecond", expected: 13123.456},
		{timeTZString: "11:12:13.123456+01:02", timeSpan: "microsecond", expected: 13123456},
		{timeTZString: "11:12:13+01:02", timeSpan: "timezone", expected: 3720},
		{timeTZString: "11:12:13+01:02", timeSpan: "timezone_hour", expected: 1},
		{timeTZString: "11:12:13+01:02", timeSpan: "timezone_minute", expected: 2},
		{timeTZString: "11:12:13-01:02", timeSpan: "timezone", expected: -3720},
		{timeTZString: "11:12:13-01:02", timeSpan: "timezone_hour", expected: -1},
		{timeTZString: "11:12:13-01:02", timeSpan: "timezone_minute", expected: -2},
		{timeTZString: "11:12:13.5+01:02", timeSpan: "epoch", expected: 36613.5},
		{timeTZString: "11:12:13.5-01:02", timeSpan: "epoch", expected: 44053.5},

		{timeTZString: "11:12:13-01:02", timeSpan: "epoch2", expectedError: "unsupported timespan: epoch2"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.timeSpan, tc.timeTZString), func(t *testing.T) {
			timeTZ, _, err := tree.ParseDTimeTZ(nil, tc.timeTZString, time.Microsecond)
			assert.NoError(t, err)

			datum, err := extractTimeSpanFromTimeTZ(timeTZ, tc.timeSpan)
			if tc.expectedError != "" {
				assert.EqualError(t, err, tc.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, *(datum.(*tree.DFloat)))
			}
		})
	}
}

func TestExtractTimeSpanFromInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		timeSpan    string
		intervalStr string
		expected    *tree.DFloat
	}{
		{"millennia", "25000 months 1000 days", tree.NewDFloat(2)},
		{"millennia", "-25000 months 1000 days", tree.NewDFloat(-2)},
		{"millennium", "25000 months 1000 days", tree.NewDFloat(2)},
		{"millenniums", "25000 months 1000 days", tree.NewDFloat(2)},

		{"century", "25000 months 1000 days", tree.NewDFloat(20)},
		{"century", "-25000 months 1000 days", tree.NewDFloat(-20)},
		{"centuries", "25000 months 1000 days", tree.NewDFloat(20)},

		{"decade", "25000 months 1000 days", tree.NewDFloat(208)},
		{"decade", "-25000 months 1000 days", tree.NewDFloat(-208)},
		{"decades", "25000 months 1000 days", tree.NewDFloat(208)},

		{"year", "25000 months 1000 days", tree.NewDFloat(2083)},
		{"year", "-25000 months 1000 days", tree.NewDFloat(-2083)},
		{"years", "25000 months 1000 days", tree.NewDFloat(2083)},

		{"month", "25000 months 1000 days", tree.NewDFloat(4)},
		{"month", "-25000 months 1000 days", tree.NewDFloat(-4)},
		{"months", "25000 months 1000 days", tree.NewDFloat(4)},

		{"day", "25000 months 1000 days", tree.NewDFloat(1000)},
		{"day", "-25000 months 1000 days", tree.NewDFloat(1000)},
		{"day", "-25000 months -1000 days", tree.NewDFloat(-1000)},
		{"days", "25000 months 1000 days", tree.NewDFloat(1000)},

		{"hour", "25-1 100:56:01.123456", tree.NewDFloat(100)},
		{"hour", "25-1 -100:56:01.123456", tree.NewDFloat(-100)},
		{"hours", "25-1 100:56:01.123456", tree.NewDFloat(100)},

		{"minute", "25-1 100:56:01.123456", tree.NewDFloat(56)},
		{"minute", "25-1 -100:56:01.123456", tree.NewDFloat(-56)},
		{"minutes", "25-1 100:56:01.123456", tree.NewDFloat(56)},

		{"second", "25-1 100:56:01.123456", tree.NewDFloat(1.123456)},
		{"second", "25-1 -100:56:01.123456", tree.NewDFloat(-1.123456)},
		{"seconds", "25-1 100:56:01.123456", tree.NewDFloat(1.123456)},

		{"millisecond", "25-1 100:56:01.123456", tree.NewDFloat(1123.456)},
		{"millisecond", "25-1 -100:56:01.123456", tree.NewDFloat(-1123.456)},
		{"milliseconds", "25-1 100:56:01.123456", tree.NewDFloat(1123.456)},

		{"microsecond", "25-1 100:56:01.123456", tree.NewDFloat(1123456)},
		{"microsecond", "25-1 -100:56:01.123456", tree.NewDFloat(-1123456)},
		{"microseconds", "25-1 100:56:01.123456", tree.NewDFloat(1123456)},

		{"epoch", "25-1 100:56:01.123456", tree.NewDFloat(791895361.123456)},
		{"epoch", "25-1 -100:56:01.123456", tree.NewDFloat(791168638.876544)},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s as %s", tc.intervalStr, tc.timeSpan), func(t *testing.T) {
			interval, err := tree.ParseDInterval(tc.intervalStr)
			assert.NoError(t, err)

			d, err := extractTimeSpanFromInterval(interval, tc.timeSpan)
			assert.NoError(t, err)

			assert.Equal(t, *tc.expected, *(d.(*tree.DFloat)))
		})
	}
}

func TestTruncateTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	loc, err := timeutil.LoadLocation("Australia/Sydney")
	require.NoError(t, err)

	testCases := []struct {
		fromTime time.Time
		timeSpan string
		expected *tree.DTimestampTZ
	}{
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"millennium",
			tree.MustMakeDTimestampTZ(time.Date(2001, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"century",
			tree.MustMakeDTimestampTZ(time.Date(2101, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"decade",
			tree.MustMakeDTimestampTZ(time.Date(2110, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"year",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"quarter",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.January, 1, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"month",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.March, 1, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"day",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.March, 11, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"week",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.March, 7, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"hour",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.March, 11, 5, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"second",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.March, 11, 5, 6, 7, 0, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"millisecond",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.March, 11, 5, 6, 7, 80000000, loc), time.Microsecond),
		},
		{
			time.Date(2118, time.March, 11, 5, 6, 7, 80009001, loc),
			"microsecond",
			tree.MustMakeDTimestampTZ(time.Date(2118, time.March, 11, 5, 6, 7, 80009000, loc), time.Microsecond),
		},

		// Test Monday and Sunday boundaries.
		{
			time.Date(2019, time.November, 11, 5, 6, 7, 80009001, loc),
			"week",
			tree.MustMakeDTimestampTZ(time.Date(2019, time.November, 11, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2019, time.November, 10, 5, 6, 7, 80009001, loc),
			"week",
			tree.MustMakeDTimestampTZ(time.Date(2019, time.November, 4, 0, 0, 0, 0, loc), time.Microsecond),
		},

		// Test DST boundaries.
		{
			time.Date(2021, time.April, 4, 0, 0, 0, 0, loc).Add(time.Hour * 2),
			"hour",
			tree.MustMakeDTimestampTZ(time.Date(2021, time.April, 4, 0, 0, 0, 0, loc).Add(time.Hour*2), time.Microsecond),
		},
		{
			time.Date(2021, time.April, 4, 0, 0, 0, 0, loc).Add(time.Hour * 2),
			"day",
			tree.MustMakeDTimestampTZ(time.Date(2021, time.April, 4, 0, 0, 0, 0, loc), time.Microsecond),
		},
		{
			time.Date(2021, time.April, 4, 2, 0, 0, 0, loc),
			"day",
			tree.MustMakeDTimestampTZ(time.Date(2021, time.April, 4, 0, 0, 0, 0, loc), time.Microsecond),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.timeSpan, func(t *testing.T) {
			result, err := truncateTimestamp(tc.fromTime, tc.timeSpan)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}
