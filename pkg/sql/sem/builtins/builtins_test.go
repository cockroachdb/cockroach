// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCategory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	getCategory := func(name string) string {
		props, _ := builtinsregistry.GetBuiltinProperties(name)
		return props.Category
	}
	if expected, actual := builtinconstants.CategoryString, getCategory("lower"); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := builtinconstants.CategoryString, getCategory("length"); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := builtinconstants.CategoryDateAndTime, getCategory("now"); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
	if expected, actual := builtinconstants.CategorySystemInfo, getCategory("version"); expected != actual {
		t.Fatalf("bad category: expected %q got %q", expected, actual)
	}
}

// TestSerialNormalizationWithUniqueUnorderedID makes sure that serial
// normalization can use the unordered_rowid mode and a large number of
// insertions (80k) results in a (somewhat) uniform distribution of the data.
func TestSerialNormalizationWithUniqueUnorderedID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "the test is too slow and the goodness of fit test "+
		"assumes large N")
	skip.UnderDeadlock(t, "the test is too slow")

	ctx := context.Background()

	// Since this is a statistical test, an occasional observation of non-uniformity
	// is not in and of itself a huge concern. As such, we'll only fail the test if
	// a majority of our observations show statistically significant amounts of
	// non-uniformity.
	const numObservations = 10
	numNonUniformObservations := 0
	for i := 0; i < numObservations; i++ {
		// We use an anonymous function because of the defer in each iteration.
		func() {
			t.Logf("starting observation %d", i)

			params := base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SQLEvalContext: &eval.TestingKnobs{
						// We disable the randomization of some batch sizes because
						// with some low values the test takes much longer.
						ForceProductionValues: true,
					},
				},
			}
			s, db, _ := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(ctx)

			tdb := sqlutils.MakeSQLRunner(db)
			// Create a new table with serial primary key i (unordered_rowid) and int j (index).
			tdb.Exec(t, `
SET serial_normalization TO 'unordered_rowid';
CREATE DATABASE t;
USE t;
CREATE TABLE t (
  i SERIAL PRIMARY KEY,
  j INT
)`)

			// Insert rows.
			numberOfRows := 80000
			tdb.Exec(t, fmt.Sprintf(`
INSERT INTO t(j) SELECT * FROM generate_series(1, %d);
`, numberOfRows))

			// Build an equi-width histogram over the key range. The below query will
			// generate the key bounds for each high-order bit pattern as defined by
			// prefixBits. For example, if this were 3, we'd get the following groups:
			//
			//            low         |        high
			//  ----------------------+----------------------
			//                      0 | 2305843009213693952
			//    2305843009213693952 | 4611686018427387904
			//    4611686018427387904 | 6917529027641081856
			//    6917529027641081856 | 9223372036854775807
			//
			const prefixBits = 4
			var keyCounts pq.Int64Array
			tdb.QueryRow(t, `
  WITH boundaries AS (
                     SELECT i << (64 - $1) AS p FROM ROWS FROM (generate_series(0, (1 << ($1 - 1)) - 1)) AS t (i)
                     UNION ALL SELECT (((1 << 62) - 1) << 1) + 1 -- int63 max value
                  ),
       groups AS (
                SELECT *
                  FROM (SELECT p AS low, lead(p) OVER () AS high FROM boundaries)
                 WHERE high IS NOT NULL
              ),
       counts AS (
                  SELECT count(i) AS c
                    FROM t, groups
                   WHERE low < i AND i <= high
                GROUP BY (low, high)
              )
SELECT array_agg(c)
  FROM counts;`, prefixBits).Scan(&keyCounts)

			t.Log("Key counts in each split range")
			for i, keyCount := range keyCounts {
				t.Logf("range %d: %d\n", i, keyCount)
			}
			require.Len(t, keyCounts, 1<<(prefixBits-1))

			// To check that the distribution over ranges is not uniform, we use a
			// chi-square goodness of fit statistic. We'll set our null hypothesis as
			// 'each range in the distribution should have the same probability of getting
			// a row inserted' and we'll check if we can reject the null hypothesis if
			// chi-squared is greater than the critical value we currently set as 35.2585,
			// a deliberate choice that gives us a p-value of 0.00001 according to
			// https://www.fourmilab.ch/rpkp/experiments/analysis/chiCalc.html. If we are
			// able to reject the null hypothesis, then the distribution is not uniform,
			// and we raise an error. This test has 7 degrees of freedom (categories - 1).
			chiSquared := discreteUniformChiSquared(keyCounts)
			criticalValue := 35.2585
			if chiSquared > criticalValue {
				t.Logf("chiSquared value of %f > criticalVal %f indicates that the distribution "+
					"is non-uniform (statistically significant, p < 0.00001)",
					chiSquared, criticalValue)
				numNonUniformObservations += 1
			} else {
				t.Logf("chiSquared value of %f <= criticalVal %f indicates that the distribution "+
					"is relatively uniform",
					chiSquared, criticalValue)
			}
		}()
	}

	if numNonUniformObservations >= numObservations/2 {
		t.Fatalf("a majority of our observations indicate the distribution is non-uniform")
	}
}

// discreteUniformChiSquared calculates the chi-squared statistic (ref:
// https://www.itl.nist.gov/div898/handbook/eda/section3/eda35f.htm) to be used
// in our hypothesis testing for the distribution of rows among ranges.
func discreteUniformChiSquared(buckets []int64) float64 {
	var n int64
	for _, c := range buckets {
		n += c
	}
	expectedPerBucket := float64(n) / float64(len(buckets))
	var chiSquared float64
	for _, c := range buckets {
		chiSquared += math.Pow(float64(c)-expectedPerBucket, 2) / expectedPerBucket
	}
	return chiSquared
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

			evalContext := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			if cmp, err := result.Compare(context.Background(), evalContext, expectedArray); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
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
			interval, err := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, tc.intervalStr)
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

func TestPGBuiltinsCalledOnNull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	params := base.TestServerArgs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(db)

	testCases := []struct {
		sql string
	}{
		{ // Case 1
			sql: "SELECT pg_get_functiondef(NULL);",
		},
		{ // Case 2
			sql: "SELECT pg_get_function_arguments(NULL);",
		},
		{ // Case 3
			sql: "SELECT pg_get_function_result(NULL);",
		},
		{ // Case 4
			sql: "SELECT pg_get_function_identity_arguments(NULL);",
		},
		{ // Case 5
			sql: "SELECT pg_get_indexdef(NULL);",
		},
		{ // Case 6
			sql: "SELECT pg_get_userbyid(NULL);",
		},
		{ // Case 7
			sql: "SELECT pg_sequence_parameters(NULL);",
		},
		{ // Case 8
			sql: "SELECT col_description(NULL, NULL);",
		},
		{ // Case 9
			sql: "SELECT col_description(NULL, 0);",
		},
		{ // Case 10
			sql: "SELECT col_description(0, NULL);",
		},
		{ // Case 11
			sql: "SELECT obj_description(NULL);",
		},
		{ // Case 12
			sql: "SELECT obj_description(NULL, NULL);",
		},
		{ // Case 13
			sql: "SELECT obj_description(NULL, 'foo');",
		},
		{ // Case 14
			sql: "SELECT obj_description(0, NULL);",
		},
		{ // Case 15
			sql: "SELECT shobj_description(NULL, NULL);",
		},
		{ // Case 16
			sql: "SELECT shobj_description(NULL, 'foo');",
		},
		{ // Case 17
			sql: "SELECT shobj_description(0, NULL);",
		},
		{ // Case 18
			sql: "SELECT pg_function_is_visible(NULL);",
		},
		{ // Case 19
			sql: "SELECT pg_table_is_visible(NULL);",
		},
		{ // Case 20
			sql: "SELECT pg_type_is_visible(NULL);",
		},
	}
	for i, tc := range testCases {
		res := tdb.QueryStr(t, tc.sql)
		require.Equalf(t, [][]string{{"NULL"}}, res, "failed test case %d", i+1)
	}
}

func TestBitmaskOrAndXor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		bitFn    func(string, string) (*tree.DBitArray, error)
		a        string
		b        string
		expected string
	}{
		{bitmaskOr, "010", "0", "010"},
		{bitmaskOr, "010", "101", "111"},
		{bitmaskOr, "010", "101", "111"},
		{bitmaskOr, "010", "1010", "1010"},
		{bitmaskOr, "0100010", "1010", "0101010"},
		{bitmaskOr, "001010010000", "0101010100", "001111010100"},
		{bitmaskOr, "001010010111", "", "001010010111"},
		{bitmaskOr, "", "1000100", "1000100"},
		{bitmaskAnd, "010", "101", "000"},
		{bitmaskAnd, "010", "01", "000"},
		{bitmaskAnd, "111", "000", "000"},
		{bitmaskAnd, "110", "101", "100"},
		{bitmaskAnd, "0100010", "1010", "0000010"},
		{bitmaskAnd, "001010010000", "0101010100", "000000010000"},
		{bitmaskAnd, "001010010000", "", "000000000000"},
		{bitmaskAnd, "", "01000100", "00000000"},
		{bitmaskXor, "010", "101", "111"},
		{bitmaskXor, "010", "01", "011"},
		{bitmaskXor, "101", "100", "001"},
		{bitmaskXor, "110", "001", "111"},
		{bitmaskXor, "0101010", "1011", "0100001"},
		{bitmaskXor, "001010010000", "0101010100", "001111000100"},
		{bitmaskXor, "001010010000", "", "001010010000"},
		{bitmaskXor, "", "01000100", "01000100"},
	}
	for _, tc := range testCases {
		bitArray, err := tc.bitFn(tc.a, tc.b)
		if err != nil {
			t.Fatal(err)
		}
		resultStr := bitArray.BitArray.String()
		if resultStr != tc.expected {
			t.Errorf("expected %s, found %s", tc.expected, resultStr)
		}
	}
}

func BenchmarkGenerateID(b *testing.B) {
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	for _, fn := range []string{
		"gen_random_uuid",
		"gen_random_ulid",
	} {
		b.Run(fn, func(b *testing.B) {
			for _, rowCount := range []int{1, 10, 100, 1000} {
				b.Run(fmt.Sprintf("rows=%d", rowCount), func(b *testing.B) {
					q := fmt.Sprintf(`select %s() from generate_series(0, %d);`, fn, rowCount)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						db.Exec(b, q)
					}
					b.StopTimer()
				})
			}
		})
	}
}
