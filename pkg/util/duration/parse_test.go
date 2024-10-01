// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package duration

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var (
	dayToHourITM = types.IntervalTypeMetadata{
		DurationField: types.IntervalDurationField{
			FromDurationType: types.IntervalDurationType_DAY,
			DurationType:     types.IntervalDurationType_HOUR,
		},
	}
	minuteToSecondITM = types.IntervalTypeMetadata{
		DurationField: types.IntervalDurationField{
			FromDurationType: types.IntervalDurationType_MINUTE,
			DurationType:     types.IntervalDurationType_SECOND,
		},
	}
)

func TestValidSQLIntervalSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		input             string
		itm               types.IntervalTypeMetadata
		output            string
		sqlStandardOutput string
	}{
		{input: `0:1`, output: `00:01:00`},
		{input: `0:1.0`, output: `00:00:01`},
		{input: `1`, output: `00:00:01`},
		{input: `1.0:0:0`, output: `1 day`},
		{input: `1.2`, output: `00:00:01.2`},
		{input: `:3:4`, output: `03:04:00`},
		{input: `:-3:4`, output: `-03:04:00`},
		{input: `:3:4.1`, output: `00:03:04.1`},
		{input: `1.2:1:1.2`, output: `1 day 04:49:01.2`},
		{input: `1.2:+1:1.2`, output: `1 day 04:49:01.2`},
		{input: `1.2:-1:1.2`, output: `1 day 04:46:58.8`},
		{input: `1:0:0`, output: `01:00:00`},
		{input: `1:1.2`, output: `00:01:01.2`},
		{input: `1:2`, output: `01:02:00`},
		{input: `1:2.3`, output: `00:01:02.3`},
		{input: `1:2:3`, output: `01:02:03`},
		{input: `1234:56:54`, output: `1234:56:54`},
		{input: `-0:1`, output: `-00:01:00`},
		{input: `-0:1.0`, output: `-00:00:01`},
		{input: `-1`, output: `-00:00:01`},
		{input: `-1.2`, output: `-00:00:01.2`},
		{input: `-1:0:0`, output: `-01:00:00`},
		{input: `-1:1.2`, output: `-00:01:01.2`},
		{input: `-1:2`, output: `-01:02:00`},
		{input: `-1:2.3`, output: `-00:01:02.3`},
		{input: `-1:2:3`, output: `-01:02:03`},
		{input: `-1234:56:54`, output: `-1234:56:54`},
		{input: `1-2`, output: `1 year 2 mons`},
		{input: `-1-2`, output: `-1 years -2 mons`},
		{input: `1-2 3`, output: `1 year 2 mons 00:00:03`},
		{
			input: `1-2 3`,
			itm: types.IntervalTypeMetadata{
				DurationField: types.IntervalDurationField{
					DurationType: types.IntervalDurationType_YEAR,
				},
			},
			output: `4 years 2 mons`,
		}, // this gets truncated later to 4 years
		{
			input: `1-2 3`,
			itm: types.IntervalTypeMetadata{
				DurationField: types.IntervalDurationField{
					DurationType: types.IntervalDurationType_MONTH,
				},
			},
			output: `1 year 5 mons`,
		},
		{
			input: `1-2 3`,
			itm: types.IntervalTypeMetadata{
				DurationField: types.IntervalDurationField{
					DurationType: types.IntervalDurationType_DAY,
				},
			},
			output: `1 year 2 mons 3 days`,
		},
		{
			input: `1-2 3`,
			itm: types.IntervalTypeMetadata{
				DurationField: types.IntervalDurationField{
					DurationType: types.IntervalDurationType_HOUR,
				},
			},
			output: `1 year 2 mons 03:00:00`,
		},
		{
			input: `1-2 3`,
			itm: types.IntervalTypeMetadata{
				DurationField: types.IntervalDurationField{
					DurationType: types.IntervalDurationType_MINUTE,
				},
			},
			output: `1 year 2 mons 00:03:00`,
		},
		{
			input: `1-2 3`,
			itm: types.IntervalTypeMetadata{
				DurationField: types.IntervalDurationField{
					DurationType: types.IntervalDurationType_SECOND,
				},
			},
			output: `1 year 2 mons 00:00:03`,
		},
		{input: `1-2 -3`, output: `1 year 2 mons -00:00:03`},
		{input: `-1-2 -3`, output: `-1 years -2 mons -00:00:03`},
		{input: `2 4:08`, output: `2 days 04:08:00`},
		{input: `2.5 4:08`, output: `2 days 16:08:00`},
		{input: `-2 4:08`, output: `-2 days +04:08:00`},
		{input: `2 -4:08`, output: `2 days -04:08:00`},
		{input: `2 -4:08.1234`, output: `2 days -00:04:08.1234`},
		{input: `2 -4:08`, itm: minuteToSecondITM, output: `2 days -00:04:08`},
		{input: `2 -4:08.1234`, itm: minuteToSecondITM, output: `2 days -00:04:08.1234`},
		{input: `1-2 4:08`, output: `1 year 2 mons 04:08:00`},
		{input: `1-2 3 4:08`, output: `1 year 2 mons 3 days 04:08:00`},
		{input: `1-2 3 4:08:05`, output: `1 year 2 mons 3 days 04:08:05`},
		{input: `1-2 4:08:23`, output: `1 year 2 mons 04:08:23`},
		{input: `1- 4:08:23`, output: `1 year 04:08:23`},
		{input: `0-2 3 4:08`, output: `2 mons 3 days 04:08:00`},
		{input: `1- 3 4:08:`, output: `1 year 3 days 04:08:00`},
		{input: `-1- 3 4:08:`, output: `-1 years +3 days 04:08:00`},
		{input: `0- 3 4:08`, output: `3 days 04:08:00`},
		{input: `-0- 3 4:08`, output: `3 days 04:08:00`},
		{input: `-0- -0 4:08`, output: `04:08:00`},
		{input: `-0- -0 0:0`, output: `00:00:00`},
		{input: `-0- -0 -0:0`, output: `00:00:00`},
		{input: `-0- -3 -4:08`, output: `-3 days -04:08:00`},
		{input: `0- 3 4::08`, output: `3 days 04:00:08`},
		{input: `	0-   3    4::08  `, output: `3 days 04:00:08`},
		{input: `2 4:08:23`, output: `2 days 04:08:23`},
		{input: `1-2 3 4:08:23`, output: `1 year 2 mons 3 days 04:08:23`},
		{input: `1-`, output: `1 year`},
		{input: `1- 2`, output: `1 year 00:00:02`},
		{input: `2 3:`, output: `2 days 03:00:00`},
		{input: `2 3:4:`, output: `2 days 03:04:00`},
		{input: `1- 3:`, output: `1 year 03:00:00`},
		{input: `1- 3:4`, output: `1 year 03:04:00`},

		{input: `2 3`, itm: dayToHourITM, output: `2 days 03:00:00`},
		{input: `-2 -3`, itm: dayToHourITM, output: `-2 days -03:00:00`},
		{input: `-2 3`, itm: dayToHourITM, output: `-2 days +03:00:00`},
		{input: `2 -3`, itm: dayToHourITM, output: `2 days -03:00:00`},
		{input: `1-2 3`, itm: dayToHourITM, output: `1 year 2 mons 03:00:00`},
		{input: `-1-2 -3`, itm: dayToHourITM, output: `-1 years -2 mons -03:00:00`},
		{input: `-1-2 3`, itm: dayToHourITM, output: `-1 years -2 mons +03:00:00`},
		{input: `1-2 -3`, itm: dayToHourITM, output: `1 year 2 mons -03:00:00`},
	}
	for _, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			dur, err := sqlStdToDuration(test.input, test.itm)
			if err != nil {
				t.Fatalf("%q: %v", test.input, err)
			}
			s := dur.String()
			if s != test.output {
				t.Fatalf(`%q: got "%s", expected "%s"`, test.input, s, test.output)
			}

			for style := range IntervalStyle_value {
				t.Run(style, func(t *testing.T) {
					styleVal := IntervalStyle(IntervalStyle_value[style])
					dur2, err := parseDuration(styleVal, s, test.itm)
					if err != nil {
						t.Fatalf(`%q: repr "%s" is not parsable: %v`, test.input, s, err)
					}
					s2 := dur2.String()
					if s2 != s {
						t.Fatalf(`%q: repr "%s" does not round-trip, got "%s" instead`,
							test.input, s, s2)
					}

					// Test that a Datum recognizes the format.
					di, err := ParseInterval(styleVal, test.input, test.itm)
					if err != nil {
						t.Fatalf(`%q: unrecognized as datum: %v`, test.input, err)
					}
					s3 := di.String()
					if s3 != test.output {
						t.Fatalf(`%q: as datum, got "%s", expected "%s"`, test.input, s3, test.output)
					}
				})
			}
		})
	}
}

func TestInvalidSQLIntervalSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		input string
		error string
	}{
		{`+`, `invalid input syntax for type interval +`},
		{`++`, `invalid input syntax for type interval ++`},
		{`--`, `invalid input syntax for type interval --`},
		{`{1,2}`, `invalid input syntax for type interval {1,2}`},
		{`0.000,0`, `invalid input syntax for type interval 0.000,0`},
		{`0,0`, `invalid input syntax for type interval 0,0`},
		{`2 3`, `invalid input syntax for type interval 2 3`},
		{`-2 3`, `invalid input syntax for type interval -2 3`},
		{`-2 -3`, `invalid input syntax for type interval -2 -3`},
		{`2 -3`, `invalid input syntax for type interval 2 -3`},
		{`0:-1`, `invalid input syntax for type interval 0:-1`},
		{`0:0:-1`, `invalid input syntax for type interval 0:0:-1`},
		{`1.0:0:-1`, `invalid input syntax for type interval 1.0:0:-1`},
		{`0:-1:0`, `invalid input syntax for type interval 0:-1:0`},
		{`0:-1:-1`, `invalid input syntax for type interval 0:-1:-1`},
		{`-1.0:0:0`, `invalid input syntax for type interval -1.0:0:0`},
		{`-:0:0`, `invalid input syntax for type interval -:0:0`},
	}
	for i, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			_, err := sqlStdToDuration(test.input, types.IntervalTypeMetadata{})
			if err != nil {
				if test.error != "" {
					if err.Error() != test.error {
						t.Errorf(`%d: %q: got error "%v", expected "%s"`, i, test.input, err, test.error)
					}
				} else {
					t.Errorf("%d: %q: %v", i, test.input, err)
				}
			} else {
				if test.error != "" {
					t.Errorf(`%d: %q: expected error "%q"`, i, test.input, test.error)
				}
			}
		})
	}
}

func TestPGIntervalSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		input             string
		itm               types.IntervalTypeMetadata
		output            string
		outputSQLStandard string
		error             string
		errorSQLStandard  string
	}{
		{input: ``, error: `interval: invalid input syntax: ""`},
		{input: `-`, error: `interval: strconv.ParseInt: parsing "-": invalid syntax`},
		{input: `123`, error: `interval: missing unit at position 3: "123"`},
		{input: `123blah`, error: `interval: unknown unit "blah" in duration "123blah"`},
		{input: `10000000000000000000000000000000000 year`, error: `interval: strconv.ParseInt: parsing "10000000000000000000000000000000000": value out of range`},

		{input: `500nanoseconds`, error: `interval: unknown unit "nanoseconds" in duration "500nanoseconds"`},
		{input: `500ns`, error: `interval: unknown unit "ns" in duration "500ns"`},

		// ns/us boundary
		{input: `.5us`, output: `00:00:00.000001`},
		{input: `-0.499us`, output: `00:00:00`},
		{input: `-0.5us`, output: `-00:00:00.000001`},
		{input: `0.000000499s`, output: `00:00:00`},
		{input: `0.0000005s`, output: `00:00:00.000001`},
		{input: `-0.000000499s`, output: `00:00:00`},
		{input: `-0.0000005s`, output: `-00:00:00.000001`},

		{input: `1.2 microsecond`, output: `00:00:00.000001`},
		{input: `1.2microseconds`, output: `00:00:00.000001`},
		{input: `1.2us`, output: `00:00:00.000001`},
		// µ = U+00B5 = micro symbol
		// μ = U+03BC = Greek letter mu
		{input: `1.2µs`, output: `00:00:00.000001`},
		{input: `1.2μs`, output: `00:00:00.000001`},
		{input: `1.2usec`, output: `00:00:00.000001`},
		{input: `1.2usecs`, output: `00:00:00.000001`},
		{input: `1.2usecond`, output: `00:00:00.000001`},
		{input: `1.2useconds`, output: `00:00:00.000001`},
		{input: `0.23us`, output: `00:00:00`},
		{input: `-0.23us`, output: `00:00:00`},
		{input: `0.2346us`, output: `00:00:00`},
		{input: `-1.2us`, output: `-00:00:00.000001`},

		{input: `1.2millisecond`, output: `00:00:00.0012`},
		{input: `1.2milliseconds`, output: `00:00:00.0012`},
		{input: `1.2ms`, output: `00:00:00.0012`},
		{input: `1.2msec`, output: `00:00:00.0012`},
		{input: `1.2msecs`, output: `00:00:00.0012`},
		{input: `1.2msecond`, output: `00:00:00.0012`},
		{input: `1.2mseconds`, output: `00:00:00.0012`},
		{input: `0.2304506ms`, output: `00:00:00.00023`},
		{input: `0.0002304506ms`, output: `00:00:00`},

		{input: `1.2second`, output: `00:00:01.2`},
		{input: `1.2seconds`, output: `00:00:01.2`},
		{input: `1.2s`, output: `00:00:01.2`},
		{input: `1.2sec`, output: `00:00:01.2`},
		{input: `1.2secs`, output: `00:00:01.2`},
		{input: `0.2304506708s`, output: `00:00:00.230451`},
		{input: `0.0002304506708s`, output: `00:00:00.00023`},
		{input: `0.0000002304506s`, output: `00:00:00`},
		{input: `75.5s`, output: `00:01:15.5`},
		{input: `3675.5s`, output: `01:01:15.5`},
		{input: `86475.5s`, output: `24:01:15.5`},
		{input: `86400s -60000ms 100us`, output: `23:59:00.0001`},

		{input: `1.2minute`, output: `00:01:12`},
		{input: `1.2minutes`, output: `00:01:12`},
		{input: `1.2m`, output: `00:01:12`},
		{input: `1.2min`, output: `00:01:12`},
		{input: `1.2mins`, output: `00:01:12`},
		{input: `1.2m 8s`, output: `00:01:20`},
		{input: `0.5m`, output: `00:00:30`},
		{input: `120.5m`, output: `02:00:30`},
		{input: `0.23045067089m`, output: `00:00:13.82704`},
		{input: `-0.23045067089m`, output: `-00:00:13.82704`},

		{input: `1.2hour`, output: `01:12:00`},
		{input: `1.2hours`, output: `01:12:00`},
		{input: `1.2h`, output: `01:12:00`},
		{input: `1.2hr`, output: `01:12:00`},
		{input: `1.2hrs`, output: `01:12:00`},
		{input: `1.2h 8m`, output: `01:20:00`},
		{input: `0.5h`, output: `00:30:00`},
		{input: `25.5h`, output: `25:30:00`},
		{input: `0.23045067089h`, output: `00:13:49.622415`},
		{input: `-0.23045067089h`, output: `-00:13:49.622415`},

		{input: `1 day`, output: `1 day`},
		{input: `1 days`, output: `1 day`},
		{input: `1d`, output: `1 day`},
		{input: `1.1d`, output: `1 day 02:24:00`},
		{input: `1.2d`, output: `1 day 04:48:00`},
		{input: `1.11d`, output: `1 day 02:38:24`},
		{input: `1.111d`, output: `1 day 02:39:50.4`},
		{input: `1.1111d`, output: `1 day 02:39:59.04`},
		{input: `60d 25h`, output: `60 days 25:00:00`},
		{
			input:            `-9223372036854775808d`,
			output:           `-9223372036854775808 days`,
			errorSQLStandard: `interval: strconv.ParseInt: parsing "9223372036854775808": value out of range`,
		},
		{input: `9223372036854775807d`, output: `9223372036854775807 days`},

		{input: `1week`, output: `7 days`},
		{input: `1weeks`, output: `7 days`},
		{input: `1w`, output: `7 days`},
		{input: `1.1w`, output: `7 days 16:48:00`},
		{input: `1.5w`, output: `10 days 12:00:00`},
		{input: `1w -1d`, output: `6 days`},

		{input: `1month`, output: `1 mon`},
		{input: `1months`, output: `1 mon`},
		{input: `1mons`, output: `1 mon`},
		{input: `1.5mon`, output: `1 mon 15 days`},
		{input: `1 mon 2 week`, output: `1 mon 14 days`},
		{input: `1.1mon`, output: `1 mon 3 days`},
		{input: `1.2mon`, output: `1 mon 6 days`},
		{input: `1.11mon`, output: `1 mon 3 days 07:12:00`},
		{
			input:            `-9223372036854775808mon`,
			output:           `-768614336404564650 years -8 mons`,
			errorSQLStandard: `interval: strconv.ParseInt: parsing "9223372036854775808": value out of range`,
		},
		{input: `9223372036854775807mon`, output: `768614336404564650 years 7 mons`},

		{input: `1year`, output: `1 year`},
		{input: `1years`, output: `1 year`},
		{input: `1y`, output: `1 year`},
		{input: `1yr`, output: `1 year`},
		{input: `1yrs`, output: `1 year`},
		{input: `1.5y`, output: `1 year 6 mons`},
		{input: `1.1y`, output: `1 year 1 mon`},
		{input: `1.19y`, output: `1 year 2 mons`},
		{input: `1.11y`, output: `1 year 1 mon`},
		{input: `-1.5y`, output: `-1 years -6 mons`},
		{input: `-1.1y`, output: `-1 years -1 mons`},
		{input: `-1.19y`, output: `-1 years -2 mons`},
		{input: `-1.11y`, output: `-1 years -1 mons`},

		// Mixed unit/HH:MM:SS formats
		{input: `1:2:3`, output: `01:02:03`},
		{input: `-1:2:3`, output: `-01:02:03`},
		{input: `-0:2:3`, output: `-00:02:03`},
		{input: `+0:2:3`, output: `00:02:03`},
		{input: `1 day 12:30`, output: `1 day 12:30:00`},
		{input: `12:30 1 day`, output: `1 day 12:30:00`},
		{input: `1 day -12:30`, output: `1 day -12:30:00`},
		{input: `1 day -00:30`, output: `1 day -00:30:00`},
		{input: `1 day -00:00:30`, output: `1 day -00:00:30`},
		{input: `-1 day +00:00:30`, output: `-1 days +00:00:30`},
		{input: `2 days -4:08.1234`, output: `2 days -00:04:08.1234`},
		{input: `2 days -4:08`, itm: minuteToSecondITM, output: `2 days -00:04:08`},
		{input: `1 day 12:30.5`, output: `1 day 00:12:30.5`},
		{input: `1 day 12:30:40`, output: `1 day 12:30:40`},
		{input: `1 day 12:30:40.5`, output: `1 day 12:30:40.5`},
		{input: `1 day 12:30:40.500500001`, output: `1 day 12:30:40.5005`},

		// Regressions

		// This was 1ns off due to float rounding.
		{input: `50 years 6 mons 75 days 1572897:25:58.535696141`, output: `50 years 6 mons 75 days 1572897:25:58.535696`},
	}
	for _, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			for style := range IntervalStyle_value {
				t.Run(style, func(t *testing.T) {
					styleVal := IntervalStyle(IntervalStyle_value[style])
					expectedError := test.error
					if test.errorSQLStandard != "" && styleVal == IntervalStyle_SQL_STANDARD {
						expectedError = test.errorSQLStandard
					}

					dur, err := parseDuration(styleVal, test.input, test.itm)
					if err != nil {
						if expectedError != "" {
							if err.Error() != expectedError {
								t.Fatalf(`%q: got error "%v", expected "%s"`, test.input, err, expectedError)
							}
						} else {
							t.Fatalf("%q: %v", test.input, err)
						}
						return
					}
					if expectedError != "" {
						t.Fatalf(`%q: expected error "%q"`, test.input, test.error)
					}
					s := dur.String()
					expected := test.output
					if test.outputSQLStandard != "" && styleVal == IntervalStyle_SQL_STANDARD {
						expected = test.outputSQLStandard
					}
					if s != expected {
						t.Fatalf(`%q: got "%s", expected "%s"`, test.input, s, expected)
					}

					dur2, err := parseDuration(styleVal, s, test.itm)
					if err != nil {
						t.Fatalf(`%q: repr "%s" is not parsable: %v`, test.input, s, err)
					}
					s2 := dur2.String()
					if s2 != s {
						t.Fatalf(`%q: repr "%s" does not round-trip, got "%s" instead`, test.input, s, s2)
					}

					// Test that a Datum recognizes the format.
					di, err := ParseInterval(styleVal, test.input, test.itm)
					if err != nil {
						t.Fatalf(`%q: unrecognized as datum: %v`, test.input, err)
					}
					s3 := di.String()
					if s3 != expected {
						t.Fatalf(`%q: as datum, got "%s", expected "%s"`, test.input, s3, expected)
					}
				})
			}
		})
	}
}

func TestISO8601IntervalSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		input  string
		itm    types.IntervalTypeMetadata
		output string
		error  string
	}{
		{`P123`, types.IntervalTypeMetadata{}, ``, `interval: missing unit at position 4: "P123"`},
		{`P123foo`, types.IntervalTypeMetadata{}, ``, `interval: unknown unit "foo" in ISO-8601 duration "P123foo"`},
		{`P 1Y`, types.IntervalTypeMetadata{}, ``, `interval: missing number at position 1: "P 1Y"`},
		{`P1Y `, types.IntervalTypeMetadata{}, ``, `interval: unknown unit "Y " in ISO-8601 duration "P1Y "`},
		{`P1H`, types.IntervalTypeMetadata{}, ``, `interval: unknown unit "H" in ISO-8601 duration "P1H"`},

		{`P`, types.IntervalTypeMetadata{}, `00:00:00`, ``},

		{`PT1.2S`, types.IntervalTypeMetadata{}, `00:00:01.2`, ``},
		{`PT0.2304506708S`, types.IntervalTypeMetadata{}, `00:00:00.230451`, ``},
		{`PT0.0002304506708S`, types.IntervalTypeMetadata{}, `00:00:00.00023`, ``},
		{`PT0.0000002304506S`, types.IntervalTypeMetadata{}, `00:00:00`, ``},
		{`PT75.5S`, types.IntervalTypeMetadata{}, `00:01:15.5`, ``},
		{`PT3675.5S`, types.IntervalTypeMetadata{}, `01:01:15.5`, ``},
		{`PT86475.5S`, types.IntervalTypeMetadata{}, `24:01:15.5`, ``},

		{`PT1.2M`, types.IntervalTypeMetadata{}, `00:01:12`, ``},
		{`PT1.2M8S`, types.IntervalTypeMetadata{}, `00:01:20`, ``},
		{`PT0.5M`, types.IntervalTypeMetadata{}, `00:00:30`, ``},
		{`PT120.5M`, types.IntervalTypeMetadata{}, `02:00:30`, ``},
		{`PT0.23045067089M`, types.IntervalTypeMetadata{}, `00:00:13.82704`, ``},

		{`PT1.2H`, types.IntervalTypeMetadata{}, `01:12:00`, ``},
		{`PT1.2H8M`, types.IntervalTypeMetadata{}, `01:20:00`, ``},
		{`PT0.5H`, types.IntervalTypeMetadata{}, `00:30:00`, ``},
		{`PT25.5H`, types.IntervalTypeMetadata{}, `25:30:00`, ``},
		{`PT0.23045067089H`, types.IntervalTypeMetadata{}, `00:13:49.622415`, ``},

		{`P1D`, types.IntervalTypeMetadata{}, `1 day`, ``},
		{`P1.1D`, types.IntervalTypeMetadata{}, `1 day 02:24:00`, ``},
		{`P1.2D`, types.IntervalTypeMetadata{}, `1 day 04:48:00`, ``},
		{`P1.11D`, types.IntervalTypeMetadata{}, `1 day 02:38:24`, ``},
		{`P1.111D`, types.IntervalTypeMetadata{}, `1 day 02:39:50.4`, ``},
		{`P1.1111D`, types.IntervalTypeMetadata{}, `1 day 02:39:59.04`, ``},
		{`P60DT25H`, types.IntervalTypeMetadata{}, `60 days 25:00:00`, ``},
		{`P9223372036854775807D`, types.IntervalTypeMetadata{}, `9223372036854775807 days`, ``},

		{`P1W`, types.IntervalTypeMetadata{}, `7 days`, ``},
		{`P1.1W`, types.IntervalTypeMetadata{}, `7 days 16:48:00`, ``},
		{`P1.5W`, types.IntervalTypeMetadata{}, `10 days 12:00:00`, ``},
		{`P1W1D`, types.IntervalTypeMetadata{}, `8 days`, ``},

		{`P1M`, types.IntervalTypeMetadata{}, `1 mon`, ``},
		{`P1.5M`, types.IntervalTypeMetadata{}, `1 mon 15 days`, ``},
		{`P1M2W`, types.IntervalTypeMetadata{}, `1 mon 14 days`, ``},
		{`P1.1M`, types.IntervalTypeMetadata{}, `1 mon 3 days`, ``},
		{`P1.2M`, types.IntervalTypeMetadata{}, `1 mon 6 days`, ``},
		{`P1.11M`, types.IntervalTypeMetadata{}, `1 mon 3 days 07:12:00`, ``},
		{`P9223372036854775807M`, types.IntervalTypeMetadata{}, `768614336404564650 years 7 mons`, ``},

		{`P1Y`, types.IntervalTypeMetadata{}, `1 year`, ``},
		{`P1.5Y`, types.IntervalTypeMetadata{}, `1 year 6 mons`, ``},
		{`P1.1Y`, types.IntervalTypeMetadata{}, `1 year 1 mon`, ``},
		{`P1.19Y`, types.IntervalTypeMetadata{}, `1 year 2 mons`, ``},
		{`P1.11Y`, types.IntervalTypeMetadata{}, `1 year 1 mon`, ``},

		// Mixed formats
		{`P1Y2M3D`, minuteToSecondITM, `1 year 2 mons 3 days`, ``},
		{`P1.3Y2.2M3.1D`, minuteToSecondITM, `1 year 5 mons 9 days 02:24:00`, ``},
		{`PT4H5M6S`, minuteToSecondITM, `04:05:06`, ``},
		{`PT4.6H5.5M6.4S`, minuteToSecondITM, `04:41:36.4`, ``},
		{`P1Y2M3DT4H5M6S`, minuteToSecondITM, `1 year 2 mons 3 days 04:05:06`, ``},
		{`P1.6Y2.5M3.4DT4.3H5.2M6.1S`, minuteToSecondITM, `1 year 9 mons 18 days 13:59:18.1`, ``},

		// This was 1ns off due to float rounding.
		{`P50Y6M75DT1572897H25M58.535696141S`, types.IntervalTypeMetadata{}, `50 years 6 mons 75 days 1572897:25:58.535696`, ``},
	}
	for _, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			dur, err := iso8601ToDuration(test.input)
			if err != nil {
				if test.error != "" {
					if err.Error() != test.error {
						t.Fatalf(`%q: got error "%v", expected "%s"`, test.input, err, test.error)
					}
				} else {
					t.Fatalf("%q: %v", test.input, err)
				}
				return
			}
			if test.error != "" {
				t.Fatalf(`%q: expected error "%q"`, test.input, test.error)
			}
			s := dur.String()
			if s != test.output {
				t.Fatalf(`%q: got "%s", expected "%s"`, test.input, s, test.output)
			}

			for style := range IntervalStyle_value {
				t.Run(style, func(t *testing.T) {
					dur2, err := parseDuration(IntervalStyle(IntervalStyle_value[style]), s, test.itm)
					if err != nil {
						t.Fatalf(`%q: repr "%s" is not parsable: %v`, test.input, s, err)
					}
					s2 := dur2.String()
					if s2 != s {
						t.Fatalf(`%q: repr "%s" does not round-trip, got "%s" instead`, test.input, s, s2)
					}

					// Test that a Datum recognizes the format.
					di, err := ParseInterval(IntervalStyle(IntervalStyle_value[style]), test.input, test.itm)
					if err != nil {
						t.Fatalf(`%q: unrecognized as datum: %v`, test.input, err)
					}
					s3 := di.String()
					if s3 != test.output {
						t.Fatalf(`%q: as datum, got "%s", expected "%s"`, test.input, s3, test.output)
					}
				})

				// Test that ISO 8601 output format also round-trips
				s4 := dur.ISO8601String()
				di2, err := ParseInterval(IntervalStyle(IntervalStyle_value[style]), s4, test.itm)
				if err != nil {
					t.Fatalf(`%q: ISO8601String "%s" unrecognized as datum: %v`, test.input, s4, err)
				}
				s5 := di2.String()
				if s != s5 {
					t.Fatalf(`%q: repr "%s" does not round-trip, got %s instead`, test.input, s4, s5)

				}
			}
		})
	}
}
