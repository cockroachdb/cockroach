// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
	testData := []struct {
		input  string
		itm    types.IntervalTypeMetadata
		output string
	}{
		{`0:1`, types.IntervalTypeMetadata{}, `00:01:00`},
		{`0:1.0`, types.IntervalTypeMetadata{}, `00:00:01`},
		{`1`, types.IntervalTypeMetadata{}, `00:00:01`},
		{`1.0:0:0`, types.IntervalTypeMetadata{}, `1 day`},
		{`1.2`, types.IntervalTypeMetadata{}, `00:00:01.2`},
		{`:3:4`, types.IntervalTypeMetadata{}, `03:04:00`},
		{`:-3:4`, types.IntervalTypeMetadata{}, `-03:04:00`},
		{`:3:4.1`, types.IntervalTypeMetadata{}, `00:03:04.1`},
		{`1.2:1:1.2`, types.IntervalTypeMetadata{}, `1 day 04:49:01.2`},
		{`1.2:+1:1.2`, types.IntervalTypeMetadata{}, `1 day 04:49:01.2`},
		{`1.2:-1:1.2`, types.IntervalTypeMetadata{}, `1 day 04:46:58.8`},
		{`1:0:0`, types.IntervalTypeMetadata{}, `01:00:00`},
		{`1:1.2`, types.IntervalTypeMetadata{}, `00:01:01.2`},
		{`1:2`, types.IntervalTypeMetadata{}, `01:02:00`},
		{`1:2.3`, types.IntervalTypeMetadata{}, `00:01:02.3`},
		{`1:2:3`, types.IntervalTypeMetadata{}, `01:02:03`},
		{`1234:56:54`, types.IntervalTypeMetadata{}, `1234:56:54`},
		{`-0:1`, types.IntervalTypeMetadata{}, `-00:01:00`},
		{`-0:1.0`, types.IntervalTypeMetadata{}, `-00:00:01`},
		{`-1`, types.IntervalTypeMetadata{}, `-00:00:01`},
		{`-1.2`, types.IntervalTypeMetadata{}, `-00:00:01.2`},
		{`-1:0:0`, types.IntervalTypeMetadata{}, `-01:00:00`},
		{`-1:1.2`, types.IntervalTypeMetadata{}, `-00:01:01.2`},
		{`-1:2`, types.IntervalTypeMetadata{}, `-01:02:00`},
		{`-1:2.3`, types.IntervalTypeMetadata{}, `-00:01:02.3`},
		{`-1:2:3`, types.IntervalTypeMetadata{}, `-01:02:03`},
		{`-1234:56:54`, types.IntervalTypeMetadata{}, `-1234:56:54`},
		{`1-2`, types.IntervalTypeMetadata{}, `1 year 2 mons`},
		{`-1-2`, types.IntervalTypeMetadata{}, `-1 years -2 mons`},
		{`1-2 3`, types.IntervalTypeMetadata{}, `1 year 2 mons 00:00:03`},
		{`1-2 3`, types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_YEAR,
			},
		}, `4 years 2 mons`}, // this gets truncated later to 4 years
		{`1-2 3`, types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_MONTH,
			},
		}, `1 year 5 mons`},
		{`1-2 3`, types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_DAY,
			},
		}, `1 year 2 mons 3 days`},
		{`1-2 3`, types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_HOUR,
			},
		}, `1 year 2 mons 03:00:00`},
		{`1-2 3`, types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_MINUTE,
			},
		}, `1 year 2 mons 00:03:00`},
		{`1-2 3`, types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_SECOND,
			},
		}, `1 year 2 mons 00:00:03`},
		{`1-2 -3`, types.IntervalTypeMetadata{}, `1 year 2 mons -00:00:03`},
		{`-1-2 -3`, types.IntervalTypeMetadata{}, `-1 years -2 mons -00:00:03`},
		{`2 4:08`, types.IntervalTypeMetadata{}, `2 days 04:08:00`},
		{`2.5 4:08`, types.IntervalTypeMetadata{}, `2 days 16:08:00`},
		{`-2 4:08`, types.IntervalTypeMetadata{}, `-2 days +04:08:00`},
		{`2 -4:08`, types.IntervalTypeMetadata{}, `2 days -04:08:00`},
		{`2 -4:08.1234`, types.IntervalTypeMetadata{}, `2 days -00:04:08.1234`},
		{`2 -4:08.1234`, minuteToSecondITM, `2 days -00:04:08.1234`},
		{`2 -4:08`, minuteToSecondITM, `2 days -00:04:08`},
		{`1-2 4:08`, types.IntervalTypeMetadata{}, `1 year 2 mons 04:08:00`},
		{`1-2 3 4:08`, types.IntervalTypeMetadata{}, `1 year 2 mons 3 days 04:08:00`},
		{`1-2 3 4:08:05`, types.IntervalTypeMetadata{}, `1 year 2 mons 3 days 04:08:05`},
		{`1-2 4:08:23`, types.IntervalTypeMetadata{}, `1 year 2 mons 04:08:23`},
		{`1- 4:08:23`, types.IntervalTypeMetadata{}, `1 year 04:08:23`},
		{`0-2 3 4:08`, types.IntervalTypeMetadata{}, `2 mons 3 days 04:08:00`},
		{`1- 3 4:08:`, types.IntervalTypeMetadata{}, `1 year 3 days 04:08:00`},
		{`-1- 3 4:08:`, types.IntervalTypeMetadata{}, `-1 years 3 days +04:08:00`},
		{`0- 3 4:08`, types.IntervalTypeMetadata{}, `3 days 04:08:00`},
		{`-0- 3 4:08`, types.IntervalTypeMetadata{}, `3 days 04:08:00`},
		{`-0- -0 4:08`, types.IntervalTypeMetadata{}, `04:08:00`},
		{`-0- -0 0:0`, types.IntervalTypeMetadata{}, `00:00:00`},
		{`-0- -0 -0:0`, types.IntervalTypeMetadata{}, `00:00:00`},
		{`-0- -3 -4:08`, types.IntervalTypeMetadata{}, `-3 days -04:08:00`},
		{`0- 3 4::08`, types.IntervalTypeMetadata{}, `3 days 04:00:08`},
		{`	0-   3    4::08  `, types.IntervalTypeMetadata{}, `3 days 04:00:08`},
		{`2 4:08:23`, types.IntervalTypeMetadata{}, `2 days 04:08:23`},
		{`1-2 3 4:08:23`, types.IntervalTypeMetadata{}, `1 year 2 mons 3 days 04:08:23`},
		{`1-`, types.IntervalTypeMetadata{}, `1 year`},
		{`1- 2`, types.IntervalTypeMetadata{}, `1 year 00:00:02`},
		{`2 3:`, types.IntervalTypeMetadata{}, `2 days 03:00:00`},
		{`2 3:4:`, types.IntervalTypeMetadata{}, `2 days 03:04:00`},
		{`1- 3:`, types.IntervalTypeMetadata{}, `1 year 03:00:00`},
		{`1- 3:4`, types.IntervalTypeMetadata{}, `1 year 03:04:00`},

		{`2 3`, dayToHourITM, `2 days 03:00:00`},
		{`-2 -3`, dayToHourITM, `-2 days -03:00:00`},
		{`-2 3`, dayToHourITM, `-2 days +03:00:00`},
		{`2 -3`, dayToHourITM, `2 days -03:00:00`},
		{`1-2 3`, dayToHourITM, `1 year 2 mons 03:00:00`},
		{`-1-2 -3`, dayToHourITM, `-1 years -2 mons -03:00:00`},
		{`-1-2 3`, dayToHourITM, `-1 years -2 mons +03:00:00`},
		{`1-2 -3`, dayToHourITM, `1 year 2 mons -03:00:00`},
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

			dur2, err := parseDuration(s, test.itm)
			if err != nil {
				t.Fatalf(`%q: repr "%s" is not parsable: %v`, test.input, s, err)
			}
			s2 := dur2.String()
			if s2 != s {
				t.Fatalf(`%q: repr "%s" does not round-trip, got "%s" instead`,
					test.input, s, s2)
			}

			// Test that a Datum recognizes the format.
			di, err := parseDInterval(test.input, test.itm)
			if err != nil {
				t.Fatalf(`%q: unrecognized as datum: %v`, test.input, err)
			}
			s3 := di.Duration.String()
			if s3 != test.output {
				t.Fatalf(`%q: as datum, got "%s", expected "%s"`, test.input, s3, test.output)
			}
		})
	}
}

func TestInvalidSQLIntervalSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		input  string
		output string
		error  string
	}{
		{`+`, ``, `invalid input syntax for type interval +`},
		{`++`, ``, `invalid input syntax for type interval ++`},
		{`--`, ``, `invalid input syntax for type interval --`},
		{`{1,2}`, ``, `invalid input syntax for type interval {1,2}`},
		{`0.000,0`, ``, `invalid input syntax for type interval 0.000,0`},
		{`0,0`, ``, `invalid input syntax for type interval 0,0`},
		{`2 3`, ``, `invalid input syntax for type interval 2 3`},
		{`-2 3`, ``, `invalid input syntax for type interval -2 3`},
		{`-2 -3`, ``, `invalid input syntax for type interval -2 -3`},
		{`2 -3`, ``, `invalid input syntax for type interval 2 -3`},
		{`0:-1`, ``, `invalid input syntax for type interval 0:-1`},
		{`0:0:-1`, ``, `invalid input syntax for type interval 0:0:-1`},
		{`1.0:0:-1`, ``, `invalid input syntax for type interval 1.0:0:-1`},
		{`0:-1:0`, ``, `invalid input syntax for type interval 0:-1:0`},
		{`0:-1:-1`, ``, `invalid input syntax for type interval 0:-1:-1`},
		{`-1.0:0:0`, ``, `invalid input syntax for type interval -1.0:0:0`},
		{`-:0:0`, ``, `invalid input syntax for type interval -:0:0`},
	}
	for i, test := range testData {
		dur, err := sqlStdToDuration(test.input, types.IntervalTypeMetadata{})
		if err != nil {
			if test.error != "" {
				if err.Error() != test.error {
					t.Errorf(`%d: %q: got error "%v", expected "%s"`, i, test.input, err, test.error)
				}
			} else {
				t.Errorf("%d: %q: %v", i, test.input, err)
			}
			continue
		} else {
			if test.error != "" {
				t.Errorf(`%d: %q: expected error "%q"`, i, test.input, test.error)
				continue
			}
		}
		s := dur.String()
		if s != test.output {
			t.Errorf(`%d: %q: got "%s", expected "%s"`, i, test.input, s, test.output)
			continue
		}

		dur2, err := parseDuration(s, types.IntervalTypeMetadata{})
		if err != nil {
			t.Errorf(`%d: %q: repr "%s" is not parsable: %v`, i, test.input, s, err)
			continue
		}
		s2 := dur2.String()
		if s2 != s {
			t.Errorf(`%d: %q: repr "%s" does not round-trip, got "%s" instead`,
				i, test.input, s, s2)
		}

		// Test that a Datum recognizes the format.
		di, err := parseDInterval(test.input, types.IntervalTypeMetadata{})
		if err != nil {
			t.Errorf(`%d: %q: unrecognized as datum: %v`, i, test.input, err)
			continue
		}
		s3 := di.Duration.String()
		if s3 != test.output {
			t.Errorf(`%d: %q: as datum, got "%s", expected "%s"`, i, test.input, s3, test.output)
		}
	}
}

func TestPGIntervalSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		input  string
		itm    types.IntervalTypeMetadata
		output string
		error  string
	}{
		{``, types.IntervalTypeMetadata{}, ``, `interval: invalid input syntax: ""`},
		{`-`, types.IntervalTypeMetadata{}, ``, `interval: missing unit at position 1: "-"`},
		{`123`, types.IntervalTypeMetadata{}, ``, `interval: missing unit at position 3: "123"`},
		{`123blah`, types.IntervalTypeMetadata{}, ``, `interval: unknown unit "blah" in duration "123blah"`},

		{`500nanoseconds`, types.IntervalTypeMetadata{}, ``, `interval: unknown unit "nanoseconds" in duration "500nanoseconds"`},
		{`500ns`, types.IntervalTypeMetadata{}, ``, `interval: unknown unit "ns" in duration "500ns"`},

		// ns/us boundary
		{`.5us`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`-0.499us`, types.IntervalTypeMetadata{}, `00:00:00`, ``},
		{`-0.5us`, types.IntervalTypeMetadata{}, `-00:00:00.000001`, ``},
		{`0.000000499s`, types.IntervalTypeMetadata{}, `00:00:00`, ``},
		{`0.0000005s`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`-0.000000499s`, types.IntervalTypeMetadata{}, `00:00:00`, ``},
		{`-0.0000005s`, types.IntervalTypeMetadata{}, `-00:00:00.000001`, ``},

		{`1.2 microsecond`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`1.2microseconds`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`1.2us`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		// µ = U+00B5 = micro symbol
		// μ = U+03BC = Greek letter mu
		{`1.2µs`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`1.2μs`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`1.2usec`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`1.2usecs`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`1.2usecond`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`1.2useconds`, types.IntervalTypeMetadata{}, `00:00:00.000001`, ``},
		{`0.23us`, types.IntervalTypeMetadata{}, `00:00:00`, ``},
		{`-0.23us`, types.IntervalTypeMetadata{}, `00:00:00`, ``},
		{`0.2346us`, types.IntervalTypeMetadata{}, `00:00:00`, ``},
		{`-1.2us`, types.IntervalTypeMetadata{}, `-00:00:00.000001`, ``},

		{`1.2millisecond`, types.IntervalTypeMetadata{}, `00:00:00.0012`, ``},
		{`1.2milliseconds`, types.IntervalTypeMetadata{}, `00:00:00.0012`, ``},
		{`1.2ms`, types.IntervalTypeMetadata{}, `00:00:00.0012`, ``},
		{`1.2msec`, types.IntervalTypeMetadata{}, `00:00:00.0012`, ``},
		{`1.2msecs`, types.IntervalTypeMetadata{}, `00:00:00.0012`, ``},
		{`1.2msecond`, types.IntervalTypeMetadata{}, `00:00:00.0012`, ``},
		{`1.2mseconds`, types.IntervalTypeMetadata{}, `00:00:00.0012`, ``},
		{`0.2304506ms`, types.IntervalTypeMetadata{}, `00:00:00.00023`, ``},
		{`0.0002304506ms`, types.IntervalTypeMetadata{}, `00:00:00`, ``},

		{`1.2second`, types.IntervalTypeMetadata{}, `00:00:01.2`, ``},
		{`1.2seconds`, types.IntervalTypeMetadata{}, `00:00:01.2`, ``},
		{`1.2s`, types.IntervalTypeMetadata{}, `00:00:01.2`, ``},
		{`1.2sec`, types.IntervalTypeMetadata{}, `00:00:01.2`, ``},
		{`1.2secs`, types.IntervalTypeMetadata{}, `00:00:01.2`, ``},
		{`0.2304506708s`, types.IntervalTypeMetadata{}, `00:00:00.230451`, ``},
		{`0.0002304506708s`, types.IntervalTypeMetadata{}, `00:00:00.00023`, ``},
		{`0.0000002304506s`, types.IntervalTypeMetadata{}, `00:00:00`, ``},
		{`75.5s`, types.IntervalTypeMetadata{}, `00:01:15.5`, ``},
		{`3675.5s`, types.IntervalTypeMetadata{}, `01:01:15.5`, ``},
		{`86475.5s`, types.IntervalTypeMetadata{}, `24:01:15.5`, ``},
		{`86400s -60000ms 100us`, types.IntervalTypeMetadata{}, `23:59:00.0001`, ``},

		{`1.2minute`, types.IntervalTypeMetadata{}, `00:01:12`, ``},
		{`1.2minutes`, types.IntervalTypeMetadata{}, `00:01:12`, ``},
		{`1.2m`, types.IntervalTypeMetadata{}, `00:01:12`, ``},
		{`1.2min`, types.IntervalTypeMetadata{}, `00:01:12`, ``},
		{`1.2mins`, types.IntervalTypeMetadata{}, `00:01:12`, ``},
		{`1.2m 8s`, types.IntervalTypeMetadata{}, `00:01:20`, ``},
		{`0.5m`, types.IntervalTypeMetadata{}, `00:00:30`, ``},
		{`120.5m`, types.IntervalTypeMetadata{}, `02:00:30`, ``},
		{`0.23045067089m`, types.IntervalTypeMetadata{}, `00:00:13.82704`, ``},
		{`-0.23045067089m`, types.IntervalTypeMetadata{}, `-00:00:13.82704`, ``},

		{`1.2hour`, types.IntervalTypeMetadata{}, `01:12:00`, ``},
		{`1.2hours`, types.IntervalTypeMetadata{}, `01:12:00`, ``},
		{`1.2h`, types.IntervalTypeMetadata{}, `01:12:00`, ``},
		{`1.2hr`, types.IntervalTypeMetadata{}, `01:12:00`, ``},
		{`1.2hrs`, types.IntervalTypeMetadata{}, `01:12:00`, ``},
		{`1.2h 8m`, types.IntervalTypeMetadata{}, `01:20:00`, ``},
		{`0.5h`, types.IntervalTypeMetadata{}, `00:30:00`, ``},
		{`25.5h`, types.IntervalTypeMetadata{}, `25:30:00`, ``},
		{`0.23045067089h`, types.IntervalTypeMetadata{}, `00:13:49.622415`, ``},
		{`-0.23045067089h`, types.IntervalTypeMetadata{}, `-00:13:49.622415`, ``},

		{`1 day`, types.IntervalTypeMetadata{}, `1 day`, ``},
		{`1 days`, types.IntervalTypeMetadata{}, `1 day`, ``},
		{`1d`, types.IntervalTypeMetadata{}, `1 day`, ``},
		{`1.1d`, types.IntervalTypeMetadata{}, `1 day 02:24:00`, ``},
		{`1.2d`, types.IntervalTypeMetadata{}, `1 day 04:48:00`, ``},
		{`1.11d`, types.IntervalTypeMetadata{}, `1 day 02:38:24`, ``},
		{`1.111d`, types.IntervalTypeMetadata{}, `1 day 02:39:50.4`, ``},
		{`1.1111d`, types.IntervalTypeMetadata{}, `1 day 02:39:59.04`, ``},
		{`60d 25h`, types.IntervalTypeMetadata{}, `60 days 25:00:00`, ``},
		{`-9223372036854775808d`, types.IntervalTypeMetadata{}, `-9223372036854775808 days`, ``},
		{`9223372036854775807d`, types.IntervalTypeMetadata{}, `9223372036854775807 days`, ``},

		{`1week`, types.IntervalTypeMetadata{}, `7 days`, ``},
		{`1weeks`, types.IntervalTypeMetadata{}, `7 days`, ``},
		{`1w`, types.IntervalTypeMetadata{}, `7 days`, ``},
		{`1.1w`, types.IntervalTypeMetadata{}, `7 days 16:48:00`, ``},
		{`1.5w`, types.IntervalTypeMetadata{}, `10 days 12:00:00`, ``},
		{`1w -1d`, types.IntervalTypeMetadata{}, `6 days`, ``},

		{`1month`, types.IntervalTypeMetadata{}, `1 mon`, ``},
		{`1months`, types.IntervalTypeMetadata{}, `1 mon`, ``},
		{`1mons`, types.IntervalTypeMetadata{}, `1 mon`, ``},
		{`1.5mon`, types.IntervalTypeMetadata{}, `1 mon 15 days`, ``},
		{`1 mon 2 week`, types.IntervalTypeMetadata{}, `1 mon 14 days`, ``},
		{`1.1mon`, types.IntervalTypeMetadata{}, `1 mon 3 days`, ``},
		{`1.2mon`, types.IntervalTypeMetadata{}, `1 mon 6 days`, ``},
		{`1.11mon`, types.IntervalTypeMetadata{}, `1 mon 3 days 07:12:00`, ``},
		{`-9223372036854775808mon`, types.IntervalTypeMetadata{}, `-768614336404564650 years -8 mons`, ``},
		{`9223372036854775807mon`, types.IntervalTypeMetadata{}, `768614336404564650 years 7 mons`, ``},

		{`1year`, types.IntervalTypeMetadata{}, `1 year`, ``},
		{`1years`, types.IntervalTypeMetadata{}, `1 year`, ``},
		{`1y`, types.IntervalTypeMetadata{}, `1 year`, ``},
		{`1yr`, types.IntervalTypeMetadata{}, `1 year`, ``},
		{`1yrs`, types.IntervalTypeMetadata{}, `1 year`, ``},
		{`1.5y`, types.IntervalTypeMetadata{}, `1 year 6 mons`, ``},
		{`1.1y`, types.IntervalTypeMetadata{}, `1 year 1 mon 6 days`, ``},
		{`1.11y`, types.IntervalTypeMetadata{}, `1 year 1 mon 9 days 14:24:00`, ``},

		// Mixed unit/HH:MM:SS formats
		{`1:2:3`, types.IntervalTypeMetadata{}, `01:02:03`, ``},
		{`-1:2:3`, types.IntervalTypeMetadata{}, `-01:02:03`, ``},
		{`-0:2:3`, types.IntervalTypeMetadata{}, `-00:02:03`, ``},
		{`+0:2:3`, types.IntervalTypeMetadata{}, `00:02:03`, ``},
		{`1 day 12:30`, types.IntervalTypeMetadata{}, `1 day 12:30:00`, ``},
		{`12:30 1 day`, types.IntervalTypeMetadata{}, `1 day 12:30:00`, ``},
		{`1 day -12:30`, types.IntervalTypeMetadata{}, `1 day -12:30:00`, ``},
		{`1 day -00:30`, types.IntervalTypeMetadata{}, `1 day -00:30:00`, ``},
		{`1 day -00:00:30`, types.IntervalTypeMetadata{}, `1 day -00:00:30`, ``},
		{`-1 day +00:00:30`, types.IntervalTypeMetadata{}, `-1 days +00:00:30`, ``},
		{`2 days -4:08.1234`, types.IntervalTypeMetadata{}, `2 days -00:04:08.1234`, ``},
		{`2 days -4:08`, minuteToSecondITM, `2 days -00:04:08`, ``},
		{`1 day 12:30.5`, types.IntervalTypeMetadata{}, `1 day 00:12:30.5`, ``},
		{`1 day 12:30:40`, types.IntervalTypeMetadata{}, `1 day 12:30:40`, ``},
		{`1 day 12:30:40.5`, types.IntervalTypeMetadata{}, `1 day 12:30:40.5`, ``},
		{`1 day 12:30:40.500500001`, types.IntervalTypeMetadata{}, `1 day 12:30:40.5005`, ``},

		// Regressions

		// This was 1ns off due to float rounding.
		{`50 years 6 mons 75 days 1572897:25:58.535696141`, types.IntervalTypeMetadata{}, `50 years 6 mons 75 days 1572897:25:58.535696`, ``},
	}
	for _, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			dur, err := parseDuration(test.input, test.itm)
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

			dur2, err := parseDuration(s, test.itm)
			if err != nil {
				t.Fatalf(`%q: repr "%s" is not parsable: %v`, test.input, s, err)
			}
			s2 := dur2.String()
			if s2 != s {
				t.Fatalf(`%q: repr "%s" does not round-trip, got "%s" instead`, test.input, s, s2)
			}

			// Test that a Datum recognizes the format.
			di, err := parseDInterval(test.input, test.itm)
			if err != nil {
				t.Fatalf(`%q: unrecognized as datum: %v`, test.input, err)
			}
			s3 := di.Duration.String()
			if s3 != test.output {
				t.Fatalf(`%q: as datum, got "%s", expected "%s"`, test.input, s3, test.output)
			}
		})
	}
}
