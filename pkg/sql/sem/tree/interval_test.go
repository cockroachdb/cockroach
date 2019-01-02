// Copyright 2017 The Cockroach Authors.
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

package tree

import "testing"

func TestValidSQLIntervalSyntax(t *testing.T) {
	testData := []struct {
		input  string
		output string
	}{
		{`0:1`, `00:01:00`},
		{`0:1.0`, `00:01:00`},
		{`1`, `00:00:01`},
		{`1.0:0:0`, `01:00:00`},
		{`1.2`, `00:00:01.2`},
		{`1.2:1:1.2`, `01:13:01.2`},
		{`1:0:0`, `01:00:00`},
		{`1:1.2`, `01:01:12`},
		{`1:2`, `01:02:00`},
		{`1:2.3`, `01:02:18`},
		{`1:2:3`, `01:02:03`},
		{`1234:56:54`, `1234:56:54`},
		{`-0:1`, `-00:01:00`},
		{`-0:1.0`, `-00:01:00`},
		{`-1`, `-00:00:01`},
		{`-1.0:0:0`, `-01:00:00`},
		{`-1.2`, `-00:00:01.2`},
		{`-1:0:0`, `-01:00:00`},
		{`-1:1.2`, `-01:01:12`},
		{`-1:2`, `-01:02:00`},
		{`-1:2.3`, `-01:02:18`},
		{`-1:2:3`, `-01:02:03`},
		{`-1234:56:54`, `-1234:56:54`},
		{`1-2`, `1 year 2 mons`},
		{`-1-2`, `-1 years -2 mons`},
		{`1-2 3`, `1 year 2 mons 00:00:03`},
		{`1-2 -3`, `1 year 2 mons -00:00:03`},
		{`-1-2 -3`, `-1 years -2 mons -00:00:03`},
		{`2 4:08`, `2 days 04:08:00`},
		{`-2 4:08`, `-2 days +04:08:00`},
		{`2 -4:08`, `2 days -04:08:00`},
		{`1-2 4:08`, `1 year 2 mons 04:08:00`},
		{`1-2 3 4:08`, `1 year 2 mons 3 days 04:08:00`},
		{`1-2 3 4:08:05`, `1 year 2 mons 3 days 04:08:05`},
		{`1-2 4:08:23`, `1 year 2 mons 04:08:23`},
		{`1- 4:08:23`, `1 year 04:08:23`},
		{`0-2 3 4:08`, `2 mons 3 days 04:08:00`},
		{`1- 3 4:08:`, `1 year 3 days 04:08:00`},
		{`-1- 3 4:08:`, `-1 years 3 days +04:08:00`},
		{`0- 3 4:08`, `3 days 04:08:00`},
		{`-0- 3 4:08`, `3 days 04:08:00`},
		{`-0- -0 4:08`, `04:08:00`},
		{`-0- -0 0:0`, `00:00:00`},
		{`-0- -0 -0:0`, `00:00:00`},
		{`-0- -3 -4:08`, `-3 days -04:08:00`},
		{`0- 3 4::08`, `3 days 04:00:08`},
		{`	0-   3    4::08  `, `3 days 04:00:08`},
		{`2 4:08:23`, `2 days 04:08:23`},
		{`1-2 3 4:08:23`, `1 year 2 mons 3 days 04:08:23`},
		{`1-`, `1 year`},
		{`1- 2`, `1 year 00:00:02`},
		{`2 3:`, `2 days 03:00:00`},
		{`2 3:4:`, `2 days 03:04:00`},
		{`1- 3:`, `1 year 03:00:00`},
		{`1- 3:4`, `1 year 03:04:00`},
	}
	for _, test := range testData {
		t.Run(test.input, func(t *testing.T) {
			dur, err := sqlStdToDuration(test.input)
			if err != nil {
				t.Fatalf("%q: %v", test.input, err)
			}
			s := dur.String()
			if s != test.output {
				t.Fatalf(`%q: got "%s", expected "%s"`, test.input, s, test.output)
			}

			dur2, err := parseDuration(s)
			if err != nil {
				t.Fatalf(`%q: repr "%s" is not parsable: %v`, test.input, s, err)
			}
			s2 := dur2.String()
			if s2 != s {
				t.Fatalf(`%q: repr "%s" does not round-trip, got "%s" instead`,
					test.input, s, s2)
			}

			// Test that a Datum recognizes the format.
			di, err := parseDInterval(test.input, Second)
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
	testData := []struct {
		input  string
		output string
		error  string
	}{
		{`+`, ``, `invalid input syntax for type interval +`},
		{`++`, ``, `invalid input syntax for type interval ++`},
		{`--`, ``, `invalid input syntax for type interval --`},
	}
	for i, test := range testData {
		dur, err := sqlStdToDuration(test.input)
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

		dur2, err := parseDuration(s)
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
		di, err := parseDInterval(test.input, Second)
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
	testData := []struct {
		input  string
		output string
		error  string
	}{
		{``, ``, `interval: invalid input syntax: ""`},
		{`-`, ``, `interval: missing unit at position 1: "-"`},
		{`123`, ``, `interval: missing unit at position 3: "123"`},
		{`123blah`, ``, `interval: unknown unit "blah" in duration "123blah"`},

		{`1.2 nanosecond`, `00:00:00.000000001`, ``},
		{`1.2 nanoseconds`, `00:00:00.000000001`, ``},
		{`1.2 ns`, `00:00:00.000000001`, ``},
		{` 1.2 ns `, `00:00:00.000000001`, ``},
		{`-1.2ns`, `-00:00:00.000000001`, ``},
		{`-1.2nsec`, `-00:00:00.000000001`, ``},
		{`-1.2nsecs`, `-00:00:00.000000001`, ``},
		{`-1.2nsecond`, `-00:00:00.000000001`, ``},
		{`-1.2nseconds`, `-00:00:00.000000001`, ``},
		{`-9223372036854775808ns`, `-2562047:47:16.854775808`, ``},
		{`9223372036854775807ns`, `2562047:47:16.854775807`, ``},

		{`1.2 microsecond`, `00:00:00.0000012`, ``},
		{`1.2microseconds`, `00:00:00.0000012`, ``},
		{`1.2us`, `00:00:00.0000012`, ``},
		// µ = U+00B5 = micro symbol
		// μ = U+03BC = Greek letter mu
		{`1.2µs`, `00:00:00.0000012`, ``},
		{`1.2μs`, `00:00:00.0000012`, ``},
		{`1.2usec`, `00:00:00.0000012`, ``},
		{`1.2usecs`, `00:00:00.0000012`, ``},
		{`1.2usecond`, `00:00:00.0000012`, ``},
		{`1.2useconds`, `00:00:00.0000012`, ``},
		{`0.23us`, `00:00:00.00000023`, ``},
		{`-0.23us`, `-00:00:00.00000023`, ``},
		{`0.2346us`, `00:00:00.000000234`, ``},
		{`-1.2us`, `-00:00:00.0000012`, ``},
		{`1.2us 3ns`, `00:00:00.000001203`, ``},
		{`  1.2us   3ns   `, `00:00:00.000001203`, ``},
		{`3ns 1.2us`, `00:00:00.000001203`, ``},

		{`1.2millisecond`, `00:00:00.0012`, ``},
		{`1.2milliseconds`, `00:00:00.0012`, ``},
		{`1.2ms`, `00:00:00.0012`, ``},
		{`1.2msec`, `00:00:00.0012`, ``},
		{`1.2msecs`, `00:00:00.0012`, ``},
		{`1.2msecond`, `00:00:00.0012`, ``},
		{`1.2mseconds`, `00:00:00.0012`, ``},
		{`0.2304506ms`, `00:00:00.00023045`, ``},
		{`0.0002304506ms`, `00:00:00.00000023`, ``},
		{`1 ms 1us 1ns`, `00:00:00.001001001`, ``},

		{`1.2second`, `00:00:01.2`, ``},
		{`1.2seconds`, `00:00:01.2`, ``},
		{`1.2s`, `00:00:01.2`, ``},
		{`1.2sec`, `00:00:01.2`, ``},
		{`1.2secs`, `00:00:01.2`, ``},
		{`0.2304506708s`, `00:00:00.23045067`, ``},
		{`0.0002304506708s`, `00:00:00.00023045`, ``},
		{`0.0000002304506s`, `00:00:00.00000023`, ``},
		{`75.5s`, `00:01:15.5`, ``},
		{`3675.5s`, `01:01:15.5`, ``},
		{`86475.5s`, `24:01:15.5`, ``},
		{`86400s -60000ms 100us -1ns`, `23:59:00.000099999`, ``},

		{`1.2minute`, `00:01:12`, ``},
		{`1.2minutes`, `00:01:12`, ``},
		{`1.2m`, `00:01:12`, ``},
		{`1.2min`, `00:01:12`, ``},
		{`1.2mins`, `00:01:12`, ``},
		{`1.2m 8s 20ns`, `00:01:20.00000002`, ``},
		{`0.5m`, `00:00:30`, ``},
		{`120.5m`, `02:00:30`, ``},
		{`0.23045067089m`, `00:00:13.827040253`, ``},
		{`-0.23045067089m`, `-00:00:13.827040253`, ``},

		{`1.2hour`, `01:12:00`, ``},
		{`1.2hours`, `01:12:00`, ``},
		{`1.2h`, `01:12:00`, ``},
		{`1.2hr`, `01:12:00`, ``},
		{`1.2hrs`, `01:12:00`, ``},
		{`1.2h 8m 20ns`, `01:20:00.00000002`, ``},
		{`0.5h`, `00:30:00`, ``},
		{`25.5h`, `25:30:00`, ``},
		{`0.23045067089h`, `00:13:49.622415204`, ``},
		{`-0.23045067089h`, `-00:13:49.622415204`, ``},

		{`1 day`, `1 day`, ``},
		{`1 days`, `1 day`, ``},
		{`1d`, `1 day`, ``},
		{`1.1d`, `1 day 02:24:00`, ``},
		{`1.2d`, `1 day 04:48:00`, ``},
		{`1.11d`, `1 day 02:38:24`, ``},
		{`1.111d`, `1 day 02:39:50.4`, ``},
		{`1.1111d`, `1 day 02:39:59.04`, ``},
		{`60d 25h`, `60 days 25:00:00`, ``},
		{`-9223372036854775808d`, `-9223372036854775808 days`, ``},
		{`9223372036854775807d`, `9223372036854775807 days`, ``},

		{`1week`, `7 days`, ``},
		{`1weeks`, `7 days`, ``},
		{`1w`, `7 days`, ``},
		{`1.1w`, `7 days 16:48:00`, ``},
		{`1.5w`, `10 days 12:00:00`, ``},
		{`1w -1d`, `6 days`, ``},

		{`1month`, `1 mon`, ``},
		{`1months`, `1 mon`, ``},
		{`1mons`, `1 mon`, ``},
		{`1.5mon`, `1 mon 15 days`, ``},
		{`1 mon 2 week`, `1 mon 14 days`, ``},
		{`1.1mon`, `1 mon 3 days`, ``},
		{`1.2mon`, `1 mon 6 days`, ``},
		{`1.11mon`, `1 mon 3 days 07:11:59.999999999`, ``},
		{`-9223372036854775808mon`, `-768614336404564650 years -8 mons`, ``},
		{`9223372036854775807mon`, `768614336404564650 years 7 mons`, ``},

		{`1year`, `1 year`, ``},
		{`1years`, `1 year`, ``},
		{`1y`, `1 year`, ``},
		{`1yr`, `1 year`, ``},
		{`1yrs`, `1 year`, ``},
		{`1.5y`, `1 year 6 mons`, ``},
		{`1.1y`, `1 year 1 mon 6 days`, ``},
		{`1.11y`, `1 year 1 mon 9 days 14:24:00`, ``},

		// Mixed unit/HH:MM:SS formats
		{`1:2:3`, `01:02:03`, ``},
		{`-1:2:3`, `-01:02:03`, ``},
		{`-0:2:3`, `-00:02:03`, ``},
		{`+0:2:3`, `00:02:03`, ``},
		{`1 day 12:30`, `1 day 12:30:00`, ``},
		{`12:30 1 day`, `1 day 12:30:00`, ``},
		{`1 day -12:30`, `1 day -12:30:00`, ``},
		{`1 day -00:30`, `1 day -00:30:00`, ``},
		{`1 day -00:00:30`, `1 day -00:00:30`, ``},
		{`-1 day +00:00:30`, `-1 days +00:00:30`, ``},
		{`1 day 12:30.5`, `1 day 00:12:30.5`, ``},
		{`1 day 12:30:40`, `1 day 12:30:40`, ``},
		{`1 day 12:30:40.5`, `1 day 12:30:40.5`, ``},
		{`1 day 12:30:40.500500001`, `1 day 12:30:40.500500001`, ``},

		// Regressions

		// This was 1ns off due to float rounding.
		{`50 years 6 mons 75 days 1572897:25:58.535696141`, `50 years 6 mons 75 days 1572897:25:58.535696141`, ``},
	}
	for i, test := range testData {
		dur, err := parseDuration(test.input)
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

		dur2, err := parseDuration(s)
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
		di, err := parseDInterval(test.input, Second)
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
