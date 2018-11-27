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
		{`0:1`, `1m`},
		{`0:1.0`, `1m`},
		{`1`, `1s`},
		{`1.0:0:0`, `1h`},
		{`1.2`, `1s200ms`},
		{`1.2:1:1.2`, `1h13m1s200ms`},
		{`1:0:0`, `1h`},
		{`1:1.2`, `1h1m12s`},
		{`1:2`, `1h2m`},
		{`1:2.3`, `1h2m18s`},
		{`1:2:3`, `1h2m3s`},
		{`1234:56:54`, `1234h56m54s`},
		{`-0:1`, `-1m`},
		{`-0:1.0`, `-1m`},
		{`-1`, `-1s`},
		{`-1.0:0:0`, `-1h`},
		{`-1.2`, `-1s-200ms`},
		{`-1:0:0`, `-1h`},
		{`-1:1.2`, `-1h-1m-12s`},
		{`-1:2`, `-1h-2m`},
		{`-1:2.3`, `-1h-2m-18s`},
		{`-1:2:3`, `-1h-2m-3s`},
		{`-1234:56:54`, `-1234h-56m-54s`},
		{`1-2`, `1y2mon`},
		{`-1-2`, `-1y-2mon`},
		{`1-2 3`, `1y2mon3s`},
		{`1-2 -3`, `1y2mon-3s`},
		{`-1-2 -3`, `-1y-2mon-3s`},
		{`2 4:08`, `2d4h8m`},
		{`-2 4:08`, `-2d4h8m`},
		{`2 -4:08`, `2d-4h-8m`},
		{`1-2 4:08`, `1y2mon4h8m`},
		{`1-2 3 4:08`, `1y2mon3d4h8m`},
		{`1-2 3 4:08:05`, `1y2mon3d4h8m5s`},
		{`1-2 4:08:23`, `1y2mon4h8m23s`},
		{`1- 4:08:23`, `1y4h8m23s`},
		{`0-2 3 4:08`, `2mon3d4h8m`},
		{`1- 3 4:08:`, `1y3d4h8m`},
		{`-1- 3 4:08:`, `-1y3d4h8m`},
		{`0- 3 4:08`, `3d4h8m`},
		{`-0- 3 4:08`, `3d4h8m`},
		{`-0- -0 4:08`, `4h8m`},
		{`-0- -0 0:0`, `0s`},
		{`-0- -0 -0:0`, `0s`},
		{`-0- -3 -4:08`, `-3d-4h-8m`},
		{`0- 3 4::08`, `3d4h8s`},
		{`	0-   3    4::08  `, `3d4h8s`},
		{`2 4:08:23`, `2d4h8m23s`},
		{`1-2 3 4:08:23`, `1y2mon3d4h8m23s`},
		{`1-`, `1y`},
		{`1- 2`, `1y2s`},
		{`2 3:`, `2d3h`},
		{`2 3:4:`, `2d3h4m`},
		{`1- 3:`, `1y3h`},
		{`1- 3:4`, `1y3h4m`},
	}
	for i, test := range testData {
		dur, err := sqlStdToDuration(test.input)
		if err != nil {
			t.Errorf("%d: %q: %v", i, test.input, err)
			continue
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

		{`1.2 nanosecond`, `1ns`, ``},
		{`1.2 nanoseconds`, `1ns`, ``},
		{`1.2 ns`, `1ns`, ``},
		{` 1.2 ns `, `1ns`, ``},
		{`-1.2ns`, `-1ns`, ``},
		{`-1.2nsec`, `-1ns`, ``},
		{`-1.2nsecs`, `-1ns`, ``},
		{`-1.2nsecond`, `-1ns`, ``},
		{`-1.2nseconds`, `-1ns`, ``},
		{`-9223372036854775808ns`, `-2562047h-47m-16s-854ms-775µs-808ns`, ``},
		{`9223372036854775807ns`, `2562047h47m16s854ms775µs807ns`, ``},

		{`1.2 microsecond`, `1µs200ns`, ``},
		{`1.2microseconds`, `1µs200ns`, ``},
		{`1.2us`, `1µs200ns`, ``},
		// µ = U+00B5 = micro symbol
		// μ = U+03BC = Greek letter mu
		{`1.2µs`, `1µs200ns`, ``},
		{`1.2μs`, `1µs200ns`, ``},
		{`1.2usec`, `1µs200ns`, ``},
		{`1.2usecs`, `1µs200ns`, ``},
		{`1.2usecond`, `1µs200ns`, ``},
		{`1.2useconds`, `1µs200ns`, ``},
		{`0.23us`, `230ns`, ``},
		{`-0.23us`, `-230ns`, ``},
		{`0.2346us`, `234ns`, ``},
		{`-1.2us`, `-1µs-200ns`, ``},
		{`1.2us 3ns`, `1µs203ns`, ``},
		{`  1.2us   3ns   `, `1µs203ns`, ``},
		{`3ns 1.2us`, `1µs203ns`, ``},

		{`1.2millisecond`, `1ms200µs`, ``},
		{`1.2milliseconds`, `1ms200µs`, ``},
		{`1.2ms`, `1ms200µs`, ``},
		{`1.2msec`, `1ms200µs`, ``},
		{`1.2msecs`, `1ms200µs`, ``},
		{`1.2msecond`, `1ms200µs`, ``},
		{`1.2mseconds`, `1ms200µs`, ``},
		{`0.2304506ms`, `230µs450ns`, ``},
		{`0.0002304506ms`, `230ns`, ``},
		{`1 ms 1us 1ns`, `1ms1µs1ns`, ``},

		{`1.2second`, `1s200ms`, ``},
		{`1.2seconds`, `1s200ms`, ``},
		{`1.2s`, `1s200ms`, ``},
		{`1.2sec`, `1s200ms`, ``},
		{`1.2secs`, `1s200ms`, ``},
		{`0.2304506708s`, `230ms450µs670ns`, ``},
		{`0.0002304506708s`, `230µs450ns`, ``},
		{`0.0000002304506s`, `230ns`, ``},
		{`75.5s`, `1m15s500ms`, ``},
		{`3675.5s`, `1h1m15s500ms`, ``},
		{`86475.5s`, `24h1m15s500ms`, ``},
		{`86400s -60000ms 100us -1ns`, `23h59m99µs999ns`, ``},

		{`1.2minute`, `1m12s`, ``},
		{`1.2minutes`, `1m12s`, ``},
		{`1.2m`, `1m12s`, ``},
		{`1.2min`, `1m12s`, ``},
		{`1.2mins`, `1m12s`, ``},
		{`1.2m 8s 20ns`, `1m20s20ns`, ``},
		{`0.5m`, `30s`, ``},
		{`120.5m`, `2h30s`, ``},
		{`0.23045067089m`, `13s827ms40µs253ns`, ``},
		{`-0.23045067089m`, `-13s-827ms-40µs-253ns`, ``},

		{`1.2hour`, `1h12m`, ``},
		{`1.2hours`, `1h12m`, ``},
		{`1.2h`, `1h12m`, ``},
		{`1.2hr`, `1h12m`, ``},
		{`1.2hrs`, `1h12m`, ``},
		{`1.2h 8m 20ns`, `1h20m20ns`, ``},
		{`0.5h`, `30m`, ``},
		{`25.5h`, `25h30m`, ``},
		{`0.23045067089h`, `13m49s622ms415µs204ns`, ``},
		{`-0.23045067089h`, `-13m-49s-622ms-415µs-204ns`, ``},

		{`1 day`, `1d`, ``},
		{`1 days`, `1d`, ``},
		{`1d`, `1d`, ``},
		{`1.1d`, `1d2h24m`, ``},
		{`1.2d`, `1d4h48m`, ``},
		{`1.11d`, `1d2h38m24s`, ``},
		{`1.111d`, `1d2h39m50s400ms`, ``},
		{`1.1111d`, `1d2h39m59s40ms`, ``},
		{`60d 25h`, `60d25h`, ``},
		{`-9223372036854775808d`, `-9223372036854775808d`, ``},
		{`9223372036854775807d`, `9223372036854775807d`, ``},

		{`1week`, `7d`, ``},
		{`1weeks`, `7d`, ``},
		{`1w`, `7d`, ``},
		{`1.1w`, `7d16h48m`, ``},
		{`1.5w`, `10d12h`, ``},
		{`1w -1d`, `6d`, ``},

		{`1month`, `1mon`, ``},
		{`1months`, `1mon`, ``},
		{`1mons`, `1mon`, ``},
		{`1.5mon`, `1mon15d`, ``},
		{`1 mon 2 week`, `1mon14d`, ``},
		{`1.1mon`, `1mon3d`, ``},
		{`1.2mon`, `1mon6d`, ``},
		{`1.11mon`, `1mon3d7h11m59s999ms999µs999ns`, ``},
		{`-9223372036854775808mon`, `-768614336404564650y-8mon`, ``},
		{`9223372036854775807mon`, `768614336404564650y7mon`, ``},

		{`1year`, `1y`, ``},
		{`1years`, `1y`, ``},
		{`1y`, `1y`, ``},
		{`1yr`, `1y`, ``},
		{`1yrs`, `1y`, ``},
		{`1.5y`, `1y6mon`, ``},
		{`1.1y`, `1y1mon6d`, ``},
		{`1.11y`, `1y1mon9d14h24m`, ``},

		// Mixed unit/HH:MM:SS formats
		{`1:2:3`, `1h2m3s`, ``},
		{`-1:2:3`, `-1h-2m-3s`, ``},
		{`1 day 12:30`, `1d12h30m`, ``},
		{`12:30 1 day`, `1d12h30m`, ``},
		{`1 day -12:30`, `1d-12h-30m`, ``},
		{`1 day 12:30.5`, `1d12m30s500ms`, ``},
		{`1 day 12:30:40`, `1d12h30m40s`, ``},
		{`1 day 12:30:40.5`, `1d12h30m40s500ms`, ``},
		{`1 day 12:30:40.500500001`, `1d12h30m40s500ms500µs1ns`, ``},
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
