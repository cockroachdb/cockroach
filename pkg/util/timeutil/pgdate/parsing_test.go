// Copyright 2018 The Cockroach Authors.
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

package pgdate_test

import (
	gosql "database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

var modes = []pgdate.ParseMode{
	pgdate.ParseModeDMY,
	pgdate.ParseModeMDY,
	pgdate.ParseModeYMD,
}

// tsTweak is a callback used by TestParseTimestamp to handle cases
// where a date or a time may parse one way individually, but
// has different results when concatenated into a timestamp.
type tsTweak func(d string, t string, mode pgdate.ParseMode, exp *time.Time, err *bool)

var (
	// If we can extract a date at all, we should wind up with the next day.
	addADay tsTweak = func(d string, t string, mode pgdate.ParseMode, exp *time.Time, err *bool) {
		s := fmt.Sprintf("%s %s", d, t)
		if _, pErr := pgdate.ParseDate(time.Time{}, mode, s); pErr == nil {
			*exp = exp.AddDate(0, 0, 1)
			*err = false
		}
	}
	// Expect an error if this value is concatenated.
	expectConcatErr tsTweak = func(_ string, _ string, _ pgdate.ParseMode, _ *time.Time, err *bool) {
		*err = true
	}
)

type dateData struct {
	s string
	// The generally-expected value.
	exp time.Time
	// Is an error expected?
	err bool
	// Override the expected value for a given ParseMode.
	modeExp map[pgdate.ParseMode]time.Time
	// Override the expected error for a given ParseMode.
	modeErr map[pgdate.ParseMode]bool
	tsTweak tsTweak
}

// expected returns the expected time or expected error condition for the mode.
func (d *dateData) expected(mode pgdate.ParseMode) (time.Time, bool) {
	if t, ok := d.modeExp[mode]; ok {
		return t, false
	}
	if _, ok := d.modeErr[mode]; ok {
		return pgdate.TimeEpoch, true
	}
	return d.exp, d.err
}

var dateTestData = []dateData{
	// The cases below are taken from
	// https://www.postgresql.org/docs/10/static/datatype-datetime.html#DATATYPE-DATETIME-DATE-TABLE
	{
		//1999-01-08	ISO 8601; January 8 in any mode (recommended format)
		s:   "1999-01-08",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		//January 8, 1999	unambiguous in any datestyle input mode
		s:   "January 8, 1999",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		//1/8/1999	January 8 in MDY mode; August 1 in DMY mode
		s:   "1/8/1999",
		err: true,
		modeExp: map[pgdate.ParseMode]time.Time{
			pgdate.ParseModeDMY: time.Date(1999, time.August, 1, 0, 0, 0, 0, time.UTC),
			pgdate.ParseModeMDY: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
		},
	},
	{
		// 1/18/1999	January 18 in MDY mode; rejected in other modes
		s:   "1/18/1999",
		err: true,
		modeExp: map[pgdate.ParseMode]time.Time{
			pgdate.ParseModeMDY: time.Date(1999, time.January, 18, 0, 0, 0, 0, time.UTC),
		},
	},
	{
		// 01/02/03	January 2, 2003 in MDY mode; February 1, 2003 in DMY mode; February 3, 2001 in YMD mode
		s: "01/02/03",
		modeExp: map[pgdate.ParseMode]time.Time{
			pgdate.ParseModeYMD: time.Date(2001, time.February, 3, 0, 0, 0, 0, time.UTC),
			pgdate.ParseModeDMY: time.Date(2003, time.February, 1, 0, 0, 0, 0, time.UTC),
			pgdate.ParseModeMDY: time.Date(2003, time.January, 2, 0, 0, 0, 0, time.UTC),
		},
	},
	{
		// 1999-Jan-08	January 8 in any mode
		s:   "1999-Jan-08",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		// Jan-08-1999	January 8 in any mode
		s:   "Jan-08-1999",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		// 08-Jan-1999	January 8 in any mode
		s:   "08-Jan-1999",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		// 99-Jan-08	January 8 in YMD mode, else error
		s:   "99-Jan-08",
		err: true,
		modeExp: map[pgdate.ParseMode]time.Time{
			pgdate.ParseModeYMD: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
		},
	},
	{
		// 08-Jan-99	January 8, except error in YMD mode
		s:   "08-Jan-99",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
		modeErr: map[pgdate.ParseMode]bool{
			pgdate.ParseModeYMD: true,
		},
	},
	{
		// Jan-08-99	January 8, except error in YMD mode
		s:   "Jan-08-99",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
		modeErr: map[pgdate.ParseMode]bool{
			pgdate.ParseModeYMD: true,
		},
	},
	{
		// 19990108	ISO 8601; January 8, 1999 in any mode
		s:   "19990108",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		// 990108	ISO 8601; January 8, 1999 in any mode
		s:   "990108",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		// 1999.008	year and day of year
		s:   "1999.008",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		// 1999 123	year and day of year
		s:   "1999 123",
		exp: time.Date(1999, time.May, 3, 0, 0, 0, 0, time.UTC),
	},
	{
		// J2451187	Julian date
		s:   "J2451187",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		// January 8, 99 BC	year 99 BC
		s: "January 8, 99 BC",
		// Note that this is off by one
		exp: time.Date(-98, time.January, 8, 0, 0, 0, 0, time.UTC),
		// Failure confirmed in pg 10.5, but not documented as such.
		modeErr: map[pgdate.ParseMode]bool{
			pgdate.ParseModeYMD: true,
		},
	},

	// ------- More tests ---------
	{
		// Two sentinels
		s:   "epoch infinity",
		err: true,
	},
	{
		// Provide too few fields
		s:   "2018",
		err: true,
	},
	{
		// Provide too few fields
		s:   "2018-10",
		err: true,
	},
	{
		// Provide a full timestamp.
		s:       "2017-12-05 04:04:04.913231+00:00",
		exp:     time.Date(2017, time.December, 05, 0, 0, 0, 0, time.UTC),
		tsTweak: expectConcatErr,
	},
	{
		// Date from a full nano-time.
		s:       "2006-07-08T00:00:00.000000123Z",
		exp:     time.Date(2006, time.July, 8, 0, 0, 0, 0, time.UTC),
		tsTweak: expectConcatErr,
	},
	{
		// Provide too many fields. We expect this to error out since
		// it's not actually a date.
		s:   "Random input",
		err: true,
	},
	{
		// Random date with a timezone.
		s:   "2018-10-23 +00",
		exp: time.Date(2018, 10, 23, 0, 0, 0, 0, time.UTC),
		tsTweak: func(d string, t string, mode pgdate.ParseMode, exp *time.Time, err *bool) {
			// Expect an error if the incoming time string has a timezone.
			if strings.HasSuffix(t, "AM") || strings.HasSuffix(t, "PM") {
				t = t[:len(t)-2]
			}

			// We'll just filter characters as an easy way to detect this.
			for _, r := range t {
				switch {
				case unicode.IsNumber(r):
				case r == ' ':
				case r == ':':
				case r == '.':
				default:
					*err = true
					return
				}
			}
		},
	},
}

var timeTestData = []struct {
	s string
	// The generally-expected value.
	exp time.Time
	// Is an error expected?
	err bool
	// Disable cross-checking for unimplemented features.
	noCrossCheck bool
	tsTweak      tsTweak
}{
	{
		// 04:05:06.789 ISO 8601
		s:   "04:05:06.789",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
	},
	{
		//  04:05:06 ISO 8601
		s:   "04:05:06",
		exp: time.Date(0, 1, 1, 4, 5, 6, 0, time.UTC),
	},
	{
		//  04:05 ISO 8601
		s:   "04:05",
		exp: time.Date(0, 1, 1, 4, 5, 0, 0, time.UTC),
	},
	{
		//  040506 ISO 8601
		s:   "040506",
		exp: time.Date(0, 1, 1, 4, 5, 6, 0, time.UTC),
	},
	{
		//  04:05 AM same as 04:05; AM does not affect value
		s:   "04:05 AM",
		exp: time.Date(0, 1, 1, 4, 5, 0, 0, time.UTC),
	},
	{
		//  04:05 PM same as 16:05; input hour must be <= 12
		s:   "04:05 PM",
		exp: time.Date(0, 1, 1, 16, 5, 0, 0, time.UTC),
	},
	{
		// 04:05:06.789-8 ISO 8601
		s:   "04:05:06.789-8",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.FixedZone("-0800", -8*60*60)),
	},
	{
		// 04:05:06.789-8:30 ISO 8601
		s:   "04:05:06.789-8:30",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.FixedZone("-0830", -8*60*60-30*60)),
	},
	{
		// 04:05-8:00 ISO 8601
		s:   "04:05-8:00",
		exp: time.Date(0, 1, 1, 4, 5, 0, 0, time.FixedZone("-0800", -8*60*60)),
	},
	{
		// 040506-08 ISO 8601
		s:   "040506-8",
		exp: time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0800", -8*60*60)),
	},
	{
		// 04:05:06 PST time zone specified by abbreviation
		// Unimplemented with message to user as such:
		// https://github.com/cockroachdb/cockroach/issues/31710
		s:            "04:05:06 PST",
		err:          true,
		noCrossCheck: true,
	},
	{
		// This test, and the next show that resolution of geographic names
		// to actual timezones is aware of daylight-savings time.  Note
		// that even though we're just parsing a time value, we do need
		// to provide a date in order to resolve the named zone to a
		// UTC offset.
		s:       "2003-01-12 04:05:06 America/New_York",
		exp:     time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0500", -5*60*60)),
		tsTweak: expectConcatErr,
	},
	{
		s:       "2003-06-12 04:05:06 America/New_York",
		exp:     time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0400", -4*60*60)),
		tsTweak: expectConcatErr,
	},

	// ----- More Tests -----

	{
		// Check positive TZ offsets.
		s:   "04:05:06.789+8:30",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.FixedZone("UTC+830", 8*60*60+30*60)),
	},
	{
		// Check UTC zone.
		s:   "04:05:06.789 UTC",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
	},
	{
		// Check GMT zone.
		s:   "04:05:06.789 GMT",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
	},
	{
		// Check Z suffix with space.
		s:   "04:05:06.789 z",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
	},
	{
		// Check Zulu suffix with space.
		s:   "04:05:06.789 zulu",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
	},
	{
		// Check Z suffix without space.
		s:   "04:05:06.789z",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
	},
	{
		// Check Zulu suffix without space.
		s:   "04:05:06.789zulu",
		exp: time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
	},
	{
		// Packed time should extra seconds.
		s:   "045:06",
		err: true,
	},
	{
		// Check 12:54 AM -> 0
		s:   "12:54 AM",
		exp: time.Date(0, 1, 1, 0, 54, 0, 0, time.UTC),
	},
	{
		// Check 12:54 PM -> 12
		s:   "12:54 PM",
		exp: time.Date(0, 1, 1, 12, 54, 0, 0, time.UTC),
	},
	{
		// Check 00:54 AM -> 0
		// This behavior is observed in pgsql 10.5.
		s:   "12:54 AM",
		exp: time.Date(0, 1, 1, 0, 54, 0, 0, time.UTC),
	},
	{
		// Check 00:54 PM -> 12
		// This behavior is observed in pgsql 10.5.
		s:   "0:54 PM",
		exp: time.Date(0, 1, 1, 12, 54, 0, 0, time.UTC),
	},
	{
		// Check nonsensical TZ.
		// This behavior is observed in pgsql 10.5.
		s:   "12:54-00:29",
		exp: time.Date(0, 1, 1, 12, 54, 0, 0, time.FixedZone("UGH", -29*60)),
	},
	{
		// Check long timezone with date month.
		s:       "June 12, 2003 04:05:06 America/New_York",
		exp:     time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0400", -4*60*60)),
		tsTweak: expectConcatErr,
	},
	{
		// Require that minutes and seconds must either be packed or have colon separators.
		s:   "01 02 03",
		err: true,
	},
	{
		// 3-digit times should not work.
		s:   "123",
		err: true,
		tsTweak: func(d string, t string, mode pgdate.ParseMode, exp *time.Time, err *bool) {
			// Special case when we inadvertently create a valid timestamp.
			if fmt.Sprintf("%s %s", d, t) == "2018 123" {
				*exp = time.Date(2018, 5, 3, 0, 0, 0, 0, time.UTC)
				*err = false
			}
		},
	},
	{
		//  Single-digits
		s:   "4:5:6",
		exp: time.Date(0, 1, 1, 4, 5, 6, 0, time.UTC),
	},
	{
		// Maximum value
		s:   "24:00:00",
		err: true,
		// Allow hour 24 to roll over when we have a date.
		tsTweak: addADay,
	},
	{
		// Exceed maximum value
		s:   "24:00:00.000001",
		err: true,
	},
	{
		s:   "23:59:60",
		err: true,
		// Allow this to roll over when we have a date.
		tsTweak: addADay,
	},
	{
		s:   "23:60:00",
		err: true,
	},
	{
		// Verify that we do support full nanosecond resolution in parsing.
		s:   "04:05:06.999999999",
		exp: time.Date(0, 1, 1, 4, 5, 6, 999999999, time.UTC),
		// No cross checking since postgres rounds to the nearest micro,
		// but we have other internal consumers that require nano precision.
		noCrossCheck: true,
	},
	{
		// Over-long fractional portion is an error
		s:   "04:05:06.9999999999",
		err: true,
		// No cross checking since postgres rounds to the nearest micro,
		// but we have other internal consumers that require nano precision.
		noCrossCheck: true,
	},
	{
		// Verify that micros are maintained.
		s:   "23:59:59.999999",
		exp: time.Date(0, 1, 1, 23, 59, 59, 999999000, time.UTC),
	},
	{
		// Verify that tenths are maintained.
		s:   "23:59:59.1",
		exp: time.Date(0, 1, 1, 23, 59, 59, 100000000, time.UTC),
	},
}

// Additional timestamp tests not generated by combining dates and times.
var timestampTestData = []struct {
	s string
	// The generally-expected value.
	exp time.Time
	// Is an error expected?
	err bool
	// Disable cross-checking for unimplemented features.
	noCrossCheck bool
}{
	{
		s:   "2000-01-01T02:02:02",
		exp: time.Date(2000, 1, 1, 2, 2, 2, 0, time.UTC),
	},
}

// TestMain will enable cross-checking of test results against a
// PostgreSQL instance if the -pgdate.db flag is set. This is mainly
// useful for developing the tests themselves and doesn't need
// to be part of a regular build.
func TestMain(m *testing.M) {
	if dbString != "" {
		if d, err := gosql.Open("postgres", dbString); err == nil {
			if err := d.Ping(); err == nil {
				db = d
			} else {
				println("could not ping database", err)
				os.Exit(-1)
			}
		} else {
			println("could not open database", err)
			os.Exit(-1)
		}
	}
	os.Exit(m.Run())
}

func TestParseDate(t *testing.T) {
	now := time.Time{}.UTC()

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for _, tc := range dateTestData {
				t.Run(tc.s, func(t *testing.T) {
					res, err := pgdate.ParseDate(now, mode, tc.s)

					expRes, expErr := tc.expected(mode)
					crossCheck(t, "date", tc.s, mode, expRes, expErr)

					if expErr {
						if err == nil {
							t.Fatalf("expected error, got result %s", res)
						} else {
							t.Logf("FYI: %s", err)
						}
						return
					}

					if err != nil {
						t.Fatalf("unexpected error: %s", err)
					}

					if !res.Equal(expRes) {
						t.Fatalf("expected %s, got %s", tc.exp, res)
					}
					t.Logf("FYI: %s", res)
				})
			}
		})
	}
}

func TestParseTime(t *testing.T) {
	now := timeutil.Unix(0, 0).UTC()

	for _, tc := range timeTestData {
		t.Run(tc.s, func(t *testing.T) {
			res, err := pgdate.ParseTime(now, tc.s)

			if !tc.noCrossCheck {
				crossCheck(t, "timetz", tc.s, 0, tc.exp, tc.err)
			}

			if err == nil {
				if tc.err {
					t.Fatalf("expected error, but succeeded %s", res)
				}
				if !res.Equal(tc.exp) {
					t.Fatalf("expected %s, got %s", tc.exp, res)
				}
			} else {
				if !tc.err {
					t.Fatalf("unexpected error: %s", err)
				}
				t.Logf("FYI: %s", err)
			}
		})
	}
}

// A timestamp is just a date concatenated with a time.  We'll
// reuse the various formats from the date and time data here.
func TestParseTimestamp(t *testing.T) {
	now := timeutil.Unix(0, 0).UTC()

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for _, dtc := range dateTestData {
				expDate, expDtcErr := dtc.expected(mode)

				for _, ttc := range timeTestData {
					expTime := ttc.exp

					year, month, day := expDate.Date()
					hour, min, sec := expTime.Clock()
					exp := time.Date(year, month, day, hour, min, sec, expTime.Nanosecond(), expTime.Location())

					expErr := expDtcErr || ttc.err
					s := fmt.Sprintf("%s %s", dtc.s, ttc.s)

					// Some of our timestamp data are fully-formed timestamps
					// or have other odd behaviors.
					if dtc.tsTweak != nil {
						dtc.tsTweak(dtc.s, ttc.s, mode, &exp, &expErr)
					}

					if ttc.tsTweak != nil {
						ttc.tsTweak(dtc.s, ttc.s, mode, &exp, &expErr)
					}

					t.Run(s, func(t *testing.T) {
						if !ttc.noCrossCheck {
							crossCheck(t, "timestamptz", s, mode, exp, expErr)
						}

						res, err := pgdate.ParseTimestamp(now, mode, s)
						if expErr {
							if err == nil {
								t.Fatalf("unexpected success %s", res)
							}
							t.Logf("FYI: %s", err)
						} else {
							if err != nil {
								t.Fatalf("unexpected error %s", err)
							}
							if !res.Equal(exp) {
								t.Fatalf("expected %s, got %s", exp, res)
							}
							t.Logf("FYI: %s", res)
						}
					})
				}
			}
		})

		// Process additional tests
		for _, tc := range timestampTestData {
			t.Run(tc.s, func(t *testing.T) {
				res, err := pgdate.ParseTimestamp(now, mode, tc.s)

				if !tc.noCrossCheck {
					crossCheck(t, "timestamptz", tc.s, 0, tc.exp, tc.err)
				}

				if err == nil {
					if tc.err {
						t.Fatalf("expected error, but succeeded %s", res)
					}
					if !res.Equal(tc.exp) {
						t.Fatalf("expected %s, got %s", tc.exp, res)
					}
				} else {
					if !tc.err {
						t.Fatalf("unexpected error: %s", err)
					}
					t.Logf("FYI: %s", err)
				}
			})
		}
	}
}

func TestOneOff(t *testing.T) {
	res, err := pgdate.ParseTime(time.Time{}, "23:59:59.999999")
	t.Logf("%v %v", res, err)
}

func BenchmarkParseDate(b *testing.B) {
	b.Run("Reference", func(b *testing.B) {
		b.Run("RFC3399", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := time.ParseInLocation(time.RFC3339, "2018-10-20", time.UTC); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("RFC3399", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := time.ParseInLocation(time.RFC822, "02 October 2018", time.UTC); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	for _, mode := range modes {
		b.Run(mode.String(), func(b *testing.B) {
			for _, tc := range dateTestData {
				if _, expectError := tc.expected(mode); expectError {
					continue
				}

				b.Run(tc.s, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						if _, err := pgdate.ParseDate(time.Time{}, mode, tc.s); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkParseTime(b *testing.B) {
	for _, tc := range timeTestData {
		if tc.err {
			continue
		}
		b.Run(tc.s, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := pgdate.ParseTime(time.Time{}, tc.s); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

var db *gosql.DB
var dbString string

func init() {
	// -pgdate.db="database=bob sslmode=disable"
	flag.StringVar(&dbString, "pgdate.db", "", "An sql.Open() string to enable cross-checks")
	flag.Parse()
}

// crossCheckSuppress contains strings that should not be cross-checked.
var crossCheckSuppress = map[string]bool{
	"June 12, 2003 04:05:06 America/New_York": true,
}

func crossCheck(
	t *testing.T, kind, s string, mode pgdate.ParseMode, expTime time.Time, expErr bool,
) {
	if db == nil {
		return
	}
	if suppress, ok := crossCheckSuppress[s]; ok && suppress {
		return
	}
	t.Run("cross-check", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}()

		if _, err := db.Exec("set timezone='UTC'"); err != nil {
			t.Fatal(err)
		}

		var style string
		switch mode {
		case pgdate.ParseModeMDY:
			style = "MDY"
		case pgdate.ParseModeDMY:
			style = "DMY"
		case pgdate.ParseModeYMD:
			style = "YMD"
		}
		if _, err := db.Exec(fmt.Sprintf("set datestyle='%s'", style)); err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow(fmt.Sprintf("select '%s'::%s", s, kind))
		var ret time.Time
		if err := row.Scan(&ret); err == nil {
			if expErr {
				t.Fatalf("expected error, got %s", ret)
			} else if ret.Equal(expTime) {
				t.Logf("FYI: %s", ret)
			} else {
				t.Fatalf("expected %s, got %s", expTime, ret)
			}
		} else {
			if expErr {
				t.Logf("FYI: %s", err)
			} else {
				t.Fatalf("unexpected error: %s", err)
			}
		}
	})
}
