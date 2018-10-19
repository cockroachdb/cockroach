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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

var modes = []pgdate.ParseMode{
	pgdate.ParseModeISO,
	pgdate.ParseModeDMY,
	pgdate.ParseModeMDY,
	pgdate.ParseModeYMD,
}

var dateTestData = []struct {
	s string
	// The generally-expected value.
	exp time.Time
	// Is an error expected?
	err bool
	// Override the expected value for a given ParseMode.
	modeExp map[pgdate.ParseMode]time.Time
	// Override the expected error for a given ParseMode.
	modeErr map[pgdate.ParseMode]bool
}{
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
			pgdate.ParseModeISO: time.Date(2001, time.February, 3, 0, 0, 0, 0, time.UTC),
			pgdate.ParseModeDMY: time.Date(2003, time.February, 1, 0, 0, 0, 0, time.UTC),
			pgdate.ParseModeMDY: time.Date(2003, time.January, 2, 0, 0, 0, 0, time.UTC),
			pgdate.ParseModeYMD: time.Date(2001, time.February, 3, 0, 0, 0, 0, time.UTC),
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
		// Provide too many fields. We expect this to error out since
		// it's not actually a date.
		s:   "2017-12-05 04:04:04.913231+00:00",
		err: true,
	},
	{
		// Provide too many fields. We expect this to error out since
		// it's not actually a date.
		s:   "Random input",
		err: true,
	},
}

func TestParseDate(t *testing.T) {
	now := time.Time{}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for _, tc := range dateTestData {
				t.Run(tc.s, func(t *testing.T) {
					res, err := pgdate.ParseDate(now, mode, tc.s)

					if _, ok := tc.modeErr[mode]; ok {
						if err == nil {
							t.Fatalf("expected error, got result %s", res)
						} else {
							t.Logf("FYI: %s", err)
						}
						return
					}

					if found, ok := tc.modeExp[mode]; ok {
						if err == nil {
							if res != found {
								t.Fatalf("expected %s, got %s", found, res)
							}
							return
						} else {
							t.Fatal(err)
						}
					}

					if err == nil {
						if tc.err {
							t.Fatalf("expected error, but succeeded %s", res)
						}
						if res != tc.exp {
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
		})
	}
}

func TestOneOff(t *testing.T) {
	res, err := pgdate.ParseDate(time.Time{}, pgdate.ParseModeISO, "2017-12-05 04:04:04.913231+00:00")
	t.Logf("%v %v", res, err)
}

func BenchmarkParseDate(b *testing.B) {
	for _, mode := range modes {
		b.Run(mode.String(), func(b *testing.B) {
			for _, tc := range dateTestData {
				b.Run(tc.s, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						pgdate.ParseDate(time.Time{}, mode, tc.s)
					}
				})
			}
		})
	}
}
