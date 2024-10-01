// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgdate

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseDate(t *testing.T) {
	var parseHelper ParseHelper
	for _, tc := range []struct {
		s      string
		err    string
		pgdays int32
	}{
		{
			s:      "2000-01-01",
			pgdays: 0,
		},
		{
			s:      "1999-12-31",
			pgdays: -1,
		},
		{
			s:      "2000-01-02",
			pgdays: 1,
		},
		{
			s:      "0001-01-01",
			pgdays: -730119,
		},
		{
			s:      "0001-12-31 BC",
			pgdays: -730120,
		},
		{
			s:      "0002-01-01 BC",
			pgdays: -730850,
		},
		{
			s:      "5874897-12-31",
			pgdays: highDays,
		},
		{
			s:      "4714-11-24 BC",
			pgdays: lowDays,
		},
		{
			s:   "4714-11-23 BC",
			err: "date is out of range",
		},
		{
			s:   "5874898-01-01",
			err: "date is out of range",
		},
		{
			s:   "0000-01-01 AD",
			err: "only positive years are permitted in AD/BC notation",
		},
	} {
		t.Run(tc.s, func(t *testing.T) {
			for _, ph := range []*ParseHelper{nil, &parseHelper} {
				d, depOnCtx, err := ParseDate(time.Time{}, DateStyle{Order: Order_YMD}, tc.s, ph)
				if tc.err != "" {
					if err == nil || !strings.Contains(err.Error(), tc.err) {
						t.Fatalf("got %v, expected %v", err, tc.err)
					}
					return
				}
				if depOnCtx {
					t.Fatalf("should not depend on context")
				}
				pg := d.PGEpochDays()
				if pg != tc.pgdays {
					t.Fatalf("%d != %d", pg, tc.pgdays)
				}
				s := d.String()
				if s != tc.s {
					t.Fatalf("%s != %s", s, tc.s)
				}
			}
		})
	}
}

func TestMakeCompatibleDateFromDisk(t *testing.T) {
	for _, tc := range []struct {
		in, out int64
	}{
		{0, 0},
		{1, 1},
		{-1, -1},
		{math.MaxInt64, math.MaxInt64},
		{math.MinInt64, math.MinInt64},
		{math.MaxInt32, math.MaxInt64},
		{math.MinInt32, math.MinInt64},
	} {
		t.Run(fmt.Sprint(tc.in), func(t *testing.T) {
			date := MakeCompatibleDateFromDisk(tc.in)
			orig := date.UnixEpochDaysWithOrig()
			if orig != tc.in {
				t.Fatalf("%d != %d", orig, tc.in)
			}
			days := date.UnixEpochDays()
			if days != tc.out {
				t.Fatalf("%d != %d", days, tc.out)
			}
		})
	}
}

func TestMakeDateFromTime(t *testing.T) {
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	pgEpochWithHourOffset := time.Date(2000, 1, 1, 1, 0, 0, 0, time.UTC)
	// These dates are negative, which makes rounding a little different.
	dayBeforeUnixEpoch := time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)
	dayBeforeUnixEpochWithHourOffset := time.Date(1969, 12, 31, 1, 0, 0, 0, time.UTC)
	twoDaysBeforeUnixEpoch := time.Date(1969, 12, 30, 0, 0, 0, 0, time.UTC)
	twoDaysBeforeUnixEpochWithHourOffset := time.Date(1969, 12, 30, 1, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		in  time.Time
		out string
	}{
		{pgEpoch.In(time.FixedZone("secsPerDay", secondsPerDay)), "2000-01-02"},
		{pgEpoch.In(time.FixedZone("secsPerDay-1", secondsPerDay-1)), "2000-01-01"},
		{pgEpoch.In(time.FixedZone("1", 1)), "2000-01-01"},
		{pgEpoch, "2000-01-01"},
		{pgEpoch.In(time.FixedZone("-1", -1)), "1999-12-31"},
		{pgEpoch.In(time.FixedZone("-secsPerDay", -secondsPerDay)), "1999-12-31"},
		{pgEpochWithHourOffset, "2000-01-01"},

		{dayBeforeUnixEpoch, "1969-12-31"},
		{dayBeforeUnixEpochWithHourOffset, "1969-12-31"},
		{twoDaysBeforeUnixEpoch, "1969-12-30"},
		{twoDaysBeforeUnixEpochWithHourOffset, "1969-12-30"},
	} {
		t.Run(tc.in.Format(time.RFC3339), func(t *testing.T) {
			d, err := MakeDateFromTime(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			exp := tc.in.Format("2006-01-02")
			// Sanity check our tests.
			if exp != tc.out {
				t.Fatalf("got %s, expected %s", exp, tc.out)
			}
			s := d.String()
			if exp != s {
				t.Fatalf("got %s, expected %s", s, exp)
			}
		})
	}
}

func TestParseDateStyle(t *testing.T) {
	for _, tc := range []struct {
		initial   DateStyle
		parse     string
		expected  DateStyle
		sqlString string
	}{
		{DefaultDateStyle(), "mdy", DateStyle{Style: Style_ISO, Order: Order_MDY}, "ISO, MDY"},
		{DefaultDateStyle(), "dmy", DateStyle{Style: Style_ISO, Order: Order_DMY}, "ISO, DMY"},
		{DefaultDateStyle(), "ymd", DateStyle{Style: Style_ISO, Order: Order_YMD}, "ISO, YMD"},

		{DefaultDateStyle(), "iso", DateStyle{Style: Style_ISO, Order: Order_MDY}, "ISO, MDY"},
		{DefaultDateStyle(), "german", DateStyle{Style: Style_GERMAN, Order: Order_MDY}, "German, MDY"},
		{DefaultDateStyle(), "sQl ", DateStyle{Style: Style_SQL, Order: Order_MDY}, "SQL, MDY"},
		{DefaultDateStyle(), "postgres", DateStyle{Style: Style_POSTGRES, Order: Order_MDY}, "Postgres, MDY"},

		{DefaultDateStyle(), "german, DMY", DateStyle{Style: Style_GERMAN, Order: Order_DMY}, "German, DMY"},
		{DefaultDateStyle(), "ymd,sql", DateStyle{Style: Style_SQL, Order: Order_YMD}, "SQL, YMD"},
		{DateStyle{Style: Style_GERMAN, Order: Order_DMY}, "sql,ymd,postgres", DateStyle{Style: Style_POSTGRES, Order: Order_YMD}, "Postgres, YMD"},
	} {
		t.Run(fmt.Sprintf("%s/%s", tc.initial.String(), tc.parse), func(t *testing.T) {
			p, err := ParseDateStyle(tc.parse, tc.initial)
			require.NoError(t, err)
			require.Equal(t, tc.expected, p)
			require.Equal(t, tc.sqlString, p.SQLString())
		})
	}

	t.Run("error", func(t *testing.T) {
		_, err := ParseDateStyle("bad", DefaultDateStyle())
		require.Error(t, err)
	})
}
