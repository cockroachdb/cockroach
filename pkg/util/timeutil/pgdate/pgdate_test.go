// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgdate

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestDateFromTime(t *testing.T) {
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
			s:   "0000-01-01",
			err: "year value 0 is out of range",
		},
	} {
		t.Run(tc.s, func(t *testing.T) {
			d, err := ParseDate(time.Time{}, ParseModeYMD, tc.s)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("got %v, expected %v", err, tc.err)
				}
				return
			}
			pg := d.PGEpochDays()
			if pg != tc.pgdays {
				t.Fatalf("%d != %d", pg, tc.pgdays)
			}
			s := d.String()
			if s != tc.s {
				t.Fatalf("%s != %s", s, tc.s)
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
	for _, tc := range []struct {
		loc *time.Location
		out string
	}{
		{time.FixedZone("secsPerDay", secondsPerDay), "2000-01-02"},
		{time.FixedZone("secsPerDay-1", secondsPerDay-1), "2000-01-01"},
		{time.FixedZone("1", 1), "2000-01-01"},
		{time.UTC, "2000-01-01"},
		{time.FixedZone("-1", -1), "1999-12-31"},
		{time.FixedZone("-secsPerDay", -secondsPerDay), "1999-12-31"},
	} {
		t.Run(tc.loc.String(), func(t *testing.T) {
			tm := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).In(tc.loc)
			d, err := MakeDateFromTime(tm)
			if err != nil {
				t.Fatal(err)
			}
			exp := tm.Format("2006-01-02")
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
