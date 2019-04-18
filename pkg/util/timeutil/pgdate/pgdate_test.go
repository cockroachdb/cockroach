// Copyright 2019 The Cockroach Authors.
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

package pgdate

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
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
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("got %v, expected %v", err, tc.err)
			} else if err != nil {
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
