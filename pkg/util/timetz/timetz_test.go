// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timetz

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTimeTZToStringRoundTrip(t *testing.T) {
	testCases := []string{
		"24:00:00-1559",
		"11:12:13+05:06:07",
		"10:11:12+0",
		"10:11:12.05+0",
	}
	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			exampleTime, _, err := ParseTimeTZ(timeutil.Now(), tc, time.Microsecond)
			assert.NoError(t, err)

			exampleTimeFromString, _, err := ParseTimeTZ(timeutil.Now(), exampleTime.String(), time.Microsecond)
			assert.NoError(t, err)

			assert.True(t, exampleTime.Equal(exampleTimeFromString))
		})
	}
}

func TestTimeTZString(t *testing.T) {
	testCases := []struct {
		input    TimeTZ
		expected string
	}{
		{MakeTimeTZ(timeofday.New(0, 0, 0, 0), 0), "00:00:00+00:00:00"},
		{MakeTimeTZ(timeofday.New(10, 11, 12, 0), 0), "10:11:12+00:00:00"},
		{MakeTimeTZ(timeofday.New(10, 11, 12, 0), -30), "10:11:12+00:00:30"},
		{MakeTimeTZ(timeofday.New(10, 11, 12, 0), 30), "10:11:12-00:00:30"},
		{MakeTimeTZ(timeofday.New(10, 11, 12, 0), 120), "10:11:12-00:02:00"},
		{MakeTimeTZ(timeofday.New(10, 11, 12, 0), 3), "10:11:12-00:00:03"},
		{MakeTimeTZ(timeofday.Max-1, -10*60*60), "23:59:59.999999+10:00:00"},
		{MakeTimeTZ(timeofday.Time2400, 10*60*60), "24:00:00-10:00:00"},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d:%s", i, tc.expected), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.input.String())
		})
	}
}

func TestTimeTZ(t *testing.T) {
	maxTime, depOnCtx, err := ParseTimeTZ(timeutil.Now(), "24:00:00-1559", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)
	minTime, depOnCtx, err := ParseTimeTZ(timeutil.Now(), "00:00:00+1559", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)

	// These are all the same UTC time equivalents.
	utcTime, depOnCtx, err := ParseTimeTZ(timeutil.Now(), "11:14:15+0", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)
	sydneyTime, depOnCtx, err := ParseTimeTZ(timeutil.Now(), "21:14:15+10", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)

	sydneyTimeWithMillisecond, depOnCtx, err := ParseTimeTZ(timeutil.Now(), "21:14:15.001+10", time.Microsecond)
	require.NoError(t, err)
	require.False(t, depOnCtx)

	// No daylight savings in Hawaii!
	hawaiiZone, err := timeutil.LoadLocation("Pacific/Honolulu")
	require.NoError(t, err)
	hawaiiTime := MakeTimeTZFromLocation(timeofday.New(1, 14, 15, 0), hawaiiZone)

	weirdTimeZone := MakeTimeTZ(timeofday.New(10, 0, 0, 0), -((5 * 60 * 60) + 30*60 + 15))

	testCases := []struct {
		t               TimeTZ
		toTime          time.Time
		toDuration      time.Duration
		largerThan      []TimeTZ
		smallerThan     []TimeTZ
		equalTo         []TimeTZ
		roundedToSecond TimeTZ
	}{
		{
			t:               weirdTimeZone,
			toTime:          time.Date(1970, 1, 1, 10, 0, 0, 0, timeutil.FixedOffsetTimeZoneToLocation((5*60*60)+(30*60)+15, "TimeTZ")),
			toDuration:      time.Duration((10*60*60 - ((5 * 60 * 60) + 30*60 + 15))) * time.Second,
			largerThan:      []TimeTZ{minTime},
			smallerThan:     []TimeTZ{maxTime},
			equalTo:         []TimeTZ{weirdTimeZone},
			roundedToSecond: weirdTimeZone,
		},
		{
			t:               utcTime,
			toTime:          time.Date(1970, 1, 1, 11, 14, 15, 0, timeutil.FixedOffsetTimeZoneToLocation(0, "TimeTZ")),
			toDuration:      time.Duration(11*60*60+14*60+15) * time.Second,
			largerThan:      []TimeTZ{minTime, sydneyTime},
			smallerThan:     []TimeTZ{maxTime, hawaiiTime},
			equalTo:         []TimeTZ{utcTime},
			roundedToSecond: utcTime,
		},
		{
			t:               sydneyTime,
			toTime:          time.Date(1970, 1, 1, 21, 14, 15, 0, timeutil.FixedOffsetTimeZoneToLocation(10*60*60, "TimeTZ")),
			toDuration:      time.Duration(11*60*60+14*60+15) * time.Second,
			largerThan:      []TimeTZ{minTime},
			smallerThan:     []TimeTZ{maxTime, utcTime, hawaiiTime},
			equalTo:         []TimeTZ{sydneyTime},
			roundedToSecond: sydneyTime,
		},
		{
			t:               sydneyTimeWithMillisecond,
			toTime:          time.Date(1970, 1, 1, 21, 14, 15, 1000000, timeutil.FixedOffsetTimeZoneToLocation(10*60*60, "TimeTZ")),
			toDuration:      time.Duration(11*60*60+14*60+15)*time.Second + 1*time.Millisecond,
			largerThan:      []TimeTZ{minTime, utcTime, hawaiiTime, sydneyTime},
			smallerThan:     []TimeTZ{maxTime},
			equalTo:         []TimeTZ{sydneyTimeWithMillisecond},
			roundedToSecond: sydneyTime,
		},
		{
			t:               hawaiiTime,
			toTime:          time.Date(1970, 1, 1, 1, 14, 15, 0, timeutil.FixedOffsetTimeZoneToLocation(-10*60*60, "TimeTZ")),
			toDuration:      time.Duration(11*60*60+14*60+15) * time.Second,
			largerThan:      []TimeTZ{minTime, utcTime, sydneyTime},
			smallerThan:     []TimeTZ{maxTime},
			equalTo:         []TimeTZ{hawaiiTime},
			roundedToSecond: hawaiiTime,
		},
		{
			t:               minTime,
			toTime:          time.Date(1970, 1, 1, 0, 0, 0, 0, timeutil.FixedOffsetTimeZoneToLocation(15*60*60+59*60, "TimeTZ")),
			toDuration:      time.Duration(-(15*60*60 + 59*60)) * time.Second,
			largerThan:      []TimeTZ{},
			smallerThan:     []TimeTZ{maxTime, utcTime, sydneyTime, hawaiiTime},
			equalTo:         []TimeTZ{minTime},
			roundedToSecond: minTime,
		},
		{
			t:               maxTime,
			toTime:          time.Date(1970, 1, 2, 0, 0, 0, 0, timeutil.FixedOffsetTimeZoneToLocation(-(15*60*60+59*60), "TimeTZ")),
			toDuration:      time.Duration(24*60*60+15*60*60+59*60) * time.Second,
			largerThan:      []TimeTZ{minTime, utcTime, sydneyTime, hawaiiTime},
			smallerThan:     []TimeTZ{},
			equalTo:         []TimeTZ{maxTime},
			roundedToSecond: maxTime,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d %s", i, tc.t.String()), func(t *testing.T) {
			assert.Equal(t, tc.toTime, tc.t.ToTime())
			assert.Equal(t, tc.roundedToSecond, tc.t.Round(time.Second))
			assert.Equal(t, tc.toDuration, tc.t.ToDuration())

			for _, largerThan := range tc.largerThan {
				assert.True(t, tc.t.After(largerThan), "%s > %s", tc.t.String(), largerThan)
			}

			for _, smallerThan := range tc.smallerThan {
				assert.True(t, tc.t.Before(smallerThan), "%s < %s", tc.t.String(), smallerThan)
			}

			for _, equalTo := range tc.equalTo {
				assert.True(t, tc.t.Equal(equalTo), "%s = %s", tc.t.String(), equalTo)
			}
		})
	}
}

func TestParseTimeTZ(t *testing.T) {
	testCases := []struct {
		str       string
		precision time.Duration

		expected         TimeTZ
		expectedDepOnCtx bool
		expectedError    bool
	}{
		{str: "01:02:03", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), 0), expectedDepOnCtx: true},
		{str: "01:02:03.000123", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 123), 0), expectedDepOnCtx: true},
		{str: "01:24:00", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 24, 0, 0), 0), expectedDepOnCtx: true},
		{str: "01:03:24", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 3, 24, 0), 0), expectedDepOnCtx: true},
		{str: "1970-01-01 01:02:03", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), 0), expectedDepOnCtx: true},
		{str: "1970-01-01T01:02:03", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), 0), expectedDepOnCtx: true},
		{str: "1970-01-01T01:02:03", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), 0), expectedDepOnCtx: true},
		{str: "0000-01-01  01:02:03", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), 0), expectedDepOnCtx: true},
		{str: "01:02:03.000123", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 123), 0), expectedDepOnCtx: true},
		{str: "4:5:6", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(4, 5, 6, 0), 0), expectedDepOnCtx: true},
		{str: "24:00", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, 0), expectedDepOnCtx: true},
		{str: "24:00:00", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, 0), expectedDepOnCtx: true},
		{str: "24:00:00.000", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, 0), expectedDepOnCtx: true},
		{str: "24:00:00.000000", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, 0), expectedDepOnCtx: true},
		{str: "01:02:03+13", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), -13*60*60), expectedDepOnCtx: false},
		{str: "01:02:03-13", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), 13*60*60), expectedDepOnCtx: false},
		{str: "01:02:03+7", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), -7*60*60), expectedDepOnCtx: false},
		{str: "01:02:03-0730", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), 7*60*60+30*60), expectedDepOnCtx: false},
		{str: "24:00+3", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, -3*60*60), expectedDepOnCtx: false},
		{str: "24:00:00+4", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, -4*60*60), expectedDepOnCtx: false},
		{str: "24:00:00.000-5", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, 5*60*60), expectedDepOnCtx: false},
		{str: "24:00:00.000000+6", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, -6*60*60), expectedDepOnCtx: false},
		{str: "24:00:00.000000+6", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, -6*60*60), expectedDepOnCtx: false},
		{str: "1970-01-01T24:00:00.000000+6", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.Time2400, -6*60*60), expectedDepOnCtx: false},
		{str: "00:00-1559", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(0, 0, 0, 0), MaxTimeTZOffsetSecs), expectedDepOnCtx: false},
		{str: "00:00+1559", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(0, 0, 0, 0), MinTimeTZOffsetSecs), expectedDepOnCtx: false},
		{str: " 01:03:24", precision: time.Microsecond, expected: MakeTimeTZ(timeofday.New(1, 3, 24, 0), 0), expectedDepOnCtx: true},

		{str: "01:02:03.000123", precision: time.Millisecond, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 0), 0), expectedDepOnCtx: true},
		{str: "01:02:03.000123", precision: time.Millisecond / 10, expected: MakeTimeTZ(timeofday.New(1, 2, 3, 100), 0), expectedDepOnCtx: true},
		{str: "01:02:03.500123", precision: time.Second, expected: MakeTimeTZ(timeofday.New(1, 2, 4, 0), 0), expectedDepOnCtx: true},

		{str: "", expectedError: true},
		{str: "foo", expectedError: true},
		{str: "01", expectedError: true},
		{str: "01:00=wat", expectedError: true},
		{str: "00:00-1600", expectedError: true},
		{str: "00:00+1600", expectedError: true},
		{str: "00:00+24:00", expectedError: true},
		{str: "1970-01-01 00:00+24:00", expectedError: true},
		{str: "2010-09-28", expectedError: true},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d: %s", i, tc.str), func(t *testing.T) {
			actual, depOnCtx, err := ParseTimeTZ(timeutil.Now(), tc.str, tc.precision)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, actual)
				assert.Equal(t, tc.expectedDepOnCtx, depOnCtx)
			}
		})
	}
}
