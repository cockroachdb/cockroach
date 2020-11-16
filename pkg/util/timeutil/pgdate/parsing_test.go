// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgdate_test

import (
	gosql "database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	_ "github.com/lib/pq"
)

var modes = []pgdate.ParseMode{
	pgdate.ParseModeDMY,
	pgdate.ParseModeMDY,
	pgdate.ParseModeYMD,
}

var db *gosql.DB
var dbString string

func init() {
	flag.StringVar(&dbString, "pgdate.db", "",
		`a postgresql connect string suitable for sql.Open(), `+
			`to enable cross-checking during development; for example: `+
			`-pgdate.db="database=bob sslmode=disable"`)
}

type timeData struct {
	s string
	// The generally-expected value.
	exp time.Time
	// Is an error expected?
	err bool
	// Allow leniency when comparing cross-checked values.
	allowCrossDelta time.Duration
	// Disable cross-checking for unimplemented features.
	expectCrossErr bool
	// Special-case for some weird times that roll over to the next day.
	isRolloverTime bool
	// This value isn't expected to be successful if concatenated.
	expectConcatErr bool
	// This text contains a timezone, so we wouldn't expect to be
	// able to combine it with another timezone-containing value.
	hasTimezone bool
	// Override the expected value for a given ParseMode.
	modeExp map[pgdate.ParseMode]time.Time
	// Override the expected error for a given ParseMode.
	modeErr map[pgdate.ParseMode]bool
	// Indicates that we don't implement a feature in PostgreSQL.
	unimplemented bool
}

// concatTime creates a derived timeData that represents date data
// concatenated with time data to produce timestamp data.
func (td timeData) concatTime(other timeData) timeData {
	add := func(d time.Time, t time.Time) time.Time {
		year, month, day := d.Date()
		hour, min, sec := t.Clock()

		// Prefer whichever has a non-UTC location. You're guaranteed
		// to get an error anyway if you concatenate TZ-containing strings.
		loc := d.Location()
		if loc == time.UTC {
			loc = t.Location()
		}

		return time.Date(year, month, day, hour, min, sec, t.Nanosecond(), loc)
	}

	concatErr := other.err || td.expectConcatErr || other.expectConcatErr || (td.hasTimezone && other.hasTimezone)

	var concatModeExp map[pgdate.ParseMode]time.Time
	if td.modeExp != nil && !concatErr {
		concatModeExp = make(map[pgdate.ParseMode]time.Time, len(td.modeExp))
		for mode, date := range td.modeExp {
			concatModeExp[mode] = add(date, other.exp)
		}
	}

	delta := td.allowCrossDelta
	if other.allowCrossDelta > delta {
		delta = other.allowCrossDelta
	}

	return timeData{
		s:               fmt.Sprintf("%s %s", td.s, other.s),
		exp:             add(td.exp, other.exp),
		err:             td.err || concatErr,
		allowCrossDelta: delta,
		expectCrossErr:  td.expectCrossErr || other.expectCrossErr,
		hasTimezone:     td.hasTimezone || other.hasTimezone,
		isRolloverTime:  td.isRolloverTime || other.isRolloverTime,
		modeExp:         concatModeExp,
		modeErr:         td.modeErr,
		unimplemented:   td.unimplemented || other.unimplemented,
	}
}

// expected returns the expected time or expected error condition for the mode.
func (td timeData) expected(mode pgdate.ParseMode) (time.Time, bool) {
	if t, ok := td.modeExp[mode]; ok {
		return t, false
	}
	if _, ok := td.modeErr[mode]; ok {
		return pgdate.TimeEpoch, true
	}
	return td.exp, td.err
}

func (td timeData) testParseDate(t *testing.T, info string, mode pgdate.ParseMode) {
	info = fmt.Sprintf("%s ParseDate", info)
	exp, expErr := td.expected(mode)
	dt, _, err := pgdate.ParseDate(time.Time{}, mode, td.s)
	res, _ := dt.ToTime()

	// HACK: This is a format that parses as a date and timestamp,
	// but is not a time.
	if td.s == "2018 123" {
		exp = time.Date(2018, 5, 3, 0, 0, 0, 0, time.UTC)
		expErr = false
	}

	// Keeps the date components, but lose everything else.
	y, m, d := exp.Date()
	exp = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)

	check(t, info, exp, expErr, res, err)

	td.crossCheck(t, info, "date", td.s, mode, exp, expErr)
}

func (td timeData) testParseTime(t *testing.T, info string, mode pgdate.ParseMode) {
	info = fmt.Sprintf("%s ParseTime", info)
	exp, expErr := td.expected(mode)
	res, _, err := pgdate.ParseTime(time.Time{}, mode, td.s)

	// Weird times like 24:00:00 or 23:59:60 aren't allowed,
	// unless there's also a date.
	if td.isRolloverTime {
		_, _, err := pgdate.ParseDate(time.Time{}, mode, td.s)
		expErr = err != nil
	}

	// Keep only the time and zone components.
	h, m, sec := exp.Clock()
	exp = time.Date(0, 1, 1, h, m, sec, td.exp.Nanosecond(), td.exp.Location())

	check(t, info, exp, expErr, res, err)
	td.crossCheck(t, info, "timetz", td.s, mode, exp, expErr)
}

func (td timeData) testParseTimestamp(t *testing.T, info string, mode pgdate.ParseMode) {
	info = fmt.Sprintf("%s ParseTimestamp", info)
	exp, expErr := td.expected(mode)
	res, _, err := pgdate.ParseTimestamp(time.Time{}, mode, td.s)

	// HACK: This is a format that parses as a date and timestamp,
	// but is not a time.
	if td.s == "2018 123" {
		exp = time.Date(2018, 5, 3, 0, 0, 0, 0, time.UTC)
		expErr = false
	}

	if td.isRolloverTime {
		exp = exp.AddDate(0, 0, 1)
	}

	check(t, info, exp, expErr, res, err)
	td.crossCheck(t, info, "timestamptz", td.s, mode, exp, expErr)
}

func (td timeData) testParseTimestampWithoutTimezone(
	t *testing.T, info string, mode pgdate.ParseMode,
) {
	info = fmt.Sprintf("%s ParseTimestampWithoutTimezone", info)
	exp, expErr := td.expected(mode)
	res, _, err := pgdate.ParseTimestampWithoutTimezone(time.Time{}, mode, td.s)

	// HACK: This is a format that parses as a date and timestamp,
	// but is not a time.
	if td.s == "2018 123" {
		exp = time.Date(2018, 5, 3, 0, 0, 0, 0, time.UTC)
		expErr = false
	}

	if td.isRolloverTime {
		exp = exp.AddDate(0, 0, 1)
	}
	// Convert the expected time to the same timestamp but in UTC.
	_, offset := exp.Zone()
	exp = exp.Add(time.Duration(offset) * time.Second).UTC()

	check(t, info, exp, expErr, res, err)
	td.crossCheck(t, info, "timestamp", td.s, mode, exp, expErr)
}

var dateTestData = []timeData{
	// The cases below are taken from
	// https://github.com/postgres/postgres/blob/REL_10_5/src/test/regress/sql/date.sql
	// and with comments from
	// https://www.postgresql.org/docs/10/static/datatype-datetime.html#DATATYPE-DATETIME-DATE-TABLE
	{
		//January 8, 1999	unambiguous in any datestyle input mode
		s:   "January 8, 1999",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		//1999-01-08	ISO 8601; January 8 in any mode (recommended format)
		s:   "1999-01-08",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		//1999-01-18	ISO 8601; January 18 in any mode (recommended format)
		s:   "1999-01-18",
		exp: time.Date(1999, time.January, 18, 0, 0, 0, 0, time.UTC),
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
		// 1/18/1999 January 18 in MDY mode; rejected in other modes
		s:   "1/18/1999",
		err: true,
		modeExp: map[pgdate.ParseMode]time.Time{
			pgdate.ParseModeMDY: time.Date(1999, time.January, 18, 0, 0, 0, 0, time.UTC),
		},
	},
	{
		// 18/1/1999 January 18 in DMY mode; rejected in other modes
		s:   "18/1/1999",
		err: true,
		modeExp: map[pgdate.ParseMode]time.Time{
			pgdate.ParseModeDMY: time.Date(1999, time.January, 18, 0, 0, 0, 0, time.UTC),
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
		// Failure confirmed in pg 10.5:
		// https://github.com/postgres/postgres/blob/REL_10_5/src/test/regress/expected/date.out#L135
		modeErr: map[pgdate.ParseMode]bool{
			pgdate.ParseModeYMD: true,
		},
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
		// 1999-Jan-08	January 8 in any mode
		s:   "1999-Jan-08",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
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
		// 08-Jan-1999	January 8 in any mode
		s:   "08-Jan-1999",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
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
		// Jan-08-1999	January 8 in any mode
		s:   "Jan-08-1999",
		exp: time.Date(1999, time.January, 8, 0, 0, 0, 0, time.UTC),
	},
	{
		// 99-08-Jan Error in all modes, because 99 isn't obviously a year
		// and there's no YDM parse mode.
		s:   "99-08-Jan",
		err: true,
	},
	{
		// 1999-08-Jan, for consistency with test above.
		s:   "1999-08-Jan",
		err: true,
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
		s:               "2017-12-05 04:04:04.913231+00:00",
		exp:             time.Date(2017, time.December, 05, 0, 0, 0, 0, time.UTC),
		expectConcatErr: true,
		hasTimezone:     true,
	},
	{
		// Date from a full nano-time.
		s:               "2006-07-08T00:00:00.000000123Z",
		exp:             time.Date(2006, time.July, 8, 0, 0, 0, 0, time.UTC),
		expectConcatErr: true,
		hasTimezone:     true,
	},
	{
		s:   "Random input",
		err: true,
	},
	{
		// Random date with a timezone.
		s:           "2018-10-23 +01",
		exp:         time.Date(2018, 10, 23, 0, 0, 0, 0, time.FixedZone("", 60*60)),
		hasTimezone: true,
	},
	{
		s:   "5874897-01-22",
		exp: time.Date(5874897, 1, 22, 0, 0, 0, 0, time.UTC),
	},
	{
		s:   "121212-01-01",
		exp: time.Date(121212, 1, 1, 0, 0, 0, 0, time.UTC),
	},
	{
		s:   "121212",
		exp: time.Date(2012, 12, 12, 0, 0, 0, 0, time.UTC),
	},
	{
		s:   "-0001-02-15",
		exp: time.Date(-1, 2, 15, 0, 0, 0, 0, time.UTC),
	},
	{
		s:   "0000-02-15",
		exp: time.Date(0, 2, 15, 0, 0, 0, 0, time.UTC),
	},
}

var timeTestData = []timeData{
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
		s:           "04:05:06.789-8",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.FixedZone("-0800", -8*60*60)),
		hasTimezone: true,
	},
	{
		// 04:05:06.789-8:30 ISO 8601
		s:           "04:05:06.789-8:30",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.FixedZone("-0830", -8*60*60-30*60)),
		hasTimezone: true,
	},
	{
		// 04:05-8:00 ISO 8601
		s:           "04:05-8:00",
		exp:         time.Date(0, 1, 1, 4, 5, 0, 0, time.FixedZone("-0800", -8*60*60)),
		hasTimezone: true,
	},
	{
		// 040506-08 ISO 8601
		s:           "040506-8",
		exp:         time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0800", -8*60*60)),
		hasTimezone: true,
	},
	{
		// 04:05:06 PST time zone specified by abbreviation
		// Unimplemented with message to user as such:
		// https://github.com/cockroachdb/cockroach/issues/31710
		s:   "04:05:06 PST",
		err: true,
		// This should be the value if/when we implement this.
		exp:           time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0800", -8*60*60)),
		hasTimezone:   true,
		unimplemented: true,
	},
	{
		// This test, and the next show that resolution of geographic names
		// to actual timezones is aware of daylight-savings time.  Note
		// that even though we're just parsing a time value, we do need
		// to provide a date in order to resolve the named zone to a
		// UTC offset.
		s:               "2003-01-12 04:05:06 America/New_York",
		exp:             time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0500", -5*60*60)),
		expectConcatErr: true,
		hasTimezone:     true,
	},
	{
		s:               "2003-06-12 04:05:06 America/New_York",
		exp:             time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0400", -4*60*60)),
		expectConcatErr: true,
		hasTimezone:     true,
	},

	// ----- More Tests -----
	{
		// Check positive TZ offsets.
		s:           "04:05:06.789+8:30",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.FixedZone("", 8*60*60+30*60)),
		hasTimezone: true,
	},
	{
		// Check TZ with seconds.
		s:           "04:05:06.789+8:30:15",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.FixedZone("", 8*60*60+30*60+15)),
		hasTimezone: true,
	},
	{
		// Check packed TZ with seconds.
		s:           "04:05:06.789+083015",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.FixedZone("", 8*60*60+30*60+15)),
		hasTimezone: true,
	},
	{
		// Check UTC zone.
		s:           "04:05:06.789 UTC",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
		hasTimezone: true,
	},
	{
		// Check GMT zone.
		s:           "04:05:06.789 GMT",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
		hasTimezone: true,
	},
	{
		// Check Z suffix with space.
		s:           "04:05:06.789 z",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
		hasTimezone: true,
	},
	{
		// Check Zulu suffix with space.
		s:           "04:05:06.789 zulu",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
		hasTimezone: true,
	},
	{
		// Check Z suffix without space.
		s:           "04:05:06.789z",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
		hasTimezone: true,
	},
	{
		// Check Zulu suffix without space.
		s:           "04:05:06.789zulu",
		exp:         time.Date(0, 1, 1, 4, 5, 6, int(789*time.Millisecond), time.UTC),
		hasTimezone: true,
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
		s:   "00:54 AM",
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
		s:           "12:54-00:29",
		exp:         time.Date(0, 1, 1, 12, 54, 0, 0, time.FixedZone("UGH", -29*60)),
		hasTimezone: true,
	},
	{
		// Check long timezone with date month.
		s:               "June 12, 2003 04:05:06 America/New_York",
		exp:             time.Date(0, 1, 1, 4, 5, 6, 0, time.FixedZone("-0400", -4*60*60)),
		expectConcatErr: true,
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
	},
	{
		//  Single-digits
		s:   "4:5:6",
		exp: time.Date(0, 1, 1, 4, 5, 6, 0, time.UTC),
	},
	{
		// Maximum value
		s: "24:00:00",
		// Allow hour 24 to roll over when we have a date.
		isRolloverTime: true,
	},
	{
		// Exceed maximum value
		s:   "24:00:00.000001",
		err: true,
	},
	{
		s: "23:59:60",
		// Allow this to roll over when we have a date.
		isRolloverTime: true,
	},
	{
		// Even though 24 and 60 are valid hours and seconds, 60 minutes is not.
		s:   "23:60:00",
		err: true,
	},
	{
		// Verify that we do support full nanosecond resolution in parsing.
		s:   "04:05:06.999999999",
		exp: time.Date(0, 1, 1, 4, 5, 6, 999999999, time.UTC),
		// PostgreSQL rounds to the nearest micro,
		// but we have other internal consumers that require nano precision.
		allowCrossDelta: time.Microsecond,
	},
	{
		// Over-long fractional portion gets truncated.
		s:   "04:05:06.9999999999",
		exp: time.Date(0, 1, 1, 4, 5, 6, 999999999, time.UTC),
		// PostgreSQL rounds to the nearest micro,
		// but we have other internal consumers that require nano precision.
		allowCrossDelta: time.Microsecond,
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
	{
		s:   "23.59.59.1",
		exp: time.Date(0, 1, 1, 23, 59, 59, 100000000, time.UTC),
	},
	{
		s:           "23.59.59.1-00:29",
		exp:         time.Date(0, 1, 1, 23, 59, 59, 100000000, time.FixedZone("UGH", -29*60)),
		hasTimezone: true,
	},
	{
		s:           "23.59.59.1-05",
		exp:         time.Date(0, 1, 1, 23, 59, 59, 100000000, time.FixedZone("-500", -5*60*60)),
		hasTimezone: true,
	},
	{
		s:           "23.59.59.1+05",
		exp:         time.Date(0, 1, 1, 23, 59, 59, 100000000, time.FixedZone("500", 5*60*60)),
		hasTimezone: true,
	},
	{
		s:   "23:59.59.",
		err: true,
	},
	{
		s:   "23.59:59.123",
		err: true,
	},
}

// Additional timestamp tests not generated by combining dates and times.
var timestampTestData = []timeData{
	{
		s:   "2000-01-01T02:02:02",
		exp: time.Date(2000, 1, 1, 2, 2, 2, 0, time.UTC),
	},
	{
		s:   "2000-01-01T02:02:02.567",
		exp: time.Date(2000, 1, 1, 2, 2, 2, 567000000, time.UTC),
	},
	{
		s:           "2000-01-01T02:02:02.567+09:30:15",
		exp:         time.Date(2000, 1, 1, 2, 2, 2, 567000000, time.FixedZone("", 9*60*60+30*60+15)),
		hasTimezone: true,
	},
	{
		s:   "2014-03-15-04.38.53.399853+07:35",
		exp: time.Date(2014, 3, 15, 4, 38, 53, 399853000, time.FixedZone("", 7*60*60+35*60)),
	},
	{
		s:   "2014-03-15-04.38.53.399853",
		exp: time.Date(2014, 3, 15, 4, 38, 53, 399853000, time.UTC),
	},
	{
		s:   "2014-03-15 04.38.53.399853",
		exp: time.Date(2014, 3, 15, 4, 38, 53, 399853000, time.UTC),
	},
	{
		s:   "2014-03-15 04.38.53.399853-07:35",
		exp: time.Date(2014, 3, 15, 4, 38, 53, 399853000, time.FixedZone("-0735", -7*60*60-35*60)),
	},
	{
		s:   "2014-03-15 04.38.53.",
		exp: time.Date(2014, 3, 15, 4, 38, 53, 0, time.UTC),
	},
	{
		s:   "2014-03-15-04.38:53.399853-07:35",
		err: true,
	},
	{
		s:   "2014-03-15-04:38.53.399853-07:35",
		err: true,
	},
	{
		s:   "2014-03-15-04:38:53.399853-07:35",
		err: true,
	},
	{
		s:   "2007-09-24-15.0",
		err: true,
	},
}

// TestMain will enable cross-checking of test results against a
// PostgreSQL instance if the -pgdate.db flag is set. This is mainly
// useful for developing the tests themselves and doesn't need
// to be part of a regular build.
func TestMain(m *testing.M) {
	flag.Parse()
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

// TestParse does the following:
// * For each parsing mode:
//   * Pick an example date input: 2018-01-01
//   * Test ParseDate()
//   * Pick an example time input: 12:34:56
//     * Derive a timestamp from date + time
//     * Test ParseTimestame()
//     * Test ParseDate()
//     * Test ParseTime()
//   * Test one-off timestamp formats
// * Pick an example time input:
//   * Test ParseTime()
func TestParse(t *testing.T) {
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for _, dtc := range dateTestData {
				dtc.testParseDate(t, dtc.s, mode)

				// Combine times with dates to create timestamps.
				for _, ttc := range timeTestData {
					info := fmt.Sprintf("%s %s", dtc.s, ttc.s)
					tstc := dtc.concatTime(ttc)
					tstc.testParseDate(t, info, mode)
					tstc.testParseTime(t, info, mode)
					tstc.testParseTimestamp(t, info, mode)
					tstc.testParseTimestampWithoutTimezone(t, info, mode)
				}
			}

			// Test some other timestamps formats we can't create
			// by just concatenating a date + time string.
			for _, ttc := range timestampTestData {
				ttc.testParseTime(t, ttc.s, mode)
			}
		})
	}

	t.Run("ParseTime", func(t *testing.T) {
		for _, ttc := range timeTestData {
			ttc.testParseTime(t, ttc.s, 0 /* mode */)
		}
	})
}

// BenchmarkParseTimestampComparison makes a single-pass comparison
// between pgdate.ParseTimestamp() and time.ParseInLocation().
// It bears repeating that ParseTimestamp() can handle all formats
// in a single go, whereas ParseInLocation() would require repeated
// calls in order to try a number of different formats.
func BenchmarkParseTimestampComparison(b *testing.B) {
	// Just a date
	bench(b, "2006-01-02", "2003-06-12", "")

	// Just a date
	bench(b, "2006-01-02 15:04:05", "2003-06-12 01:02:03", "")

	// This is the standard wire format.
	bench(b, "2006-01-02 15:04:05.999999999Z07:00", "2003-06-12 04:05:06.789-04:00", "")

	// 2006-01-02 15:04:05.999999999Z07:00
	bench(b, time.RFC3339Nano, "2000-01-01T02:02:02.567+09:30", "")

	// Show what happens when a named TZ is used.
	bench(b, "2006-01-02 15:04:05.999999999", "2003-06-12 04:05:06.789", "America/New_York")
}

// bench compares our ParseTimestamp to ParseInLocation, optionally
// chained with a timeutil.LoadLocation() for resolving named zones.
// The layout parameter is only used for time.ParseInLocation().
// When a named timezone is used, it must be passed via locationName
// so that it may be resolved to a time.Location. It will be
// appended to the string being benchmarked by pgdate.ParseTimestamp().
func bench(b *testing.B, layout string, s string, locationName string) {
	b.Run(strings.TrimSpace(s+" "+locationName), func(b *testing.B) {
		b.Run("ParseTimestamp", func(b *testing.B) {
			benchS := s
			if locationName != "" {
				benchS += " " + locationName
			}
			bytes := int64(len(benchS))

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, _, err := pgdate.ParseTimestamp(time.Time{}, 0, benchS); err != nil {
						b.Fatal(err)
					}
					b.SetBytes(bytes)
				}
			})
		})

		b.Run("ParseInLocation", func(b *testing.B) {
			bytes := int64(len(s))
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					loc := time.UTC
					if locationName != "" {
						var err error
						loc, err = timeutil.LoadLocation(locationName)
						if err != nil {
							b.Fatal(err)
						}
					}
					if _, err := time.ParseInLocation(layout, s, loc); err != nil {
						b.Fatal(err)
					}
					b.SetBytes(bytes)
				}
			})
		})
	})
}

// check is a helper function to compare expected and actual
// outputs and error conditions.
func check(t testing.TB, info string, expTime time.Time, expErr bool, res time.Time, err error) {
	t.Helper()

	if err == nil {
		if expErr {
			t.Errorf("%s: expected error, but succeeded %s", info, res)
		} else if !res.Equal(expTime) {
			t.Errorf("%s: expected %s, got %s", info, expTime, res)
		}
	} else if !expErr {
		t.Errorf("%s: unexpected error: %v", info, err)
	}
}

// crossCheck executes the parsing on a remote sql connection.
func (td timeData) crossCheck(
	t *testing.T, info string, kind, s string, mode pgdate.ParseMode, expTime time.Time, expErr bool,
) {
	if db == nil {
		return
	}

	switch {
	case db == nil:
		return
	case td.unimplemented:
		return
	case td.expectCrossErr:
		expErr = true
	}

	info = fmt.Sprintf("%s cross-check", info)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%s: %v", info, err)
	}

	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Fatalf("%s: %v", info, err)
		}
	}()

	if _, err := db.Exec("set time zone 'UTC'"); err != nil {
		t.Fatalf("%s: %v", info, err)
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
		t.Fatalf("%s: %v", info, err)
	}

	row := db.QueryRow(fmt.Sprintf("select '%s'::%s", s, kind))
	var ret time.Time
	if err := row.Scan(&ret); err == nil {
		switch {
		case expErr:
			t.Errorf("%s: expected error, got %s", info, ret)
		case ret.Round(td.allowCrossDelta).Equal(expTime.Round(td.allowCrossDelta)):
			// Got expected value.
		default:
			t.Errorf("%s: expected %s, got %s", info, expTime, ret)
		}
	} else {
		switch {
		case expErr:
			// Got expected error.
		case kind == "time", kind == "timetz":
			// Our parser is quite a bit more lenient than the
			// PostgreSQL 10.5 implementation. For instance:
			// '1999.123 12:54 PM +11'::timetz --> fail
			// '1999.123 12:54 PM America/New_York'::timetz --> OK
			// Trying to run this down is too much of a time-sink,
			// and as long as we're not producing erroneous values,
			// it's reasonable to treat cases where we can parse,
			// but pg doesn't as a soft failure.
		default:
			t.Errorf(`%s: unexpected error from "%s": %s`, info, s, err)
		}
	}
}

func TestDependsOnContext(t *testing.T) {
	// Each test case contains the expected output for each of ParseDate,
	// ParseTime, ParseTimeWithoutTimezone, ParseTimestamp,
	// ParseTimestampWithoutTimezone.
	//
	// The output contains the result with "yes/no" appended to indicate context
	// dependence. If an error is expected, "error" is used.
	testCases := []struct {
		s             string
		date          string
		time          string
		timeNoTZ      string
		timestamp     string
		timestampNoTZ string
	}{
		{
			s:             "04:05:06",
			date:          "error",
			time:          "0000-01-01 04:05:06 +0500 +0500 yes",
			timeNoTZ:      "0000-01-01 04:05:06 +0000 UTC no",
			timestamp:     "error",
			timestampNoTZ: "error",
		},
		{
			s:             "04:05:06.000001+00",
			date:          "error",
			time:          "0000-01-01 04:05:06.000001 +0000 +0000 no",
			timeNoTZ:      "0000-01-01 04:05:06.000001 +0000 UTC no",
			timestamp:     "error",
			timestampNoTZ: "error",
		},
		{
			s:             "04:05:06.000001-04",
			date:          "error",
			time:          "0000-01-01 04:05:06.000001 -0400 -0400 no",
			timeNoTZ:      "0000-01-01 04:05:06.000001 +0000 UTC no",
			timestamp:     "error",
			timestampNoTZ: "error",
		},
		{
			s:             "2017-03-03 01:00:00.00000",
			date:          "2017-03-03 no",
			time:          "0000-01-01 01:00:00 +0500 +0500 yes",
			timeNoTZ:      "0000-01-01 01:00:00 +0000 UTC no",
			timestamp:     "2017-03-03 01:00:00 +0500 foo yes",
			timestampNoTZ: "2017-03-03 01:00:00 +0000 UTC no",
		},
		{
			s:             "2017-03-03 01:00:00.00000-04",
			date:          "2017-03-03 no",
			time:          "0000-01-01 01:00:00 -0400 -0400 no",
			timeNoTZ:      "0000-01-01 01:00:00 +0000 UTC no",
			timestamp:     "2017-03-03 01:00:00 -0400 -040000 no",
			timestampNoTZ: "2017-03-03 01:00:00 +0000 UTC no",
		},
		{
			s:             "2017-03-03 01:00:00.00000 Europe/Berlin",
			date:          "2017-03-03 no",
			time:          "0000-01-01 01:00:00 +0100 +0100 no",
			timeNoTZ:      "0000-01-01 01:00:00 +0000 UTC no",
			timestamp:     "2017-03-03 01:00:00 +0100 CET no",
			timestampNoTZ: "2017-03-03 01:00:00 +0000 UTC no",
		},
		{
			s:             "now",
			date:          "2001-02-03 yes",
			time:          "2001-02-03 04:05:06.000001 +0500 foo yes",
			timeNoTZ:      "2001-02-03 04:05:06.000001 +0000 UTC yes",
			timestamp:     "2001-02-03 04:05:06.000001 +0500 foo yes",
			timestampNoTZ: "2001-02-03 04:05:06.000001 +0000 UTC yes",
		},
		{
			s:             "tomorrow",
			date:          "2001-02-04 yes",
			time:          "error",
			timeNoTZ:      "error",
			timestamp:     "2001-02-04 00:00:00 +0500 foo yes",
			timestampNoTZ: "2001-02-04 00:00:00 +0000 UTC yes",
		},
	}

	now := time.Date(2001, time.February, 3, 4, 5, 6, 1000, time.FixedZone("foo", 18000))
	mode := pgdate.ParseModeYMD
	for _, tc := range testCases {
		t.Run(tc.s, func(t *testing.T) {
			toStr := func(result interface{}, depOnCtx bool, err error) string {
				if err != nil {
					return "error"
				}
				if s := fmt.Sprint(result); depOnCtx {
					return s + " yes"
				} else {
					return s + " no"
				}
			}
			check := func(what string, expected string, actual string) {
				t.Helper()
				if expected != actual {
					t.Errorf("%s: expected '%s', got '%s'", what, expected, actual)
				}
			}
			check("ParseDate", tc.date, toStr(pgdate.ParseDate(now, mode, tc.s)))
			check("ParseTime", tc.time, toStr(pgdate.ParseTime(now, mode, tc.s)))
			check(
				"ParseTimeWithoutTimezone", tc.timeNoTZ,
				toStr(pgdate.ParseTimeWithoutTimezone(now, mode, tc.s)),
			)
			check("ParseTimestamp", tc.timestamp, toStr(pgdate.ParseTimestamp(now, mode, tc.s)))
			check("ParseTimestampWithoutTimezone",
				tc.timestampNoTZ, toStr(pgdate.ParseTimestampWithoutTimezone(now, mode, tc.s)),
			)
		})
	}
}
