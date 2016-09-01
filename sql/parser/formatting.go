// Copyright 2015 The Cockroach Authors.
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
//
// Author: Yan Long (rafealyim@qq.com)

package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

/* ----------
 * Routines type
 * ----------
 */
const (
	typeDatetime = 1
	typeNumber   = 2
)

/* ----------
 * Format-pictures (KeyWord).
 *
 * The KeyWord field; alphabetic sorted, *BUT* strings alike is sorted
 *		  complicated -to-> easy:
 *
 *	(example: "DDD","DD","Day","D" )
 *
 * (this specific sort needs the algorithm for sequential search for strings,
 * which not has exact end; -> How keyword is in "HH12blabla" ? - "HH"
 * or "HH12"? You must first try "HH12", because "HH" is in string, but
 * it is not good.
 *
 * (!)
 *	 - Position for the keyword is similar as position in the enum DCH/NUM_poz.
 * (!)
 *
 * For fast search is used the 'int index[]', index is ascii table from position
 * 32 (' ') to 126 (~), in this index is DCH_ / NUM_ enums for each ASCII
 * position or -1 if char is not used in the KeyWord. Search example for
 * string "MM":
 *	1)	see in index to index['M' - 32],
 *	2)	take keywords position (enum DCH_MI) from index
 *	3)	run sequential search in keywords[] from this position
 *
 * ----------
 */
const (
	idxUpperAnnoDotDomin = iota
	idxUpperAnnoDotMeridiem
	idxUpperAnnoDomin
	idxUpperAnnoMeridiem
	idxUpperBeforeDotChrist
	idxUpperBeforeChrist
	idxUpperCentury
	idxUpperDay
	idxUpperDayOfYear
	idxUpperDayOfMonth
	idxUpperAbbreviatedDayName
	idxMixedCaseDay
	idxMixedCaseAbbreviatedDayName
	idxDayOfWeek
	idxFixFormatGlobalOption
	idxHourOfDay24
	idxHourOfDay12
	idxHourOfDay
	idxUpperISODayOfYear
	idxUpperISODayOfWeek
	idxISOWeekOfYear
	idxISOYear
	idx3DigitsOfISOYear
	idx2DigitsOfISOYear
	idxDigitsOfISOYear
	idxJulianDay
	idxMinute
	idxMonthNumber
	idxUpperMonthName
	idxAbbreviatedUpperMonthName
	idxMillisecond
	idxMixedCaseMonthName
	idxAbbreviatedMixedCaseMonthName
	idxUpperOF
	idxUpperPostDotMeridiem
	idxUpperPostMeridiem
	idxQuarter
	idxUpperMonthInRomanNumerals
	idxUpperSecondPastMidnight
	idxUpperSecond
	idxUpperTimeZone
	idxUpperMicrosecond
	idxUpperWeekOfYear
	idxUpperWeekOfMonth
	idxUpperYearWithComma
	idxUpper4DigitsOfYear
	idxUpper3DigitsOfYear
	idxUpper2DigitsOfYear
	idxUpperLastDigitsOfYear
	idxLowerAnnoDotDomin
	idxLowerAnnoDotMeridiem
	idxLowerAnnoDomin
	idxLowerAnnoMeridiem
	idxLowerBeforeDotChrist
	idxLowerBeforeChrist
	idxLowerCentury
	idxLowerDay
	idxLowerDayOfYear
	idxLowerDayOfMonth
	idxLowerAbbreviatedDayName
	idxLowerFixedFormatGlobalOption
	idxLowerOurOfDay24
	idxLowerISODayOfYear
	idxLowerJulianDay
	idxLowerMinute
	idxLowerMonthName
	idxAbbreviatedLowerMonthName
	idxLowerPostDotMeridiem
	idxLowerPostMeridiem
	idxLowerQuarter
	idxLowerMonthInRomanNumerals
	idxLowerSecondPastMidnight
	idxLowerTimeZone
	idxLowerMicrosecond
	idxLowerWeekNumberOfYear
	idxLowerYearWithComma
)

const (
	suffTypePrefix = 1 + iota
	suffTypePostfix
)

const (
	clock12Hour = 1
)

type keySuffix struct {
	name  string /* suffix string		*/
	len   int    /* suffix length		*/
	id    int    /* used in node->suffix */
	sType int    /* prefix / postfix			*/
}

/* ----------
 * FromCharDateMode
 * ----------
 *
 * This value is used to nominate one of several distinct (and mutually
 * exclusive) date conventions that a keyword can belong to.
 */
const (
	fromCharDateNone      = iota /* Value does not affect date mode. */
	fromCharDateGregorian        /* Gregorian (day, month, year) style date */
	fromCharDateISOweek          /* ISO 8601 week date */
)

const (
	suffFillMode           = 0x01
	suffUpperOrdinalNumber = 0x02
	suffLowerOrdinalNumber = 0x04
	suffSpellMode          = 0x08
)

/* ----------
 * Suffixes definition for DATE-TIME TO/FROM CHAR
 * ----------
 */
var datetimeFormatSuffix = []*keySuffix{
	{"FM", 2, suffFillMode, suffTypePrefix},
	{"fm", 2, suffFillMode, suffTypePrefix},
	//	{"TM", 2, dchSTM, suffTypePrefix},
	//	{"tm", 2, dchSTM, suffTypePrefix},
	{"TH", 2, suffUpperOrdinalNumber, suffTypePostfix},
	{"th", 2, suffLowerOrdinalNumber, suffTypePostfix},
	{"SP", 2, suffSpellMode, suffTypePostfix},
}

type keyWord struct {
	name     string
	len, id  int
	isDigit  bool
	dateMode int
}

type formatNode struct {
	nType     int      /* node type			*/
	key       *keyWord /* if node type is KEYWORD	*/
	character byte     /* if node type is CHAR		*/
	suffix    int      /* keyword suffix		*/
}

const (
	nodeTypeEnd = 1 + iota
	nodeTypeAction
	nodeTypeChar
)

var dateTimeFmtKeyword = []*keyWord{
	// name, len, id, isDigit, dateMode
	{"A.D.", 4, idxUpperAnnoDotDomin, false, fromCharDateNone}, /* A */
	{"A.M.", 4, idxUpperAnnoDotMeridiem, false, fromCharDateNone},
	{"AD", 2, idxUpperAnnoDomin, false, fromCharDateNone},
	{"AM", 2, idxUpperAnnoMeridiem, false, fromCharDateNone},
	{"B.C.", 4, idxUpperBeforeDotChrist, false, fromCharDateNone}, /* B */
	{"BC", 2, idxUpperBeforeChrist, false, fromCharDateNone},
	{"CC", 2, idxUpperCentury, true, fromCharDateNone}, /* C */
	{"DAY", 3, idxUpperDay, false, fromCharDateNone},   /* D */
	{"DDD", 3, idxUpperDayOfYear, true, fromCharDateGregorian},
	{"DD", 2, idxUpperDayOfMonth, true, fromCharDateGregorian},
	{"DY", 2, idxUpperAbbreviatedDayName, false, fromCharDateNone},
	{"Day", 3, idxMixedCaseDay, false, fromCharDateNone},
	{"Dy", 2, idxMixedCaseAbbreviatedDayName, false, fromCharDateNone},
	{"D", 1, idxDayOfWeek, true, fromCharDateGregorian},
	{"FX", 2, idxFixFormatGlobalOption, false, fromCharDateNone}, /* F */
	{"HH24", 4, idxHourOfDay24, true, fromCharDateNone},          /* H */
	{"HH12", 4, idxHourOfDay12, true, fromCharDateNone},
	{"HH", 2, idxHourOfDay, true, fromCharDateNone},
	{"IDDD", 4, idxUpperISODayOfYear, true, fromCharDateISOweek}, /* I */
	{"ID", 2, idxUpperISODayOfWeek, true, fromCharDateISOweek},
	{"IW", 2, idxISOWeekOfYear, true, fromCharDateISOweek},
	{"IYYY", 4, idxISOYear, true, fromCharDateISOweek},
	{"IYY", 3, idx3DigitsOfISOYear, true, fromCharDateISOweek},
	{"IY", 2, idx2DigitsOfISOYear, true, fromCharDateISOweek},
	{"I", 1, idxDigitsOfISOYear, true, fromCharDateISOweek},
	{"J", 1, idxJulianDay, true, fromCharDateNone}, /* J */
	{"MI", 2, idxMinute, true, fromCharDateNone},   /* M */
	{"MM", 2, idxMonthNumber, true, fromCharDateGregorian},
	{"MONTH", 5, idxUpperMonthName, false, fromCharDateGregorian},
	{"MON", 3, idxAbbreviatedUpperMonthName, false, fromCharDateGregorian},
	{"MS", 2, idxMillisecond, true, fromCharDateNone},
	{"Month", 5, idxMixedCaseMonthName, false, fromCharDateGregorian},
	{"Mon", 3, idxAbbreviatedMixedCaseMonthName, false, fromCharDateGregorian},
	{"OF", 2, idxUpperOF, false, fromCharDateNone},                /* O */
	{"P.M.", 4, idxUpperPostDotMeridiem, false, fromCharDateNone}, /* P */
	{"PM", 2, idxUpperPostMeridiem, false, fromCharDateNone},
	{"Q", 1, idxQuarter, true, fromCharDateNone},                          /* Q */
	{"RM", 2, idxUpperMonthInRomanNumerals, false, fromCharDateGregorian}, /* R */
	{"SSSS", 4, idxUpperSecondPastMidnight, true, fromCharDateNone},       /* S */
	{"SS", 2, idxUpperSecond, true, fromCharDateNone},
	{"TZ", 2, idxUpperTimeZone, false, fromCharDateNone},       /* T */
	{"US", 2, idxUpperMicrosecond, true, fromCharDateNone},     /* U */
	{"WW", 2, idxUpperWeekOfYear, true, fromCharDateGregorian}, /* W */
	{"W", 1, idxUpperWeekOfMonth, true, fromCharDateGregorian},
	{"Y,YYY", 5, idxUpperYearWithComma, true, fromCharDateGregorian}, /* Y */
	{"YYYY", 4, idxUpper4DigitsOfYear, true, fromCharDateGregorian},
	{"YYY", 3, idxUpper3DigitsOfYear, true, fromCharDateGregorian},
	{"YY", 2, idxUpper2DigitsOfYear, true, fromCharDateGregorian},
	{"Y", 1, idxUpperLastDigitsOfYear, true, fromCharDateGregorian},
	{"a.d.", 4, idxLowerAnnoDotDomin, false, fromCharDateNone}, /* a */
	{"a.m.", 4, idxLowerAnnoDotMeridiem, false, fromCharDateNone},
	{"ad", 2, idxLowerAnnoDomin, false, fromCharDateNone},
	{"am", 2, idxLowerAnnoMeridiem, false, fromCharDateNone},
	{"b.c.", 4, idxLowerBeforeDotChrist, false, fromCharDateNone}, /* b */
	{"bc", 2, idxLowerBeforeChrist, false, fromCharDateNone},
	{"cc", 2, idxLowerCentury, true, fromCharDateNone}, /* c */
	{"day", 3, idxLowerDay, false, fromCharDateNone},   /* d */
	{"ddd", 3, idxLowerDayOfYear, true, fromCharDateGregorian},
	{"dd", 2, idxLowerDayOfMonth, true, fromCharDateGregorian},
	{"dy", 2, idxLowerAbbreviatedDayName, false, fromCharDateNone},
	{"d", 1, idxDayOfWeek, true, fromCharDateGregorian},
	{"fx", 2, idxFixFormatGlobalOption, false, fromCharDateNone}, /* f */
	{"hh24", 4, idxHourOfDay24, true, fromCharDateNone},          /* h */
	{"hh12", 4, idxHourOfDay12, true, fromCharDateNone},
	{"hh", 2, idxHourOfDay, true, fromCharDateNone},
	{"iddd", 4, idxUpperISODayOfYear, true, fromCharDateISOweek}, /* i */
	{"id", 2, idxUpperISODayOfWeek, true, fromCharDateISOweek},
	{"iw", 2, idxISOWeekOfYear, true, fromCharDateISOweek},
	{"iyyy", 4, idxISOYear, true, fromCharDateISOweek},
	{"iyy", 3, idx3DigitsOfISOYear, true, fromCharDateISOweek},
	{"iy", 2, idx2DigitsOfISOYear, true, fromCharDateISOweek},
	{"i", 1, idxDigitsOfISOYear, true, fromCharDateISOweek},
	{"j", 1, idxJulianDay, true, fromCharDateNone}, /* j */
	{"mi", 2, idxMinute, true, fromCharDateNone},   /* m */
	{"mm", 2, idxMonthNumber, true, fromCharDateGregorian},
	{"month", 5, idxLowerMonthName, false, fromCharDateGregorian},
	{"mon", 3, idxAbbreviatedLowerMonthName, false, fromCharDateGregorian},
	{"ms", 2, idxMillisecond, true, fromCharDateNone},
	{"p.m.", 4, idxLowerPostDotMeridiem, false, fromCharDateNone}, /* p */
	{"pm", 2, idxLowerPostMeridiem, false, fromCharDateNone},
	{"q", 1, idxQuarter, true, fromCharDateNone},                          /* q */
	{"rm", 2, idxLowerMonthInRomanNumerals, false, fromCharDateGregorian}, /* r */
	{"ssss", 4, idxUpperSecondPastMidnight, true, fromCharDateNone},       /* s */
	{"ss", 2, idxUpperSecond, true, fromCharDateNone},
	{"tz", 2, idxLowerTimeZone, false, fromCharDateNone},       /* t */
	{"us", 2, idxUpperMicrosecond, true, fromCharDateNone},     /* u */
	{"ww", 2, idxUpperWeekOfYear, true, fromCharDateGregorian}, /* w */
	{"w", 1, idxUpperWeekOfMonth, true, fromCharDateGregorian},
	{"y,yyy", 5, idxUpperYearWithComma, true, fromCharDateGregorian}, /* y */
	{"yyyy", 4, idxUpper4DigitsOfYear, true, fromCharDateGregorian},
	{"yyy", 3, idxUpper3DigitsOfYear, true, fromCharDateGregorian},
	{"yy", 2, idxUpper2DigitsOfYear, true, fromCharDateGregorian},
	{"y", 1, idxUpperLastDigitsOfYear, true, fromCharDateGregorian},
}

var datetimeFmtIdx = []int{
	/*
	   0	1	2	3	4	5	6	7	8	9
	*/
	/*---- first 0..31 chars are skipped ----*/

	-1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, idxUpperAnnoDotDomin, idxUpperBeforeDotChrist, idxUpperCentury, idxUpperDay, -1,
	idxFixFormatGlobalOption, -1, idxHourOfDay24, idxUpperISODayOfYear, idxJulianDay, -1, -1, idxMinute, -1, idxUpperOF,
	idxUpperPostDotMeridiem, idxQuarter, idxUpperMonthInRomanNumerals, idxUpperSecondPastMidnight, idxUpperTimeZone, idxUpperMicrosecond, -1, idxUpperWeekOfYear, -1, idxUpperYearWithComma,
	-1, -1, -1, -1, -1, -1, -1, idxLowerAnnoDotDomin, idxLowerBeforeDotChrist, idxLowerCentury,
	idxMixedCaseDay, -1, idxLowerFixedFormatGlobalOption, -1, idxLowerOurOfDay24, idxLowerISODayOfYear, idxLowerJulianDay, -1, -1, idxLowerMinute,
	-1, -1, idxLowerPostDotMeridiem, idxLowerQuarter, idxLowerMonthInRomanNumerals, idxLowerSecondPastMidnight, idxLowerTimeZone, idxLowerMicrosecond, -1, idxLowerWeekNumberOfYear,
	-1, idxLowerYearWithComma, -1, -1, -1, -1,
	/*---- chars over 126 are skipped ----*/
}

type numDesc struct {
}

// PgTimestamp is PG style timestamp
type PgTimestamp struct {
	sec, min, hour, mday, mon, year, wday, yday int
	nsec                                        int64
}

/* ----------
 * For char->date/time conversion
 * ----------
 */
type tmFromChar struct {
	mode,
	hh,
	pm,
	mi,
	ss,
	ssss,
	d, /* stored as 1-7, Sunday = 1, 0 means missing */
	dd,
	ddd,
	mm,
	ms,
	year,
	bc,
	ww,
	w,
	cc,
	j,
	us,
	yysz, /* is it YY or YYYY ? */
	clock int /* 12 or 24 hour clock? */
}

const (
	oneUpper = 1 + iota
	allUpper
	allLower
)

const (
	maxMonthLen = 9
	maxMonLen   = 3
	maxDayLen   = 9
	maxDyLen    = 3
	maxRmLen    = 4
)

const (
	aDotmUpper = "A.M"
	aDotmLower = "a.m"
	amUpper    = "AM"
	amLower    = "am"

	pDotmUpper = "P.M"
	pDotmLower = "p.m"
	pmUpper    = "PM"
	pmLower    = "pm"
)

const (
	aDotdUpper = "A.D."
	aDotdLower = "a.d."
	adUpper    = "AD"
	adLower    = "ad"

	bDotcUpper = "B.C."
	bDotcLower = "b.c."
	bcUpper    = "BC"
	bcLower    = "bc"
)

const (
	secondPerHour = 3600
	secondPerMin  = 60
	hoursPerDay   = 24
	monthsPerYear = 12
)

var ampmStrings = []string{
	amLower, pmLower, amUpper, pmUpper,
}

var ampmStringsLong = []string{
	aDotmLower, pDotmLower, aDotmUpper, pDotmUpper,
}

var adbcStrings = []string{
	adLower, bcLower, adUpper, bcUpper,
}

var adbcStringsLong = []string{
	aDotdLower, bDotcLower, aDotdUpper, bDotcUpper,
}

var monthFull = []string{
	"January", "February", "March", "April", "May", "June", "July",
	"August", "September", "October", "November", "December",
}

var monthShort = []string{
	"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
}

var dayFull = []string{
	"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
}

var dayShort = []string{
	"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat",
}

var romanNumeralsUpper = []string{
	"XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I",
}

var romanNumeralsLower = []string{
	"xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i",
}

// DoToTimestamp parses and returns the *PgTimestamp value represented by the provided
// string with the provided format, or an error if parsing is unsuccessful.
func DoToTimestamp(dateTxt string, fmtStr string) (*PgTimestamp, error) {
	format, err := parseFormat(fmtStr, dateTimeFmtKeyword, datetimeFormatSuffix, datetimeFmtIdx, typeDatetime, nil)
	if err != nil {
		return nil, err
	}

	tm, err := datetimeDecodeFromString(format, dateTxt)
	if err != nil {
		return nil, err
	}

	ts := PgTimestamp{}
	if tm.ssss > 0 {
		x := tm.ssss
		ts.hour = x / secondPerHour
		x %= secondPerHour
		ts.min = x / secondPerMin
		x %= secondPerMin
		ts.sec = x
	}

	if tm.ss > 0 {
		ts.sec = tm.ss
	}
	if tm.mi > 0 {
		ts.min = tm.mi
	}
	if tm.hh > 0 {
		ts.hour = tm.hh
	}

	if tm.clock == clock12Hour {
		if ts.hour < 1 || ts.hour > hoursPerDay/2 {
			return nil, fmt.Errorf("hour \"%d\" is invalid for 12-hour-clock", ts.hour)
		}

		if tm.pm > 0 && ts.hour < hoursPerDay/2 {
			ts.hour += hoursPerDay / 2
		} else if tm.pm == 0 && ts.hour == hoursPerDay/2 {
			ts.hour = 0
		}
	}

	if tm.year > 0 {
		/*
		 * If CC and YY (or Y) are provided, use YY as 2 low-order digits for
		 * the year in the given century.  Keep in mind that the 21st century
		 * AD runs from 2001-2100, not 2000-2099; 6th century BC runs from
		 * 600BC to 501BC.
		 */
		if tm.cc > 0 && tm.yysz <= 2 {
			if tm.bc > 0 {
				tm.cc = -tm.cc
			}
			ts.year = tm.year % 100
			if ts.year > 0 {
				if tm.cc >= 0 {
					ts.year += (tm.cc - 1) * 100
				} else {
					ts.year = (tm.cc+1)*100 - ts.year + 1
				}
			} else if tm.cc >= 0 {
				ts.year = tm.cc*100 + 0
			} else {
				ts.year = tm.cc*100 + 1
			}
		} else { /* If a 4-digit year is provided, we use that and ignore CC. */
			ts.year = tm.year
			if tm.bc > 0 && ts.year > 0 {
				ts.year = -(ts.year - 1)
			}
		}
	} else if tm.cc > 0 { /* use first year of century */
		if tm.cc > 0 {
			tm.cc = -tm.cc
		}
		if tm.cc >= 0 {
			ts.year = (tm.cc-1)*100 + 1
		} else {
			ts.year = tm.cc*100 + 1
		}
	}

	if tm.j > 0 {
		j2date(tm.j, &ts.year, &ts.mon, &ts.mday)
	}

	if tm.ww > 0 {
		if tm.mode == fromCharDateISOweek {
			/*
			 * If tmfc.d is not set, then the date is left at the beginning of
			 * the ISO week (Monday).
			 */
			if tm.d > 0 {
				isoWeekDate2Date(tm.ww, tm.d, &(ts.year), &(ts.mon), &(ts.mday))
			} else {
				isoWeek2Date(tm.ww, &(ts.year), &(ts.mon), &(ts.mday))
			}
		} else {
			tm.ddd = (tm.ww-1)*7 + 1
		}
	}

	if tm.w > 0 {
		tm.dd = (tm.w-1)*7 + 1
	}
	if tm.d > 0 {
		ts.wday = tm.d - 1
	}
	if tm.dd > 0 {
		ts.mday = tm.dd
	}
	if tm.ddd > 0 {
		ts.yday = tm.ddd
	}
	if tm.mm > 0 {
		ts.mon = tm.mm
	}

	if tm.ddd > 0 && (ts.mon <= 1 || ts.mday <= 1) {
		/*
		 * The month and day field have not been set, so we use the
		 * day-of-year field to populate them.  Depending on the date mode,
		 * this field may be interpreted as a Gregorian day-of-year, or an ISO
		 * week date day-of-year.
		 */
		if ts.year == 0 && tm.bc == 0 {
			return nil, fmt.Errorf("cannot calculate day of year without year information")
		}

		if tm.mode == fromCharDateISOweek {
			j0 := isoWeek2J(ts.year, 1) - 1
			j2date(j0+tm.ddd, &(ts.year), &(ts.mon), &(ts.mday))
		} else {
			ysum := [][]int{
				{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365},
				{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366},
			}
			y := ysum[isLeap(ts.year)]
			var i int
			for i = 1; i <= monthsPerYear; i++ {
				if tm.ddd < y[i] {
					break
				}
			}

			if ts.mon <= 1 {
				ts.mon = i
			}
			if ts.mday <= 1 {
				ts.mday = tm.ddd - y[i-1]
			}
		}
	}

	if tm.ms > 0 {
		ts.nsec += int64(tm.ms) * int64(time.Millisecond)
	}
	if tm.us > 0 {
		ts.nsec += int64(tm.us) * int64(time.Microsecond)
	}
	return &ts, nil
}

/* ----------
 * Format parser, search small keywords and keyword's suffixes, and make
 * format-node tree.
 *
 * for DATE-TIME & NUMBER version
 * ----------
 */
func parseFormat(formatStr string, kw []*keyWord, suf []*keySuffix, index []int, ver int, num []*numDesc) ([]*formatNode, error) {
	var ns []*formatNode
	nodeSet, suffix := 0, 0
	last := byte(0)
	var n *formatNode

	for i := 0; i < len(formatStr); {
		suffix = 0

		if ver == typeDatetime {
			if s := suffSearch(formatStr[i:], suf, suffTypePrefix); s != nil {
				suffix |= s.id
				if s.len > 0 {
					i += s.len
				}
			}
		}

		if i < len(formatStr) {
			if nkey := indexSeqSearch([]byte(formatStr[i:]), kw, index); nkey != nil {
				n = &formatNode{
					nodeTypeAction,
					nkey,
					0,
					0,
				}
				nodeSet = 1
				if n.key.len > 0 {
					i += n.key.len
				}

				if ver == typeNumber {

				}

				if ver == typeDatetime && i < len(formatStr) {
					if s := suffSearch(formatStr[i:], suf, suffTypePostfix); s != nil {
						suffix |= s.id
						if s.len >= 0 {
							i += s.len
						}
					}
				}
				ns = append(ns, n)
			} else if i < len(formatStr) {
				if formatStr[i] == '"' && last != '\\' {
					x := byte(0)
					for i++; i < len(formatStr); i++ {
						if formatStr[i] == '"' && x != '\\' {
							i = i + 1
							break
						} else if formatStr[i] == '\\' && x != '\\' {
							x = '\\'
							continue
						}
						n = &formatNode{
							nodeTypeChar,
							nil,
							formatStr[i],
							0,
						}
						ns = append(ns, n)
						x = formatStr[i]
					}
					nodeSet = 0
					suffix = 0
					last = 0
				} else if formatStr[i] == '\\' && last != '\\' && formatStr[i+1] == '"' {
					i = i + 1
					last = formatStr[i]
				} else if i < len(formatStr) {
					n = &formatNode{
						nodeTypeChar,
						nil,
						formatStr[i],
						0,
					}
					i = i + 1
					ns = append(ns, n)
					nodeSet = 1
					last = 0
				}
			}
		}
		if nodeSet != 0 {
			n.suffix = suffix
			if n.nType == nodeTypeAction {
			}
			n.suffix = 0
			nodeSet = 0
		}
	}
	n = &formatNode{
		nodeTypeEnd,
		nil,
		0,
		0,
	}
	ns = append(ns, n)
	return ns, nil
}

func datetimeDecodeFromString(node []*formatNode, in string) (*tmFromChar, error) {
	fixMode := false
	out := &tmFromChar{}

	for i, j := 0, 0; i < len(node) && j < len(in); i++ {
		n := node[i]
		if n.nType == nodeTypeEnd {
			break
		}

		if n.nType != nodeTypeAction {
			j++
			continue
		}

		if !fixMode && n.key.id != idxFixFormatGlobalOption {
			for j < len(in) && isSpace(int(in[j])) {
				j++
			}
		}
		if _, err := fromCharSetMode(out, n.key.dateMode); err != nil {
			return nil, err
		}
		switch n.key.id {
		case idxFixFormatGlobalOption:
			fixMode = true
		case idxUpperAnnoDotMeridiem, idxLowerAnnoDotMeridiem, idxUpperPostDotMeridiem, idxLowerPostDotMeridiem:
			val, length := seqSearch(in[j:], ampmStringsLong, allUpper, n.key.len)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.pm = val % 2
			j += length
			out.clock = clock12Hour
		case idxUpperAnnoMeridiem, idxUpperPostMeridiem, idxLowerAnnoMeridiem, idxLowerPostMeridiem:
			val, length := seqSearch(in[j:], ampmStrings, allUpper, n.key.len)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.pm = val % 2
			j += length
			out.clock = clock12Hour
		case idxHourOfDay, idxHourOfDay12:
			used, err := fromCharParseIntLen(&(out.hh), in[j:], 2, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			out.clock = clock12Hour
			skipTHSuffix(&j, n.suffix)
		case idxHourOfDay24:
			used, err := fromCharParseIntLen(&(out.hh), in[j:], 2, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxMinute:
			used, err := fromCharParseIntLen(&(out.mi), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperSecond:
			used, err := fromCharParseIntLen(&(out.ss), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxMillisecond:
			used, err := fromCharParseIntLen(&(out.ms), in[j:], 3, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			if used == 1 {
				out.ms *= 100
			} else if used == 2 {
				out.ms *= 10
			}
			skipTHSuffix(&j, n.suffix)
		case idxUpperMicrosecond:
			used, err := fromCharParseIntLen(&(out.us), in[j:], 6, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			if used == 1 {
				out.us *= 100000
			} else if used == 2 {
				out.us *= 10000
			} else if used == 3 {
				out.us *= 1000
			} else if used == 4 {
				out.us *= 100
			} else if used == 5 {
				out.us *= 10
			}
			skipTHSuffix(&j, n.suffix)
		case idxUpperSecondPastMidnight:
			used, err := fromCharParseIntLen(&(out.ssss), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxLowerTimeZone, idxUpperTimeZone, idxUpperOF:
			return nil, fmt.Errorf("TZ,tz,OF format patterns are not supported in to_date")
		case idxUpperAnnoDotDomin, idxUpperBeforeDotChrist, idxLowerAnnoDotDomin, idxLowerBeforeDotChrist:
			val, length := seqSearch(in[j:], adbcStringsLong, allUpper, n.key.len)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.bc = val % 2
			j += length
		case idxUpperAnnoDomin, idxUpperBeforeChrist, idxLowerAnnoDomin, idxLowerBeforeChrist:
			val, length := seqSearch(in[j:], adbcStrings, allUpper, n.key.len)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.bc = val % 2
			j += length
		case idxUpperMonthName, idxMixedCaseMonthName, idxLowerMonthName:
			val, length := seqSearch(in[j:], monthFull, oneUpper, maxMonthLen)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.mm = val + 1
			j += length
		case idxAbbreviatedUpperMonthName, idxAbbreviatedMixedCaseMonthName, idxAbbreviatedLowerMonthName:
			val, length := seqSearch(in[j:], monthShort, oneUpper, maxMonLen)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.mm = val + 1
			j += length
		case idxMonthNumber:
			used, err := fromCharParseIntLen(&(out.mm), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperDay, idxMixedCaseDay, idxLowerDay:
			val, length := seqSearch(in[j:], dayFull, oneUpper, maxDayLen)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.d = val
			out.d++
			j += length
		case idxUpperAbbreviatedDayName, idxMixedCaseAbbreviatedDayName, idxLowerAbbreviatedDayName:
			val, length := seqSearch(in[j:], dayShort, oneUpper, maxDyLen)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.d = val
			out.d++
			j += length
		case idxUpperDayOfYear:
			used, err := fromCharParseIntLen(&(out.ddd), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperISODayOfYear:
			used, err := fromCharParseIntLen(&(out.ddd), in[j:], 3, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperDayOfMonth:
			used, err := fromCharParseIntLen(&(out.dd), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxDayOfWeek:
			used, err := fromCharParseIntLen(&(out.d), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperISODayOfWeek:
			used, err := fromCharParseIntLen(&(out.d), in[j:], 1, node[i:])
			if err != nil {
				return nil, err
			}
			if out.d++; out.d > 7 {
				out.d = 1
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperWeekOfYear, idxISOWeekOfYear:
			used, err := fromCharParseIntLen(&(out.ww), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxQuarter:
			used, err := fromCharParseIntLen(nil, in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperCentury:
			used, err := fromCharParseIntLen(&(out.cc), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperYearWithComma:
			var millennia, years int
			matched, err := fmt.Sscanf(in[j:], "%d,%03d", &millennia, &years)
			if err != nil || matched != 2 {
				return nil, fmt.Errorf("invalid input string for \"Y,YYY\"")
			}
			years += (millennia * 1000)
			out.year = years
			out.yysz = 4
			j += strDigitLen(in[j:]) + 4
			skipTHSuffix(&j, n.suffix)
		case idxUpper4DigitsOfYear, idxISOYear:
			used, err := fromCharParseIntLen(&(out.year), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			out.yysz = 4
			skipTHSuffix(&j, n.suffix)
		case idxUpper3DigitsOfYear, idx3DigitsOfISOYear:
			used, err := fromCharParseIntLen(&(out.year), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			if used < 4 {
				out.year = adjustPartialYearTo2020(out.year)
			}
			out.yysz = 3
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpper2DigitsOfYear, idx2DigitsOfISOYear:
			used, err := fromCharParseIntLen(&(out.year), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			if used < 4 {
				out.year = adjustPartialYearTo2020(out.year)
			}
			out.yysz = 2
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperLastDigitsOfYear, idxDigitsOfISOYear:
			used, err := fromCharParseIntLen(&(out.year), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			if used < 4 {
				out.year = adjustPartialYearTo2020(out.year)
			}
			out.yysz = 1
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxUpperMonthInRomanNumerals:
			val, length := seqSearch(in[j:], romanNumeralsUpper, allUpper, maxRmLen)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.mm = monthsPerYear - val
			j += length
		case idxLowerMonthInRomanNumerals:
			val, length := seqSearch(in[j:], romanNumeralsLower, allLower, maxRmLen)
			if val < 0 {
				return nil, fmt.Errorf("invalid value \"%s\" for \"%s\"", in[j:], n.key.name)
			}
			out.mm = monthsPerYear - val
			j += length
		case idxUpperWeekOfMonth:
			used, err := fromCharParseIntLen(&(out.w), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		case idxJulianDay:
			used, err := fromCharParseIntLen(&(out.j), in[j:], n.key.len, node[i:])
			if err != nil {
				return nil, err
			}
			j += used
			skipTHSuffix(&j, n.suffix)
		}

	}
	return out, nil
}

func fromCharSetMode(tmfc *tmFromChar, mode int) (int, error) {
	if mode != fromCharDateNone {
		if tmfc.mode == fromCharDateNone {
			tmfc.mode = mode
		} else if tmfc.mode != mode {
			return -1, fmt.Errorf("invalid combination of date conversions")
		}
	}
	return 0, nil
}

func suffSearch(str string, suf []*keySuffix, sType int) *keySuffix {
	for _, s := range suf {
		if s.sType != sType {
			continue
		}

		if strings.HasPrefix(str, s.name) {
			return s
		}
	}
	return nil
}

/* ----------
 * Fast sequential search, use index for data selection which
 * go to seq. cycle (it is very fast for unwanted strings)
 * (can't be used binary search in format parsing)
 * ----------
 */
func indexSeqSearch(str []byte, kw []*keyWord, index []int) *keyWord {

	if str[0] < ' ' || str[0] > '~' {
		return nil
	}
	if poz := index[str[0]-' ']; poz > -1 {

		for _, k := range kw[poz:] {
			if strings.HasPrefix(string(str), k.name) {
				return k
			}

			if k.name == "" {
				return nil
			}
		}
	}
	return nil
}

func seqSearch(name string, array []string, typ, max int) (int, int) {

	if len(name) == 0 {
		return -1, 0
	}

	if typ == allUpper {
		name = strings.ToUpper(name)
	} else if typ == oneUpper {
		name = strings.ToUpper(name[0:1]) + strings.ToLower(name[1:])
	} else if typ == allLower {
		name = strings.ToLower(name)
	}

	for i, a := range array {
		if ((max != 0 && len(a) <= max) || max == 0) && strings.HasPrefix(name, a) {
			return i, len(a)
		}
		if (max != 0 && len(a) > max) && strings.HasPrefix(name, a[0:max]) {
			return i, max
		}
	}
	return -1, 0
}

func fromCharParseIntLen(dst *int, src string, length int, node []*formatNode) (int, error) {
	spaceLen := strSpaceLen(src)
	strCpy := src[spaceLen:]
	used := spaceLen

	if node[0].suffix&suffFillMode != 0 || isNextSeparator(node) {
		l := strDigitLen(strCpy)
		if l == 0 {
			return 0, fmt.Errorf("no digits in source string")
		}
		used += l
		res, err := strconv.ParseInt(strCpy[0:l], 10, 32)
		if err != nil {
			return 0, err
		}
		*dst = int(res)
	} else {
		if len(strCpy) < length {
			return 0, fmt.Errorf("source string too short for \"%s\" formatting field", node[0].key.name)
		}

		res, err := strconv.ParseInt(src[0:length], 10, 32)
		if err != nil {
			return 0, err
		}
		*dst = int(res)
		used += length
	}

	return used, nil
}

/* ----------
 * Return TRUE if next format picture is not digit value
 * ----------
 */
func isNextSeparator(node []*formatNode) bool {
	n := node[0]

	if n.nType == nodeTypeEnd {
		return false
	}

	if n.nType == nodeTypeAction && (n.suffix&suffUpperOrdinalNumber != 0 || n.suffix&suffLowerOrdinalNumber != 0) {
		return true
	}

	n = node[1]
	if n.nType == nodeTypeEnd {
		return true
	}

	if n.nType == nodeTypeAction {
		return !n.key.isDigit
	} else if isDigit(int(n.character)) {
		return false
	}

	return true
}

func isSpace(ch int) bool {
	if ch == '\t' || ch == '\n' || ch == '\v' || ch == '\f' || ch == '\r' || ch == ' ' {
		return true
	}
	return false
}

func strSpaceLen(str string) int {
	length := 0
	for i, ch := range str {
		if !isSpace(int(ch)) {
			break
		}
		length = i + 1
	}

	return length
}

func strDigitLen(str string) int {
	length := 0
	for i, ch := range str {
		if !isDigit(int(ch)) {
			break
		}
		length = i + 1
	}
	return length
}

func j2date(jd int, year *int, month *int, day *int) {
	var julian uint
	var quad uint
	var extra uint
	var y int

	julian = uint(jd)
	julian += 32044
	quad = julian / 146097
	extra = (julian-quad*146097)*4 + 3
	julian += 60 + quad*3 + extra/146097
	quad = julian / 1461
	julian -= quad * 1461
	y = int(julian * 4 / 1461)
	if y != 0 {
		julian = (julian + 305) % 365
	} else {
		julian = ((julian + 306) % 366) + 123
	}
	y += int(quad * 4)
	*year = y - 4800
	quad = julian * 2141 / 65536
	*day = int(julian - 7834*quad/256)
	*month = int((quad+10)%monthsPerYear + 1)
}

func isLeap(year int) int {
	if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		return 1
	}

	return 0
}

func skipTHSuffix(idx *int, suffix int) {
	if suffix&suffUpperOrdinalNumber != 0 || suffix&suffLowerOrdinalNumber != 0 {
		*idx += 2
	}
}

func adjustPartialYearTo2020(year int) int {
	/*
	 * Adjust all dates toward 2020; this is effectively what happens when we
	 * assume '70' is 1970 and '69' is 2069.
	 */
	/* Force 0-69 into the 2000's */
	if year < 70 {
		return year + 2000
	} else if year < 100 {
		/* Force 70-99 into the 1900's */
		return year + 1900
	} else if year < 520 {
		/* Force 100-519 into the 2000's */
		return year + 2000
	} else if year < 1000 {
		/* Force 520-999 into the 1000's */
		return year + 1000
	}
	return year
}

/* isoWeek2Date()
 * Convert ISO week of year number to date.
 * The year field must be specified with the ISO year!
 * karel 2000/08/07
 */
func isoWeek2Date(woy int, year, mon, mday *int) {
	j2date(isoWeek2J(*year, woy), year, mon, mday)
}

/* isoWeekDate2Date()
 *
 *	Convert an ISO 8601 week date (ISO year, ISO week) into a Gregorian date.
 *	Gregorian day of week sent so weekday strings can be supplied.
 *	Populates year, mon, and mday with the correct Gregorian values.
 *	year must be passed in as the ISO year.
 */
func isoWeekDate2Date(isoweek, wday int, year, mon, mday *int) {
	jday := isoWeek2J(*year, isoweek)

	if wday > 1 {
		jday += (wday - 2)
	} else {
		jday += 6
	}

	j2date(jday, year, mon, mday)
}

func isoWeek2J(year, week int) int {
	/* fourth day of current year */
	day4 := date2j(year, 1, 4)

	/* day0 == offset to first day of week (Monday) */
	day0 := j2day(day4 - 1)
	return (week-1)*7 + (day4 - day0)
}

/*
 * Calendar time to Julian date conversions.
 * Julian date is commonly used in astronomical applications,
 *	since it is numerically accurate and computationally simple.
 * The algorithms here will accurately convert between Julian day
 *	and calendar date for all non-negative Julian days
 *	(i.e. from Nov 24, -4713 on).
 *
 * These routines will be used by other date/time packages
 * - thomas 97/02/25
 *
 * Rewritten to eliminate overflow problems. This now allows the
 * routines to work correctly for all Julian day counts from
 * 0 to 2147483647	(Nov 24, -4713 to Jun 3, 5874898) assuming
 * a 32-bit integer. Longer types should also work to the limits
 * of their precision.
 */
func date2j(y, m, d int) int {
	var julian, century int

	if m > 2 {
		m++
		y += 4800
	} else {
		m += 13
		y += 4799
	}

	century = y / 100
	julian = y*365 - 32167
	julian += y/4 - century + century/4
	julian += 7834*m/256 + d

	return julian
}

/*
 * j2day - convert Julian date to day-of-week (0..6 == Sun..Sat)
 *
 * Note: various places use the locution j2day(date - 1) to produce a
 * result according to the convention 0..6 = Mon..Sun.  This is a bit of
 * a crock, but will work as long as the computation here is just a modulo.
 */
func j2day(date int) int {
	var day uint

	day = uint(date)
	day++
	day %= 7

	return int(day)
}
