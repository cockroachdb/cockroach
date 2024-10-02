// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
)

// TimeToChar converts a time and a `to_char` format string to a string.
func TimeToChar(t time.Time, c *FormatCache, f string) (string, error) {
	return timeToChar(timeWrapper{t}, c.lookup(f))
}

// DurationToChar converts a duration and a `to_char` format string to a string.
func DurationToChar(d duration.Duration, c *FormatCache, f string) (string, error) {
	return timeToChar(makeToCharDuration(d), c.lookup(f))
}

// timeInterface is intended as a pass through to timeToChar.
type timeInterface interface {
	Year() int
	Month() time.Month
	Day() int
	Minute() int
	Hour() int
	Second() int
	Nanosecond() int
	YearDay() int
	Weekday() time.Weekday
	ISOWeek() (int, int)
	Zone() (string, int)
	isInterval() bool
}

// timeWrapper wraps time.Time to implement timeInterface.
type timeWrapper struct {
	time.Time
}

func (timeWrapper) isInterval() bool {
	return false
}

// toCharDuration maps as a duration to usable for timeInterface.
// We use the same logic as PG to convert intervals to a fake time here.
type toCharDuration struct {
	year, day                   int
	month                       time.Month
	hour, minute, second, nanos int
	yday                        int
}

// makeToCharDuration converts a duration.Duration to a toCharDuration.
func makeToCharDuration(d duration.Duration) toCharDuration {
	n := time.Duration(d.Nanos())
	return toCharDuration{
		year:   int(d.Months / 12),
		month:  time.Month(d.Months % 12),
		day:    int(d.Days),
		nanos:  int(n % time.Second),
		second: int(n/time.Second) % 60,
		minute: int(n/time.Minute) % 60,
		hour:   int(n / time.Hour),
		// Approximation that PostgreSQL uses.
		yday: int(d.Months*30 + d.Days),
	}
}

func (d toCharDuration) Year() int {
	return d.year
}

func (d toCharDuration) Month() time.Month {
	return d.month
}

func (d toCharDuration) Day() int {
	return d.day
}

func (d toCharDuration) Minute() int {
	return d.minute
}

func (d toCharDuration) Hour() int {
	return d.hour
}

func (d toCharDuration) Second() int {
	return d.second
}

func (d toCharDuration) Nanosecond() int {
	return d.nanos
}

func (d toCharDuration) YearDay() int {
	return d.yday
}

func (d toCharDuration) Weekday() time.Weekday {
	return time.Monday
}

func (d toCharDuration) ISOWeek() (int, int) {
	return 0, 0
}

func (d toCharDuration) Zone() (string, int) {
	return "", 0
}

func (d toCharDuration) isInterval() bool {
	return true
}

func makeIntervalUnsupportedError(s string) error {
	return errors.WithDetailf(
		errors.WithHint(
			pgerror.New(
				pgcode.InvalidDatetimeFormat,
				"invalid format specification for an interval value",
			),
			"intervals are not tied to specific calendar dates",
		),
		"format %s",
		s,
	)
}

// timeToChar maps to DCHToChar.
func timeToChar(t timeInterface, formatNodes []formatNode) (string, error) {
	var sb strings.Builder
	for _, fn := range formatNodes {
		if fn.typ != formatNodeAction {
			sb.WriteString(fn.character)
			continue
		}

		// NOTE(otan): postgres does localization logic if TM is specified.
		// This is not supported on a number of these fields.
		switch fn.key.id {
		case DCH_A_M, DCH_P_M:
			if t.Hour() >= 12 {
				sb.WriteString("P.M.")
			} else {
				sb.WriteString("A.M.")
			}
		case DCH_AM, DCH_PM:
			if t.Hour() >= 12 {
				sb.WriteString("PM")
			} else {
				sb.WriteString("AM")
			}
		case DCH_a_m, DCH_p_m:
			if t.Hour() >= 12 {
				sb.WriteString("p.m.")
			} else {
				sb.WriteString("a.m.")
			}
		case DCH_am, DCH_pm:
			if t.Hour() >= 12 {
				sb.WriteString("pm")
			} else {
				sb.WriteString("am")
			}
		case DCH_HH, DCH_HH12:
			hour := t.Hour()
			if hour%12 == 0 {
				hour = 12
			} else {
				hour = hour % 12
			}
			// Note t.Hour() and hour are deliberately different.
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad2PlusNeg(t.Hour()), hour))
			sb.WriteString(fn.suffix.thVal(hour))
		case DCH_HH24:
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad2PlusNeg(t.Hour()), t.Hour()))
			sb.WriteString(fn.suffix.thVal(t.Hour()))
		case DCH_MI:
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad2PlusNeg(t.Minute()), t.Minute()))
			sb.WriteString(fn.suffix.thVal(t.Minute()))
		case DCH_SS:
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad2PlusNeg(t.Second()), t.Second()))
			sb.WriteString(fn.suffix.thVal(t.Second()))
		case DCH_FF1:
			val := t.Nanosecond() / 100000000
			sb.WriteString(fmt.Sprintf("%01d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_FF2:
			val := t.Nanosecond() / 10000000
			sb.WriteString(fmt.Sprintf("%02d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_FF3, DCH_MS:
			val := t.Nanosecond() / 1000000
			sb.WriteString(fmt.Sprintf("%03d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_FF4:
			val := t.Nanosecond() / 100000
			sb.WriteString(fmt.Sprintf("%04d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_FF5:
			val := t.Nanosecond() / 10000
			sb.WriteString(fmt.Sprintf("%05d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_FF6, DCH_US:
			val := t.Nanosecond() / 1000
			sb.WriteString(fmt.Sprintf("%06d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_SSSS:
			val := t.Hour()*60*60 + t.Minute()*60 + t.Second()
			sb.WriteString(fmt.Sprintf("%d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_tz:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			n, _ := t.Zone()
			sb.WriteString(strings.ToLower(n))
		case DCH_TZ:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			n, _ := t.Zone()
			sb.WriteString(n)
		case DCH_TZH:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			_, offset := t.Zone()
			posStr := "+"
			if offset < 0 {
				posStr = "-"
				offset = -offset
			}
			sb.WriteString(posStr)
			sb.WriteString(fmt.Sprintf("%02d", offset/3600))
		case DCH_TZM:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			_, offset := t.Zone()
			if offset < 0 {
				offset = -offset
			}
			sb.WriteString(fmt.Sprintf("%02d", (offset%3600)/60))
		case DCH_OF:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			_, offset := t.Zone()
			posStr := "+"
			if offset < 0 {
				posStr = "-"
				offset = -offset
			}
			sb.WriteString(posStr)
			var zeroPad int
			if !fn.suffix.FM() {
				zeroPad = 2
			}
			sb.WriteString(fmt.Sprintf("%0*d", zeroPad, offset/3600))
			minOffset := (offset % 3600) / 60
			if minOffset != 0 {
				sb.WriteString(fmt.Sprintf(":%02d", minOffset))
			}
		case DCH_A_D, DCH_B_C:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			if t.Year() <= 0 {
				sb.WriteString("B.C.")
			} else {
				sb.WriteString("A.D.")
			}
		case DCH_AD, DCH_BC:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			if t.Year() <= 0 {
				sb.WriteString("BC")
			} else {
				sb.WriteString("AD")
			}
		case DCH_a_d, DCH_b_c:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			if t.Year() <= 0 {
				sb.WriteString("b.c.")
			} else {
				sb.WriteString("a.d.")
			}
		case DCH_ad, DCH_bc:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			if t.Year() <= 0 {
				sb.WriteString("bc")
			} else {
				sb.WriteString("ad")
			}
		case DCH_MONTH, DCH_Month, DCH_month:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			m := t.Month().String()
			switch fn.key.id {
			case DCH_MONTH:
				m = strings.ToUpper(m)
			case DCH_month:
				m = strings.ToLower(m)
			}
			sb.WriteString(fmt.Sprintf("%*s", fn.suffix.zeroPad(-9), m))
		case DCH_MON, DCH_Mon, DCH_mon:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			m := t.Month().String()[:3]
			switch fn.key.id {
			case DCH_MON:
				m = strings.ToUpper(m)
			case DCH_mon:
				m = strings.ToLower(m)
			}
			sb.WriteString(m)
		case DCH_MM:
			val := int(t.Month())
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad2PlusNeg(val), val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_DAY, DCH_Day, DCH_day:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			d := t.Weekday().String()
			switch fn.key.id {
			case DCH_DAY:
				d = strings.ToLower(d)
			case DCH_day:
				d = strings.ToLower(d)
			}
			sb.WriteString(fmt.Sprintf("%*s", fn.suffix.zeroPad(-9), d))
		case DCH_DY, DCH_Dy, DCH_dy:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			d := t.Weekday().String()[:3]
			switch fn.key.id {
			case DCH_DY:
				d = strings.ToLower(d)
			case DCH_dy:
				d = strings.ToLower(d)
			}
			sb.WriteString(d)
		case DCH_DDD:
			val := t.YearDay()
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad(3), val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_DD:
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad(2), t.Day()))
			sb.WriteString(fn.suffix.thVal(t.Day()))
		case DCH_IDDD:
			// Added by us, as ISOWeek() does not work.
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			_, w := t.ISOWeek()
			val := (w-1)*7 + int(t.Weekday())
			if t.Weekday() == 0 {
				val += 7
			}
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad(3), val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_D:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			val := int(t.Weekday()) + 1
			sb.WriteString(fmt.Sprintf("%d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_ID:
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			val := int(t.Weekday())
			if val == 0 {
				val = 7
			}
			sb.WriteString(fmt.Sprintf("%d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_WW:
			val := (t.YearDay()-1)/7 + 1
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad(2), val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_IW:
			// Added by us, as ISOWeek() does not work.
			if t.isInterval() {
				return "", makeIntervalUnsupportedError(fn.key.name)
			}
			_, val := t.ISOWeek()
			sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad(2), val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_Q:
			// For intervals.
			if int(t.Month()) == 0 {
				break
			}
			val := (int(t.Month())-1)/3 + 1
			sb.WriteString(fmt.Sprintf("%d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_CC:
			var val int
			if t.isInterval() {
				val = t.Year() / 100
			} else {
				if t.Year() > 0 {
					val = (t.Year()-1)/100 + 1
				} else {
					val = t.Year()/100 - 1
				}
			}
			if val <= 99 || val >= 99 {
				sb.WriteString(fmt.Sprintf("%0*d", fn.suffix.zeroPad2PlusNeg(val), val))
			} else {
				sb.WriteString(fmt.Sprintf("%d", val))
			}
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_Y_YYY:
			year := t.Year()
			if !t.isInterval() && year < 0 {
				year = -year + 1
			}
			preComma := year / 1000
			postComma := year % 1000
			sb.WriteString(fmt.Sprintf("%d,%03d", preComma, postComma))
			sb.WriteString(fn.suffix.thVal(postComma))
		case DCH_YYYY, DCH_IYYY, DCH_YYY, DCH_IYY, DCH_YY, DCH_IY, DCH_Y, DCH_I:
			val := t.Year()
			switch fn.key.id {
			case DCH_IYYY, DCH_IYY, DCH_IY, DCH_I:
				// Added by us, as ISOWeek() does not work.
				if t.isInterval() {
					return "", makeIntervalUnsupportedError(fn.key.name)
				}
				val, _ = t.ISOWeek()
			}
			if !t.isInterval() && val < 0 {
				val = -val + 1
			}
			zeroPad := 0
			if !fn.suffix.FM() {
				switch fn.key.id {
				case DCH_IYYY, DCH_YYYY:
					zeroPad = 4
				case DCH_IYY, DCH_YYY:
					zeroPad = 3
				case DCH_IY, DCH_YY:
					zeroPad = 2
				case DCH_I, DCH_Y:
					zeroPad = 1
				}
			}
			switch fn.key.id {
			case DCH_IYY, DCH_YYY:
				val %= 1000
			case DCH_IY, DCH_YY:
				val %= 100
			case DCH_I, DCH_Y:
				val %= 10
			}
			// For intervals, negative values get an extra digit.
			if t.isInterval() && val < 0 {
				zeroPad++
			}
			sb.WriteString(fmt.Sprintf("%0*d", zeroPad, val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_RM, DCH_rm:
			if t.Month() == 0 && t.Year() == 0 {
				continue
			}
			// Roman numerals are in reverse order, which is why the indexes
			// are reversed.
			var idx int
			if t.Month() == 0 {
				if t.Year() >= 0 {
					idx = 0
				} else {
					idx = 11
				}
			} else if t.Month() < 0 {
				idx = -1 * int(t.Month())
			} else {
				idx = 12 - int(t.Month())
			}
			var str string
			if fn.key.id == DCH_RM {
				str = ucMonthRomanNumerals[idx]
			} else {
				str = lcMonthRomanNumerals[idx]
			}
			sb.WriteString(fmt.Sprintf("%*s", fn.suffix.zeroPad(-4), str))
		case DCH_W:
			val := (t.Day()-1)/7 + 1
			sb.WriteString(fmt.Sprintf("%d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_J:
			val := pgdate.DateToJulianDay(t.Year(), int(t.Month()), t.Day())
			sb.WriteString(fmt.Sprintf("%d", val))
			sb.WriteString(fn.suffix.thVal(val))
		case DCH_FX, DCH_fx:
			// Does nothing.
		default:
			return "", errors.AssertionFailedf("unexpected key for to_char: %s", errors.Safe(fn.key.name))
		}
	}
	return sb.String(), nil
}

func (d dchSuffix) thVal(val int) string {
	if d.THth() {
		return getTH(val, d.TH())
	}
	return ""
}

// getTH returns the suffix for the given value as a position, e.g.
// 1 => st for 1st, 4 => th for 4th.
func getTH(v int, upper bool) string {
	if v < 0 {
		v = -v
	}
	v = v % 100
	//  11, 12 and 13 are still -th, so don't go in here.
	if v < 11 || v > 13 {
		switch v % 10 {
		case 1:
			if upper {
				return "ST"
			}
			return "st"
		case 2:
			if upper {
				return "ND"
			}
			return "nd"
		case 3:
			if upper {
				return "RD"
			}
			return "rd"
		}
	}
	if upper {
		return "TH"
	}
	return "th"
}

func (d dchSuffix) zeroPad(nonFMVal int) int {
	var zeroPad int
	if !d.FM() {
		zeroPad = nonFMVal
	}
	return zeroPad
}

// zeroPad2PlusNeg pads 2 if FM is not set, accounting for the negative digit
// where necessary.
func (d dchSuffix) zeroPad2PlusNeg(val int) int {
	var zeroPad int
	if !d.FM() {
		if val >= 0 {
			zeroPad = 2
		} else {
			zeroPad = 3
		}
	}
	return zeroPad
}

// parseFormat matches parse_format. We do not take in a flags like PG  as we only do
// datetime related items (the logic here will change if we support numeric types).
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L1146.
func parseFormat(f string) []formatNode {
	var ret []formatNode
	for len(f) > 0 {
		var suffix dchSuffix
		if s := suffSearch(f, keySuffixPrefix); s != nil {
			suffix |= s.id
			f = f[len(s.name):]
		}

		if len(f) > 0 {
			kw := indexSeqSearch(f)
			if kw != nil {
				ret = append(ret, formatNode{
					typ:    formatNodeAction,
					suffix: suffix,
					key:    kw,
				})
				f = f[len(kw.name):]
				if len(f) > 0 {
					if s := suffSearch(f, keySuffixPostfix); s != nil {
						ret[len(ret)-1].suffix |= s.id
						f = f[len(s.name):]
					}
				}
			} else {
				if f[0] == '"' {
					// Process any quoted text.
					f = f[1:]
					for len(f) > 0 {
						if f[0] == '"' {
							f = f[1:]
							break
						}
						// Escape the next character.
						if f[0] == '\\' {
							f = f[1:]
						}
						r, size := utf8.DecodeRuneInString(f)
						ret = append(ret, formatNode{
							typ:       formatNodeChar,
							character: string(r),
						})
						f = f[size:]
					}
				} else {
					// Outside double-quoted strings, backslash is only special if it
					// immediately precedes a double quote.
					if f[0] == '\\' && len(f) > 1 && f[1] == '"' {
						f = f[1:]
					}

					r, size := utf8.DecodeRuneInString(f)
					typ := formatNodeChar
					if isSeparatorChar(r) {
						typ = formatNodeSeparator
					} else if unicode.IsSpace(r) {
						typ = formatNodeSpace
					}
					ret = append(ret, formatNode{
						typ:       typ,
						character: string(r),
					})
					f = f[size:]
				}
			}
		}
	}
	ret = append(ret, formatNode{typ: formatNodeEnd})
	return ret
}

func isSeparatorChar(c rune) bool {
	/* ASCII printable character, but not letter or digit */
	return c > 0x20 && c < 0x7F &&
		!(c >= 'A' && c <= 'Z') &&
		!(c >= 'a' && c <= 'z') &&
		!(c >= '0' && c <= '9')
}

// indexSeqSearch matches index_seq_search.
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L194-L198.
func indexSeqSearch(s string) *keyWord {
	if len(s) == 0 || s[0] <= ' ' || s[0] >= '~' {
		return nil
	}
	idxOffset := int(s[0] - ' ')
	if idxOffset > -1 && idxOffset < len(dchIndex) {
		pos := int(dchIndex[idxOffset])
		for pos >= 0 && pos < len(dchKeywords) {
			k := dchKeywords[pos]
			if strings.HasPrefix(s, k.name) {
				return &dchKeywords[pos]
			}
			pos++
			// Since we have dchKeywords sorted in alphabetical order, so if we don't
			// match the first character, break.
			if pos >= len(dchKeywords) || dchKeywords[pos].name[0] != s[0] {
				break
			}
		}
	}
	return nil
}

// suffSearch matches suff_search.
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L1146.
func suffSearch(s string, typ keySuffixType) *keySuffix {
	for _, suff := range dchSuffixes {
		if suff.typ != typ {
			continue
		}
		if strings.HasPrefix(s, suff.name) {
			return &suff
		}
	}
	return nil
}
