// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
)

// tmFromChar accumulates parsed datetime components from a format string.
// Mirrors PostgreSQL's TmFromChar (formatting.c lines 399-425).
type tmFromChar struct {
	mode fromCharDateMode

	hh, pm, mi, ss, ssss int
	d, dd, ddd           int
	mm, ms, us           int
	year, bc, cc, j      int
	ww, w                int
	// yysz is the number of digits parsed for the year.
	yysz int
	// clock is 12 or 24 depending on which format was used.
	clock int

	tzsign, tzh, tzm int
	ff               int
}

// Sentinel value indicating a field has not been set.
const fromCharFieldUnset = -1

func newTmFromChar() tmFromChar {
	return tmFromChar{
		hh: fromCharFieldUnset, pm: fromCharFieldUnset,
		mi: fromCharFieldUnset, ss: fromCharFieldUnset, ssss: fromCharFieldUnset,
		d: fromCharFieldUnset, dd: fromCharFieldUnset, ddd: fromCharFieldUnset,
		mm: fromCharFieldUnset, ms: fromCharFieldUnset, us: fromCharFieldUnset,
		year: fromCharFieldUnset, bc: fromCharFieldUnset, cc: fromCharFieldUnset,
		j: fromCharFieldUnset, ww: fromCharFieldUnset, w: fromCharFieldUnset,
		yysz:   0,
		clock:  fromCharFieldUnset,
		tzsign: 1, tzh: fromCharFieldUnset, tzm: fromCharFieldUnset,
		ff: fromCharFieldUnset,
	}
}

// String lookup arrays for case-insensitive matching during parsing.
var (
	monthsFull = []string{
		"January", "February", "March", "April", "May", "June",
		"July", "August", "September", "October", "November", "December",
	}
	monthsAbbrev = []string{
		"Jan", "Feb", "Mar", "Apr", "May", "Jun",
		"Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
	}
	daysFull = []string{
		"Sunday", "Monday", "Tuesday", "Wednesday",
		"Thursday", "Friday", "Saturday",
	}
	daysAbbrev = []string{
		"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat",
	}
	ampmStrings = []string{"AM", "PM", "A.M.", "P.M."}
	adbcStrings = []string{"AD", "BC", "A.D.", "B.C."}
)

// fromCharSetMode validates that Gregorian and ISO week date modes are not
// mixed in a single format string.
func fromCharSetMode(tmfc *tmFromChar, mode fromCharDateMode) error {
	if mode != fromCharDateNone {
		if tmfc.mode == fromCharDateNone {
			tmfc.mode = mode
		} else if tmfc.mode != mode {
			return pgerror.New(
				pgcode.InvalidDatetimeFormat,
				"invalid combination of date conventions",
			)
		}
	}
	return nil
}

// fromCharSetInt stores a value with conflict detection.
func fromCharSetInt(dest *int, value int, node *keyWord) error {
	if *dest != fromCharFieldUnset {
		return pgerror.Newf(
			pgcode.InvalidDatetimeFormat,
			"conflicting values for \"%s\" field in formatting string",
			node.name,
		)
	}
	*dest = value
	return nil
}

// adjustPartialYearTo2020 adjusts a partial year value to a full year, using
// the year 2020 as the pivot. This matches PostgreSQL's
// adjust_partial_year_to_2020 function (formatting.c).
func adjustPartialYearTo2020(year int) int {
	if year < 70 {
		return year + 2000
	} else if year < 100 {
		return year + 1900
	} else if year < 520 {
		return year + 2000
	} else if year < 1000 {
		return year + 1000
	}
	return year
}

// fromCharParseIntLen parses a fixed-width integer from the input string.
// In FM mode, it reads as many digits as available (up to maxLen).
// Returns the parsed value, number of digits consumed, and any error.
func fromCharParseIntLen(s string, pos int, maxLen int, fm bool) (int, int, error) {
	if pos >= len(s) {
		return 0, 0, pgerror.New(
			pgcode.InvalidDatetimeFormat,
			"source string too short for format string",
		)
	}

	// Determine how many digits to read.
	end := pos
	for end < len(s) && end-pos < maxLen && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end == pos {
		return 0, 0, pgerror.Newf(
			pgcode.InvalidDatetimeFormat,
			"invalid value \"%s\" for \"%d\"",
			s[pos:min(pos+maxLen, len(s))],
			maxLen,
		)
	}

	// In non-FM mode with fixed width, we require exactly maxLen digits.
	if !fm && end-pos < maxLen {
		// PostgreSQL is lenient here in non-FX mode; we'll accept what we got.
	}

	val := 0
	for i := pos; i < end; i++ {
		val = val*10 + int(s[i]-'0')
	}
	return val, end - pos, nil
}

// seqSearchASCII performs a case-insensitive search for a prefix match in the
// given string array. Returns the 0-based index and the length consumed, or
// -1 if no match.
func seqSearchASCII(s string, pos int, arr []string) (int, int) {
	for i, name := range arr {
		if len(s)-pos >= len(name) && strings.EqualFold(s[pos:pos+len(name)], name) {
			return i, len(name)
		}
	}
	return -1, 0
}

// skipTHth skips ordinal suffix characters (TH, th, ST, st, ND, nd, RD, rd)
// in input if the format node has a TH/th suffix.
func skipTHth(s string, pos int, suffix dchSuffix) int {
	if !suffix.THth() {
		return pos
	}
	if pos+2 <= len(s) {
		return pos + 2
	}
	return pos
}

// CharToTimestamp parses a timestamp from a formatted string using the given
// format pattern. This is the main entry point for to_timestamp(text, text).
func CharToTimestamp(
	s string, c *FormatCache, format string, loc *time.Location,
) (time.Time, error) {
	formatNodes := c.lookupDCH(format)
	tmfc := newTmFromChar()
	if err := dchFromChar(&tmfc, s, formatNodes); err != nil {
		return time.Time{}, err
	}
	return tmFromCharToTime(&tmfc, loc)
}

// CharToDate parses a date from a formatted string using the given format
// pattern. Returns year, month, day. This is the main entry point for
// to_date(text, text).
func CharToDate(s string, c *FormatCache, format string) (int, time.Month, int, error) {
	formatNodes := c.lookupDCH(format)
	tmfc := newTmFromChar()
	if err := dchFromChar(&tmfc, s, formatNodes); err != nil {
		return 0, 0, 0, err
	}
	t, err := tmFromCharToTime(&tmfc, time.UTC)
	if err != nil {
		return 0, 0, 0, err
	}
	return t.Year(), t.Month(), t.Day(), nil
}

// dchFromChar is the main from_char parser. It walks the format nodes and
// input string in parallel, extracting datetime component values into tmfc.
func dchFromChar(tmfc *tmFromChar, s string, formatNodes []formatNode) error {
	pos := 0
	fx := false // FX (fixed-width) mode
	for _, fn := range formatNodes {
		if fn.typ == formatNodeEnd {
			break
		}

		// In non-FX mode, skip leading whitespace in input before each node.
		if !fx && fn.typ != formatNodeAction {
			for pos < len(s) && unicode.IsSpace(rune(s[pos])) {
				pos++
			}
		}

		switch fn.typ {
		case formatNodeChar, formatNodeSeparator:
			// Match literal or separator character.
			if pos < len(s) {
				if fx {
					// In FX mode, character must match exactly.
					r, size := decodeRune(s, pos)
					expectR, _ := decodeRune(fn.character, 0)
					if r != expectR {
						return pgerror.Newf(
							pgcode.InvalidDatetimeFormat,
							"invalid value for \"%s\"",
							fn.character,
						)
					}
					pos += size
				} else {
					// In non-FX mode, any separator in the input matches any
					// separator in the format. Skip whitespace too.
					if isSeparatorChar(rune(fn.character[0])) {
						// Skip any separators and whitespace in input.
						for pos < len(s) && (isSeparatorChar(rune(s[pos])) || unicode.IsSpace(rune(s[pos]))) {
							pos++
						}
					} else {
						_, size := decodeRune(s, pos)
						pos += size
					}
				}
			}
		case formatNodeSpace:
			// Skip whitespace in input.
			if fx {
				if pos < len(s) && unicode.IsSpace(rune(s[pos])) {
					pos++
				}
			} else {
				for pos < len(s) && unicode.IsSpace(rune(s[pos])) {
					pos++
				}
			}
		case formatNodeAction:
			fm := fn.suffix.FM()

			// In non-FX mode, skip leading whitespace before keywords.
			if !fx {
				for pos < len(s) && unicode.IsSpace(rune(s[pos])) {
					pos++
				}
			}

			if err := fromCharSetMode(tmfc, fn.key.dateMode); err != nil {
				return err
			}

			switch fn.key.id {
			case DCH_FX, DCH_fx:
				fx = true

			case DCH_A_M, DCH_P_M, DCH_a_m, DCH_p_m:
				// A.M. / P.M. with dots.
				idx, n := seqSearchASCII(s, pos, ampmStrings)
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for AM/PM")
				}
				if err := fromCharSetInt(&tmfc.pm, idx%2, fn.key); err != nil {
					return err
				}
				tmfc.clock = 12
				pos += n

			case DCH_AM, DCH_PM, DCH_am, DCH_pm:
				idx, n := seqSearchASCII(s, pos, ampmStrings)
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for AM/PM")
				}
				if err := fromCharSetInt(&tmfc.pm, idx%2, fn.key); err != nil {
					return err
				}
				tmfc.clock = 12
				pos += n

			case DCH_HH, DCH_HH12:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.hh, val, fn.key); err != nil {
					return err
				}
				tmfc.clock = 12
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_HH24:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.hh, val, fn.key); err != nil {
					return err
				}
				tmfc.clock = 24
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_MI:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.mi, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_SS:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.ss, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_MS:
				val, n, err := fromCharParseIntLen(s, pos, 3, fm)
				if err != nil {
					return err
				}
				// Pad right to 3 digits.
				origN := n
				for n < 3 {
					val *= 10
					n++
				}
				if err := fromCharSetInt(&tmfc.ms, val, fn.key); err != nil {
					return err
				}
				pos += origN
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_US:
				val, n, err := fromCharParseIntLen(s, pos, 6, fm)
				if err != nil {
					return err
				}
				// Pad right to 6 digits.
				origN := n
				for n < 6 {
					val *= 10
					n++
				}
				if err := fromCharSetInt(&tmfc.us, val, fn.key); err != nil {
					return err
				}
				pos += origN
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_FF1, DCH_ff1:
				val, n, err := fromCharParseIntLen(s, pos, 1, fm)
				if err != nil {
					return err
				}
				val *= 100000 // Scale to microseconds.
				if err := fromCharSetInt(&tmfc.ff, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_FF2, DCH_ff2:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				origN := n
				for n < 2 {
					val *= 10
					n++
				}
				val *= 10000 // Scale to microseconds.
				if err := fromCharSetInt(&tmfc.ff, val, fn.key); err != nil {
					return err
				}
				pos += origN
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_FF3, DCH_ff3:
				val, n, err := fromCharParseIntLen(s, pos, 3, fm)
				if err != nil {
					return err
				}
				origN := n
				for n < 3 {
					val *= 10
					n++
				}
				val *= 1000 // Scale to microseconds.
				if err := fromCharSetInt(&tmfc.ff, val, fn.key); err != nil {
					return err
				}
				pos += origN
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_FF4, DCH_ff4:
				val, n, err := fromCharParseIntLen(s, pos, 4, fm)
				if err != nil {
					return err
				}
				origN := n
				for n < 4 {
					val *= 10
					n++
				}
				val *= 100 // Scale to microseconds.
				if err := fromCharSetInt(&tmfc.ff, val, fn.key); err != nil {
					return err
				}
				pos += origN
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_FF5, DCH_ff5:
				val, n, err := fromCharParseIntLen(s, pos, 5, fm)
				if err != nil {
					return err
				}
				origN := n
				for n < 5 {
					val *= 10
					n++
				}
				val *= 10 // Scale to microseconds.
				if err := fromCharSetInt(&tmfc.ff, val, fn.key); err != nil {
					return err
				}
				pos += origN
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_FF6, DCH_ff6:
				val, n, err := fromCharParseIntLen(s, pos, 6, fm)
				if err != nil {
					return err
				}
				origN := n
				for n < 6 {
					val *= 10
					n++
				}
				if err := fromCharSetInt(&tmfc.ff, val, fn.key); err != nil {
					return err
				}
				pos += origN
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_SSSS, DCH_ssss, DCH_SSSSS, DCH_sssss:
				val, n, err := fromCharParseIntLen(s, pos, 5, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.ssss, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_YYYY, DCH_yyyy:
				val, n, err := fromCharParseIntLen(s, pos, 4, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = n
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_YYY, DCH_yyy:
				val, n, err := fromCharParseIntLen(s, pos, 3, fm)
				if err != nil {
					return err
				}
				val = adjustPartialYearTo2020(val)
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = n
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_YY, DCH_yy:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				val = adjustPartialYearTo2020(val)
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = n
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_Y, DCH_y:
				val, n, err := fromCharParseIntLen(s, pos, 1, fm)
				if err != nil {
					return err
				}
				val = adjustPartialYearTo2020(val)
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = n
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_Y_YYY, DCH_y_yyy:
				// Format: Y,YYY. Parse digit(s), comma, then 3 digits.
				val, n, err := fromCharParseIntLen(s, pos, 4, fm)
				if err != nil {
					return err
				}
				pos += n
				if pos < len(s) && s[pos] == ',' {
					pos++
				}
				val2, n2, err := fromCharParseIntLen(s, pos, 3, fm)
				if err != nil {
					return err
				}
				val = val*1000 + val2
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = 4
				pos += n2
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_IYYY, DCH_iyyy:
				val, n, err := fromCharParseIntLen(s, pos, 4, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = n
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_IYY, DCH_iyy:
				val, n, err := fromCharParseIntLen(s, pos, 3, fm)
				if err != nil {
					return err
				}
				val = adjustPartialYearTo2020(val)
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = n
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_IY, DCH_iy:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				val = adjustPartialYearTo2020(val)
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = n
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_I, DCH_i:
				val, n, err := fromCharParseIntLen(s, pos, 1, fm)
				if err != nil {
					return err
				}
				val = adjustPartialYearTo2020(val)
				if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
					return err
				}
				tmfc.yysz = n
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_RM, DCH_rm:
				// Roman numeral month. Search in reverse order so VIII matches
				// before V, etc.
				idx, n := seqSearchASCII(s, pos, ucMonthRomanNumerals)
				if idx < 0 {
					idx, n = seqSearchASCII(s, pos, lcMonthRomanNumerals)
				}
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for roman numeral month")
				}
				// ucMonthRomanNumerals is in reverse order: XII=0, I=11.
				month := 12 - idx
				if err := fromCharSetInt(&tmfc.mm, month, fn.key); err != nil {
					return err
				}
				pos += n

			case DCH_MM, DCH_mm:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.mm, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_MONTH, DCH_Month, DCH_month:
				idx, n := seqSearchASCII(s, pos, monthsFull)
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for month name")
				}
				if err := fromCharSetInt(&tmfc.mm, idx+1, fn.key); err != nil {
					return err
				}
				pos += n

			case DCH_MON, DCH_Mon, DCH_mon:
				idx, n := seqSearchASCII(s, pos, monthsAbbrev)
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for abbreviated month name")
				}
				if err := fromCharSetInt(&tmfc.mm, idx+1, fn.key); err != nil {
					return err
				}
				pos += n

			case DCH_DAY, DCH_Day, DCH_day:
				idx, n := seqSearchASCII(s, pos, daysFull)
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for day name")
				}
				if err := fromCharSetInt(&tmfc.d, idx, fn.key); err != nil {
					return err
				}
				pos += n

			case DCH_DY, DCH_Dy, DCH_dy:
				idx, n := seqSearchASCII(s, pos, daysAbbrev)
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for abbreviated day name")
				}
				if err := fromCharSetInt(&tmfc.d, idx, fn.key); err != nil {
					return err
				}
				pos += n

			case DCH_DDD, DCH_ddd:
				val, n, err := fromCharParseIntLen(s, pos, 3, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.ddd, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_IDDD, DCH_iddd:
				val, n, err := fromCharParseIntLen(s, pos, 3, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.ddd, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_DD, DCH_dd:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.dd, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_D, DCH_d:
				val, n, err := fromCharParseIntLen(s, pos, 1, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.d, val-1, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_ID, DCH_id:
				val, n, err := fromCharParseIntLen(s, pos, 1, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.d, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_WW, DCH_ww:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.ww, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_IW, DCH_iw:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.ww, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_W, DCH_w:
				val, n, err := fromCharParseIntLen(s, pos, 1, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.w, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_Q, DCH_q:
				// Quarter — parsed but ignored in PostgreSQL's to_timestamp/to_date.
				_, n, err := fromCharParseIntLen(s, pos, 1, fm)
				if err != nil {
					return err
				}
				pos += n

			case DCH_CC, DCH_cc:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.cc, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_J, DCH_j:
				val, n, err := fromCharParseIntLen(s, pos, 7, fm)
				if err != nil {
					return err
				}
				if err := fromCharSetInt(&tmfc.j, val, fn.key); err != nil {
					return err
				}
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_A_D, DCH_B_C, DCH_a_d, DCH_b_c:
				idx, n := seqSearchASCII(s, pos, adbcStrings)
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for AD/BC")
				}
				bc := 0
				if idx%2 == 1 { // BC or B.C. entries are at odd indices.
					bc = 1
				}
				if err := fromCharSetInt(&tmfc.bc, bc, fn.key); err != nil {
					return err
				}
				pos += n

			case DCH_AD, DCH_BC, DCH_ad, DCH_bc:
				idx, n := seqSearchASCII(s, pos, adbcStrings)
				if idx < 0 {
					return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for AD/BC")
				}
				bc := 0
				if idx%2 == 1 {
					bc = 1
				}
				if err := fromCharSetInt(&tmfc.bc, bc, fn.key); err != nil {
					return err
				}
				pos += n

			case DCH_TZH:
				// Parse timezone sign and hour.
				sign := 1
				if pos < len(s) {
					if s[pos] == '+' {
						pos++
					} else if s[pos] == '-' {
						sign = -1
						pos++
					}
				}
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				tmfc.tzsign = sign
				tmfc.tzh = val
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_tzh:
				sign := 1
				if pos < len(s) {
					if s[pos] == '+' {
						pos++
					} else if s[pos] == '-' {
						sign = -1
						pos++
					}
				}
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				tmfc.tzsign = sign
				tmfc.tzh = val
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_TZM, DCH_tzm:
				val, n, err := fromCharParseIntLen(s, pos, 2, fm)
				if err != nil {
					return err
				}
				tmfc.tzm = val
				pos += n
				pos = skipTHth(s, pos, fn.suffix)

			case DCH_TZ, DCH_tz:
				return pgerror.New(
					pgcode.FeatureNotSupported,
					"\"TZ\"/\"tz\" format patterns are not supported in to_date/to_timestamp",
				)

			case DCH_OF, DCH_of:
				return pgerror.New(
					pgcode.FeatureNotSupported,
					"\"OF\" format pattern is not supported in to_date/to_timestamp",
				)

			default:
				return errors.AssertionFailedf("unexpected key for from_char: %s", errors.Safe(fn.key.name))
			}
		}
	}
	return nil
}

// tmFromCharToTime converts the parsed tmFromChar into a time.Time value.
// This follows PostgreSQL's do_to_timestamp logic.
func tmFromCharToTime(tmfc *tmFromChar, loc *time.Location) (time.Time, error) {
	year := 1
	month := time.January
	day := 1
	hour := 0
	min := 0
	sec := 0
	usec := 0

	// SSSS (seconds since midnight) overrides hh/mi/ss.
	if tmfc.ssss != fromCharFieldUnset {
		hour = tmfc.ssss / 3600
		min = (tmfc.ssss % 3600) / 60
		sec = tmfc.ssss % 60
	} else {
		if tmfc.ss != fromCharFieldUnset {
			sec = tmfc.ss
		}
		if tmfc.mi != fromCharFieldUnset {
			min = tmfc.mi
		}
		if tmfc.hh != fromCharFieldUnset {
			hour = tmfc.hh
		}
	}

	// 12-hour clock adjustment.
	if tmfc.clock == 12 && tmfc.pm != fromCharFieldUnset {
		if tmfc.hh != fromCharFieldUnset {
			if hour < 1 || hour > 12 {
				return time.Time{}, pgerror.Newf(
					pgcode.InvalidDatetimeFormat,
					"hour \"%d\" is invalid for a 12-hour clock",
					hour,
				)
			}
			if hour == 12 {
				hour = 0
			}
			if tmfc.pm == 1 { // PM
				hour += 12
			}
		}
	}

	// Microseconds from MS, US, or FF fields.
	if tmfc.ms != fromCharFieldUnset {
		usec = tmfc.ms * 1000
	}
	if tmfc.us != fromCharFieldUnset {
		usec = tmfc.us
	}
	if tmfc.ff != fromCharFieldUnset {
		usec = tmfc.ff
	}

	// Year handling.
	if tmfc.year != fromCharFieldUnset {
		year = tmfc.year
	}

	// Century handling: if CC is provided and year was not provided (or was
	// only 2 digits), compute year from century.
	if tmfc.cc != fromCharFieldUnset && tmfc.year == fromCharFieldUnset {
		if tmfc.cc > 0 {
			year = (tmfc.cc-1)*100 + 1
		} else {
			year = tmfc.cc * 100
		}
	}

	// BC adjustment.
	if tmfc.bc != fromCharFieldUnset && tmfc.bc == 1 {
		if year > 0 {
			year = -(year - 1)
		}
	}

	// Julian day.
	if tmfc.j != fromCharFieldUnset {
		jYear, jMonth, jDay := pgdate.JulianDayToDate(tmfc.j)
		year = jYear
		month = time.Month(jMonth)
		day = jDay
	} else if tmfc.mode == fromCharDateISOWeek {
		// ISO week date mode.
		if tmfc.ww != fromCharFieldUnset {
			isoYear := year
			isoWeek := tmfc.ww
			isoDay := 1 // Monday by default.
			if tmfc.d != fromCharFieldUnset {
				isoDay = tmfc.d
			}
			if tmfc.ddd != fromCharFieldUnset {
				// IDDD: day of ISO year (1-based).
				isoWeek = (tmfc.ddd-1)/7 + 1
				isoDay = ((tmfc.ddd - 1) % 7) + 1
			}
			y, m, d := isoWeekDateToGregorian(isoYear, isoWeek, isoDay)
			year = y
			month = time.Month(m)
			day = d
		}
	} else {
		// Gregorian mode.
		if tmfc.mm != fromCharFieldUnset {
			month = time.Month(tmfc.mm)
		}
		if tmfc.dd != fromCharFieldUnset {
			day = tmfc.dd
		}

		// Day-of-year overrides month/day.
		if tmfc.ddd != fromCharFieldUnset {
			jd := pgdate.DateToJulianDay(year, 1, 1) + tmfc.ddd - 1
			_, jm, jd2 := pgdate.JulianDayToDate(jd)
			month = time.Month(jm)
			day = jd2
		}
	}

	// Validate ranges.
	if month < 1 || month > 12 {
		return time.Time{}, pgerror.Newf(
			pgcode.DatetimeFieldOverflow,
			"date/time field value out of range: month must be between 1 and 12, got %d",
			int(month),
		)
	}
	if hour < 0 || hour > 23 {
		return time.Time{}, pgerror.Newf(
			pgcode.DatetimeFieldOverflow,
			"date/time field value out of range: hour must be between 0 and 23, got %d",
			hour,
		)
	}
	if min < 0 || min > 59 {
		return time.Time{}, pgerror.Newf(
			pgcode.DatetimeFieldOverflow,
			"date/time field value out of range: minute must be between 0 and 59, got %d",
			min,
		)
	}
	if sec < 0 || sec > 59 {
		return time.Time{}, pgerror.Newf(
			pgcode.DatetimeFieldOverflow,
			"date/time field value out of range: second must be between 0 and 59, got %d",
			sec,
		)
	}

	// Timezone handling. Check tzh/tzm to determine if a timezone was parsed.
	// tzsign is initialized to 1 (positive) rather than fromCharFieldUnset,
	// because -1 (negative sign) would collide with the sentinel value.
	if tmfc.tzh != fromCharFieldUnset || tmfc.tzm != fromCharFieldUnset {
		tzOffsetSec := 0
		if tmfc.tzh != fromCharFieldUnset {
			tzOffsetSec = tmfc.tzh * 3600
		}
		if tmfc.tzm != fromCharFieldUnset {
			tzOffsetSec += tmfc.tzm * 60
		}
		tzOffsetSec *= tmfc.tzsign
		loc = time.FixedZone("", tzOffsetSec)
	}

	nsec := usec * 1000 // Convert microseconds to nanoseconds.
	return time.Date(year, month, day, hour, min, sec, nsec, loc), nil
}

// isoWeekDateToGregorian converts an ISO week date (year, week, dayOfWeek)
// to a Gregorian date. dayOfWeek: 1=Monday ... 7=Sunday.
func isoWeekDateToGregorian(isoYear, isoWeek, isoDayOfWeek int) (year, month, day int) {
	// Find January 4 of the ISO year (which is always in ISO week 1).
	jan4 := pgdate.DateToJulianDay(isoYear, 1, 4)
	// Find the Monday of ISO week 1.
	// j2day returns day of week: 0=Monday ... 6=Sunday.
	jan4dow := j2day(jan4)
	mondayWeek1 := jan4 - jan4dow
	// Compute the target Julian day.
	jd := mondayWeek1 + (isoWeek-1)*7 + (isoDayOfWeek - 1)
	year, month, day = pgdate.JulianDayToDate(jd)
	return year, month, day
}

// j2day returns the day of week for a Julian day: 0=Monday ... 6=Sunday.
// Matches PostgreSQL's j2day.
func j2day(jd int) int {
	jd++
	d := jd % 7
	if d < 0 {
		d += 7
	}
	return d
}

// decodeRune decodes a rune at position pos in string s.
func decodeRune(s string, pos int) (rune, int) {
	if pos >= len(s) {
		return 0, 0
	}
	if s[pos] < 0x80 {
		return rune(s[pos]), 1
	}
	r := rune(0)
	size := 1
	b := s[pos]
	switch {
	case b&0xE0 == 0xC0:
		size = 2
	case b&0xF0 == 0xE0:
		size = 3
	case b&0xF8 == 0xF0:
		size = 4
	}
	if pos+size > len(s) {
		return 0, 1
	}
	switch size {
	case 2:
		r = rune(b&0x1F)<<6 | rune(s[pos+1]&0x3F)
	case 3:
		r = rune(b&0x0F)<<12 | rune(s[pos+1]&0x3F)<<6 | rune(s[pos+2]&0x3F)
	case 4:
		r = rune(b&0x07)<<18 | rune(s[pos+1]&0x3F)<<12 | rune(s[pos+2]&0x3F)<<6 | rune(s[pos+3]&0x3F)
	}
	return r, size
}
