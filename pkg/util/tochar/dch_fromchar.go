// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
)

// tmFromChar accumulates parsed datetime components from a format string.
// Mirrors PostgreSQL's TmFromChar (formatting.c lines 399-425).
// All fields are initialized to 0, which acts as the "unset" sentinel,
// matching PostgreSQL's ZERO_tmfc macro.
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
	// ff stores the fractional precision (1-6) when FF1-FF6 are used.
	// The actual fractional value is stored in the us field.
	ff int
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
// Matches PostgreSQL's from_char_set_int: only errors when the destination
// has previously been set to a non-zero value that differs from the new value.
func fromCharSetInt(dest *int, value int, node *keyWord) error {
	if *dest != 0 && *dest != value {
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

// isNextSeparator returns true if the format node following the current one
// is a non-digit value (separator, space, end-of-format, non-digit keyword,
// or a TH suffix on the current node). This mirrors PostgreSQL's
// is_next_separator (formatting.c) and controls whether digit-parsing reads
// a fixed number of characters or slurps all available digits.
func isNextSeparator(formatNodes []formatNode, idx int) bool {
	fn := formatNodes[idx]
	// A TH/th suffix acts as a separator.
	if fn.suffix.THth() {
		return true
	}
	// Check the next node.
	next := idx + 1
	if next >= len(formatNodes) || formatNodes[next].typ == formatNodeEnd {
		// End of format string is treated as a separator.
		return true
	}
	nextNode := formatNodes[next]
	if nextNode.typ == formatNodeAction {
		// Another keyword: separator only if it's a non-digit keyword.
		return !nextNode.key.isDigit
	}
	// Character or space node: separator unless it's a single digit character.
	if len(nextNode.character) == 1 && nextNode.character[0] >= '0' && nextNode.character[0] <= '9' {
		return false
	}
	return true
}

// fromCharParseIntLen parses an integer from the input string.
// fixedLen is the expected field width for fixed-width parsing.
// In "slurp" mode (FM mode or when the next format node is a separator),
// it reads as many digits as available. In fixed-width mode, it reads
// exactly fixedLen characters. This matches PostgreSQL's
// from_char_parse_int_len behavior (formatting.c).
// Returns the parsed value, number of digits consumed, and any error.
// The stop return value is true when no digits are available in slurp mode,
// indicating that dchFromChar should stop processing further format nodes.
// This matches PostgreSQL's soft-error mechanism (ereturn/escontext pattern
// in DCH_from_char where `if (len < 0) return;` stops processing).
func fromCharParseIntLen(
	s string, pos int, fixedLen int, fm bool, isNextSep bool, nodeName string,
) (val int, n int, stop bool, _ error) {
	if pos >= len(s) {
		return 0, 0, true, nil
	}

	// Handle optional leading sign, matching PG's strtol behavior.
	// The sign character counts toward the total consumed length.
	sign := 1
	start := pos
	if pos < len(s) && (s[pos] == '-' || s[pos] == '+') {
		if s[pos] == '-' {
			sign = -1
		}
		pos++
	}

	if fm || isNextSep {
		// Slurp mode: read as many digits as available.
		end := pos
		for end < len(s) && s[end] >= '0' && s[end] <= '9' {
			end++
		}
		if end == pos {
			// No digits found. This covers both the no-sign case and the
			// sign-but-no-digits case (e.g. "-D"). In PostgreSQL, strtol
			// returns endptr == init in both situations, yielding used == 0,
			// which always triggers the error path.
			return 0, 0, false, pgerror.Newf(
				pgcode.InvalidDatetimeFormat,
				"invalid value \"%s\" for \"%s\"",
				s[start:min(start+fixedLen, len(s))],
				nodeName,
			)
		}
		v := 0
		for i := pos; i < end; i++ {
			v = v*10 + int(s[i]-'0')
		}
		v *= sign
		// PostgreSQL checks for int32 overflow.
		if v > math.MaxInt32 || v < math.MinInt32 {
			return 0, 0, false, pgerror.Newf(
				pgcode.DatetimeFieldOverflow,
				"value for \"%s\" in source string is out of range",
				nodeName,
			)
		}
		return v, end - start, false, nil
	}

	// Fixed-width mode: read digits within the remaining fixedLen window.
	// The sign (if any) already consumed one character of the window.
	remaining := fixedLen - (pos - start)
	if pos+remaining > len(s) {
		return 0, 0, false, pgerror.Newf(
			pgcode.InvalidDatetimeFormat,
			"source string too short for \"%s\" formatting field",
			nodeName,
		)
	}
	end := pos
	for end < pos+remaining && end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end-pos < remaining {
		return 0, 0, false, pgerror.Newf(
			pgcode.InvalidDatetimeFormat,
			"invalid value \"%s\" for \"%s\"",
			s[start:min(start+fixedLen, len(s))],
			nodeName,
		)
	}
	v := 0
	for i := pos; i < end; i++ {
		v = v*10 + int(s[i]-'0')
	}
	v *= sign
	return v, end - start, false, nil
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
	tmfc := tmFromChar{}
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
	tmfc := tmFromChar{}
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

	// extraSkip tracks extra characters consumed beyond format string
	// expectations (whitespace skipped after action nodes). This allows
	// literal character nodes to "absorb" already-consumed padding without
	// advancing the input pointer. Matches PostgreSQL's extra_skip in
	// DCH_from_char (formatting.c).
	extraSkip := 0

nodeLoop:
	for fnIdx, fn := range formatNodes {
		if fn.typ == formatNodeEnd {
			break
		}

		if pos >= len(s) {
			break
		}

		// Pre-node whitespace skip. In non-FX mode, skip whitespace before
		// action nodes (except the FX action itself) and before the very
		// first node regardless of type. Each skipped character increments
		// extraSkip. Matches PostgreSQL's DCH_from_char top-of-loop skip.
		if !fx &&
			!(fn.typ == formatNodeAction && (fn.key.id == DCH_FX || fn.key.id == DCH_fx)) &&
			(fn.typ == formatNodeAction || fnIdx == 0) {
			for pos < len(s) && unicode.IsSpace(rune(s[pos])) {
				pos++
				extraSkip++
			}
		}

		if fn.typ == formatNodeSpace || fn.typ == formatNodeSeparator {
			if fx {
				// In FX mode, consume exactly one input character without
				// checking for a match. Matches PostgreSQL.
				if pos < len(s) {
					_, size := utf8.DecodeRuneInString(s[pos:])
					pos += size
				}
			} else {
				// In non-FX mode, one format separator/space matches one
				// input separator or space. Decrement extraSkip to account
				// for this format node, then optionally consume one input
				// character. Matches PostgreSQL's DCH_from_char.
				extraSkip--
				if pos < len(s) &&
					(unicode.IsSpace(rune(s[pos])) || isSeparatorChar(rune(s[pos]))) {
					pos++
					extraSkip++
				}
			}
			continue
		}

		if fn.typ != formatNodeAction {
			// Text character node (NODE_TYPE_CHAR). Consume one input
			// character, but if extraSkip > 0, we already consumed extra
			// characters (whitespace after a previous action), so just
			// decrement extraSkip instead. Matches PostgreSQL.
			if !fx && extraSkip > 0 {
				extraSkip--
			} else if pos < len(s) {
				_, size := utf8.DecodeRuneInString(s[pos:])
				pos += size
			}
			continue
		}

		// formatNodeAction handling below.
		fm := fn.suffix.FM()
		nextSep := isNextSeparator(formatNodes, fnIdx)

		if err := fromCharSetMode(tmfc, fn.key.dateMode); err != nil {
			return err
		}

		switch fn.key.id {
		case DCH_FX, DCH_fx:
			fx = true

		case DCH_A_M, DCH_P_M, DCH_a_m, DCH_p_m,
			DCH_AM, DCH_PM, DCH_am, DCH_pm:
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
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.hh, val, fn.key); err != nil {
				return err
			}
			tmfc.clock = 12
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_HH24:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.hh, val, fn.key); err != nil {
				return err
			}
			// Unlike HH/HH12, HH24 does NOT set tmfc.clock. This matches
			// PostgreSQL, where only HH/HH12/AM/PM set clock=CLOCK_12_HOUR.
			// When both HH and HH24 appear, the 12-hour clock adjustment
			// still applies (from the earlier HH), so hour 12 with no PM
			// becomes 0.
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_MI:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.mi, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_SS:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.ss, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_MS:
			val, n, stop, err := fromCharParseIntLen(s, pos, 3, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			origN := n
			// Conflict check uses raw value, then scale. Matches PostgreSQL.
			if err := fromCharSetInt(&tmfc.ms, val, fn.key); err != nil {
				return err
			}
			// Pad right to 3 digits.
			for n < 3 {
				tmfc.ms *= 10
				n++
			}
			pos += origN
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_US:
			val, n, stop, err := fromCharParseIntLen(s, pos, 6, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			origN := n
			// Conflict check uses raw value, then scale. Matches PostgreSQL.
			if err := fromCharSetInt(&tmfc.us, val, fn.key); err != nil {
				return err
			}
			// Pad right to 6 digits.
			for n < 6 {
				tmfc.us *= 10
				n++
			}
			pos += origN
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_FF1, DCH_ff1, DCH_FF2, DCH_ff2, DCH_FF3, DCH_ff3,
			DCH_FF4, DCH_ff4, DCH_FF5, DCH_ff5, DCH_FF6, DCH_ff6:
			// FF1-FF6: parse fractional seconds into the us field,
			// matching PostgreSQL where FFn falls through to US handling.
			// The precision (1-6) is stored in tmfc.ff.
			var maxLen int
			switch fn.key.id {
			case DCH_FF1, DCH_ff1:
				maxLen = 1
			case DCH_FF2, DCH_ff2:
				maxLen = 2
			case DCH_FF3, DCH_ff3:
				maxLen = 3
			case DCH_FF4, DCH_ff4:
				maxLen = 4
			case DCH_FF5, DCH_ff5:
				maxLen = 5
			case DCH_FF6, DCH_ff6:
				maxLen = 6
			}
			val, n, stop, err := fromCharParseIntLen(s, pos, maxLen, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			tmfc.ff = maxLen
			origN := n
			// Conflict check uses raw value, then scale. Matches PostgreSQL
			// where from_char_set_int is called before the scaling multiply.
			if err := fromCharSetInt(&tmfc.us, val, fn.key); err != nil {
				return err
			}
			// Pad right to 6 digits (microseconds).
			for n < 6 {
				tmfc.us *= 10
				n++
			}
			pos += origN
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_SSSS, DCH_ssss, DCH_SSSSS, DCH_sssss:
			// Use key len (4 for "SSSS", 5 for "SSSSS"), matching PG's
			// from_char_parse_int which uses node->key->len.
			val, n, stop, err := fromCharParseIntLen(s, pos, len(fn.key.name), fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.ssss, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_YYYY, DCH_yyyy:
			val, n, stop, err := fromCharParseIntLen(s, pos, 4, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			// PG sets yysz to the keyword's fixed width, not digits consumed.
			tmfc.yysz = 4
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_YYY, DCH_yyy:
			val, n, stop, err := fromCharParseIntLen(s, pos, 3, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			if n < 4 {
				tmfc.year = adjustPartialYearTo2020(tmfc.year)
			}
			tmfc.yysz = 3
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_YY, DCH_yy:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			if n < 4 {
				tmfc.year = adjustPartialYearTo2020(tmfc.year)
			}
			tmfc.yysz = 2
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_Y, DCH_y:
			val, n, stop, err := fromCharParseIntLen(s, pos, 1, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			if n < 4 {
				tmfc.year = adjustPartialYearTo2020(tmfc.year)
			}
			tmfc.yysz = 1
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_Y_YYY, DCH_y_yyy:
			// Format: Y,YYY. Matches PostgreSQL's sscanf-based parsing:
			// parse digits (millennia), comma, then 3 digits (years).
			start := pos
			for pos < len(s) && s[pos] >= '0' && s[pos] <= '9' {
				pos++
			}
			if pos == start {
				return pgerror.New(
					pgcode.InvalidDatetimeFormat,
					"invalid input string for \"Y,YYY\"",
				)
			}
			millennia := 0
			for i := start; i < pos; i++ {
				millennia = millennia*10 + int(s[i]-'0')
			}
			if pos >= len(s) || s[pos] != ',' {
				return pgerror.New(
					pgcode.InvalidDatetimeFormat,
					"invalid input string for \"Y,YYY\"",
				)
			}
			pos++ // skip comma
			yearStart := pos
			for pos < len(s) && pos < yearStart+3 && s[pos] >= '0' && s[pos] <= '9' {
				pos++
			}
			if pos-yearStart != 3 {
				return pgerror.New(
					pgcode.InvalidDatetimeFormat,
					"invalid input string for \"Y,YYY\"",
				)
			}
			years := 0
			for i := yearStart; i < pos; i++ {
				years = years*10 + int(s[i]-'0')
			}
			val := millennia*1000 + years
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			tmfc.yysz = 4
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_IYYY, DCH_iyyy:
			val, n, stop, err := fromCharParseIntLen(s, pos, 4, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			tmfc.yysz = 4
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_IYY, DCH_iyy:
			val, n, stop, err := fromCharParseIntLen(s, pos, 3, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			if n < 4 {
				tmfc.year = adjustPartialYearTo2020(tmfc.year)
			}
			tmfc.yysz = 3
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_IY, DCH_iy:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			if n < 4 {
				tmfc.year = adjustPartialYearTo2020(tmfc.year)
			}
			tmfc.yysz = 2
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_I, DCH_i:
			val, n, stop, err := fromCharParseIntLen(s, pos, 1, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.year, val, fn.key); err != nil {
				return err
			}
			if n < 4 {
				tmfc.year = adjustPartialYearTo2020(tmfc.year)
			}
			tmfc.yysz = 1
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
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
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
			tmfc.d++
			pos += n

		case DCH_DY, DCH_Dy, DCH_dy:
			idx, n := seqSearchASCII(s, pos, daysAbbrev)
			if idx < 0 {
				return pgerror.New(pgcode.InvalidDatetimeFormat, "invalid value for abbreviated day name")
			}
			if err := fromCharSetInt(&tmfc.d, idx, fn.key); err != nil {
				return err
			}
			tmfc.d++
			pos += n

		case DCH_DDD, DCH_ddd:
			val, n, stop, err := fromCharParseIntLen(s, pos, 3, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.ddd, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_IDDD, DCH_iddd:
			val, n, stop, err := fromCharParseIntLen(s, pos, 3, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.ddd, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_DD, DCH_dd:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.dd, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_D, DCH_d:
			val, n, stop, err := fromCharParseIntLen(s, pos, 1, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			// Store 1-indexed, matching PostgreSQL. DAY/DY also store
			// 1-indexed (0-indexed search result + increment).
			if err := fromCharSetInt(&tmfc.d, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_ID, DCH_id:
			val, n, stop, err := fromCharParseIntLen(s, pos, 1, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			// Shift ISO day-of-week (1=Monday..7=Sunday) to Gregorian
			// (1=Sunday..7=Saturday), matching PostgreSQL.
			val++
			if val > 7 {
				val = 1
			}
			if err := fromCharSetInt(&tmfc.d, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_WW, DCH_ww:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.ww, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_IW, DCH_iw:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.ww, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_W, DCH_w:
			val, n, stop, err := fromCharParseIntLen(s, pos, 1, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.w, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_Q, DCH_q:
			// Quarter — parsed but ignored in PostgreSQL's to_timestamp/to_date.
			_, n, stop, err := fromCharParseIntLen(s, pos, 1, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_CC, DCH_cc:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.cc, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_J, DCH_j:
			// J uses key len (1) as the fixed width, matching PostgreSQL's
			// from_char_parse_int which uses node->key->len. In slurp mode
			// (FM or next is separator), all available digits are consumed.
			val, n, stop, err := fromCharParseIntLen(s, pos, len(fn.key.name), fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			if err := fromCharSetInt(&tmfc.j, val, fn.key); err != nil {
				return err
			}
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_A_D, DCH_B_C, DCH_a_d, DCH_b_c,
			DCH_AD, DCH_BC, DCH_ad, DCH_bc:
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

		case DCH_TZH, DCH_tzh:
			// Parse timezone sign and hour. The sign character may have
			// been consumed as a separator by a preceding space/separator
			// node. In that case, extraSkip > 0 and the character just
			// before the current position is the consumed '-'. This
			// matches PostgreSQL's DCH_from_char TZH handler.
			sign := 1
			if pos < len(s) && (s[pos] == '+' || s[pos] == '-' || s[pos] == ' ') {
				if s[pos] == '-' {
					sign = -1
				}
				pos++
			} else {
				if extraSkip > 0 && pos > 0 && s[pos-1] == '-' {
					sign = -1
				}
			}
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
			}
			tmfc.tzsign = sign
			tmfc.tzh = val
			pos += n
			pos = skipTHth(s, pos, fn.suffix)

		case DCH_TZM, DCH_tzm:
			val, n, stop, err := fromCharParseIntLen(s, pos, 2, fm, nextSep, fn.key.name)
			if err != nil {
				return err
			}
			if stop {
				break nodeLoop
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

		// Post-action whitespace skip. After processing each action
		// node, reset extraSkip and skip any trailing whitespace,
		// counting skipped chars. This handles padding from
		// DAY/MONTH/RM and allows subsequent literal/separator nodes
		// to absorb the already-consumed characters via extraSkip.
		// Matches PostgreSQL's DCH_from_char bottom-of-loop skip.
		if !fx {
			extraSkip = 0
			for pos < len(s) && unicode.IsSpace(rune(s[pos])) {
				pos++
				extraSkip++
			}
		}
	}
	return nil
}

// tmFromCharToTime converts the parsed tmFromChar into a time.Time value.
// This follows PostgreSQL's do_to_timestamp logic (formatting.c).
func tmFromCharToTime(tmfc *tmFromChar, loc *time.Location) (time.Time, error) {
	// PostgreSQL initializes tm_year to 0, which represents 1 BC.
	// All fields default to 0, matching PostgreSQL's ZERO_tm.
	year := 0
	month := 0
	day := 0
	hour := 0
	min := 0
	sec := 0
	usec := 0

	// SSSS (seconds since midnight) overrides hh/mi/ss.
	if tmfc.ssss != 0 {
		hour = tmfc.ssss / 3600
		min = (tmfc.ssss % 3600) / 60
		sec = tmfc.ssss % 60
	}

	if tmfc.ss != 0 {
		sec = tmfc.ss
	}
	if tmfc.mi != 0 {
		min = tmfc.mi
	}
	if tmfc.hh != 0 {
		hour = tmfc.hh
	}

	// 12-hour clock: validate range and apply AM/PM adjustment.
	// Matches PostgreSQL's do_to_timestamp (formatting.c).
	if tmfc.clock == 12 {
		if hour < 1 || hour > 12 {
			return time.Time{}, pgerror.Newf(
				pgcode.InvalidDatetimeFormat,
				"hour \"%d\" is invalid for the 12-hour clock",
				hour,
			)
		}
		if tmfc.pm != 0 && hour < 12 {
			hour += 12
		} else if tmfc.pm == 0 && hour == 12 {
			hour = 0
		}
	}

	// Microseconds from MS and US fields. Matches PostgreSQL's
	// do_to_timestamp: MS and US are additive (formatting.c lines 4728-4731).
	if tmfc.ms != 0 {
		usec += tmfc.ms * 1000
	}
	if tmfc.us != 0 {
		usec += tmfc.us
	}

	// Validate that combined fractional seconds don't overflow.
	// PostgreSQL rejects when MS + US/FF produce a value >= 1 second.
	if usec >= 1000000 || usec < 0 {
		return time.Time{}, pgerror.New(
			pgcode.DatetimeFieldOverflow,
			"date/time field value out of range",
		)
	}

	// Year/CC/BC handling. Matches PostgreSQL's do_to_timestamp logic
	// (formatting.c lines 4587-4637).
	if tmfc.year != 0 {
		if tmfc.cc != 0 && tmfc.yysz <= 2 {
			// CC + 1-2 digit year: use year as low-order digits within
			// the given century.
			cc := tmfc.cc
			if tmfc.bc != 0 {
				cc = -cc
			}
			year = tmfc.year % 100
			if year != 0 {
				if cc >= 0 {
					year += (cc - 1) * 100
				} else {
					year = (cc+1)*100 - year + 1
				}
			} else {
				// Year ending in "00".
				if cc >= 0 {
					year = cc * 100
				} else {
					year = cc*100 + 1
				}
			}
		} else {
			// 3-4 digit year (or no CC): use year directly, ignore CC.
			year = tmfc.year
			if tmfc.bc != 0 {
				year = -year
			}
			// Correct for PostgreSQL's representation of BC years.
			if year < 0 {
				year++
			}
		}
	} else if tmfc.cc != 0 {
		// Only CC provided, no year: use first year of the century.
		cc := tmfc.cc
		if tmfc.bc != 0 {
			cc = -cc
		}
		if cc >= 0 {
			// +1 because 21st century started in 2001.
			year = (cc-1)*100 + 1
		} else {
			// +1 because year == 599 is 600 BC.
			year = cc*100 + 1
		}
	}

	// Julian day. Processed first, then WW/W/DD/MM can override parts
	// of the result. Matches PostgreSQL's sequential processing.
	if tmfc.j != 0 {
		jYear, jMonth, jDay := pgdate.JulianDayToDate(tmfc.j)
		year = jYear
		month = jMonth
		day = jDay
	}

	if tmfc.ww != 0 {
		if tmfc.mode == fromCharDateISOWeek {
			// ISO week date mode. Matches PostgreSQL's isoweekdate2date.
			if tmfc.d != 0 {
				// Convert Gregorian day-of-week (1=Sunday..7=Saturday) to
				// ISO week day offset, matching PostgreSQL's isoweekdate2date:
				//   Sunday(1)→+6, Mon(2)→+0, ..., Sat(7)→+5
				jd := isoweek2j(year, tmfc.ww)
				if tmfc.d > 1 {
					jd += tmfc.d - 2
				} else {
					jd += 6
				}
				year, month, day = pgdate.JulianDayToDate(jd)
			} else if tmfc.ddd != 0 {
				// IDDD: day of ISO year (1-based).
				isoWeek := (tmfc.ddd-1)/7 + 1
				isoDay := ((tmfc.ddd - 1) % 7) + 1
				y, m, d := isoWeekDateToGregorian(year, isoWeek, isoDay)
				year = y
				month = m
				day = d
			} else {
				// No day specified: use Monday (start of ISO week).
				jd := isoweek2j(year, tmfc.ww)
				year, month, day = pgdate.JulianDayToDate(jd)
			}
		} else {
			// WW (week of year) computes day-of-year. Matches PostgreSQL
			// (formatting.c line 4660).
			tmfc.ddd = (tmfc.ww-1)*7 + 1
		}
	}

	// W (week of month) computes day of month. Matches PostgreSQL
	// (formatting.c line 4664).
	if tmfc.w != 0 {
		tmfc.dd = (tmfc.w-1)*7 + 1
	}

	if tmfc.dd != 0 {
		day = tmfc.dd
	}
	if tmfc.mm != 0 {
		month = tmfc.mm
	}

	// Day-of-year overrides month/day when they haven't been explicitly
	// set. Matches PostgreSQL (formatting.c line 4676).
	if tmfc.ddd != 0 && (month <= 1 || day <= 1) {
		if year == 0 && tmfc.bc == 0 {
			return time.Time{}, pgerror.New(
				pgcode.InvalidDatetimeFormat,
				"cannot calculate day of year without year information",
			)
		}
		daysInYear := 365
		if isLeapYear(year) {
			daysInYear = 366
		}
		if tmfc.ddd < 1 || tmfc.ddd > daysInYear {
			return time.Time{}, pgerror.New(
				pgcode.DatetimeFieldOverflow,
				"date/time field value out of range",
			)
		}
		jd := pgdate.DateToJulianDay(year, 1, 1) + tmfc.ddd - 1
		_, jm, jd2 := pgdate.JulianDayToDate(jd)
		if month <= 1 {
			month = jm
		}
		if day <= 1 {
			day = jd2
		}
	}

	// Default month and day to 1 if not set, matching PostgreSQL.
	// Use == 0 (not <= 0) so that explicitly-parsed negative values
	// are caught by the range checks below rather than silently
	// defaulted to 1.
	if month == 0 {
		month = 1
	}
	if day == 0 {
		day = 1
	}

	// Validate ranges. Matches PostgreSQL's ValidateDate / DateTimeParseError.
	if month < 1 || month > 12 {
		return time.Time{}, pgerror.Newf(
			pgcode.DatetimeFieldOverflow,
			"date/time field value out of range: month must be between 1 and 12, got %d",
			month,
		)
	}
	if day < 1 || day > daysInMonth(year, month) {
		return time.Time{}, pgerror.Newf(
			pgcode.DatetimeFieldOverflow,
			"date/time field value out of range: day must be between 1 and %d for month %d, got %d",
			daysInMonth(year, month), month, day,
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

	// Apply FF precision rounding, matching PostgreSQL's AdjustTimestampForTypmod.
	// PG rounds the full int64 timestamp toward zero, so for negative (BC)
	// timestamps the fractional part rounds differently than for positive (AD).
	// We must compute this after the year is known.
	if tmfc.ff > 0 && tmfc.ff < 6 {
		floorVals := [7]int{1000000, 100000, 10000, 1000, 100, 10, 1}
		roundVals := [7]int{500000, 50000, 5000, 500, 50, 5, 0}
		scale := floorVals[tmfc.ff]
		offset := roundVals[tmfc.ff]
		if year > 0 {
			// AD (positive PG timestamp): round half up.
			usec = ((usec + offset) / scale) * scale
		} else {
			// BC (negative PG timestamp): PG's toward-zero rounding on the
			// full timestamp means the fractional part effectively rounds
			// using: new_usec = -floor((offset - usec) / scale) * scale.
			if usec <= offset {
				usec = 0
			} else {
				x := usec - offset
				usec = ((x + scale - 1) / scale) * scale
			}
		}
	}

	// Timezone handling. Matches PostgreSQL: check tzsign to detect
	// if a timezone was parsed (tzsign defaults to 0, set to +1/-1
	// when TZH is parsed).
	if tmfc.tzsign != 0 {
		tzOffsetSec := tmfc.tzh*3600 + tmfc.tzm*60
		tzOffsetSec *= tmfc.tzsign
		loc = time.FixedZone("", tzOffsetSec)
	}

	nsec := usec * 1000 // Convert microseconds to nanoseconds.
	return time.Date(year, time.Month(month), day, hour, min, sec, nsec, loc), nil
}

// isoweek2j returns the Julian day of the Monday of the given ISO week.
// Matches PostgreSQL's isoweek2j (timestamp.c).
func isoweek2j(isoYear, isoWeek int) int {
	// January 4 is always in ISO week 1.
	day4 := pgdate.DateToJulianDay(isoYear, 1, 4)
	// day0 is the offset to the Monday of the week containing Jan 4.
	day0 := j2day(day4 - 1)
	return (isoWeek-1)*7 + (day4 - day0)
}

// isoWeekDateToGregorian converts an ISO week date (year, week, dayOfWeek)
// to a Gregorian date. dayOfWeek: 1=Monday ... 7=Sunday (ISO convention).
func isoWeekDateToGregorian(isoYear, isoWeek, isoDayOfWeek int) (year, month, day int) {
	jd := isoweek2j(isoYear, isoWeek) + (isoDayOfWeek - 1)
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

// isLeapYear returns true if the given year is a leap year.
func isLeapYear(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

// daysInMonth returns the number of days in the given month for the given year.
func daysInMonth(year, month int) int {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if isLeapYear(year) {
			return 29
		}
		return 28
	}
	return 31
}
