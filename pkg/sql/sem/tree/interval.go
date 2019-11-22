// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

type intervalLexer struct {
	str    string
	offset int
	err    error
}

// consumeNum consumes the next decimal number.
// 1st return value is the integer part.
// 2nd return value is whether a decimal part was encountered.
// 3rd return value is the decimal part as a float.
// If the number is negative, both the 1st and 3rd return
// value are negative.
// The decimal value is returned separately from the integer value so
// as to support large integer values which would not fit in a float.
func (l *intervalLexer) consumeNum() (int64, bool, float64) {
	if l.err != nil {
		return 0, false, 0
	}

	offset := l.offset

	neg := false
	if l.offset < len(l.str) && l.str[l.offset] == '-' {
		// Remember a leading negative sign. We can't use "intPart < 0"
		// below, because when the input syntax is "-0.xxxx" intPart is 0.
		neg = true
	}

	// Integer part before the decimal separator.
	intPart := l.consumeInt()

	var decPart float64
	hasDecimal := false
	if l.offset < len(l.str) && l.str[l.offset] == '.' {
		hasDecimal = true
		start := l.offset

		// Advance offset to prepare a valid argument to ParseFloat().
		l.offset++
		for ; l.offset < len(l.str) && l.str[l.offset] >= '0' && l.str[l.offset] <= '9'; l.offset++ {
		}
		// Try to convert.
		value, err := strconv.ParseFloat(l.str[start:l.offset], 64)
		if err != nil {
			l.err = pgerror.Newf(
				pgcode.InvalidDatetimeFormat, "interval: %v", err)
			return 0, false, 0
		}
		decPart = value
	}

	// Ensure we have something.
	if offset == l.offset {
		l.err = pgerror.Newf(
			pgcode.InvalidDatetimeFormat, "interval: missing number at position %d: %q", offset, l.str)
		return 0, false, 0
	}

	if neg {
		decPart = -decPart
	}
	return intPart, hasDecimal, decPart
}

// Consumes the next integer.
func (l *intervalLexer) consumeInt() int64 {
	if l.err != nil {
		return 0
	}

	start := l.offset

	// Advance offset to prepare a valid argument to ParseInt().
	if l.offset < len(l.str) && (l.str[l.offset] == '-' || l.str[l.offset] == '+') {
		l.offset++
	}
	for ; l.offset < len(l.str) && l.str[l.offset] >= '0' && l.str[l.offset] <= '9'; l.offset++ {
	}
	// Check if we have something like ".X".
	if start == l.offset && len(l.str) > (l.offset+1) && l.str[l.offset] == '.' {
		return 0
	}

	x, err := strconv.ParseInt(l.str[start:l.offset], 10, 64)
	if err != nil {
		l.err = pgerror.Newf(
			pgcode.InvalidDatetimeFormat, "interval: %v", err)
		return 0
	}
	if start == l.offset {
		l.err = pgerror.Newf(
			pgcode.InvalidDatetimeFormat, "interval: missing number at position %d: %q", start, l.str)
		return 0
	}
	return x
}

// Consumes the next unit.
func (l *intervalLexer) consumeUnit(skipCharacter byte) string {
	if l.err != nil {
		return ""
	}

	offset := l.offset
	for ; l.offset < len(l.str); l.offset++ {
		if (l.str[l.offset] >= '0' && l.str[l.offset] <= '9') ||
			l.str[l.offset] == skipCharacter ||
			l.str[l.offset] == '-' {
			break
		}
	}

	if offset == l.offset {
		l.err = pgerror.Newf(
			pgcode.InvalidDatetimeFormat, "interval: missing unit at position %d: %q", offset, l.str)
		return ""
	}
	return l.str[offset:l.offset]
}

// Consumes any number of spaces.
func (l *intervalLexer) consumeSpaces() {
	if l.err != nil {
		return
	}
	for ; l.offset < len(l.str) && l.str[l.offset] == ' '; l.offset++ {
	}
}

// ISO Units.
var isoDateUnitMap = map[string]duration.Duration{
	"D": duration.MakeDuration(0, 1, 0),
	"W": duration.MakeDuration(0, 7, 0),
	"M": duration.MakeDuration(0, 0, 1),
	"Y": duration.MakeDuration(0, 0, 12),
}

var isoTimeUnitMap = map[string]duration.Duration{
	"S": duration.MakeDuration(time.Second.Nanoseconds(), 0, 0),
	"M": duration.MakeDuration(time.Minute.Nanoseconds(), 0, 0),
	"H": duration.MakeDuration(time.Hour.Nanoseconds(), 0, 0),
}

const errInvalidSQLDuration = "invalid input syntax for type interval %s"

type parsedIndex uint8

const (
	nothingParsed parsedIndex = iota
	hmsParsed
	dayParsed
	yearMonthParsed
)

func newInvalidSQLDurationError(s string) error {
	return pgerror.Newf(pgcode.InvalidDatetimeFormat, errInvalidSQLDuration, s)
}

// Parses a SQL standard interval string.
// See the following links for exampels:
//  - http://www.postgresql.org/docs/9.1/static/datatype-datetime.html#DATATYPE-INTERVAL-INPUT-EXAMPLES
//  - http://www.ibm.com/support/knowledgecenter/SSGU8G_12.1.0/com.ibm.esqlc.doc/ids_esqlc_0190.htm
func sqlStdToDuration(s string) (duration.Duration, error) {
	var d duration.Duration
	parts := strings.Fields(s)
	if len(parts) > 3 || len(parts) == 0 {
		return d, newInvalidSQLDurationError(s)
	}
	// Index of which part(s) have been parsed for detecting bad order such as `HH:MM:SS Year-Month`.
	parsedIdx := nothingParsed
	// Both 'Day' and 'Second' can be float, but 'Day Second'::interval is invalid.
	floatParsed := false
	// Parsing backward makes it easy to distinguish 'Day' and 'Second' when encountering a single value.
	//   `1-2 5 9:` and `1-2 5`
	//        |              |
	// day ---+              |
	// second ---------------+
	for i := len(parts) - 1; i >= 0; i-- {
		// Parses leading sign
		part := parts[i]
		neg := false
		// Consumes [-+]
		if part != "" {
			c := part[0]
			if c == '-' || c == '+' {
				neg = c == '-'
				part = part[1:]
			}
		}
		if len(part) == 0 {
			return d, newInvalidSQLDurationError(s)
		}
		if part[0] == '-' || part[0] == '+' {
			return d, newInvalidSQLDurationError(s)
		}

		if strings.ContainsRune(part, ':') {
			// Try to parse as HH:MM:SS
			if parsedIdx != nothingParsed {
				return d, newInvalidSQLDurationError(s)
			}
			parsedIdx = hmsParsed
			// Colon-separated intervals in Postgres are odd. They have day, hour,
			// minute, or second parts depending on number of fields and if the field
			// is an int or float.
			//
			// Instead of supporting unit changing based on int or float, use the
			// following rules:
			// - One field is S.
			// - Two fields is H:M.
			// - Three fields is H:M:S.
			// - All fields support both int and float.
			hms := strings.Split(part, ":")
			var err error
			var dur time.Duration
			// Support such as `HH:` and `HH:MM:` as postgres do. Set the last part to "0".
			if hms[len(hms)-1] == "" {
				hms[len(hms)-1] = "0"
			}
			switch len(hms) {
			case 2:
				toParse := hms[0] + "h" + hms[1] + "m"
				if neg {
					toParse = "-" + toParse
				}
				dur, err = time.ParseDuration(toParse)
			case 3:
				// Support such as `HH::SS` as postgres do. Set minute part to 0.
				// TODO(hainesc): `:1:2 -> 1 hour 2 min` as postgres do?
				if hms[1] == "" {
					hms[1] = "0"
				}
				toParse := hms[0] + "h" + hms[1] + "m" + hms[2] + "s"
				if neg {
					toParse = "-" + toParse
				}
				dur, err = time.ParseDuration(toParse)
			default:
				return d, newInvalidSQLDurationError(s)
			}
			if err != nil {
				return d, makeParseError(part, types.Interval, err)
			}
			d = d.Add(duration.MakeDuration(dur.Nanoseconds(), 0, 0))
		} else if strings.ContainsRune(part, '-') {
			// Try to parse as Year-Month.
			if parsedIdx >= yearMonthParsed {
				return d, newInvalidSQLDurationError(s)
			}
			parsedIdx = yearMonthParsed

			yms := strings.Split(part, "-")
			if len(yms) != 2 {
				return d, newInvalidSQLDurationError(s)
			}
			year, errYear := strconv.Atoi(yms[0])
			var month int
			var errMonth error
			if yms[1] != "" {
				month, errMonth = strconv.Atoi(yms[1])
			}
			if errYear == nil && errMonth == nil {
				delta := duration.MakeDuration(0, 0, 1).Mul(int64(year)*12 + int64(month))
				if neg {
					d = d.Sub(delta)
				} else {
					d = d.Add(delta)
				}
			} else {
				return d, newInvalidSQLDurationError(s)
			}

		} else if value, err := strconv.ParseFloat(part, 64); err == nil {
			// Try to parse as Day or Second.
			var dur time.Duration
			var err error
			// Make sure 'Day Second'::interval invalid.
			if floatParsed {
				return d, newInvalidSQLDurationError(s)
			}
			floatParsed = true
			if parsedIdx == nothingParsed {
				// It must be 'Second' part because nothing has been parsed.
				toParse := part + "s"
				if neg {
					toParse = "-" + toParse
				}
				dur, err = time.ParseDuration(toParse)

				if err != nil {
					return d, newInvalidSQLDurationError(s)
				}
				d = d.Add(duration.MakeDuration(dur.Nanoseconds(), 0, 0))
				parsedIdx = hmsParsed
			} else if parsedIdx == hmsParsed {
				// Day part.
				// TODO(hainesc): support float value in day part?
				delta := duration.MakeDuration(0, 1, 0).Mul(int64(value))
				if neg {
					d = d.Sub(delta)
				} else {
					d = d.Add(delta)
				}
				parsedIdx = dayParsed
			} else {
				return d, newInvalidSQLDurationError(s)
			}
		} else {
			return d, newInvalidSQLDurationError(s)
		}
	}
	return d, nil
}

// Parses an ISO8601 (with designators) string.
// See the following links for examples:
//  - http://www.postgresql.org/docs/9.1/static/datatype-datetime.html#DATATYPE-INTERVAL-INPUT-EXAMPLES
//  - https://en.wikipedia.org/wiki/ISO_8601#Time_intervals
//  - https://en.wikipedia.org/wiki/ISO_8601#Durations
func iso8601ToDuration(s string) (duration.Duration, error) {
	var d duration.Duration
	if len(s) == 0 || s[0] != 'P' {
		return d, newInvalidSQLDurationError(s)
	}

	// Advance to offset 1, since we don't care about the leading P.
	l := intervalLexer{str: s, offset: 1, err: nil}
	unitMap := isoDateUnitMap

	for l.offset < len(s) {
		// Check if we're in the time part yet.
		if s[l.offset] == 'T' {
			unitMap = isoTimeUnitMap
			l.offset++
		}

		v := l.consumeInt()
		u := l.consumeUnit('T')
		if l.err != nil {
			return d, l.err
		}

		if unit, ok := unitMap[u]; ok {
			d = d.Add(unit.Mul(v))
		} else {
			return d, pgerror.Newf(
				pgcode.InvalidDatetimeFormat,
				"interval: unknown unit %s in ISO-8601 duration %s", u, s)
		}
	}

	return d, nil
}

// unitMap defines for each unit name what is the time duration for
// that unit.
var unitMap = func(
	units map[string]duration.Duration,
	aliases map[string][]string,
) map[string]duration.Duration {
	for a, alist := range aliases {
		// Pluralize.
		units[a+"s"] = units[a]
		for _, alias := range alist {
			// Populate the remaining aliases.
			units[alias] = units[a]
		}
	}
	return units
}(map[string]duration.Duration{
	// Use DecodeDuration here because ns is the only unit for which we do not
	// want to round nanoseconds since it is only used for multiplication.
	"microsecond": duration.MakeDuration(time.Microsecond.Nanoseconds(), 0, 0),
	"millisecond": duration.MakeDuration(time.Millisecond.Nanoseconds(), 0, 0),
	"second":      duration.MakeDuration(time.Second.Nanoseconds(), 0, 0),
	"minute":      duration.MakeDuration(time.Minute.Nanoseconds(), 0, 0),
	"hour":        duration.MakeDuration(time.Hour.Nanoseconds(), 0, 0),
	"day":         duration.MakeDuration(0, 1, 0),
	"week":        duration.MakeDuration(0, 7, 0),
	"month":       duration.MakeDuration(0, 0, 1),
	"year":        duration.MakeDuration(0, 0, 12),
}, map[string][]string{
	// Include PostgreSQL's unit keywords for compatibility; see
	// https://github.com/postgres/postgres/blob/a01d0fa1d889cc2003e1941e8b98707c4d701ba9/src/backend/utils/adt/datetime.c#L175-L240
	//
	// µ = U+00B5 = micro symbol
	// μ = U+03BC = Greek letter mu
	"microsecond": {"us", "µs", "μs", "usec", "usecs", "usecond", "useconds"},
	"millisecond": {"ms", "msec", "msecs", "msecond", "mseconds"},
	"second":      {"s", "sec", "secs"},
	"minute":      {"m", "min", "mins"},
	"hour":        {"h", "hr", "hrs"},
	"day":         {"d"},
	"week":        {"w"},
	"month":       {"mon", "mons"},
	"year":        {"y", "yr", "yrs"},
})

// parseDuration parses a duration in the "traditional" Postgres
// format (e.g. '1 day 2 hours', '1 day 03:02:04', etc.) or golang
// format (e.g. '1d2h', '1d3h2m4s', etc.)
func parseDuration(s string) (duration.Duration, error) {
	var d duration.Duration
	l := intervalLexer{str: s, offset: 0, err: nil}
	l.consumeSpaces()

	if l.offset == len(l.str) {
		return d, pgerror.Newf(
			pgcode.InvalidDatetimeFormat, "interval: invalid input syntax: %q", l.str)
	}
	for l.offset != len(l.str) {
		// To support -00:XX:XX we record the sign here since -0 doesn't exist
		// as an int64.
		sign := l.str[l.offset] == '-'
		// Parse the next number.
		v, hasDecimal, vp := l.consumeNum()
		l.consumeSpaces()

		if l.offset < len(l.str) && l.str[l.offset] == ':' && !hasDecimal {
			// Special case: HH:MM[:SS.ffff] or MM:SS.ffff
			delta, err := l.parseShortDuration(v, sign)
			if err != nil {
				return d, err
			}
			d = d.Add(delta)
			continue
		}

		// Parse the unit.
		u := l.consumeUnit(' ')
		l.consumeSpaces()
		if unit, ok := unitMap[strings.ToLower(u)]; ok {
			// A regular number followed by a unit, such as "9 day".
			d = d.Add(unit.Mul(v))
			if hasDecimal {
				d = addFrac(d, unit, vp)
			}
			continue
		}

		if u != "" {
			return d, pgerror.Newf(
				pgcode.InvalidDatetimeFormat, "interval: unknown unit %q in duration %q", u, s)
		}
		return d, pgerror.Newf(
			pgcode.InvalidDatetimeFormat, "interval: missing unit at position %d: %q", l.offset, s)
	}
	return d, l.err
}

func (l *intervalLexer) parseShortDuration(h int64, hasSign bool) (duration.Duration, error) {
	sign := int64(1)
	if hasSign {
		sign = -1
	}
	// postgresToDuration() has rewound the cursor to just after the
	// first number, so that we can check here there are no unwanted
	// spaces.
	if l.str[l.offset] != ':' {
		return duration.Duration{}, pgerror.Newf(
			pgcode.InvalidDatetimeFormat, "interval: invalid format %s", l.str[l.offset:])
	}
	l.offset++
	// Parse the second number.
	m, hasDecimal, mp := l.consumeNum()

	if m < 0 {
		return duration.Duration{}, pgerror.Newf(
			pgcode.InvalidDatetimeFormat, "interval: invalid format: %s", l.str)
	}
	// We have three possible formats:
	// - MM:SS.mmmmm
	// - HH:MM
	// - HH:MM:SS[.mmmmm]
	//
	// The top format has the "h" field parsed above actually
	// represent minutes. Get this out of the way first.
	if hasDecimal {
		l.consumeSpaces()
		return duration.MakeDuration(
			h*time.Minute.Nanoseconds()+
				sign*(m*time.Second.Nanoseconds()+
					floatToNanos(mp)),
			0,
			0,
		), nil
	}

	// Remaining formats
	var s int64
	var sp float64
	if l.offset != len(l.str) && l.str[l.offset] == ':' {
		// The last :NN part.
		l.offset++
		s, _, sp = l.consumeNum()
		if s < 0 {
			return duration.Duration{}, pgerror.Newf(
				pgcode.InvalidDatetimeFormat, "interval: invalid format: %s", l.str)
		}
	}

	l.consumeSpaces()
	return duration.MakeDuration(
		h*time.Hour.Nanoseconds()+
			sign*(m*time.Minute.Nanoseconds()+
				int64(mp*float64(time.Minute.Nanoseconds()))+
				s*time.Second.Nanoseconds()+
				floatToNanos(sp)),
		0,
		0,
	), nil
}

// addFrac increases the duration given as first argument by the unit
// given as second argument multiplied by the factor in the third
// argument. For computing fractions there are 30 days to a month and
// 24 hours to a day.
func addFrac(d duration.Duration, unit duration.Duration, f float64) duration.Duration {
	if unit.Months > 0 {
		f = f * float64(unit.Months)
		d.Months += int64(f)
		f = math.Mod(f, 1) * 30
		d.Days += int64(f)
		f = math.Mod(f, 1) * 24
		d.SetNanos(d.Nanos() + int64(float64(time.Hour.Nanoseconds())*f))
	} else if unit.Days > 0 {
		f = f * float64(unit.Days)
		d.Days += int64(f)
		f = math.Mod(f, 1) * 24
		d.SetNanos(d.Nanos() + int64(float64(time.Hour.Nanoseconds())*f))
	} else {
		d.SetNanos(d.Nanos() + int64(float64(unit.Nanos())*f))
	}
	return d
}

// floatToNanos converts a fractional number representing nanoseconds to the
// number of integer nanoseconds. For example: ".354874219" to "354874219"
// or ".123" to "123000000". This function takes care to round correctly
// when a naive conversion would incorrectly truncate due to floating point
// inaccuracies. This function should match the semantics of rint() from
// Postgres. See:
// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/backend/utils/adt/timestamp.c;h=449164ae7e5b00f6580771017888d4922685a73c;hb=HEAD#l1511
// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/port/rint.c;h=d59d9ab774307b7db2f7cb2347815a30da563fc5;hb=HEAD
func floatToNanos(f float64) int64 {
	return int64(math.Round(f * float64(time.Second.Nanoseconds())))
}
