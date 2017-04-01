// Copyright 2016 The Cockroach Authors.
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
// Author: Karl Southern (karl@theangryangel.co.uk)

package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

type intervalLexer struct {
	str    string
	offset int
	err    error
}

// Consumes the next decimal number.
// 1st return value is the integer part.
// 2nd return value is whether a decimal part was encountered.
// 3rd return value is the decimal part as a float.
// If the number is negative, both the 1st and 3rd return
// value are negative.
// The decimal value is returned separately from the integer value so
// as to support large integer value which would not fit in a float.
func (l *intervalLexer) consumeNum() (int64, bool, float64) {
	if l.err != nil {
		return 0, false, 0
	}

	hasDecimal := false
	sign := int64(1)
	offset := l.offset
	// Accept a leading negative sign.
	if l.offset < len(l.str) && l.str[l.offset] == '-' {
		l.offset++
		sign = -1
	}

	// Integer part before the decimal separator.
	intPart := l.consumeInt()
	var decPart float64
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
			l.err = fmt.Errorf("interval: %v", err)
			return 0, false, 0
		}
		decPart = value
	}

	// Ensure we have something.
	if offset == l.offset {
		l.err = fmt.Errorf("interval: missing number at %v", l.str[offset])
		return 0, false, 0
	}

	return intPart * sign, hasDecimal, decPart * float64(sign)
}

// Consumes the next integer.
func (l *intervalLexer) consumeInt() int64 {
	if l.err != nil {
		return 0
	}

	offset := l.offset
	var x int64
	for ; l.offset < len(l.str); l.offset++ {
		if l.str[l.offset] < '0' || l.str[l.offset] > '9' {
			break
		}
		if x > (1<<63-1)/10 {
			// Handle overflow.
			l.err = errors.New("interval: bad [0-9]*")
			return x
		}
		x = x*10 + int64(l.str[l.offset]) - '0'
		if x < 0 {
			// Handle overflow.
			l.err = errors.New("interval: bad [0-9]*")
			return x
		}
	}
	if offset == l.offset {
		l.err = fmt.Errorf("interval: missing int at offset %d, %v", offset, l.str[offset])
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
		l.err = fmt.Errorf("interval: missing unit in duration %v", l.str)
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
	"D": {Days: 1},
	"W": {Days: 7},
	"M": {Months: 1},
	"Y": {Months: 12},
}

var isoTimeUnitMap = map[string]duration.Duration{
	"S": {Nanos: time.Second.Nanoseconds()},
	"M": {Nanos: time.Minute.Nanoseconds()},
	"H": {Nanos: time.Hour.Nanoseconds()},
}

const errInvalidSQLDuration = "invalid input syntax for type interval %s"

type parsedIndex uint8

const (
	nothingParsed parsedIndex = iota
	hmsParsed
	dayParsed
	yearMonthParsed
)

// Parses a SQL standard interval string.
// See the following links for exampels:
//  - http://www.postgresql.org/docs/9.1/static/datatype-datetime.html#DATATYPE-INTERVAL-INPUT-EXAMPLES
//  - http://www.ibm.com/support/knowledgecenter/SSGU8G_12.1.0/com.ibm.esqlc.doc/ids_esqlc_0190.htm
func sqlStdToDuration(s string) (duration.Duration, error) {
	var d duration.Duration
	parts := strings.Fields(s)
	if len(parts) > 3 || len(parts) == 0 {
		return d, fmt.Errorf(errInvalidSQLDuration, s)
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
		if part[0] == '-' {
			return d, fmt.Errorf(errInvalidSQLDuration, s)
		}

		if strings.ContainsRune(part, ':') {
			// Try to parse as HH:MM:SS
			if parsedIdx != nothingParsed {
				return d, fmt.Errorf(errInvalidSQLDuration, s)
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
				return d, fmt.Errorf(errInvalidSQLDuration, s)
			}
			if err != nil {
				return d, makeParseError(part, TypeInterval, err)
			}
			d = d.Add(duration.Duration{Nanos: dur.Nanoseconds()})
		} else if strings.ContainsRune(part, '-') {
			// Try to parse as Year-Month.
			if parsedIdx >= yearMonthParsed {
				return d, fmt.Errorf(errInvalidSQLDuration, s)
			}
			parsedIdx = yearMonthParsed

			yms := strings.Split(part, "-")
			if len(yms) != 2 {
				return d, fmt.Errorf(errInvalidSQLDuration, s)
			}
			year, errYear := strconv.Atoi(yms[0])
			var month int
			var errMonth error
			if yms[1] != "" {
				month, errMonth = strconv.Atoi(yms[1])
			}
			if errYear == nil && errMonth == nil {
				delta := duration.Duration{Months: 1}.Mul(int64(year)*12 + int64(month))
				if neg {
					d = d.Sub(delta)
				} else {
					d = d.Add(delta)
				}
			} else {
				return d, fmt.Errorf(errInvalidSQLDuration, s)
			}

		} else if value, err := strconv.ParseFloat(part, 64); err == nil {
			// Try to parse as Day or Second.
			var dur time.Duration
			var err error
			// Make sure 'Day Second'::interval invalid.
			if floatParsed {
				return d, fmt.Errorf(errInvalidSQLDuration, s)
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
					return d, fmt.Errorf(errInvalidSQLDuration, s)
				}
				d = d.Add(duration.Duration{Nanos: dur.Nanoseconds()})
				parsedIdx = hmsParsed
			} else if parsedIdx == hmsParsed {
				// Day part.
				// TODO(hainesc): support float value in day part?
				delta := duration.Duration{Days: 1}.Mul(int64(value))
				if neg {
					d = d.Sub(delta)
				} else {
					d = d.Add(delta)
				}
				parsedIdx = dayParsed
			} else {
				return d, fmt.Errorf(errInvalidSQLDuration, s)
			}
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
		return d, fmt.Errorf("interval: invalid iso8601 duration %s", s)
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
			return d, fmt.Errorf("interval: unknown unit %s in iso8601 duration %s", u, s)
		}
	}

	return d, nil
}

// Postgres Units.
var postgresUnitMap = map[string]duration.Duration{
	"ns":          {Nanos: time.Nanosecond.Nanoseconds()},
	"nanosecond":  {Nanos: time.Nanosecond.Nanoseconds()},
	"nanoseconds": {Nanos: time.Nanosecond.Nanoseconds()},
	"us":          {Nanos: time.Microsecond.Nanoseconds()},
	// The two micron symbols below are not the same unicode character
	// (one is greek small letter mu, the other one is micro).
	"µs":           {Nanos: time.Microsecond.Nanoseconds()},
	"μs":           {Nanos: time.Microsecond.Nanoseconds()},
	"microsecond":  {Nanos: time.Microsecond.Nanoseconds()},
	"microseconds": {Nanos: time.Microsecond.Nanoseconds()},
	"ms":           {Nanos: time.Millisecond.Nanoseconds()},
	"s":            {Nanos: time.Second.Nanoseconds()},
	"second":       {Nanos: time.Second.Nanoseconds()},
	"seconds":      {Nanos: time.Second.Nanoseconds()},
	"m":            {Nanos: time.Minute.Nanoseconds()},
	"minute":       {Nanos: time.Minute.Nanoseconds()},
	"minutes":      {Nanos: time.Minute.Nanoseconds()},
	"h":            {Nanos: time.Hour.Nanoseconds()},
	"hour":         {Nanos: time.Hour.Nanoseconds()},
	"hours":        {Nanos: time.Hour.Nanoseconds()},
	"d":            {Days: 1},
	"day":          {Days: 1},
	"days":         {Days: 1},
	"w":            {Days: 7},
	"week":         {Days: 7},
	"weeks":        {Days: 7},
	"mon":          {Months: 1},
	"month":        {Months: 1},
	"months":       {Months: 1},
	"y":            {Months: 12},
	"year":         {Months: 12},
	"years":        {Months: 12},
}

// Parses a duration in the "traditional" Postgres format.
func postgresToDuration(s string) (duration.Duration, error) {
	s = strings.ToLower(s)
	var d duration.Duration
	l := intervalLexer{str: s, offset: 0, err: nil}
	for l.offset != len(l.str) {
		v, hasDecimal, vp := l.consumeNum()
		curOffset := l.offset
		l.consumeSpaces()
		u := l.consumeUnit(' ')
		l.consumeSpaces()
		if u == ":" && !hasDecimal {
			// pg interval strings can also contain the syntax
			// 'hh:mm[:ss[.fff]]' or mm:ss[.fff]. So first, rewind, to just
			// after the hour number, then move forward again. The reason
			// why we rewind is that the common code above has skipped over
			// spaces, whereas no space is allowed within a XX:YY pair.
			l.offset = curOffset
			dd, err := l.parseShortDuration(v)
			if err != nil {
				return d, err
			}
			d = d.Add(dd)
		} else if unit, ok := postgresUnitMap[u]; ok {
			// A regular number followed by a unit, such as "9 day".
			d = d.Add(unit.Mul(v))
			d = d.Add(unit.MulFloat(vp))
		} else if u != "" {
			return d, fmt.Errorf("interval: unknown unit %s in duration %s", u, s)
		} else {
			return d, fmt.Errorf("interval: missing unit in duration %s", s)
		}
	}
	return d, l.err
}

func (l *intervalLexer) parseShortDuration(h int64) (duration.Duration, error) {
	// postgresToDuration() has rewound the cursor to just after the
	// first number, so that we can check here there are no unwanted
	// spaces.
	if l.str[l.offset] != ':' {
		return duration.Duration{}, fmt.Errorf("interval: invalid format %s", l.str[l.offset:])
	}
	l.offset++
	// Parse the second number.
	m, hasDecimal, mp := l.consumeNum()

	if m < 0 {
		return duration.Duration{}, fmt.Errorf("interval: invalid format: %s", l.str)
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
		return duration.Duration{
			Nanos: h*time.Minute.Nanoseconds() +
				m*time.Second.Nanoseconds() +
				int64(mp*float64(time.Second.Nanoseconds())),
		}, nil
	}

	// Remaining formats
	var s int64
	var sp float64
	if l.offset != len(l.str) && l.str[l.offset] == ':' {
		// The last :NN part.
		l.offset++
		s, _, sp = l.consumeNum()
		if s < 0 {
			return duration.Duration{}, fmt.Errorf("interval: invalid format: %s", l.str)
		}
	}

	l.consumeSpaces()
	return duration.Duration{
		Nanos: h*time.Hour.Nanoseconds() +
			m*time.Minute.Nanoseconds() +
			int64(mp*float64(time.Minute.Nanoseconds())) +
			s*time.Second.Nanoseconds() +
			int64(sp*float64(time.Second.Nanoseconds())),
	}, nil
}
