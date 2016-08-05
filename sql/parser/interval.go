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
	"time"

	"github.com/cockroachdb/cockroach/util/duration"
)

type intervalLexer struct {
	str    string
	offset int
	err    error
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
		if (l.str[l.offset] >= '0' && l.str[l.offset] <= '9') || l.str[l.offset] == skipCharacter {
			break
		}
	}

	if offset == l.offset {
		l.err = fmt.Errorf("interval: missing unit at offset %d, %v", offset, l.str[offset])
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
	"second":  {Nanos: time.Second.Nanoseconds()},
	"seconds": {Nanos: time.Second.Nanoseconds()},
	"minute":  {Nanos: time.Minute.Nanoseconds()},
	"minutes": {Nanos: time.Minute.Nanoseconds()},
	"hour":    {Nanos: time.Hour.Nanoseconds()},
	"hours":   {Nanos: time.Hour.Nanoseconds()},
	"day":     {Days: 1},
	"days":    {Days: 1},
	"week":    {Days: 7},
	"weeks":   {Days: 7},
	"month":   {Months: 1},
	"months":  {Months: 1},
	"year":    {Months: 12},
	"years":   {Months: 12},
}

// Parses a duration in the "traditional" Postgres format.
func postgresToDuration(s string) (duration.Duration, error) {
	var d duration.Duration
	l := intervalLexer{str: s, offset: 0, err: nil}
	for l.offset != len(l.str) {
		v := l.consumeInt()
		l.consumeSpaces()
		u := l.consumeUnit(' ')
		l.consumeSpaces()
		if unit, ok := postgresUnitMap[u]; ok {
			d = d.Add(unit.Mul(v))
		} else {
			return d, fmt.Errorf("interval: unknown unit %s in postgres duration %s", u, s)
		}
	}

	return d, nil
}
