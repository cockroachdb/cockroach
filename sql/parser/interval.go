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
	"strings"
	"time"
)

// Lifted directly from time/format.go and renamed from leadingInt
// Consumes the next integer from a string.
func consumeInt(s string) (x int64, rem string, err error) {
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if x > (1<<63-1)/10 {
			// overflow
			return 0, "", errors.New("interval: bad [0-9]*")
		}
		x = x*10 + int64(c) - '0'
		if x < 0 {
			// overflow
			return 0, "", errors.New("interval: bad [0-9]*")
		}
	}
	return x, s[i:], nil
}

// Much like consumeInt, this consumes the next unit from the string.
func consumeUnit(s string, skipCharacter byte) (unit string, rem string, err error) {
	i := 0

	for ; i < len(s); i++ {
		if ('0' <= s[i] && s[i] <= '9') || s[i] == skipCharacter {
			break
		}
	}

	if i == 0 {
		return "", "", errors.New("interval: missing unit")
	}

	return s[:i], s[i:], nil
}

// ISO Units.
var isoDateUnitMap = map[string]int64{
	"D": int64(time.Hour) * 24,
	"M": int64(time.Hour) * 24 * 30,       // ISO 8601 suggests we use 30 days
	"Y": int64(time.Hour) * 24 * 30 * 365, // Doesn't account for DST, or leap seconds
}

var isoTimeUnitMap = map[string]int64{
	"S": int64(time.Second),
	"M": int64(time.Minute),
	"H": int64(time.Hour),
}

// Parses a iso8601 (with designators) string.
// See the following links for examples:
//  - http://www.postgresql.org/docs/9.1/static/datatype-datetime.html#DATATYPE-INTERVAL-INPUT-EXAMPLES
//  - https://en.wikipedia.org/wiki/ISO_8601#Time_intervals
//  - https://en.wikipedia.org/wiki/ISO_8601#Durations
func iso8601ToDuration(s string) (time.Duration, error) {
	original := s
	stdErr := errors.New("interval: invalid iso8601 duration " + original)

	if s == "" || s[0] != 'P' {
		return 0, stdErr
	}

	// Advance.
	s = s[1:]

	inDate := true

	var d int64

	for s != "" {
		var (
			v   int64
			u   string
			err error
		)

		// Check if we're in the time part yet.
		if s[0] == 'T' {
			inDate = false
			s = s[1:]
		}

		// next character must be 0-9
		if '0' <= s[0] && s[0] <= 9 {
			return 0, stdErr
		}

		v, s, err = consumeInt(s)
		if err != nil {
			return 0, stdErr
		}

		u, s, err = consumeUnit(s, 'T')
		if err != nil {
			return 0, stdErr
		}

		var (
			unit int64
			ok   bool
		)

		if inDate == true {
			unit, ok = isoDateUnitMap[u]
		} else {
			unit, ok = isoTimeUnitMap[u]
		}

		if !ok {
			return 0, errors.New("interval: unknown unit " + u + " in iso8601 duration " + original)
		}

		d += v * unit
	}

	return time.Duration(d), nil
}

// Postgres Units.
var postgresUnitMap = map[string]int64{
	"second": int64(time.Second),
	"minute": int64(time.Minute),
	"hour":   int64(time.Hour),
	"day":    int64(time.Hour) * 24,
	"month":  int64(time.Hour) * 24 * 30,       // Following postgres' example, we use 30 days
	"year":   int64(time.Hour) * 24 * 30 * 365, // Also like postgres, doesn't account for DST, or leap seconds
}

func postgresToDuration(s string) (time.Duration, error) {
	original := s

	s = strings.Replace(s, " ", "", -1)

	stdErr := errors.New("interval: invalid postgresql duration " + original)

	var d int64

	for s != "" {
		var (
			v   int64
			u   string
			err error
		)

		// Next character must be valid.
		if '0' <= s[0] && s[0] <= 9 {
			return 0, stdErr
		}

		v, s, err = consumeInt(s)
		if err != nil {
			return 0, stdErr
		}

		u, s, err = consumeUnit(s, ' ')
		if err != nil {
			return 0, stdErr
		}

		// Remove the trailing 's', so it can be found in the unitmap.
		if u[len(u)-1] == 's' {
			u = u[:len(u)-1]
		}

		unit, ok := postgresUnitMap[u]

		if !ok {
			return 0, errors.New("interval: unknown unit " + u + " in postgres duration " + original)
		}

		d += v * unit
	}

	return time.Duration(d), nil
}
